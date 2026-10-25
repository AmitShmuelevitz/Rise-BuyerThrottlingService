// Package evaluator periodically turns each buyer's aggregated window stats
// into a new throttle state and publishes it for the auction pods.
//
// Several replicas may run; a lease in the store makes exactly one of them
// active. Each window is committed at most once (the store rejects windows
// that are not newer than the last committed one), so a failover in the
// middle of an evaluation cannot apply a window twice.
package evaluator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

// Metrics receives evaluator telemetry.
type Metrics interface {
	SetLeader(bool)
	ObserveRun(outcome string, d time.Duration)
	ObserveDecision(d policy.Decision)
}

type nopMetrics struct{}

func (nopMetrics) SetLeader(bool)                   {}
func (nopMetrics) ObserveRun(string, time.Duration) {}
func (nopMetrics) ObserveDecision(policy.Decision)  {}

// Options configures the Service. Zero values take the defaults noted.
type Options struct {
	Policy policy.Config
	// Grace is how long after a window closes the evaluator waits before
	// reading it, so pods' last flushes for that window have landed. It must
	// exceed the pods' flush interval plus clock skew. Default 5s.
	Grace time.Duration
	// TickInterval is how often the evaluator checks for due windows and
	// renews its lease. Default 1s.
	TickInterval time.Duration
	// LeaseTTL is how long the lease survives without renewal; it bounds
	// failover time. Default 15s.
	LeaseTTL time.Duration
	// Owner identifies this replica in the lease. Required.
	Owner string
	// StoreTimeout bounds each tick's store calls. Default 5s.
	StoreTimeout time.Duration

	Clock   clock.Clock
	Logger  *slog.Logger
	Metrics Metrics
}

func (o *Options) setDefaults() {
	if o.Grace <= 0 {
		o.Grace = 5 * time.Second
	}
	if o.TickInterval <= 0 {
		o.TickInterval = time.Second
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 15 * time.Second
	}
	if o.StoreTimeout <= 0 {
		o.StoreTimeout = 5 * time.Second
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Metrics == nil {
		o.Metrics = nopMetrics{}
	}
}

// Service is the evaluator.
type Service struct {
	opts   Options
	stats  store.StatsSource
	states store.StateWriter
	lease  store.Leaser

	leader   bool
	lastDone window.ID // newest window known to be committed; avoids a store read per tick
}

// New validates options and returns a Service.
func New(stats store.StatsSource, states store.StateWriter, lease store.Leaser, opts Options) (*Service, error) {
	opts.setDefaults()
	if err := opts.Policy.Validate(); err != nil {
		return nil, fmt.Errorf("invalid policy: %w", err)
	}
	if opts.Owner == "" {
		return nil, errors.New("owner is required")
	}
	if opts.Grace >= opts.Policy.Window {
		return nil, fmt.Errorf("grace (%v) must be shorter than the window (%v)", opts.Grace, opts.Policy.Window)
	}
	return &Service{opts: opts, stats: stats, states: states, lease: lease}, nil
}

// Run ticks until ctx is done, then releases the lease.
func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.TickInterval)
	defer ticker.Stop()
	for {
		if err := s.Tick(ctx); err != nil && ctx.Err() == nil {
			s.opts.Logger.Error("evaluation tick failed", "err", err)
		}
		select {
		case <-ctx.Done():
			relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opts.StoreTimeout)
			defer cancel()
			if s.leader {
				if err := s.lease.Release(relCtx, s.opts.Owner); err != nil {
					return fmt.Errorf("release lease: %w", err)
				}
				s.opts.Logger.Info("released leadership", "owner", s.opts.Owner)
			}
			return nil
		case <-ticker.C:
		}
	}
}

// DueWindow is the newest window that is closed and past its grace period.
func (s *Service) DueWindow(now time.Time) window.ID {
	size := s.opts.Policy.Window
	return window.Of(now.Add(-s.opts.Grace), size).Prev(size)
}

// Tick renews or acquires the lease and, if this replica leads, evaluates the
// due window unless it was already committed.
func (s *Service) Tick(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.opts.StoreTimeout)
	defer cancel()

	leader, err := s.lease.TryAcquire(ctx, s.opts.Owner, s.opts.LeaseTTL)
	if err != nil {
		// Without a confirmed lease we must assume someone else may lead.
		s.setLeader(false)
		return fmt.Errorf("lease: %w", err)
	}
	s.setLeader(leader)
	if !leader {
		return nil
	}

	w := s.DueWindow(s.opts.Clock.Now())
	if w <= s.lastDone {
		return nil
	}
	_, err = s.EvaluateWindow(ctx, w)
	return err
}

func (s *Service) setLeader(leader bool) {
	if leader != s.leader {
		if leader {
			s.opts.Logger.Info("acquired leadership", "owner", s.opts.Owner)
		} else {
			s.opts.Logger.Warn("lost leadership", "owner", s.opts.Owner)
			s.lastDone = 0 // another replica may have moved on; re-read from the store
		}
	}
	s.leader = leader
	s.opts.Metrics.SetLeader(leader)
}

// Report summarizes one window evaluation.
type Report struct {
	Window    window.ID
	Applied   bool
	Version   uint64
	Decisions []policy.Decision
}

// EvaluateWindow evaluates every known buyer against window w's stats and
// commits the result. Buyers are those with stats in w plus those with
// existing state, so a buyer that went silent keeps (and holds) its state.
func (s *Service) EvaluateWindow(ctx context.Context, w window.ID) (Report, error) {
	start := time.Now()
	rep, err := s.evaluate(ctx, w)
	outcome := "applied"
	switch {
	case err != nil:
		outcome = "error"
	case !rep.Applied:
		outcome = "skipped"
	}
	s.opts.Metrics.ObserveRun(outcome, time.Since(start))
	return rep, err
}

func (s *Service) evaluate(ctx context.Context, w window.ID) (Report, error) {
	rep := Report{Window: w}
	snap, err := s.states.LoadState(ctx)
	if err != nil {
		return rep, fmt.Errorf("load state: %w", err)
	}
	if snap.Version > 0 && snap.LastWindow >= w {
		s.lastDone = snap.LastWindow
		return rep, nil
	}
	stats, err := s.stats.WindowStats(ctx, w)
	if err != nil {
		return rep, fmt.Errorf("load stats: %w", err)
	}

	buyers := make([]string, 0, len(stats)+len(snap.States))
	for b := range stats {
		buyers = append(buyers, b)
	}
	for b := range snap.States {
		if _, ok := stats[b]; !ok {
			buyers = append(buyers, b)
		}
	}
	slices.Sort(buyers)

	now := s.opts.Clock.Now().UTC()
	records := make([]store.Record, 0, len(buyers))
	for _, b := range buyers {
		prev, known := snap.States[b]
		if !known {
			prev = store.Record{State: policy.State{BuyerID: b}, UpdatedAt: now}
		}
		d := policy.Evaluate(prev.State, stats[b], s.opts.Policy)
		rep.Decisions = append(rep.Decisions, d)

		rec := store.Record{State: d.Next, Window: w, UpdatedAt: prev.UpdatedAt, WindowSeconds: int64(s.opts.Policy.Window / time.Second)}
		if d.Changed() {
			rec.UpdatedAt = now
		}
		records = append(records, rec)
	}

	applied, version, err := s.states.CommitState(ctx, w, records)
	if err != nil {
		return rep, fmt.Errorf("commit: %w", err)
	}
	rep.Applied, rep.Version = applied, version
	s.lastDone = w
	if !applied {
		s.opts.Logger.Info("window already committed by another evaluator", "window", w)
		return rep, nil
	}

	for _, d := range rep.Decisions {
		s.opts.Metrics.ObserveDecision(d)
		s.logDecision(w, d)
	}
	s.opts.Logger.Info("window evaluated",
		"window", w, "window_start", w.Start(), "buyers", len(buyers), "version", version)
	return rep, nil
}

func (s *Service) logDecision(w window.ID, d policy.Decision) {
	attrs := []any{
		"buyer", d.Next.BuyerID,
		"event", d.Event,
		"rate_from", d.Prev.ThrottleRate,
		"rate_to", d.Next.ThrottleRate,
		"reason", d.Next.Reason,
		"total", d.Stats.Total,
		"failure_rate", round3(d.Stats.FailureRate()),
		"timeout_rate", round3(d.Stats.TimeoutRate()),
		"window", w,
	}
	switch {
	case d.Changed():
		s.opts.Logger.Info(message(d), attrs...)
	case d.Prev.ThrottleRate > 0:
		// Throttled buyers that did not move are worth seeing in the demo.
		s.opts.Logger.Info(message(d), attrs...)
	default:
		s.opts.Logger.Debug(message(d), attrs...)
	}
}

func message(d policy.Decision) string {
	switch d.Event {
	case policy.EventBlocked:
		if d.Prev.ThrottleRate == d.Next.ThrottleRate {
			return "buyer still violating, stays blocked"
		}
		return "buyer blocked"
	case policy.EventRecoveryStep:
		return "buyer recovering"
	case policy.EventRecovered:
		return "buyer fully recovered"
	case policy.EventStaleRecovery:
		return "buyer recovering after prolonged low traffic"
	case policy.EventInsufficient:
		return "insufficient sample, holding rate"
	default:
		return "buyer healthy"
	}
}

func round3(v float64) float64 { return float64(int64(v*1000+0.5)) / 1000 }
