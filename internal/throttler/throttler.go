// Package throttler is the auction-side throttling component. Every auction
// pod embeds one Throttler:
//
//   - ShouldCall decides, from an in-memory snapshot of throttle rates, whether
//     to send a request to a buyer. It never blocks and never does I/O.
//   - Record counts each call's result in lock-free per-buyer counters.
//
// Two background loops keep it connected to the shared store: a syncer that
// reloads throttle state (on a poll interval and on pub/sub notifications),
// and a flusher that pushes counted results to the store once per flush
// interval, bucketed into evaluation windows.
package throttler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

// Result is the outcome of one call to a buyer.
type Result uint8

const (
	ResultOK Result = iota
	ResultFailed
	ResultTimeout
)

func (r Result) String() string {
	switch r {
	case ResultOK:
		return "ok"
	case ResultFailed:
		return "failed"
	case ResultTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

// Metrics receives telemetry. Calls happen off the hot path (from the flusher
// and syncer goroutines), so implementations may be relatively expensive.
type Metrics interface {
	AddCalls(buyerID string, ok, failed, timeouts int64)
	AddSkipped(buyerID string, n int64)
	SetRates(rates map[string]float64)
	// SetState reports the loaded state's version and when the window it
	// was evaluated from ended (zero if unknown), for a state age gauge.
	SetState(version uint64, evaluatedWindowEnd time.Time)
	SetConfigMismatch(bool)
	IncFlushErrors()
	AddDroppedStats(n int64)
	IncSyncErrors()
}

type nopMetrics struct{}

func (nopMetrics) AddCalls(string, int64, int64, int64) {}
func (nopMetrics) AddSkipped(string, int64)             {}
func (nopMetrics) SetRates(map[string]float64)          {}
func (nopMetrics) SetState(uint64, time.Time)           {}
func (nopMetrics) SetConfigMismatch(bool)               {}
func (nopMetrics) IncFlushErrors()                      {}
func (nopMetrics) AddDroppedStats(int64)                {}
func (nopMetrics) IncSyncErrors()                       {}

// Options configures a Throttler. Zero values take the defaults noted.
type Options struct {
	// Window must equal the evaluator's window. Default 1m.
	Window time.Duration
	// FlushInterval is how often counted results are pushed to the store.
	// Default 1s.
	FlushInterval time.Duration
	// SyncInterval is how often throttle state is reloaded even without a
	// pub/sub notification. Default 1s.
	SyncInterval time.Duration
	// MaxPendingAge bounds how long after a window ends its unflushed stats
	// are retried following a store failure. It should equal the evaluator's
	// grace period: the evaluator reads a window once, at its end plus the
	// grace, so stats written later would be stored but never counted.
	// They are dropped and reported instead. Default 5s, the evaluator's
	// default grace.
	MaxPendingAge time.Duration
	// InitialLoadTimeout bounds the first state load in Run. Default 2s.
	InitialLoadTimeout time.Duration
	// StoreTimeout bounds each store call. Default 2s.
	StoreTimeout time.Duration

	Clock   clock.Clock
	Logger  *slog.Logger
	Metrics Metrics
	// Rand returns a uniform float in [0, 1). Default math/rand/v2.Float64,
	// which is safe for concurrent use and lock-free.
	Rand func() float64
}

func (o *Options) setDefaults() {
	if o.Window <= 0 {
		o.Window = time.Minute
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = time.Second
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = time.Second
	}
	if o.MaxPendingAge <= 0 {
		o.MaxPendingAge = 5 * time.Second
	}
	if o.InitialLoadTimeout <= 0 {
		o.InitialLoadTimeout = 2 * time.Second
	}
	if o.StoreTimeout <= 0 {
		o.StoreTimeout = 2 * time.Second
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
	if o.Rand == nil {
		o.Rand = rand.Float64
	}
}

// snapshot is an immutable view of throttle state. It is replaced, never
// mutated, so readers need no locks.
type snapshot struct {
	rates      map[string]float64
	records    map[string]store.Record
	version    uint64
	lastWindow window.ID
	loadedAt   time.Time
}

// counters are one buyer's results since the last flush.
type counters struct {
	ok, failed, timeouts, skipped atomic.Int64
}

// Throttler implements ShouldCall and Record for one auction pod.
type Throttler struct {
	opts   Options
	reader store.StateReader
	sink   store.StatsSink

	snap     atomic.Pointer[snapshot]
	counters sync.Map // buyerID -> *counters

	mismatchLogged atomic.Bool
	staleLogged    atomic.Bool

	flushMu   sync.Mutex // serializes Flush; guards fields below
	lastFlush time.Time
	pending   map[window.ID]map[string]policy.WindowStats
}

// New returns a Throttler. Call Run to start syncing state and flushing stats;
// until state is loaded, ShouldCall lets every request through (fail open).
func New(reader store.StateReader, sink store.StatsSink, opts Options) *Throttler {
	opts.setDefaults()
	t := &Throttler{
		opts:    opts,
		reader:  reader,
		sink:    sink,
		pending: make(map[window.ID]map[string]policy.WindowStats),
	}
	t.snap.Store(&snapshot{})
	t.lastFlush = opts.Clock.Now()
	return t
}

// ShouldCall reports whether this request should be sent to the buyer. A
// buyer with throttle rate r is skipped with probability r. Unknown buyers
// are always called.
func (t *Throttler) ShouldCall(buyerID string) bool {
	rate := t.snap.Load().rates[buyerID]
	if rate <= 0 {
		return true
	}
	if t.opts.Rand() >= rate {
		return true
	}
	t.counter(buyerID).skipped.Add(1)
	return false
}

// Record counts the result of a call that was actually made. Unknown result
// values are counted as failures: a caller bug should look unhealthy, not
// healthy.
func (t *Throttler) Record(buyerID string, r Result) {
	c := t.counter(buyerID)
	switch r {
	case ResultOK:
		c.ok.Add(1)
	case ResultTimeout:
		c.timeouts.Add(1)
	default:
		c.failed.Add(1)
	}
}

func (t *Throttler) counter(buyerID string) *counters {
	if c, ok := t.counters.Load(buyerID); ok {
		return c.(*counters)
	}
	c, _ := t.counters.LoadOrStore(buyerID, &counters{})
	return c.(*counters)
}

// Rate returns the buyer's current throttle rate in this pod's view.
func (t *Throttler) Rate(buyerID string) float64 { return t.snap.Load().rates[buyerID] }

// Ready reports whether throttle state has been loaded at least once.
func (t *Throttler) Ready() bool { return !t.snap.Load().loadedAt.IsZero() }

// States returns this pod's view of all buyers' throttle state.
func (t *Throttler) States() (records map[string]store.Record, version uint64, loadedAt time.Time) {
	s := t.snap.Load()
	return s.records, s.version, s.loadedAt
}

// Run loads state, then syncs state and flushes stats until ctx is done. On
// return it performs a final flush with a fresh timeout, so a graceful
// shutdown does not lose counted results.
func (t *Throttler) Run(ctx context.Context) error {
	initCtx, cancel := context.WithTimeout(ctx, t.opts.InitialLoadTimeout)
	if err := t.Sync(initCtx); err != nil {
		t.opts.Logger.Warn("initial throttle state load failed, failing open until it succeeds", "err", err)
	}
	cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); t.syncLoop(ctx) }()
	go func() { defer wg.Done(); t.flushLoop(ctx) }()
	wg.Wait()

	finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.opts.StoreTimeout)
	defer cancel()
	if err := t.Flush(finalCtx); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}
	return nil
}

// Sync checks the state version and, only if it changed, loads the full state
// and swaps it in. Polling therefore costs one small read per pod, however
// many buyers there are.
func (t *Throttler) Sync(ctx context.Context) error {
	cur := t.snap.Load()
	if !cur.loadedAt.IsZero() {
		v, err := t.reader.Version(ctx)
		if err != nil {
			t.opts.Metrics.IncSyncErrors()
			return err
		}
		if v == cur.version {
			now := t.opts.Clock.Now()
			next := *cur
			next.loadedAt = now
			t.snap.Store(&next)
			t.checkStale(store.Snapshot{Version: cur.version, LastWindow: cur.lastWindow}, now)
			return nil
		}
	}

	snap, err := t.reader.LoadState(ctx)
	if err != nil {
		t.opts.Metrics.IncSyncErrors()
		return err
	}
	now := t.opts.Clock.Now()

	rates := make(map[string]float64, len(snap.States))
	for id, r := range snap.States {
		if r.ThrottleRate > 0 {
			rates[id] = r.ThrottleRate
		}
	}
	t.snap.Store(&snapshot{rates: rates, records: snap.States, version: snap.Version, lastWindow: snap.LastWindow, loadedAt: now})
	t.opts.Metrics.SetRates(rates)
	var evaluatedEnd time.Time
	if snap.Version > 0 {
		evaluatedEnd = snap.LastWindow.End(t.opts.Window)
	}
	t.opts.Metrics.SetState(snap.Version, evaluatedEnd)
	if snap.Version != cur.version {
		t.opts.Logger.Info("throttle state updated", "version", snap.Version, "previous_version", cur.version, "throttled", rates)
	}
	t.checkWindowSize(snap)
	t.checkStale(snap, now)
	return nil
}

// checkWindowSize reports when the evaluator uses a different window size
// than this pod. Stats would then be bucketed into windows the evaluator
// never reads (or reads only partly), so this is logged as an error.
func (t *Throttler) checkWindowSize(snap store.Snapshot) {
	own := int64(t.opts.Window / time.Second)
	for _, r := range snap.States {
		if r.WindowSeconds == 0 {
			continue // written by an older evaluator: unknown
		}
		mismatch := r.WindowSeconds != own
		t.opts.Metrics.SetConfigMismatch(mismatch)
		if mismatch && !t.mismatchLogged.Swap(true) {
			t.opts.Logger.Error("window size mismatch: this pod's stats will be misattributed; set THROTTLE_WINDOW to the same value on the evaluator and every pod",
				"pod_window_seconds", own, "evaluator_window_seconds", r.WindowSeconds)
		}
		if !mismatch {
			t.mismatchLogged.Store(false)
		}
		return
	}
}

// staleAfter is how many windows may pass without a new evaluation before
// the state is reported as stale.
const staleAfter = 3

// checkStale warns once when the evaluator has stopped committing, for
// example because every replica is down. Pods keep serving the last state.
func (t *Throttler) checkStale(snap store.Snapshot, now time.Time) {
	if snap.Version == 0 {
		return
	}
	age := now.Sub(snap.LastWindow.End(t.opts.Window))
	stale := age > staleAfter*t.opts.Window
	if stale && !t.staleLogged.Swap(true) {
		t.opts.Logger.Warn("throttle state is stale: no evaluation committed recently, serving last known state",
			"age", age.Round(time.Second), "last_window", snap.LastWindow, "version", snap.Version)
	}
	if !stale && t.staleLogged.Swap(false) {
		t.opts.Logger.Info("throttle state is fresh again", "version", snap.Version)
	}
}

func (t *Throttler) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(t.opts.SyncInterval)
	defer ticker.Stop()

	var updates <-chan uint64
	subscribe := func() {
		ch, err := t.reader.SubscribeUpdates(ctx)
		if err != nil {
			t.opts.Logger.Warn("subscribe to throttle updates failed, relying on polling", "err", err)
			return
		}
		updates = ch
	}
	subscribe()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-updates:
			if !ok {
				updates = nil // closed: fall back to polling, resubscribe on next tick
				continue
			}
		case <-ticker.C:
			if updates == nil {
				subscribe()
			}
		}
		syncCtx, cancel := context.WithTimeout(ctx, t.opts.StoreTimeout)
		if err := t.Sync(syncCtx); err != nil && ctx.Err() == nil {
			t.opts.Logger.Warn("throttle state sync failed, keeping last known state", "err", err, "version", t.snap.Load().version)
		}
		cancel()
	}
}

func (t *Throttler) flushLoop(ctx context.Context) {
	for {
		// Align flushes to multiples of the flush interval so a flush lands on
		// every window boundary and counts are not smeared across windows.
		now := t.opts.Clock.Now()
		next := now.Truncate(t.opts.FlushInterval).Add(t.opts.FlushInterval)
		timer := time.NewTimer(next.Sub(now))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		flushCtx, cancel := context.WithTimeout(ctx, t.opts.StoreTimeout)
		if err := t.Flush(flushCtx); err != nil && ctx.Err() == nil {
			t.opts.Logger.Warn("stats flush failed, will retry", "err", err)
		}
		cancel()
	}
}

// Flush moves counted results into the pending buffer and pushes every
// pending window to the store. Windows that fail stay pending and are
// retried on the next flush until they exceed MaxPendingAge.
func (t *Throttler) Flush(ctx context.Context) error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	now := t.opts.Clock.Now()
	// Attribute this interval's counts to the window containing its midpoint.
	// Flushes are aligned to window boundaries, so in practice an interval
	// never straddles two windows by more than scheduling jitter.
	w := window.Of(t.lastFlush.Add(now.Sub(t.lastFlush)/2), t.opts.Window)
	t.lastFlush = now

	t.collect(w)

	var errs []error
	for pw, deltas := range t.pending {
		if age := now.Sub(pw.End(t.opts.Window)); age > t.opts.MaxPendingAge {
			var n int64
			for _, d := range deltas {
				n += d.Total
			}
			t.opts.Metrics.AddDroppedStats(n)
			t.opts.Logger.Warn("dropping stats the evaluator has already passed", "window", pw, "calls", n, "age", age)
			delete(t.pending, pw)
			continue
		}
		if err := t.sink.AddStats(ctx, pw, deltas); err != nil {
			t.opts.Metrics.IncFlushErrors()
			errs = append(errs, err)
			continue
		}
		delete(t.pending, pw)
	}
	return errors.Join(errs...)
}

// collect swaps every buyer's counters to zero and adds them to window w's
// pending deltas. Telemetry is updated here, off the hot path.
func (t *Throttler) collect(w window.ID) {
	t.counters.Range(func(key, value any) bool {
		buyer, c := key.(string), value.(*counters)
		ok, failed, timeouts := c.ok.Swap(0), c.failed.Swap(0), c.timeouts.Swap(0)
		if skipped := c.skipped.Swap(0); skipped > 0 {
			t.opts.Metrics.AddSkipped(buyer, skipped)
		}
		if ok+failed+timeouts == 0 {
			return true
		}
		t.opts.Metrics.AddCalls(buyer, ok, failed, timeouts)
		deltas, exists := t.pending[w]
		if !exists {
			deltas = make(map[string]policy.WindowStats)
			t.pending[w] = deltas
		}
		deltas[buyer] = deltas[buyer].Add(policy.WindowStats{Total: ok + failed + timeouts, Failed: failed, Timeouts: timeouts})
		return true
	})
}
