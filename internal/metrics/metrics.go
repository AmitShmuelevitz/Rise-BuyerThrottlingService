// Package metrics implements the throttler and evaluator telemetry
// interfaces with Prometheus collectors.
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/evaluator"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/throttler"
)

var (
	_ throttler.Metrics = (*Throttler)(nil)
	_ evaluator.Metrics = (*Evaluator)(nil)
)

// Throttler holds the auction pod's metrics.
type Throttler struct {
	calls        *prometheus.CounterVec
	skipped      *prometheus.CounterVec
	rate         *prometheus.GaugeVec
	version      prometheus.Gauge
	flushErrors  prometheus.Counter
	droppedStats prometheus.Counter
	syncErrors   prometheus.Counter

	mu    sync.Mutex
	known map[string]struct{} // buyers with a rate gauge, so cleared buyers are reset to 0
}

// NewThrottler registers the pod's metrics on reg.
func NewThrottler(reg prometheus.Registerer) *Throttler {
	m := &Throttler{
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "buyer_calls_total", Help: "Calls made to buyers, by result.",
		}, []string{"buyer", "result"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "buyer_skipped_total", Help: "Requests not sent to a buyer because of throttling.",
		}, []string{"buyer"}),
		rate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "throttle_rate", Help: "Throttle rate per buyer as currently applied.",
		}, []string{"buyer"}),
		version: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "throttle_state_version", Help: "Version of the throttle state loaded by this pod.",
		}),
		flushErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "throttle_stats_flush_errors_total", Help: "Failed attempts to push stats to the store.",
		}),
		droppedStats: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "throttle_stats_dropped_total", Help: "Calls whose stats were dropped after the store was unavailable too long.",
		}),
		syncErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "throttle_state_sync_errors_total", Help: "Failed attempts to load throttle state.",
		}),
		known: make(map[string]struct{}),
	}
	reg.MustRegister(m.calls, m.skipped, m.rate, m.version, m.flushErrors, m.droppedStats, m.syncErrors)
	return m
}

func (m *Throttler) AddCalls(buyer string, ok, failed, timeouts int64) {
	m.calls.WithLabelValues(buyer, "ok").Add(float64(ok))
	m.calls.WithLabelValues(buyer, "failed").Add(float64(failed))
	m.calls.WithLabelValues(buyer, "timeout").Add(float64(timeouts))
}

func (m *Throttler) AddSkipped(buyer string, n int64) {
	m.skipped.WithLabelValues(buyer).Add(float64(n))
}

func (m *Throttler) SetRates(rates map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for b := range m.known {
		if _, ok := rates[b]; !ok {
			m.rate.WithLabelValues(b).Set(0)
		}
	}
	for b, r := range rates {
		m.rate.WithLabelValues(b).Set(r)
		m.known[b] = struct{}{}
	}
}

func (m *Throttler) SetStateVersion(v uint64) { m.version.Set(float64(v)) }
func (m *Throttler) IncFlushErrors()          { m.flushErrors.Inc() }
func (m *Throttler) AddDroppedStats(n int64)  { m.droppedStats.Add(float64(n)) }
func (m *Throttler) IncSyncErrors()           { m.syncErrors.Inc() }

// Evaluator holds the evaluator's metrics.
type Evaluator struct {
	leader      prometheus.Gauge
	runs        *prometheus.CounterVec
	duration    prometheus.Histogram
	rate        *prometheus.GaugeVec
	requests    *prometheus.GaugeVec
	failureRate *prometheus.GaugeVec
	timeoutRate *prometheus.GaugeVec
	events      *prometheus.CounterVec
}

// NewEvaluator registers the evaluator's metrics on reg.
func NewEvaluator(reg prometheus.Registerer) *Evaluator {
	m := &Evaluator{
		leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "evaluator_is_leader", Help: "1 if this replica holds the evaluator lease.",
		}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "evaluator_runs_total", Help: "Window evaluations, by outcome (applied, skipped, error).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "evaluator_run_duration_seconds", Help: "Time to evaluate and commit one window.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
		}),
		rate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "evaluator_throttle_rate", Help: "Throttle rate per buyer decided by the evaluator.",
		}, []string{"buyer"}),
		requests: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "evaluator_window_requests", Help: "Requests to the buyer in the last evaluated window, across all pods.",
		}, []string{"buyer"}),
		failureRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "evaluator_window_failure_rate", Help: "failed/total in the last evaluated window.",
		}, []string{"buyer"}),
		timeoutRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "evaluator_window_timeout_rate", Help: "timeouts/total in the last evaluated window.",
		}, []string{"buyer"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "evaluator_decisions_total", Help: "Per-buyer decisions, by event.",
		}, []string{"buyer", "event"}),
	}
	reg.MustRegister(m.leader, m.runs, m.duration, m.rate, m.requests, m.failureRate, m.timeoutRate, m.events)
	return m
}

func (m *Evaluator) SetLeader(leader bool) {
	if leader {
		m.leader.Set(1)
	} else {
		m.leader.Set(0)
	}
}

func (m *Evaluator) ObserveRun(outcome string, d time.Duration) {
	m.runs.WithLabelValues(outcome).Inc()
	if outcome == "applied" {
		m.duration.Observe(d.Seconds())
	}
}

func (m *Evaluator) ObserveDecision(d policy.Decision) {
	b := d.Next.BuyerID
	m.rate.WithLabelValues(b).Set(d.Next.ThrottleRate)
	m.requests.WithLabelValues(b).Set(float64(d.Stats.Total))
	m.failureRate.WithLabelValues(b).Set(d.Stats.FailureRate())
	m.timeoutRate.WithLabelValues(b).Set(d.Stats.TimeoutRate())
	m.events.WithLabelValues(b, string(d.Event)).Inc()
}
