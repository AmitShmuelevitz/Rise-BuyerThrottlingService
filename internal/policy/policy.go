package policy

import "math"

// Reason explains why a buyer has its current throttle rate.
type Reason string

const (
	ReasonNone        Reason = ""
	ReasonFailureRate Reason = "failure_rate"
	ReasonTimeoutRate Reason = "timeout_rate"
	ReasonRecovering  Reason = "recovering"
)

// WindowStats are a buyer's aggregated call results for one window, summed
// across all auction pods. Results are mutually exclusive, so
// Total = ok + Failed + Timeouts.
type WindowStats struct {
	Total    int64 `json:"total"`
	Failed   int64 `json:"failed"`
	Timeouts int64 `json:"timeouts"`
}

// Add returns the element-wise sum of s and o.
func (s WindowStats) Add(o WindowStats) WindowStats {
	return WindowStats{
		Total:    s.Total + o.Total,
		Failed:   s.Failed + o.Failed,
		Timeouts: s.Timeouts + o.Timeouts,
	}
}

// IsZero reports whether no calls were recorded.
func (s WindowStats) IsZero() bool { return s == WindowStats{} }

// FailureRate is failed/total, or 0 for an empty window.
func (s WindowStats) FailureRate() float64 { return ratio(s.Failed, s.Total) }

// TimeoutRate is timeouts/total, or 0 for an empty window.
func (s WindowStats) TimeoutRate() float64 { return ratio(s.Timeouts, s.Total) }

func ratio(n, d int64) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / float64(d)
}

// State is a buyer's throttle state as seen by the auction pods.
type State struct {
	BuyerID      string  `json:"buyer_id"`
	ThrottleRate float64 `json:"throttle_rate"`
	Reason       Reason  `json:"reason"`
	// UnderSampleStreak counts consecutive windows below the minimum sample
	// size. It only matters when Config.StaleRecoveryWindows is enabled.
	UnderSampleStreak int `json:"under_sample_streak,omitempty"`
}

// Event classifies what an evaluation did, mainly for logs and metrics.
type Event string

const (
	EventUnchanged     Event = "unchanged"      // healthy and fully open
	EventBlocked       Event = "blocked"        // moved to (or re-entered) the block level
	EventRecoveryStep  Event = "recovery_step"  // rate lowered by one step, still > 0
	EventRecovered     Event = "recovered"      // rate reached 0
	EventInsufficient  Event = "insufficient"   // under min sample, rate held
	EventStaleRecovery Event = "stale_recovery" // under min sample for too long, took one step
)

// Decision is the outcome of evaluating one buyer for one window.
type Decision struct {
	Prev  State
	Next  State
	Event Event
	Stats WindowStats
}

// Changed reports whether the visible throttle state changed.
func (d Decision) Changed() bool {
	return d.Prev.ThrottleRate != d.Next.ThrottleRate || d.Prev.Reason != d.Next.Reason
}

// ratePrecision is the number of decimals throttle rates are rounded to, so
// repeated subtraction of the step never leaves float residue (0.05 - 0.1
// lands on exactly 0, not -1e-17, and 0.95 reaches 0 in exactly 10 steps).
const ratePrecision = 1e4

func roundRate(r float64) float64 { return math.Round(r*ratePrecision) / ratePrecision }

// Evaluate applies the throttling rules to one buyer for one closed window.
//
// Rules, checked in order:
//  1. Fewer than MinSampleSize requests: the rates are not meaningful, so the
//     window is neutral and the rate is held (it is neither a violation nor a
//     clean window). Optionally, after StaleRecoveryWindows such windows in a
//     row, a throttled buyer takes one recovery step.
//  2. failed/total >= FailureThreshold: block, reason failure_rate.
//  3. timeouts/total >= TimeoutThreshold: block, reason timeout_rate.
//  4. Otherwise the window is clean: lower the rate by RecoveryStep, down to 0.
func Evaluate(prev State, stats WindowStats, cfg Config) Decision {
	d := Decision{Prev: prev, Stats: stats}
	// A rate above the block level can only come from a config change that
	// lowered it, so the buyer is clamped to the new level.
	prev.ThrottleRate = clamp(prev.ThrottleRate, 0, cfg.BlockLevel)
	next := prev

	if stats.Total < cfg.MinSampleSize {
		next.UnderSampleStreak = prev.UnderSampleStreak + 1
		d.Event = EventInsufficient
		if cfg.StaleRecoveryWindows > 0 && next.ThrottleRate > 0 &&
			next.UnderSampleStreak >= cfg.StaleRecoveryWindows {
			next = step(next, cfg)
			next.UnderSampleStreak = 0
			d.Event = EventStaleRecovery
		}
		d.Next = next
		return d
	}

	next.UnderSampleStreak = 0
	switch {
	case stats.FailureRate() >= cfg.FailureThreshold:
		next.ThrottleRate, next.Reason, d.Event = cfg.BlockLevel, ReasonFailureRate, EventBlocked
	case stats.TimeoutRate() >= cfg.TimeoutThreshold:
		next.ThrottleRate, next.Reason, d.Event = cfg.BlockLevel, ReasonTimeoutRate, EventBlocked
	case next.ThrottleRate > 0:
		next = step(next, cfg)
		d.Event = EventRecoveryStep
		if next.ThrottleRate == 0 {
			d.Event = EventRecovered
		}
	default:
		next.Reason = ReasonNone
		d.Event = EventUnchanged
	}
	d.Next = next
	return d
}

// step lowers the rate by one recovery step and sets the matching reason.
func step(s State, cfg Config) State {
	s.ThrottleRate = roundRate(math.Max(0, s.ThrottleRate-cfg.RecoveryStep))
	if s.ThrottleRate == 0 {
		s.Reason = ReasonNone
	} else {
		s.Reason = ReasonRecovering
	}
	return s
}

func clamp(v, lo, hi float64) float64 {
	if math.IsNaN(v) {
		return lo
	}
	return math.Min(math.Max(v, lo), hi)
}
