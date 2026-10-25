package policy

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// Property-based tests: rapid generates random valid configurations and
// random histories of windows, checks the invariants after every window, and
// shrinks any failure to a minimal counterexample.

// genConfig draws a valid configuration. Block level and step are drawn in
// hundredths, as an operator would configure them.
func genConfig(t *rapid.T) Config {
	return Config{
		FailureThreshold:     float64(rapid.IntRange(1, 100).Draw(t, "failure_pct")) / 100,
		TimeoutThreshold:     float64(rapid.IntRange(1, 100).Draw(t, "timeout_pct")) / 100,
		MinSampleSize:        rapid.Int64Range(1, 500).Draw(t, "min_sample"),
		BlockLevel:           float64(rapid.IntRange(1, 99).Draw(t, "block_pct")) / 100,
		RecoveryStep:         float64(rapid.IntRange(1, 100).Draw(t, "step_pct")) / 100,
		Window:               time.Minute,
		StaleRecoveryWindows: rapid.IntRange(0, 5).Draw(t, "stale_windows"),
	}
}

func genStats(t *rapid.T, label string) WindowStats {
	total := rapid.Int64Range(0, 2000).Draw(t, label+"_total")
	failed := rapid.Int64Range(0, total).Draw(t, label+"_failed")
	timeouts := rapid.Int64Range(0, total-failed).Draw(t, label+"_timeouts")
	return WindowStats{Total: total, Failed: failed, Timeouts: timeouts}
}

func violates(s WindowStats, cfg Config) bool {
	return s.FailureRate() >= cfg.FailureThreshold || s.TimeoutRate() >= cfg.TimeoutThreshold
}

// TestProperty_Invariants checks every rule on every window of random
// histories.
func TestProperty_Invariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cfg := genConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("generated invalid config: %v", err)
		}
		s := State{BuyerID: "b"}
		n := rapid.IntRange(1, 60).Draw(t, "windows")
		for i := range n {
			st := genStats(t, "w")
			d := Evaluate(s, st, cfg)
			prev, next := s, d.Next

			// The rate stays within [0, block level]: traffic is never cut to zero.
			if next.ThrottleRate < 0 || next.ThrottleRate > cfg.BlockLevel {
				t.Fatalf("window %d: rate %v outside [0, %v]", i, next.ThrottleRate, cfg.BlockLevel)
			}
			// A zero rate and an empty reason always go together.
			if (next.ThrottleRate == 0) != (next.Reason == ReasonNone) {
				t.Fatalf("window %d: rate %v with reason %q", i, next.ThrottleRate, next.Reason)
			}

			switch {
			case st.Total < cfg.MinSampleSize:
				// Too few requests: never a block, and without the probe nothing changes.
				if next.ThrottleRate > prev.ThrottleRate {
					t.Fatalf("window %d: under-sample window raised the rate %v -> %v", i, prev.ThrottleRate, next.ThrottleRate)
				}
				if cfg.StaleRecoveryWindows == 0 && (next.ThrottleRate != prev.ThrottleRate || next.Reason != prev.Reason) {
					t.Fatalf("window %d: under-sample window changed state %+v -> %+v", i, prev, next)
				}
			case violates(st, cfg):
				// Any violation goes straight to the block level.
				if next.ThrottleRate != cfg.BlockLevel || (next.Reason != ReasonFailureRate && next.Reason != ReasonTimeoutRate) {
					t.Fatalf("window %d: violation %+v gave %+v", i, st, next)
				}
			default:
				// A clean window lowers the rate by exactly one step, never below 0.
				want := math.Max(0, roundRate(prev.ThrottleRate-cfg.RecoveryStep))
				if next.ThrottleRate != want {
					t.Fatalf("window %d: clean window took %v to %v, want %v", i, prev.ThrottleRate, next.ThrottleRate, want)
				}
			}
			// The rate only ever rises to the block level, and only on a violation.
			if next.ThrottleRate > prev.ThrottleRate && (st.Total < cfg.MinSampleSize || !violates(st, cfg)) {
				t.Fatalf("window %d: rate rose %v -> %v without a violation", i, prev.ThrottleRate, next.ThrottleRate)
			}
			s = next
		}
	})
}

// TestProperty_RecoveryLength checks that a blocked buyer returns to 0 after
// exactly ceil(block level / step) clean windows, whatever the configuration,
// and that under-sample windows in between do not change that count.
func TestProperty_RecoveryLength(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cfg := genConfig(t)
		cfg.StaleRecoveryWindows = 0
		blockPct := int(math.Round(cfg.BlockLevel * 100))
		stepPct := int(math.Round(cfg.RecoveryStep * 100))
		want := (blockPct + stepPct - 1) / stepPct // integer ceil

		clean := WindowStats{Total: cfg.MinSampleSize}
		quiet := WindowStats{Total: cfg.MinSampleSize - 1, Failed: cfg.MinSampleSize - 1}
		s := Evaluate(State{BuyerID: "b"}, WindowStats{Total: cfg.MinSampleSize, Failed: cfg.MinSampleSize}, cfg).Next
		if s.ThrottleRate != cfg.BlockLevel {
			t.Fatalf("not blocked: %+v", s)
		}

		steps := 0
		for s.ThrottleRate > 0 {
			if rapid.Bool().Draw(t, "insert_quiet_window") {
				s = Evaluate(s, quiet, cfg).Next
				continue
			}
			s = Evaluate(s, clean, cfg).Next
			steps++
			if steps > want {
				t.Fatalf("still at %v after %d clean windows, want 0 after %d", s.ThrottleRate, steps, want)
			}
		}
		if steps != want {
			t.Fatalf("recovered after %d clean windows, want %d (block %v, step %v)", steps, want, cfg.BlockLevel, cfg.RecoveryStep)
		}
	})
}

// FuzzEvaluate feeds arbitrary previous rates and stats, including nonsense
// values such as NaN, negative counts and failures above the total, through
// the default policy. Run it with `make fuzz`; plain `go test` runs the seeds.
func FuzzEvaluate(f *testing.F) {
	f.Add(0.0, int64(1000), int64(800), int64(0))
	f.Add(0.95, int64(1000), int64(0), int64(0))
	f.Add(0.35, int64(99), int64(99), int64(0))
	f.Add(math.NaN(), int64(-5), int64(10), int64(20))
	f.Add(2.0, int64(100), int64(0), int64(300))
	cfg := DefaultConfig()
	f.Fuzz(func(t *testing.T, prevRate float64, total, failed, timeouts int64) {
		d := Evaluate(State{BuyerID: "b", ThrottleRate: prevRate}, WindowStats{Total: total, Failed: failed, Timeouts: timeouts}, cfg)
		r := d.Next.ThrottleRate
		if math.IsNaN(r) || r < 0 || r > cfg.BlockLevel {
			t.Fatalf("Evaluate(rate=%v, total=%d, failed=%d, timeouts=%d) gave rate %v", prevRate, total, failed, timeouts, r)
		}
		if (r == 0) != (d.Next.Reason == ReasonNone) && d.Event != EventInsufficient {
			t.Fatalf("rate %v with reason %q (event %s)", r, d.Next.Reason, d.Event)
		}
	})
}
