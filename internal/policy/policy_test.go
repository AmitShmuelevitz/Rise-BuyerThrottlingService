package policy

import (
	"strings"
	"testing"
	"time"
)

func stats(total, failed, timeouts int64) WindowStats {
	return WindowStats{Total: total, Failed: failed, Timeouts: timeouts}
}

func open() State { return State{BuyerID: "b1"} }

func blocked() State {
	return State{BuyerID: "b1", ThrottleRate: 0.95, Reason: ReasonFailureRate}
}

func at(rate float64) State {
	return State{BuyerID: "b1", ThrottleRate: rate, Reason: ReasonRecovering}
}

func TestEvaluate(t *testing.T) {
	cfg := DefaultConfig()

	tests := []struct {
		name      string
		prev      State
		stats     WindowStats
		wantRate  float64
		wantWhy   Reason
		wantEvent Event
	}{
		{"healthy buyer stays open", open(), stats(1000, 10, 10), 0, ReasonNone, EventUnchanged},
		{"failure rate over threshold blocks", open(), stats(1000, 800, 0), 0.95, ReasonFailureRate, EventBlocked},
		{"failure rate exactly at threshold blocks", open(), stats(1000, 700, 0), 0.95, ReasonFailureRate, EventBlocked},
		{"failure rate just below threshold does not block", open(), stats(1000, 699, 0), 0, ReasonNone, EventUnchanged},
		{"timeout rate over threshold blocks", open(), stats(1000, 0, 400), 0.95, ReasonTimeoutRate, EventBlocked},
		{"timeout rate exactly at threshold blocks", open(), stats(1000, 0, 300), 0.95, ReasonTimeoutRate, EventBlocked},
		{"timeout rate just below threshold does not block", open(), stats(1000, 0, 299), 0, ReasonNone, EventUnchanged},
		{"both thresholds broken reports failure rate", open(), stats(1000, 700, 300), 0.95, ReasonFailureRate, EventBlocked},
		{"failures and timeouts are not summed", open(), stats(1000, 600, 250), 0, ReasonNone, EventUnchanged},
		{"exactly min sample is evaluated", open(), stats(100, 100, 0), 0.95, ReasonFailureRate, EventBlocked},
		{"under min sample never blocks, even at 100% failure", open(), stats(99, 99, 0), 0, ReasonNone, EventInsufficient},
		{"under min sample never blocks on timeouts", open(), stats(50, 0, 50), 0, ReasonNone, EventInsufficient},
		{"empty window holds", open(), stats(0, 0, 0), 0, ReasonNone, EventInsufficient},
		{"clean window after block steps down", blocked(), stats(1000, 0, 0), 0.85, ReasonRecovering, EventRecoveryStep},
		{"clean window mid recovery steps down", at(0.55), stats(1000, 0, 0), 0.45, ReasonRecovering, EventRecoveryStep},
		{"last step reaches zero and clears reason", at(0.05), stats(1000, 0, 0), 0, ReasonNone, EventRecovered},
		{"step never goes below zero", at(0.03), stats(1000, 0, 0), 0, ReasonNone, EventRecovered},
		{"violation mid recovery snaps back to block", at(0.35), stats(1000, 900, 0), 0.95, ReasonFailureRate, EventBlocked},
		{"timeout mid recovery snaps back to block", at(0.15), stats(1000, 0, 500), 0.95, ReasonTimeoutRate, EventBlocked},
		{"violation while blocked stays blocked and refreshes reason", blocked(), stats(1000, 0, 500), 0.95, ReasonTimeoutRate, EventBlocked},
		{"under min sample while blocked holds", blocked(), stats(10, 0, 0), 0.95, ReasonFailureRate, EventInsufficient},
		{"under min sample mid recovery holds", at(0.45), stats(10, 0, 0), 0.45, ReasonRecovering, EventInsufficient},
		{"under min sample violation mid recovery holds", at(0.45), stats(10, 10, 0), 0.45, ReasonRecovering, EventInsufficient},
		{"rate above a lowered block level is clamped", State{BuyerID: "b1", ThrottleRate: 0.99, Reason: ReasonFailureRate}, stats(10, 0, 0), 0.95, ReasonFailureRate, EventInsufficient},
		{"negative rate is treated as open", State{BuyerID: "b1", ThrottleRate: -0.5}, stats(1000, 0, 0), 0, ReasonNone, EventUnchanged},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Evaluate(tt.prev, tt.stats, cfg)
			if d.Next.ThrottleRate != tt.wantRate {
				t.Errorf("rate = %v, want %v", d.Next.ThrottleRate, tt.wantRate)
			}
			if d.Next.Reason != tt.wantWhy {
				t.Errorf("reason = %q, want %q", d.Next.Reason, tt.wantWhy)
			}
			if d.Event != tt.wantEvent {
				t.Errorf("event = %q, want %q", d.Event, tt.wantEvent)
			}
			if d.Next.BuyerID != tt.prev.BuyerID {
				t.Errorf("buyer id = %q, want %q", d.Next.BuyerID, tt.prev.BuyerID)
			}
		})
	}
}

func TestEvaluate_FullRecoveryTakesExactlyTenCleanWindows(t *testing.T) {
	cfg := DefaultConfig()
	s := Evaluate(open(), stats(1000, 1000, 0), cfg).Next

	want := []float64{0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05, 0}
	for i, w := range want {
		s = Evaluate(s, stats(1000, 0, 0), cfg).Next
		if s.ThrottleRate != w {
			t.Fatalf("after clean window %d: rate = %v, want exactly %v", i+1, s.ThrottleRate, w)
		}
	}
	if s.Reason != ReasonNone {
		t.Errorf("reason after full recovery = %q, want empty", s.Reason)
	}
}

func TestEvaluate_CustomConfig(t *testing.T) {
	cfg := Config{
		FailureThreshold: 0.5,
		TimeoutThreshold: 0.1,
		MinSampleSize:    10,
		BlockLevel:       0.8,
		RecoveryStep:     0.25,
		Window:           10 * time.Second,
	}

	s := Evaluate(open(), stats(10, 0, 1), cfg).Next
	if s.ThrottleRate != 0.8 || s.Reason != ReasonTimeoutRate {
		t.Fatalf("custom thresholds: got %+v, want blocked at 0.8 on timeout_rate", s)
	}
	for _, w := range []float64{0.55, 0.3, 0.05, 0} {
		s = Evaluate(s, stats(10, 0, 0), cfg).Next
		if s.ThrottleRate != w {
			t.Fatalf("rate = %v, want %v", s.ThrottleRate, w)
		}
	}
}

func TestEvaluate_StaleRecovery(t *testing.T) {
	t.Run("disabled by default: under-sample windows hold forever", func(t *testing.T) {
		cfg := DefaultConfig()
		s := blocked()
		for range 50 {
			s = Evaluate(s, stats(5, 0, 0), cfg).Next
		}
		if s.ThrottleRate != 0.95 {
			t.Fatalf("rate = %v, want 0.95 held", s.ThrottleRate)
		}
		if s.UnderSampleStreak != 50 {
			t.Fatalf("streak = %d, want 50", s.UnderSampleStreak)
		}
	})

	t.Run("enabled: steps once per N under-sample windows", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StaleRecoveryWindows = 3
		s := blocked()

		var events []Event
		for range 6 {
			d := Evaluate(s, stats(5, 5, 0), cfg)
			events = append(events, d.Event)
			s = d.Next
		}
		want := []Event{EventInsufficient, EventInsufficient, EventStaleRecovery, EventInsufficient, EventInsufficient, EventStaleRecovery}
		for i := range want {
			if events[i] != want[i] {
				t.Fatalf("events = %v, want %v", events, want)
			}
		}
		if s.ThrottleRate != 0.75 || s.Reason != ReasonRecovering {
			t.Fatalf("state = %+v, want 0.75 recovering", s)
		}
	})

	t.Run("enabled: a full window resets the streak", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StaleRecoveryWindows = 3
		s := blocked()
		s = Evaluate(s, stats(5, 0, 0), cfg).Next
		s = Evaluate(s, stats(5, 0, 0), cfg).Next
		s = Evaluate(s, stats(1000, 0, 0), cfg).Next // clean, steps to 0.85, resets streak
		s = Evaluate(s, stats(5, 0, 0), cfg).Next
		s = Evaluate(s, stats(5, 0, 0), cfg).Next
		if s.ThrottleRate != 0.85 || s.UnderSampleStreak != 2 {
			t.Fatalf("state = %+v, want 0.85 with streak 2", s)
		}
	})

	t.Run("enabled: an open buyer does not change", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.StaleRecoveryWindows = 1
		d := Evaluate(open(), stats(5, 0, 0), cfg)
		if d.Next.ThrottleRate != 0 || d.Event != EventInsufficient {
			t.Fatalf("got %+v event %s, want open and insufficient", d.Next, d.Event)
		}
	})
}

// TestEvaluate_Scenario plays a buyer's life over many windows and checks the
// rate after each one, so the rules are proven to compose over time.
func TestEvaluate_Scenario(t *testing.T) {
	cfg := DefaultConfig()
	healthy, outage, slow, quiet := stats(2000, 20, 10), stats(2000, 1900, 0), stats(100, 0, 40), stats(40, 40, 0)

	steps := []struct {
		stats WindowStats
		want  float64
		why   Reason
	}{
		{healthy, 0, ReasonNone},
		{outage, 0.95, ReasonFailureRate},
		{quiet, 0.95, ReasonFailureRate}, // too few samples: held, not clean
		{healthy, 0.85, ReasonRecovering},
		{healthy, 0.75, ReasonRecovering},
		{healthy, 0.65, ReasonRecovering},
		{slow, 0.95, ReasonTimeoutRate}, // relapse mid recovery
		{healthy, 0.85, ReasonRecovering},
		{healthy, 0.75, ReasonRecovering},
		{quiet, 0.75, ReasonRecovering},
		{healthy, 0.65, ReasonRecovering},
		{healthy, 0.55, ReasonRecovering},
		{healthy, 0.45, ReasonRecovering},
		{healthy, 0.35, ReasonRecovering},
		{healthy, 0.25, ReasonRecovering},
		{healthy, 0.15, ReasonRecovering},
		{healthy, 0.05, ReasonRecovering},
		{healthy, 0, ReasonNone},
		{healthy, 0, ReasonNone},
	}

	s := open()
	for i, st := range steps {
		s = Evaluate(s, st.stats, cfg).Next
		if s.ThrottleRate != st.want || s.Reason != st.why {
			t.Fatalf("window %d: got rate=%v reason=%q, want rate=%v reason=%q", i, s.ThrottleRate, s.Reason, st.want, st.why)
		}
	}
}

func TestDecision_Changed(t *testing.T) {
	cfg := DefaultConfig()
	if Evaluate(open(), stats(1000, 0, 0), cfg).Changed() {
		t.Error("healthy open buyer reported as changed")
	}
	if !Evaluate(open(), stats(1000, 1000, 0), cfg).Changed() {
		t.Error("block not reported as changed")
	}
	if !Evaluate(blocked(), stats(1000, 0, 1000), cfg).Changed() {
		t.Error("reason change not reported as changed")
	}
	if Evaluate(blocked(), stats(10, 0, 0), cfg).Changed() {
		t.Error("held state reported as changed")
	}
}

func TestWindowStats(t *testing.T) {
	s := stats(10, 3, 2).Add(stats(10, 1, 0))
	if s != stats(20, 4, 2) {
		t.Fatalf("Add = %+v", s)
	}
	if s.FailureRate() != 0.2 || s.TimeoutRate() != 0.1 {
		t.Fatalf("rates = %v, %v", s.FailureRate(), s.TimeoutRate())
	}
	if (WindowStats{}).FailureRate() != 0 || !(WindowStats{}).IsZero() {
		t.Fatal("empty stats should have zero rates and be zero")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"zero failure threshold", func(c *Config) { c.FailureThreshold = 0 }, "failure threshold"},
		{"failure threshold above 1", func(c *Config) { c.FailureThreshold = 1.1 }, "failure threshold"},
		{"zero timeout threshold", func(c *Config) { c.TimeoutThreshold = 0 }, "timeout threshold"},
		{"zero min sample", func(c *Config) { c.MinSampleSize = 0 }, "min sample"},
		{"block level of 1 would cut traffic to zero", func(c *Config) { c.BlockLevel = 1 }, "block level"},
		{"zero block level", func(c *Config) { c.BlockLevel = 0 }, "block level"},
		{"zero step", func(c *Config) { c.RecoveryStep = 0 }, "recovery step"},
		{"sub-second window", func(c *Config) { c.Window = 500 * time.Millisecond }, "window"},
		{"fractional-second window", func(c *Config) { c.Window = 1500 * time.Millisecond }, "window"},
		{"negative stale recovery", func(c *Config) { c.StaleRecoveryWindows = -1 }, "stale recovery"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			tt.mutate(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want error mentioning %q", err, tt.want)
			}
		})
	}

	t.Run("reports all errors at once", func(t *testing.T) {
		c := DefaultConfig()
		c.FailureThreshold, c.BlockLevel = 0, 1
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "failure threshold") || !strings.Contains(err.Error(), "block level") {
			t.Fatalf("Validate() = %v, want both errors", err)
		}
	})
}
