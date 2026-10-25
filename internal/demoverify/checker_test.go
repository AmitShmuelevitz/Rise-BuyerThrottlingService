package demoverify

import (
	"strings"
	"testing"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/simulator"
)

func spec(id string, e ...simulator.Expectation) simulator.BuyerSpec {
	return simulator.BuyerSpec{ID: id, Expect: e}
}

func obs(rate float64, reason string) Observation { return Observation{Rate: rate, Reason: reason} }

func TestChecker_FullStory(t *testing.T) {
	c := NewChecker([]simulator.BuyerSpec{
		spec("bad", simulator.ExpectBlockedOnFailures, simulator.ExpectRecovered),
		spec("relapse", simulator.ExpectBlockedOnFailures, simulator.ExpectSnapBack, simulator.ExpectRecovered),
		spec("slow", simulator.ExpectBlockedOnTimeouts),
		spec("tiny", simulator.ExpectNeverThrottled),
		spec("untracked"),
	}, 0.95)

	steps := []map[string]Observation{
		{},
		{"bad": obs(0.95, "failure_rate"), "relapse": obs(0.95, "failure_rate"), "slow": obs(0.95, "timeout_rate")},
		{"bad": obs(0.85, "recovering"), "relapse": obs(0.75, "recovering"), "tiny": obs(0, "")},
		{"bad": obs(0, ""), "relapse": obs(0.95, "failure_rate"), "untracked": obs(0.95, "failure_rate")},
	}
	for _, s := range steps {
		if c.Done() {
			t.Fatal("done too early")
		}
		c.Observe(s)
	}
	if got := c.Pending(); len(got) != 1 || !strings.HasPrefix(got[0], "relapse: recovered") {
		t.Fatalf("pending = %v, want only relapse recovery", got)
	}
	c.Observe(map[string]Observation{"relapse": obs(0, "")})
	if !c.Done() {
		t.Fatalf("not done: pending=%v failed=%v", c.Pending(), c.Failed())
	}
}

func TestChecker_NeverThrottledViolation(t *testing.T) {
	c := NewChecker([]simulator.BuyerSpec{spec("tiny", simulator.ExpectNeverThrottled)}, 0.95)
	if !c.Done() {
		t.Fatal("a never_throttled buyer with no observations should already be done")
	}
	c.Observe(map[string]Observation{"tiny": obs(0.95, "failure_rate")})
	if f := c.Failed(); len(f) != 1 || !strings.Contains(f[0], "tiny was throttled") {
		t.Fatalf("failed = %v", f)
	}
	c.Observe(map[string]Observation{"tiny": obs(0, "")})
	if c.Done() {
		t.Fatal("a violation must stick even after the buyer reopens")
	}
}

func TestChecker_WrongBlockReasonIsNotAccepted(t *testing.T) {
	c := NewChecker([]simulator.BuyerSpec{spec("slow", simulator.ExpectBlockedOnTimeouts)}, 0.95)
	c.Observe(map[string]Observation{"slow": obs(0.95, "failure_rate")})
	if c.Done() {
		t.Fatal("blocked on failures must not satisfy blocked_on_timeouts")
	}
}

func TestChecker_RecoveredRequiresPriorBlock(t *testing.T) {
	c := NewChecker([]simulator.BuyerSpec{spec("b", simulator.ExpectRecovered)}, 0.95)
	c.Observe(map[string]Observation{"b": obs(0, "")})
	if c.Done() {
		t.Fatal("an open buyer that was never blocked has not recovered")
	}
}

func TestChecker_SnapBackNeedsPartialRecoveryFirst(t *testing.T) {
	c := NewChecker([]simulator.BuyerSpec{spec("b", simulator.ExpectSnapBack)}, 0.95)
	c.Observe(map[string]Observation{"b": obs(0.95, "failure_rate")})
	c.Observe(map[string]Observation{"b": obs(0.95, "timeout_rate")})
	if c.Done() {
		t.Fatal("block -> block is not a snap back")
	}
	c.Observe(map[string]Observation{"b": obs(0.85, "recovering")})
	c.Observe(map[string]Observation{"b": obs(0.95, "failure_rate")})
	if !c.Done() {
		t.Fatal("0.85 -> 0.95 is a snap back")
	}
}
