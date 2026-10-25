package config

import (
	"strings"
	"testing"
	"time"

	"github.com/amit/buyer-throttling/internal/policy"
)

func TestPolicy_Defaults(t *testing.T) {
	e := FromMap(nil)
	if got := e.Policy(); got != policy.DefaultConfig() {
		t.Fatalf("Policy() = %+v, want defaults", got)
	}
	if err := e.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestPolicy_Overrides(t *testing.T) {
	e := FromMap(map[string]string{
		"THROTTLE_FAILURE_THRESHOLD":      "0.5",
		"THROTTLE_TIMEOUT_THRESHOLD":      "0.2",
		"THROTTLE_MIN_SAMPLE_SIZE":        "50",
		"THROTTLE_BLOCK_LEVEL":            "0.9",
		"THROTTLE_RECOVERY_STEP":          "0.3",
		"THROTTLE_WINDOW":                 "10s",
		"THROTTLE_STALE_RECOVERY_WINDOWS": "5",
	})
	want := policy.Config{FailureThreshold: 0.5, TimeoutThreshold: 0.2, MinSampleSize: 50, BlockLevel: 0.9, RecoveryStep: 0.3, Window: 10 * time.Second, StaleRecoveryWindows: 5}
	if got := e.Policy(); got != want {
		t.Fatalf("Policy() = %+v, want %+v", got, want)
	}
	if err := e.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestPolicy_ReportsAllErrors(t *testing.T) {
	e := FromMap(map[string]string{
		"THROTTLE_FAILURE_THRESHOLD": "lots",
		"THROTTLE_WINDOW":            "soon",
		"THROTTLE_BLOCK_LEVEL":       "1",
	})
	e.Policy()
	err := e.Err()
	for _, want := range []string{"THROTTLE_FAILURE_THRESHOLD", "THROTTLE_WINDOW", "block level"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Err() = %v, want it to mention %q", err, want)
		}
	}
}

func TestLogger_BadLevel(t *testing.T) {
	e := FromMap(map[string]string{"LOG_LEVEL": "loud"})
	e.Logger("x")
	if e.Err() == nil {
		t.Fatal("expected LOG_LEVEL error")
	}
}
