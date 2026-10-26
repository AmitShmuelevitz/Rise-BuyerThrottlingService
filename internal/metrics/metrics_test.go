package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
)

func TestThrottler(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewThrottler(reg, time.Minute)
	m.AddCalls("b", 3, 2, 1)
	m.AddSkipped("b", 5)
	m.SetRates(map[string]float64{"b": 0.95})
	m.SetRates(map[string]float64{}) // b recovered: gauge must drop to 0, not stay stale

	want := `
# HELP buyer_calls_total Calls made to buyers, by result.
# TYPE buyer_calls_total counter
buyer_calls_total{buyer="b",result="failed"} 2
buyer_calls_total{buyer="b",result="ok"} 3
buyer_calls_total{buyer="b",result="timeout"} 1
# HELP buyer_skipped_total Requests not sent to a buyer because of throttling.
# TYPE buyer_skipped_total counter
buyer_skipped_total{buyer="b"} 5
# HELP throttle_rate Throttle rate per buyer as currently applied.
# TYPE throttle_rate gauge
throttle_rate{buyer="b"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "buyer_calls_total", "buyer_skipped_total", "throttle_rate"); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluator(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewEvaluator(reg)
	m.SetLeader(true)
	m.ObserveRun("applied", 10*time.Millisecond)
	m.ObserveDecision(policy.Decision{
		Next:  policy.State{BuyerID: "b", ThrottleRate: 0.95, Reason: policy.ReasonFailureRate},
		Event: policy.EventBlocked,
		Stats: policy.WindowStats{Total: 100, Failed: 80, Timeouts: 10},
	})

	checks := map[string]float64{
		"evaluator_is_leader":           1,
		"evaluator_throttle_rate":       0.95,
		"evaluator_window_requests":     100,
		"evaluator_window_failure_rate": 0.8,
		"evaluator_window_timeout_rate": 0.1,
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		want, ok := checks[mf.GetName()]
		if !ok {
			continue
		}
		if got := mf.GetMetric()[0].GetGauge().GetValue(); got != want {
			t.Errorf("%s = %v, want %v", mf.GetName(), got, want)
		}
		delete(checks, mf.GetName())
	}
	if len(checks) != 0 {
		t.Errorf("metrics not found: %v", checks)
	}
}

func TestThrottler_StateAgeAndMismatch(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewThrottler(reg, time.Minute)

	get := func(name string) float64 {
		t.Helper()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, mf := range mfs {
			if mf.GetName() == name {
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
		t.Fatalf("metric %s not found", name)
		return 0
	}

	if age := get("throttle_state_age_seconds"); age != 0 {
		t.Fatalf("age before any state = %v, want 0", age)
	}
	m.SetState(3, time.Now().Add(-90*time.Second))
	if age := get("throttle_state_age_seconds"); age < 89 || age > 95 {
		t.Fatalf("age = %v, want about 90", age)
	}
	m.SetConfigMismatch(true)
	if get("throttle_config_mismatch") != 1 {
		t.Fatal("mismatch gauge not set")
	}
}
