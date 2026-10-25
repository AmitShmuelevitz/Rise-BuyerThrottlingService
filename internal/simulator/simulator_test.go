package simulator

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
)

const sample = `
rps: 100
latency: 5ms
buyers:
  - id: bad
    phases:
      - {duration: 30s, failure_rate: 0.9}
      - {duration: 10s, timeout_rate: 0.5}
      - {failure_rate: 0.01}
  - id: tiny
    share: 0.01
    phases:
      - {failure_rate: 1}
`

func TestParseScenario(t *testing.T) {
	s, err := ParseScenario([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if s.RPS != 100 || s.Latency.D() != 5*time.Millisecond || s.TimeoutLatency.D() != 300*time.Millisecond {
		t.Fatalf("scenario = %+v", s)
	}
	bad, tiny := s.Buyers[0], s.Buyers[1]
	tests := []struct {
		at   time.Duration
		want Behavior
	}{
		{0, Behavior{FailureRate: 0.9}},
		{29 * time.Second, Behavior{FailureRate: 0.9}},
		{30 * time.Second, Behavior{TimeoutRate: 0.5}},
		{40 * time.Second, Behavior{FailureRate: 0.01}},
		{time.Hour, Behavior{FailureRate: 0.01}},
	}
	for _, tt := range tests {
		if got := bad.At(tt.at); got != tt.want {
			t.Errorf("At(%v) = %+v, want %+v", tt.at, got, tt.want)
		}
	}
	if bad.ShareOrDefault() != 1 || tiny.ShareOrDefault() != 0.01 {
		t.Errorf("shares = %v, %v", bad.ShareOrDefault(), tiny.ShareOrDefault())
	}
}

func TestParseScenario_Invalid(t *testing.T) {
	tests := map[string]string{
		"no rps":           "buyers: [{id: a, phases: [{}]}]",
		"no buyers":        "rps: 1",
		"duplicate":        "rps: 1\nbuyers: [{id: a, phases: [{}]}, {id: a, phases: [{}]}]",
		"no phases":        "rps: 1\nbuyers: [{id: a}]",
		"rates over 1":     "rps: 1\nbuyers: [{id: a, phases: [{failure_rate: 0.6, timeout_rate: 0.6}]}]",
		"missing duration": "rps: 1\nbuyers: [{id: a, phases: [{failure_rate: 1}, {}]}]",
		"bad share":        "rps: 1\nbuyers: [{id: a, share: 2, phases: [{}]}]",
		"bad duration":     "rps: 1\nlatency: soon\nbuyers: [{id: a, phases: [{}]}]",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseScenario([]byte(raw)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadScenario_ShippedFile(t *testing.T) {
	if _, err := LoadScenario("../../deploy/scenario.yaml"); err != nil {
		t.Fatalf("deploy/scenario.yaml is invalid: %v", err)
	}
}

func newTestBuyers(t *testing.T, draw float64) (*Buyers, *clock.Fake) {
	s, err := ParseScenario([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	c := clock.NewFake(time.Unix(0, 0))
	b := NewBuyers(s, c, slog.New(slog.DiscardHandler))
	b.rand = func() float64 { return draw }
	b.sleep = func(*http.Request, time.Duration) {}
	return b, c
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestBid_FollowsScenario(t *testing.T) {
	b, c := newTestBuyers(t, 0.5)
	h := b.Handler()

	if code := get(h, "/bid/bad").Code; code != 500 {
		t.Fatalf("bad during outage = %d, want 500", code)
	}
	c.Advance(45 * time.Second)
	if code := get(h, "/bid/bad").Code; code != 200 {
		t.Fatalf("bad after outage = %d, want 200", code)
	}
	if code := get(h, "/bid/unknown").Code; code != 200 {
		t.Fatalf("unknown buyer = %d, want 200 (healthy)", code)
	}
}

func TestBid_TimeoutSleepsTimeoutLatency(t *testing.T) {
	b, c := newTestBuyers(t, 0.2)
	var slept time.Duration
	b.sleep = func(_ *http.Request, d time.Duration) { slept = d }
	c.Advance(35 * time.Second) // timeout phase
	if rec := get(b.Handler(), "/bid/bad"); rec.Code != 200 || slept != 300*time.Millisecond {
		t.Fatalf("code=%d slept=%v, want 200 after 300ms", rec.Code, slept)
	}
}

func TestOverride(t *testing.T) {
	b, _ := newTestBuyers(t, 0.5)
	h := b.Handler()

	put := func(body string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("PUT", "/buyers/tiny", strings.NewReader(body)))
		return rec.Code
	}
	if code := put(`{"failure_rate": 0}`); code != 200 {
		t.Fatalf("PUT = %d", code)
	}
	if code := get(h, "/bid/tiny").Code; code != 200 {
		t.Fatalf("overridden buyer = %d, want 200", code)
	}
	if code := put(`{"failure_rate": 2}`); code != 400 {
		t.Fatalf("invalid PUT = %d, want 400", code)
	}
	if !strings.Contains(get(h, "/buyers").Body.String(), `"overridden": true`) {
		t.Fatal("GET /buyers does not show the override")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("DELETE", "/buyers/tiny", nil))
	if rec.Code != 204 || get(h, "/bid/tiny").Code != 500 {
		t.Fatal("DELETE did not restore the scenario behaviour")
	}
}
