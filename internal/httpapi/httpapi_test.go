package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
)

func TestStateHandler(t *testing.T) {
	get := func(context.Context) (map[string]store.Record, uint64, error) {
		return map[string]store.Record{
			"buyer_789": {State: policy.State{BuyerID: "buyer_789"}},
			"buyer_123": {State: policy.State{BuyerID: "buyer_123", ThrottleRate: 0.95, Reason: policy.ReasonFailureRate}},
			"buyer_456": {State: policy.State{BuyerID: "buyer_456", ThrottleRate: 0.35, Reason: policy.ReasonRecovering}},
		}, 7, nil
	}
	rec := httptest.NewRecorder()
	StateHandler(get, slog.New(slog.DiscardHandler)).ServeHTTP(rec, httptest.NewRequest("GET", "/state", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got StateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := []BuyerState{
		{BuyerID: "buyer_123", ThrottleRate: 0.95, Reason: "failure_rate"},
		{BuyerID: "buyer_456", ThrottleRate: 0.35, Reason: "recovering"},
		{BuyerID: "buyer_789", ThrottleRate: 0, Reason: ""},
	}
	if got.Version != 7 || len(got.Buyers) != 3 {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got.Buyers[i] != want[i] {
			t.Errorf("buyers[%d] = %+v, want %+v", i, got.Buyers[i], want[i])
		}
	}
	// The spec's field names must be present even for zero values.
	if !strings.Contains(rec.Body.String(), `"reason": ""`) || !strings.Contains(rec.Body.String(), `"throttle_rate": 0`) {
		t.Errorf("body missing zero-valued fields:\n%s", rec.Body.String())
	}
}

func TestStateHandler_StoreError(t *testing.T) {
	get := func(context.Context) (map[string]store.Record, uint64, error) { return nil, 0, errors.New("down") }
	rec := httptest.NewRecorder()
	StateHandler(get, slog.New(slog.DiscardHandler)).ServeHTTP(rec, httptest.NewRequest("GET", "/state", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestOps(t *testing.T) {
	mux := http.NewServeMux()
	ready := false
	Ops(mux, func(context.Context) (map[string]store.Record, uint64, error) { return nil, 0, nil },
		func() bool { return ready }, prometheus.NewRegistry(), slog.New(slog.DiscardHandler))

	tests := []struct {
		path string
		want int
	}{
		{"/healthz", 200},
		{"/readyz", 503},
		{"/metrics", 200},
		{"/state", 200},
	}
	for _, tt := range tests {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", tt.path, nil))
		if rec.Code != tt.want {
			t.Errorf("GET %s = %d, want %d", tt.path, rec.Code, tt.want)
		}
	}
	ready = true
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 200 {
		t.Errorf("GET /readyz when ready = %d", rec.Code)
	}
}
