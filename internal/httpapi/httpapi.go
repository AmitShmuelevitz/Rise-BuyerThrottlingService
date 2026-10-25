// Package httpapi serves the operational HTTP endpoints shared by the
// evaluator and the auction pods.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
)

// StateFunc returns the throttle state to serve.
type StateFunc func(ctx context.Context) (records map[string]store.Record, version uint64, err error)

// BuyerState is one entry of the GET /state response.
type BuyerState struct {
	BuyerID      string    `json:"buyer_id"`
	ThrottleRate float64   `json:"throttle_rate"`
	Reason       string    `json:"reason"`
	Window       int64     `json:"window,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitzero"`
}

// StateResponse is the GET /state response body.
type StateResponse struct {
	Version uint64       `json:"version"`
	Buyers  []BuyerState `json:"buyers"`
}

// StateHandler serves GET /state: every buyer's throttle state, sorted by id.
func StateHandler(get StateFunc, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		records, version, err := get(r.Context())
		if err != nil {
			logger.Error("serve /state", "err", err)
			http.Error(w, "state unavailable", http.StatusServiceUnavailable)
			return
		}
		resp := StateResponse{Version: version, Buyers: make([]BuyerState, 0, len(records))}
		for _, rec := range records {
			resp.Buyers = append(resp.Buyers, BuyerState{
				BuyerID:      rec.BuyerID,
				ThrottleRate: rec.ThrottleRate,
				Reason:       string(rec.Reason),
				Window:       int64(rec.Window),
				UpdatedAt:    rec.UpdatedAt,
			})
		}
		slices.SortFunc(resp.Buyers, func(a, b BuyerState) int { return strings.Compare(a.BuyerID, b.BuyerID) })
		writeJSON(w, http.StatusOK, resp)
	})
}

// Ops registers /state, /healthz, /readyz and /metrics on mux.
func Ops(mux *http.ServeMux, state StateFunc, ready func() bool, gatherer prometheus.Gatherer, logger *slog.Logger) {
	mux.Handle("GET /state", StateHandler(state, logger))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil && !ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
