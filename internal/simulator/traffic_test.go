package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amit/buyer-throttling/internal/auction"
)

func TestTraffic_SendsAtRateAndReports(t *testing.T) {
	var auctions atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req auction.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Buyers) == 0 {
			t.Errorf("bad auction request: %v %+v", err, req)
		}
		auctions.Add(1)
		_ = json.NewEncoder(w).Encode(auction.Result{Called: []string{"a"}, Skipped: []string{"b"}})
	}))
	defer srv.Close()

	var logs bytes.Buffer
	share := 1.0
	tr := &Traffic{
		Targets:     []string{srv.URL},
		RPS:         200,
		Buyers:      []BuyerSpec{{ID: "a", Share: &share}, {ID: "b"}},
		Client:      srv.Client(),
		Logger:      slog.New(slog.NewJSONHandler(&logs, nil)),
		ReportEvery: 250 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	tr.Run(ctx)

	// 200 rps for 0.6s is ~120 auctions; allow scheduler slack.
	if n := auctions.Load(); n < 80 || n > 140 {
		t.Fatalf("sent %d auctions, want about 120", n)
	}
	if out := logs.String(); !strings.Contains(out, `"traffic summary"`) || !strings.Contains(out, `"skipped_ratio":1`) {
		t.Fatalf("missing or wrong traffic summary:\n%s", out)
	}
}
