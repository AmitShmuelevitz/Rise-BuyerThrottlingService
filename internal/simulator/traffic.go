package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/auction"
)

// Traffic sends ad requests to the auction pods at a fixed rate, picking the
// eligible buyers for each request by their share, and logs a periodic
// summary of how many calls each buyer got versus how many were throttled.
type Traffic struct {
	Targets     []string // auction pod base URLs, used round robin
	RPS         int
	Buyers      []BuyerSpec
	Client      *http.Client
	Logger      *slog.Logger
	MaxInFlight int           // default 2000
	ReportEvery time.Duration // default 10s

	next     atomic.Uint64
	sent     atomic.Int64
	failures atomic.Int64

	mu      sync.Mutex
	called  map[string]int64
	skipped map[string]int64
}

// Run sends traffic until ctx is done.
func (t *Traffic) Run(ctx context.Context) {
	if t.MaxInFlight <= 0 {
		t.MaxInFlight = 2000
	}
	if t.ReportEvery <= 0 {
		t.ReportEvery = 10 * time.Second
	}
	t.called, t.skipped = map[string]int64{}, map[string]int64{}

	// Send in small batches every 10ms; carry the fractional remainder so the
	// long-run rate matches RPS exactly.
	const tick = 10 * time.Millisecond
	perTick := float64(t.RPS) * tick.Seconds()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	report := time.NewTicker(t.ReportEvery)
	defer report.Stop()

	sem := make(chan struct{}, t.MaxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()
	carry := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case <-report.C:
			t.report()
		case <-ticker.C:
			carry += perTick
			n := int(carry)
			carry -= float64(n)
			for range n {
				select {
				case sem <- struct{}{}:
				default:
					t.failures.Add(1) // pods are saturated; shed instead of queueing
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					t.send(ctx)
				}()
			}
		}
	}
}

func (t *Traffic) pickBuyers() []string {
	buyers := make([]string, 0, len(t.Buyers))
	for _, b := range t.Buyers {
		if rand.Float64() < b.ShareOrDefault() {
			buyers = append(buyers, b.ID)
		}
	}
	return buyers
}

func (t *Traffic) send(ctx context.Context) {
	buyers := t.pickBuyers()
	if len(buyers) == 0 {
		return
	}
	body, _ := json.Marshal(auction.Request{Buyers: buyers})
	target := t.Targets[t.next.Add(1)%uint64(len(t.Targets))]

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/auction", bytes.NewReader(body))
	if err != nil {
		t.failures.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		t.failures.Add(1)
		return
	}
	defer resp.Body.Close()
	var res auction.Result
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&res) != nil {
		t.failures.Add(1)
		return
	}
	t.sent.Add(1)
	t.mu.Lock()
	for _, b := range res.Called {
		t.called[b]++
	}
	for _, b := range res.Skipped {
		t.skipped[b]++
	}
	t.mu.Unlock()
}

// report logs, per buyer, the share of eligible requests that were throttled
// since the last report. This is the traffic-side view of the throttle rate.
func (t *Traffic) report() {
	t.mu.Lock()
	called, skipped := t.called, t.skipped
	t.called, t.skipped = map[string]int64{}, map[string]int64{}
	t.mu.Unlock()

	ids := make([]string, 0, len(t.Buyers))
	for _, b := range t.Buyers {
		ids = append(ids, b.ID)
	}
	sort.Strings(ids)
	group := make([]any, 0, len(ids))
	for _, id := range ids {
		c, s := called[id], skipped[id]
		ratio := 0.0
		if c+s > 0 {
			ratio = float64(s) / float64(c+s)
		}
		group = append(group, slog.Group(id, "called", c, "skipped", s, "skipped_ratio", float64(int(ratio*1000))/1000))
	}
	t.Logger.Info("traffic summary",
		append([]any{"auctions_ok", t.sent.Swap(0), "auctions_failed", t.failures.Swap(0)}, group...)...)
}
