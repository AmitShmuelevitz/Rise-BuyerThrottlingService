package evaluator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/memstore"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

const size = time.Minute

var t0 = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC) // a window boundary

func newService(t *testing.T, s *memstore.Store, c *clock.Fake, owner string) *Service {
	t.Helper()
	svc, err := New(s, s, s, Options{Policy: policy.DefaultConfig(), Owner: owner, Clock: c})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func addStats(t *testing.T, s *memstore.Store, w window.ID, buyer string, st policy.WindowStats) {
	t.Helper()
	if err := s.AddStats(context.Background(), w, map[string]policy.WindowStats{buyer: st}); err != nil {
		t.Fatal(err)
	}
}

func state(t *testing.T, s *memstore.Store, buyer string) store.Record {
	t.Helper()
	snap, err := s.LoadState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snap.States[buyer]
}

func TestNew_Validates(t *testing.T) {
	s := memstore.New(nil)
	bad := policy.DefaultConfig()
	bad.BlockLevel = 1
	tests := []struct {
		name string
		opts Options
		want string
	}{
		{"invalid policy", Options{Policy: bad, Owner: "e"}, "block level"},
		{"missing owner", Options{Policy: policy.DefaultConfig()}, "owner"},
		{"grace not shorter than window", Options{Policy: policy.DefaultConfig(), Owner: "e", Grace: time.Minute}, "grace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(s, s, s, tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() error = %v, want mentioning %q", err, tt.want)
			}
		})
	}
}

func TestDueWindow_WaitsForGrace(t *testing.T) {
	c := clock.NewFake(t0)
	svc := newService(t, memstore.New(c), c, "e1")
	prev := window.Of(t0, size).Prev(size)

	tests := []struct {
		at   time.Time
		want window.ID
	}{
		{t0.Add(4 * time.Second), prev.Prev(size)}, // window [t0-1m, t0) closed but still in grace
		{t0.Add(5 * time.Second), prev},            // grace over: due
		{t0.Add(59 * time.Second), prev},
	}
	for _, tt := range tests {
		if got := svc.DueWindow(tt.at); got != tt.want {
			t.Errorf("DueWindow(%v) = %v, want %v", tt.at, got.Start(), tt.want.Start())
		}
	}
}

func TestTick_BlocksAndRecoversAcrossWindows(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	svc := newService(t, s, c, "e1")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 900})
	addStats(t, s, w, "good", policy.WindowStats{Total: 1000, Failed: 5})

	c.Set(w.End(size).Add(6 * time.Second))
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if r := state(t, s, "bad"); r.ThrottleRate != 0.95 || r.Reason != policy.ReasonFailureRate || r.Window != w {
		t.Fatalf("bad after outage = %+v", r)
	}
	if r := state(t, s, "good"); r.ThrottleRate != 0 {
		t.Fatalf("good = %+v", r)
	}

	// Ten clean windows bring it back to 0.
	for i := range 10 {
		w = w.Next(size)
		addStats(t, s, w, "bad", policy.WindowStats{Total: 200})
		c.Set(w.End(size).Add(6 * time.Second))
		if err := svc.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		want := []float64{0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05, 0}[i]
		if got := state(t, s, "bad").ThrottleRate; got != want {
			t.Fatalf("window %d: rate = %v, want %v", i+1, got, want)
		}
	}
}

func TestTick_SilentBuyerKeepsState(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	svc := newService(t, s, c, "e1")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 1000})
	c.Set(w.End(size).Add(6 * time.Second))
	_ = svc.Tick(ctx)

	// Next window: no traffic at all for "bad".
	c.Set(w.Next(size).End(size).Add(6 * time.Second))
	if err := svc.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	r := state(t, s, "bad")
	if r.ThrottleRate != 0.95 || r.Window != w.Next(size) || r.UnderSampleStreak != 1 {
		t.Fatalf("silent buyer = %+v, want held at 0.95 and evaluated for the new window", r)
	}
}

func TestTick_EvaluatesEachWindowOnce(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	svc := newService(t, s, c, "e1")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 1000})
	addStats(t, s, w.Next(size), "bad", policy.WindowStats{Total: 1000})
	c.Set(w.End(size).Add(6 * time.Second))

	for range 5 {
		if err := svc.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	snap, _ := s.LoadState(ctx)
	if snap.Version != 1 {
		t.Fatalf("version = %d after repeated ticks in one window, want 1", snap.Version)
	}
}

func TestTick_OnlyLeaderEvaluates(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	e1, e2 := newService(t, s, c, "e1"), newService(t, s, c, "e2")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 1000})
	c.Set(w.End(size).Add(6 * time.Second))

	_ = e1.Tick(ctx)
	_ = e2.Tick(ctx)
	if !e1.leader || e2.leader {
		t.Fatalf("leaders: e1=%v e2=%v, want only e1", e1.leader, e2.leader)
	}

	// e1 dies (stops ticking); after the lease TTL e2 takes over and
	// continues with the next window, without re-applying the first one.
	w2 := w.Next(size)
	addStats(t, s, w2, "bad", policy.WindowStats{Total: 1000})
	c.Set(w2.End(size).Add(6 * time.Second)) // > lease TTL later
	if err := e2.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if !e2.leader {
		t.Fatal("e2 did not take over an expired lease")
	}
	snap, _ := s.LoadState(ctx)
	if snap.Version != 2 || snap.States["bad"].ThrottleRate != 0.85 {
		t.Fatalf("after failover: version=%d bad=%+v", snap.Version, snap.States["bad"])
	}
}

func TestEvaluateWindow_ConcurrentEvaluatorsCommitOnce(t *testing.T) {
	// Simulates split brain: both replicas believe they lead and evaluate the
	// same window. The store accepts only the first commit.
	c := clock.NewFake(t0)
	s := memstore.New(c)
	e1, e2 := newService(t, s, c, "e1"), newService(t, s, c, "e2")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 1000})

	r1, err1 := e1.EvaluateWindow(ctx, w)
	r2, err2 := e2.EvaluateWindow(ctx, w)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if !r1.Applied || r2.Applied {
		t.Fatalf("applied: e1=%v e2=%v, want only the first", r1.Applied, r2.Applied)
	}
}

func TestEvaluateWindow_UpdatedAtOnlyMovesOnChange(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	svc := newService(t, s, c, "e1")
	ctx := context.Background()

	w := window.Of(t0, size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 1000, Failed: 1000})
	c.Set(w.End(size).Add(6 * time.Second))
	_ = svc.Tick(ctx)
	blockedAt := state(t, s, "bad").UpdatedAt

	w = w.Next(size)
	addStats(t, s, w, "bad", policy.WindowStats{Total: 10}) // under sample: held
	c.Set(w.End(size).Add(6 * time.Second))
	_ = svc.Tick(ctx)
	if got := state(t, s, "bad").UpdatedAt; !got.Equal(blockedAt) {
		t.Fatalf("updated_at moved on a held window: %v -> %v", blockedAt, got)
	}
}

type failingStats struct{ store.StatsSource }

func (failingStats) WindowStats(context.Context, window.ID) (map[string]policy.WindowStats, error) {
	return nil, errors.New("redis down")
}

func TestTick_StoreErrorLeavesStateAndRetries(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	svc, err := New(failingStats{}, s, s, Options{Policy: policy.DefaultConfig(), Owner: "e1", Clock: c})
	if err != nil {
		t.Fatal(err)
	}
	c.Set(t0.Add(size + 6*time.Second))
	if err := svc.Tick(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if svc.lastDone != 0 {
		t.Fatal("failed window marked as done; it would never be retried")
	}
}

func TestRun_ReleasesLeaseOnShutdown(t *testing.T) {
	s := memstore.New(nil)
	svc, err := New(s, s, s, Options{Policy: policy.DefaultConfig(), Owner: "e1", TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.TryAcquire(context.Background(), "e2", time.Minute); !ok {
		t.Fatal("lease still held after shutdown")
	}
}
