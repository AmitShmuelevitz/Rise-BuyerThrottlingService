package throttler

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/memstore"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

var t0 = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

func commit(t *testing.T, s *memstore.Store, w window.ID, states ...policy.State) {
	t.Helper()
	recs := make([]store.Record, len(states))
	for i, st := range states {
		recs[i] = store.Record{State: st, Window: w}
	}
	if _, _, err := s.CommitState(context.Background(), w, recs); err != nil {
		t.Fatal(err)
	}
}

func newTestThrottler(t *testing.T, opts Options) (*Throttler, *memstore.Store, *clock.Fake) {
	t.Helper()
	c := clock.NewFake(t0)
	s := memstore.New(c)
	if opts.Window == 0 {
		opts.Window = time.Minute
	}
	opts.Clock = c
	return New(s, s, opts), s, c
}

func TestShouldCall_BeforeStateLoadsEveryBuyerIsCalled(t *testing.T) {
	th, _, _ := newTestThrottler(t, Options{Rand: func() float64 { return 0 }})
	if th.Ready() {
		t.Fatal("Ready before any load")
	}
	for range 100 {
		if !th.ShouldCall("any") {
			t.Fatal("ShouldCall = false before state was loaded; want fail open")
		}
	}
}

func TestShouldCall_FollowsRate(t *testing.T) {
	th, s, _ := newTestThrottler(t, Options{})
	commit(t, s, 60,
		policy.State{BuyerID: "open"},
		policy.State{BuyerID: "half", ThrottleRate: 0.5, Reason: policy.ReasonRecovering},
		policy.State{BuyerID: "blocked", ThrottleRate: 0.95, Reason: policy.ReasonFailureRate},
	)
	if err := th.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		buyer string
		draw  float64
		want  bool
	}{
		{"open", 0, true},
		{"unknown", 0, true},
		{"half", 0.49, false},
		{"half", 0.5, true},
		{"blocked", 0.94, false},
		{"blocked", 0.95, true},
		{"blocked", 0.999, true},
	}
	for _, tt := range tests {
		th.opts.Rand = func() float64 { return tt.draw }
		if got := th.ShouldCall(tt.buyer); got != tt.want {
			t.Errorf("ShouldCall(%q) with draw %v = %v, want %v", tt.buyer, tt.draw, got, tt.want)
		}
	}
}

func TestShouldCall_OpenBuyerDoesNotDrawRandom(t *testing.T) {
	th, _, _ := newTestThrottler(t, Options{Rand: func() float64 { panic("random drawn for an open buyer") }})
	th.ShouldCall("open")
}

func TestShouldCall_SkipRatioMatchesRate(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	th, s, _ := newTestThrottler(t, Options{Rand: rng.Float64})
	commit(t, s, 60, policy.State{BuyerID: "b", ThrottleRate: 0.95})
	if err := th.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	const n = 200_000
	called := 0
	for range n {
		if th.ShouldCall("b") {
			called++
		}
	}
	// Binomial(n, 0.05): stddev ~ 97. Allow 5 sigma.
	if got, want := float64(called), 0.05*n; math.Abs(got-want) > 5*math.Sqrt(n*0.05*0.95) {
		t.Fatalf("called %d of %d at rate 0.95, want about %.0f", called, n, want)
	}
}

func TestRecordAndFlush_SumsConcurrentRecords(t *testing.T) {
	th, s, c := newTestThrottler(t, Options{})
	c.Set(t0.Add(10 * time.Second))

	var wg sync.WaitGroup
	for g := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				switch {
				case i%10 == 0:
					th.Record("b", ResultTimeout)
				case i%4 == 0:
					th.Record("b", ResultFailed)
				default:
					th.Record("b", ResultOK)
				}
				if g == 0 && i%100 == 0 {
					_ = th.Flush(context.Background()) // flush concurrently with records
				}
			}
		}()
	}
	wg.Wait()
	if err := th.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	got, _ := s.WindowStats(context.Background(), window.Of(t0, time.Minute))
	want := policy.WindowStats{Total: 64_000, Timeouts: 64 * 100, Failed: 64 * 200} // multiples of 4 that are not multiples of 10
	if got["b"] != want {
		t.Fatalf("stats = %+v, want %+v", got["b"], want)
	}
}

func TestRecord_UnknownResultCountsAsFailure(t *testing.T) {
	th, s, _ := newTestThrottler(t, Options{})
	th.Record("b", Result(42))
	if err := th.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.WindowStats(context.Background(), window.Of(t0, time.Minute))
	if got["b"] != (policy.WindowStats{Total: 1, Failed: 1}) {
		t.Fatalf("stats = %+v", got["b"])
	}
}

func TestFlush_BucketsByWindow(t *testing.T) {
	th, s, c := newTestThrottler(t, Options{Window: 10 * time.Second})
	ctx := context.Background()

	c.Set(t0.Add(time.Second))
	th.Record("b", ResultOK)
	_ = th.Flush(ctx) // interval [t0, t0+1s] -> window t0

	c.Set(t0.Add(11 * time.Second))
	th.Record("b", ResultOK)
	th.Record("b", ResultOK)
	_ = th.Flush(ctx) // interval [t0+1s, t0+11s], midpoint t0+6s -> window t0

	c.Set(t0.Add(12 * time.Second))
	th.Record("b", ResultFailed)
	_ = th.Flush(ctx) // interval [t0+11s, t0+12s] -> window t0+10s

	first, _ := s.WindowStats(ctx, window.Of(t0, 10*time.Second))
	second, _ := s.WindowStats(ctx, window.Of(t0.Add(10*time.Second), 10*time.Second))
	if first["b"].Total != 3 || second["b"] != (policy.WindowStats{Total: 1, Failed: 1}) {
		t.Fatalf("first = %+v, second = %+v", first["b"], second["b"])
	}
}

// failingSink fails while down is set, and otherwise forwards to next.
type failingSink struct {
	mu   sync.Mutex
	down bool
	next store.StatsSink
}

func (f *failingSink) AddStats(ctx context.Context, w window.ID, d map[string]policy.WindowStats) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errors.New("store down")
	}
	return f.next.AddStats(ctx, w, d)
}

func TestFlush_RetriesAfterStoreFailure(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	sink := &failingSink{down: true, next: s}
	th := New(s, sink, Options{Clock: c})
	ctx := context.Background()

	th.Record("b", ResultOK)
	c.Advance(time.Second)
	if err := th.Flush(ctx); err == nil {
		t.Fatal("expected flush error while store is down")
	}
	th.Record("b", ResultFailed)
	c.Advance(time.Second)
	_ = th.Flush(ctx)

	sink.down = false
	c.Advance(time.Second)
	if err := th.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := s.WindowStats(ctx, window.Of(t0, time.Minute))
	if got["b"] != (policy.WindowStats{Total: 2, Failed: 1}) {
		t.Fatalf("stats after recovery = %+v, want both calls", got["b"])
	}
}

type recordingMetrics struct {
	nopMetrics
	mu      sync.Mutex
	dropped int64
	skipped map[string]int64
	calls   map[string][3]int64
}

func (m *recordingMetrics) AddDroppedStats(n int64) { m.mu.Lock(); m.dropped += n; m.mu.Unlock() }
func (m *recordingMetrics) AddSkipped(b string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.skipped == nil {
		m.skipped = map[string]int64{}
	}
	m.skipped[b] += n
}
func (m *recordingMetrics) AddCalls(b string, ok, failed, timeouts int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls == nil {
		m.calls = map[string][3]int64{}
	}
	v := m.calls[b]
	m.calls[b] = [3]int64{v[0] + ok, v[1] + failed, v[2] + timeouts}
}

func TestFlush_DropsStatsOlderThanMaxPendingAge(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	sink := &failingSink{down: true, next: s}
	m := &recordingMetrics{}
	th := New(s, sink, Options{Clock: c, Window: 10 * time.Second, MaxPendingAge: 10 * time.Second, Metrics: m})
	ctx := context.Background()

	th.Record("b", ResultOK)
	th.Record("b", ResultOK)
	c.Advance(time.Second)
	_ = th.Flush(ctx) // window t0 pending

	sink.down = false
	c.Set(t0.Add(21 * time.Second)) // window t0 ended 11s ago > 10s
	if err := th.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := s.WindowStats(ctx, window.Of(t0, 10*time.Second))
	if len(got) != 0 {
		t.Fatalf("stale stats were written: %+v", got)
	}
	if m.dropped != 2 {
		t.Fatalf("dropped = %d, want 2", m.dropped)
	}
}

func TestMetrics_ReportedOnFlush(t *testing.T) {
	m := &recordingMetrics{}
	th, s, _ := newTestThrottler(t, Options{Metrics: m, Rand: func() float64 { return 0 }})
	commit(t, s, 60, policy.State{BuyerID: "b", ThrottleRate: 0.95})
	_ = th.Sync(context.Background())

	th.ShouldCall("b")
	th.ShouldCall("b")
	th.Record("b", ResultOK)
	th.Record("b", ResultTimeout)
	if err := th.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.skipped["b"] != 2 || m.calls["b"] != [3]int64{1, 0, 1} {
		t.Fatalf("skipped=%v calls=%v", m.skipped, m.calls)
	}
}

func TestSync_KeepsLastStateWhenStoreFails(t *testing.T) {
	c := clock.NewFake(t0)
	s := memstore.New(c)
	reader := &flakyReader{StateReader: s}
	th := New(reader, s, Options{Clock: c})
	commit(t, s, 60, policy.State{BuyerID: "b", ThrottleRate: 0.95})
	if err := th.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	reader.fail = true
	if err := th.Sync(context.Background()); err == nil {
		t.Fatal("expected sync error")
	}
	if th.Rate("b") != 0.95 || !th.Ready() {
		t.Fatalf("rate = %v ready = %v after failed sync; want last known state kept", th.Rate("b"), th.Ready())
	}
}

func TestSync_PicksUpNewVersion(t *testing.T) {
	th, s, _ := newTestThrottler(t, Options{})
	ctx := context.Background()
	commit(t, s, 60, policy.State{BuyerID: "b", ThrottleRate: 0.95})
	_ = th.Sync(ctx)
	commit(t, s, 120, policy.State{BuyerID: "b", ThrottleRate: 0.85, Reason: policy.ReasonRecovering})
	_ = th.Sync(ctx)

	recs, v, _ := th.States()
	if th.Rate("b") != 0.85 || v != 2 || recs["b"].Reason != policy.ReasonRecovering {
		t.Fatalf("rate=%v version=%d record=%+v", th.Rate("b"), v, recs["b"])
	}
}

type flakyReader struct {
	store.StateReader
	fail bool
}

func (f *flakyReader) LoadState(ctx context.Context) (store.Snapshot, error) {
	if f.fail {
		return store.Snapshot{}, errors.New("store down")
	}
	return f.StateReader.LoadState(ctx)
}

func TestRun_SyncsOnNotificationAndFlushesOnShutdown(t *testing.T) {
	s := memstore.New(nil)
	// Long poll interval: the update can only arrive via pub/sub.
	th := New(s, s, Options{SyncInterval: time.Hour, FlushInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- th.Run(ctx) }()

	waitFor(t, th.Ready)
	commit(t, s, window.Of(time.Now(), time.Minute), policy.State{BuyerID: "b", ThrottleRate: 0.95})
	waitFor(t, func() bool { return th.Rate("b") == 0.95 })

	th.Record("b", ResultOK)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// The run may straddle a window boundary, so look at both candidates.
	cur := window.Of(time.Now(), time.Minute)
	a, _ := s.WindowStats(context.Background(), cur)
	b, _ := s.WindowStats(context.Background(), cur.Prev(time.Minute))
	if a["b"].Total+b["b"].Total != 1 {
		t.Fatalf("final flush did not run: %+v %+v", a, b)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestResultString(t *testing.T) {
	for r, want := range map[Result]string{ResultOK: "ok", ResultFailed: "failed", ResultTimeout: "timeout", 9: "unknown"} {
		if r.String() != want {
			t.Errorf("%d.String() = %q, want %q", r, r.String(), want)
		}
	}
}
