// Package storetest is a conformance suite every store.Store implementation
// must pass, so the in-memory store used in tests behaves like Redis.
package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/amit/buyer-throttling/internal/policy"
	"github.com/amit/buyer-throttling/internal/store"
	"github.com/amit/buyer-throttling/internal/window"
)

// Factory returns a fresh, empty store. advance moves the store's notion of
// time forward (used for lease expiry); it may be nil if the store uses real
// time, in which case lease expiry is tested with a real sleep.
type Factory func(t *testing.T) (s store.Store, advance func(time.Duration))

// Run runs the suite.
func Run(t *testing.T, newStore Factory) {
	t.Run("stats are summed across writers", func(t *testing.T) { testStatsSum(t, newStore) })
	t.Run("stats are isolated per window", func(t *testing.T) { testStatsWindows(t, newStore) })
	t.Run("buyer ids with colons survive", func(t *testing.T) { testBuyerIDs(t, newStore) })
	t.Run("empty state", func(t *testing.T) { testEmptyState(t, newStore) })
	t.Run("commit then load", func(t *testing.T) { testCommitLoad(t, newStore) })
	t.Run("commit is idempotent per window", func(t *testing.T) { testCommitIdempotent(t, newStore) })
	t.Run("commit notifies subscribers", func(t *testing.T) { testSubscribe(t, newStore) })
	t.Run("lease", func(t *testing.T) { testLease(t, newStore) })
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func testStatsSum(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	const w = window.ID(600)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				err := s.AddStats(c, w, map[string]policy.WindowStats{
					"a": {Total: 4, Failed: 2, Timeouts: 1},
					"b": {Total: 1},
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := s.WindowStats(c, w)
	if err != nil {
		t.Fatal(err)
	}
	if want := (policy.WindowStats{Total: 800, Failed: 400, Timeouts: 200}); got["a"] != want {
		t.Errorf("a = %+v, want %+v", got["a"], want)
	}
	if want := (policy.WindowStats{Total: 200}); got["b"] != want {
		t.Errorf("b = %+v, want %+v", got["b"], want)
	}
}

func testStatsWindows(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	must(t, s.AddStats(c, 60, map[string]policy.WindowStats{"a": {Total: 1}}))
	must(t, s.AddStats(c, 120, map[string]policy.WindowStats{"a": {Total: 5}}))

	got, err := s.WindowStats(c, 60)
	must(t, err)
	if got["a"].Total != 1 {
		t.Errorf("window 60 total = %d, want 1", got["a"].Total)
	}
	empty, err := s.WindowStats(c, 180)
	must(t, err)
	if len(empty) != 0 {
		t.Errorf("unknown window = %v, want empty", empty)
	}
}

func testBuyerIDs(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	must(t, s.AddStats(c, 60, map[string]policy.WindowStats{"ssp:buyer:42": {Total: 3, Failed: 1}}))
	got, err := s.WindowStats(c, 60)
	must(t, err)
	if want := (policy.WindowStats{Total: 3, Failed: 1}); got["ssp:buyer:42"] != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func testEmptyState(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	snap, err := s.LoadState(ctx(t))
	must(t, err)
	if snap.Version != 0 || len(snap.States) != 0 {
		t.Fatalf("empty store snapshot = %+v", snap)
	}
}

func testCommitLoad(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	at := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	recs := []store.Record{
		{State: policy.State{BuyerID: "a", ThrottleRate: 0.95, Reason: policy.ReasonFailureRate}, Window: 60, UpdatedAt: at},
		{State: policy.State{BuyerID: "b", ThrottleRate: 0, UnderSampleStreak: 2}, Window: 60, UpdatedAt: at},
	}
	applied, v, err := s.CommitState(c, 60, recs)
	must(t, err)
	if !applied || v != 1 {
		t.Fatalf("commit = (%v, %d), want (true, 1)", applied, v)
	}

	snap, err := s.LoadState(c)
	must(t, err)
	if snap.Version != 1 || snap.LastWindow != 60 {
		t.Fatalf("snapshot version/window = %d/%d, want 1/60", snap.Version, snap.LastWindow)
	}
	for _, r := range recs {
		got := snap.States[r.BuyerID]
		if got.State != r.State || got.Window != r.Window || !got.UpdatedAt.Equal(r.UpdatedAt) {
			t.Errorf("buyer %s = %+v, want %+v", r.BuyerID, got, r)
		}
	}

	// A later commit updates only the buyers it carries.
	_, v, err = s.CommitState(c, 120, []store.Record{{State: policy.State{BuyerID: "a", ThrottleRate: 0.85, Reason: policy.ReasonRecovering}, Window: 120}})
	must(t, err)
	snap, err = s.LoadState(c)
	must(t, err)
	if v != 2 || snap.Version != 2 || snap.States["a"].ThrottleRate != 0.85 || snap.States["b"].Window != 60 {
		t.Fatalf("after second commit: v=%d snapshot=%+v", v, snap)
	}
}

func testCommitIdempotent(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	rec := func(rate float64) []store.Record {
		return []store.Record{{State: policy.State{BuyerID: "a", ThrottleRate: rate}}}
	}
	_, _, err := s.CommitState(c, 120, rec(0.95))
	must(t, err)

	for _, w := range []window.ID{120, 60} {
		applied, v, err := s.CommitState(c, w, rec(0.1))
		must(t, err)
		if applied || v != 1 {
			t.Fatalf("re-commit of window %d = (%v, %d), want (false, 1)", w, applied, v)
		}
	}
	snap, err := s.LoadState(c)
	must(t, err)
	if snap.States["a"].ThrottleRate != 0.95 {
		t.Fatalf("stale commit overwrote state: %+v", snap.States["a"])
	}
}

func testSubscribe(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	c := ctx(t)
	subCtx, cancel := context.WithCancel(c)
	updates, err := s.SubscribeUpdates(subCtx)
	must(t, err)

	_, _, err = s.CommitState(c, 60, []store.Record{{State: policy.State{BuyerID: "a"}}})
	must(t, err)

	select {
	case v := <-updates:
		if v != 1 {
			t.Fatalf("update version = %d, want 1", v)
		}
	case <-c.Done():
		t.Fatal("no update received")
	}

	cancel()
	select {
	case _, ok := <-updates:
		for ok {
			_, ok = <-updates
		}
	case <-c.Done():
		t.Fatal("channel not closed after cancel")
	}
}

func testLease(t *testing.T, newStore Factory) {
	s, advance := newStore(t)
	c := ctx(t)
	ttl := 200 * time.Millisecond

	ok, err := s.TryAcquire(c, "e1", ttl)
	must(t, err)
	if !ok {
		t.Fatal("e1 could not take a free lease")
	}
	if ok, _ := s.TryAcquire(c, "e2", ttl); ok {
		t.Fatal("e2 took a held lease")
	}
	if ok, _ := s.TryAcquire(c, "e1", ttl); !ok {
		t.Fatal("e1 could not renew its own lease")
	}

	must(t, s.Release(c, "e2")) // not the owner: no effect
	if ok, _ := s.TryAcquire(c, "e2", ttl); ok {
		t.Fatal("release by non-owner freed the lease")
	}

	must(t, s.Release(c, "e1"))
	if ok, _ := s.TryAcquire(c, "e2", ttl); !ok {
		t.Fatal("e2 could not take a released lease")
	}

	// Expiry: e2 stops renewing, e1 takes over.
	if advance != nil {
		advance(ttl + time.Millisecond)
	} else {
		time.Sleep(ttl + 50*time.Millisecond)
	}
	if ok, _ := s.TryAcquire(c, "e1", ttl); !ok {
		t.Fatal("e1 could not take an expired lease")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
