package e2e

// Tests in this file run the real background loops (pod syncers and
// flushers, evaluator ticks and leases) in real time against Redis: a real
// server when REDIS_ADDR is set (CI does this), miniredis otherwise. They take
// a few seconds each and are skipped with -short. With REDIS_ADDR they flush
// that Redis, so run them with -p 1 next to the redisstore package.

import (
	"context"
	"errors"
	"math"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/evaluator"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/redisstore"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/throttler"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

// liveRedis returns the address of an empty Redis.
func liveRedis(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("real-time test skipped with -short")
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		defer rdb.Close()
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flush redis at %s: %v", addr, err)
		}
		t.Logf("using real Redis at %s", addr)
		return addr
	}
	return realTimeMiniredis(t).Addr()
}

// realTimeMiniredis returns a miniredis whose key TTLs expire with wall-clock
// time (miniredis only expires keys when told to).
func realTimeMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr := miniredis.RunT(t)
	const step = 20 * time.Millisecond
	runInBackground(t, func(ctx context.Context) {
		tick := time.NewTicker(step)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				mr.FastForward(step)
			}
		}
	})
	return mr
}

func newRedisStore(t *testing.T, addr string) *redisstore.Store {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	return redisstore.New(rdb, redisstore.Options{})
}

// runInBackground runs fn until the test ends and waits for it to return.
func runInBackground(t *testing.T, fn func(ctx context.Context)) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); fn(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// noPubSub hides pub/sub so a pod can only learn about new state by polling.
type noPubSub struct{ store.StateReader }

func (noPubSub) SubscribeUpdates(context.Context) (<-chan uint64, error) {
	return nil, errors.New("pub/sub disabled for this test")
}

// TestPodsConvergeOnEveryCommit commits a series of state versions and
// measures, for each one, how long until every pod serves it. With pub/sub the
// bound is a few milliseconds; with polling only it must still be under two
// sync intervals.
func TestPodsConvergeOnEveryCommit(t *testing.T) {
	const syncInterval = 250 * time.Millisecond
	tests := []struct {
		name  string
		wrap  func(store.StateReader) store.StateReader
		bound time.Duration
	}{
		{"pub/sub and polling", func(r store.StateReader) store.StateReader { return r }, syncInterval},
		{"polling only", func(r store.StateReader) store.StateReader { return noPubSub{r} }, 2 * syncInterval},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := liveRedis(t)
			pods := make([]*throttler.Throttler, 3)
			for i := range pods {
				st := newRedisStore(t, addr)
				pods[i] = throttler.New(tt.wrap(st), st, throttler.Options{SyncInterval: syncInterval, FlushInterval: time.Hour})
				p := pods[i]
				runInBackground(t, func(ctx context.Context) { _ = p.Run(ctx) })
			}
			eventually(t, 2*time.Second, "pods to load initial state", func() bool {
				return pods[0].Ready() && pods[1].Ready() && pods[2].Ready()
			})

			writer := newRedisStore(t, addr)
			var lags []time.Duration
			for i := 1; i <= 10; i++ {
				rate := []float64{0.95, 0.85, 0.75}[i%3]
				committed := time.Now()
				_, version, err := writer.CommitState(context.Background(), window.ID(i*60), []store.Record{
					{State: policy.State{BuyerID: "b", ThrottleRate: rate}},
				})
				if err != nil {
					t.Fatal(err)
				}
				eventually(t, 4*syncInterval, "all pods to serve the new version", func() bool {
					for _, p := range pods {
						if _, v, _ := p.States(); v < version {
							return false
						}
					}
					return true
				})
				lags = append(lags, time.Since(committed))
				for j, p := range pods {
					if got := p.Rate("b"); got != rate {
						t.Fatalf("commit %d: pod %d serves rate %v, want %v", i, j, got, rate)
					}
				}
				// Commit at varying points of the poll cycle.
				time.Sleep(time.Duration(i*37%250) * time.Millisecond)
			}

			slices.Sort(lags)
			worst := lags[len(lags)-1]
			t.Logf("time until all 3 pods served a new version: median %v, worst %v (bound %v)", lags[len(lags)/2], worst, tt.bound)
			if worst > tt.bound {
				t.Fatalf("worst convergence %v exceeds %v", worst, tt.bound)
			}
		})
	}
}

// crashLeaser models a process that dies: it can take and renew the lease but
// never releases it, so the standby must wait for the lease to expire.
type crashLeaser struct{ store.Leaser }

func (crashLeaser) Release(context.Context, string) error { return nil }

// commitLog records every applied commit, across all evaluators.
type commitLog struct {
	store.StateWriter
	mu      sync.Mutex
	windows []window.ID
	rates   []float64
}

func (c *commitLog) CommitState(ctx context.Context, w window.ID, recs []store.Record) (bool, uint64, error) {
	applied, v, err := c.StateWriter.CommitState(ctx, w, recs)
	if applied {
		c.mu.Lock()
		c.windows = append(c.windows, w)
		for _, r := range recs {
			if r.BuyerID == "bad" {
				c.rates = append(c.rates, r.ThrottleRate)
			}
		}
		c.mu.Unlock()
	}
	return applied, v, err
}

func (c *commitLog) snapshot() ([]window.ID, []float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.windows), slices.Clone(c.rates)
}

// TestEvaluatorFailoverAfterLeaderCrash runs two evaluators on one-second
// windows while a bad buyer recovers, crashes the leader without releasing its
// lease, and checks that the standby takes over within the lease TTL and
// continues the recovery from where it was: no window applied twice, no
// window going backwards, and every committed rate exactly one step below the
// previous one.
func TestEvaluatorFailoverAfterLeaderCrash(t *testing.T) {
	const (
		size     = time.Second
		leaseTTL = 1500 * time.Millisecond
	)
	addr := liveRedis(t)
	pol := policy.DefaultConfig()
	pol.Window = size

	// Traffic: the buyer fails until the end of the window after the current
	// one (so at least one full window fails), then is healthy. 250 requests
	// per full window, above the minimum sample.
	statsStore := newRedisStore(t, addr)
	outageWindow := window.Of(time.Now(), size)
	runInBackground(t, func(ctx context.Context) {
		tick := time.NewTicker(40 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				w := window.Of(now, size)
				st := policy.WindowStats{Total: 10}
				if w <= outageWindow.Next(size) {
					st.Failed = 10
				}
				_ = statsStore.AddStats(ctx, w, map[string]policy.WindowStats{"bad": st})
			}
		}
	})

	log := &commitLog{StateWriter: newRedisStore(t, addr)}
	newEvaluator := func(owner string, lease store.Leaser) *evaluator.Service {
		src := newRedisStore(t, addr)
		svc, err := evaluator.New(src, log, lease, evaluator.Options{
			Policy: pol, Owner: owner, Grace: 200 * time.Millisecond,
			TickInterval: 50 * time.Millisecond, LeaseTTL: leaseTTL,
		})
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}
	e1 := newEvaluator("e1", crashLeaser{newRedisStore(t, addr)})
	crashE1 := runInBackground(t, func(ctx context.Context) { _ = e1.Run(ctx) })
	time.Sleep(300 * time.Millisecond) // e1 takes the lease first
	e2 := newEvaluator("e2", newRedisStore(t, addr))
	runInBackground(t, func(ctx context.Context) { _ = e2.Run(ctx) })

	// Let e1 block the buyer and start the recovery.
	eventually(t, 5*time.Second, "the block and two recovery steps", func() bool {
		_, rates := log.snapshot()
		return slices.Contains(rates, 0.75)
	})
	windowsBefore, _ := log.snapshot()
	crashedAt := time.Now()
	crashE1()

	// e2 must take over within the lease TTL and finish the recovery.
	eventually(t, 15*time.Second, "full recovery after failover", func() bool {
		_, rates := log.snapshot()
		return rates[len(rates)-1] == 0
	})
	windows, rates := log.snapshot()
	firstAfter := windows[len(windowsBefore)]
	takeover := firstAfter.End(size).Add(200 * time.Millisecond).Sub(crashedAt)
	t.Logf("leader crashed; standby committed its first window %v later (lease TTL %v); rates %v", takeover.Round(10*time.Millisecond), leaseTTL, rates)

	for i := 1; i < len(windows); i++ {
		if windows[i] <= windows[i-1] {
			t.Fatalf("windows not strictly increasing (applied twice or out of order): %v", windows)
		}
	}
	if takeover > leaseTTL+2*size {
		t.Fatalf("standby took %v to take over, want under lease TTL + 2 windows", takeover)
	}
	// From the last block on, every commit is exactly one recovery step: the
	// standby continued from the leader's state instead of restarting it.
	start := slices.Index(rates, 0.95)
	for start >= 0 && start+1 < len(rates) && rates[start+1] == 0.95 {
		start++ // still failing: stays blocked
	}
	if start < 0 {
		t.Fatalf("buyer was never blocked: %v", rates)
	}
	for i := start + 1; i < len(rates); i++ {
		want := math.Max(0, math.Round((rates[i-1]-0.1)*1e4)/1e4)
		if rates[i] != want {
			t.Fatalf("rate went %v -> %v, want %v; full sequence %v", rates[i-1], rates[i], want, rates)
		}
	}
}

// TestRedisOutage stops Redis while a pod keeps serving auctions, then
// restarts it. During the outage the pod must keep throttling from its last
// known state; afterwards every recorded call must be in Redis exactly once
// and new state must reach the pod again.
func TestRedisOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("real-time test skipped with -short")
	}
	mr := miniredis.RunT(t) // needs a server it can stop and restart
	const size = 2 * time.Second
	writer := newRedisStore(t, mr.Addr())
	if _, _, err := writer.CommitState(context.Background(), 1, []store.Record{{State: policy.State{BuyerID: "blocked", ThrottleRate: 0.95}}}); err != nil {
		t.Fatal(err)
	}

	podStore := newRedisStore(t, mr.Addr())
	pod := throttler.New(podStore, podStore, throttler.Options{
		Window: size, FlushInterval: 100 * time.Millisecond, SyncInterval: 100 * time.Millisecond,
		MaxPendingAge: time.Minute, StoreTimeout: 200 * time.Millisecond,
	})
	runInBackground(t, func(ctx context.Context) { _ = pod.Run(ctx) })
	eventually(t, 2*time.Second, "pod to load state", func() bool { return pod.Rate("blocked") == 0.95 })

	// Steady traffic: one call to "b" every millisecond.
	var recorded int64
	var mu sync.Mutex
	stopTraffic := runInBackground(t, func(ctx context.Context) {
		for ctx.Err() == nil {
			pod.Record("b", throttler.ResultOK)
			mu.Lock()
			recorded++
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	})

	time.Sleep(300 * time.Millisecond)
	mr.Close() // outage
	outageStart := time.Now()
	for time.Since(outageStart) < 700*time.Millisecond {
		if pod.Rate("blocked") != 0.95 || !pod.Ready() {
			t.Fatal("pod dropped its last known state during the outage")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := mr.Restart(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	stopTraffic()

	// Everything recorded must land in Redis exactly once.
	eventually(t, 3*time.Second, "all stats to be flushed", func() bool {
		var stored int64
		now := time.Now()
		for w := window.Of(now.Add(-time.Minute), size); w <= window.Of(now, size); w = w.Next(size) {
			st, err := writer.WindowStats(context.Background(), w)
			if err != nil {
				return false
			}
			stored += st["b"].Total
		}
		mu.Lock()
		defer mu.Unlock()
		if stored > recorded {
			t.Fatalf("stored %d calls but only %d were recorded: double counting", stored, recorded)
		}
		return stored == recorded
	})

	// New state reaches the pod again.
	if _, _, err := writer.CommitState(context.Background(), 2, []store.Record{{State: policy.State{BuyerID: "blocked", ThrottleRate: 0.85}}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "pod to pick up state committed after the outage", func() bool { return pod.Rate("blocked") == 0.85 })
}
