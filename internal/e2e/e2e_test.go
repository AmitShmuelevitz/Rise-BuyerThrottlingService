// Package e2e wires the real components together (two auction-pod
// throttlers, one evaluator, the Redis store on miniredis) under a fake clock
// and checks the full loop: stats flow up, state flows down, and every pod
// converges on the same throttle rate.
package e2e

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/amit/buyer-throttling/internal/clock"
	"github.com/amit/buyer-throttling/internal/evaluator"
	"github.com/amit/buyer-throttling/internal/policy"
	"github.com/amit/buyer-throttling/internal/store/redisstore"
	"github.com/amit/buyer-throttling/internal/throttler"
	"github.com/amit/buyer-throttling/internal/window"
)

const (
	size  = 10 * time.Second
	grace = 3 * time.Second
)

type cluster struct {
	t     *testing.T
	clock *clock.Fake
	pods  []*throttler.Throttler
	eval  *evaluator.Service
	rng   *rand.Rand
	w     window.ID
}

func newCluster(t *testing.T, pods int) *cluster {
	mr := miniredis.RunT(t)
	start := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	c := &cluster{t: t, clock: clock.NewFake(start), rng: rand.New(rand.NewPCG(7, 7)), w: window.Of(start, size)}

	newStore := func() *redisstore.Store {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return redisstore.New(rdb, redisstore.Options{})
	}
	for i := range pods {
		st := newStore() // each pod has its own connection, like separate processes
		rng := rand.New(rand.NewPCG(uint64(i), 99))
		c.pods = append(c.pods, throttler.New(st, st, throttler.Options{Window: size, Clock: c.clock, Rand: rng.Float64}))
	}
	pol := policy.DefaultConfig()
	pol.Window = size
	st := newStore()
	var err error
	c.eval, err = evaluator.New(st, st, st, evaluator.Options{Policy: pol, Grace: grace, Owner: "e1", Clock: c.clock})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// runWindow sends perPod requests for each buyer through every pod, with the
// buyer's given failure probability, then flushes, evaluates the window and
// lets every pod sync.
func (c *cluster) runWindow(perPod int, failure map[string]float64) {
	c.t.Helper()
	ctx := context.Background()
	c.clock.Set(c.w.Start().Add(size / 2))
	for _, p := range c.pods {
		for range perPod {
			for buyer, f := range failure {
				if !p.ShouldCall(buyer) {
					continue
				}
				if c.rng.Float64() < f {
					p.Record(buyer, throttler.ResultFailed)
				} else {
					p.Record(buyer, throttler.ResultOK)
				}
			}
		}
		if err := p.Flush(ctx); err != nil {
			c.t.Fatal(err)
		}
	}
	c.clock.Set(c.w.End(size).Add(grace))
	if err := c.eval.Tick(ctx); err != nil {
		c.t.Fatal(err)
	}
	for _, p := range c.pods {
		if err := p.Sync(ctx); err != nil {
			c.t.Fatal(err)
		}
	}
	c.w = c.w.Next(size)
}

func (c *cluster) assertRate(buyer string, want float64) {
	c.t.Helper()
	for i, p := range c.pods {
		if got := p.Rate(buyer); got != want {
			c.t.Fatalf("window %s: pod %d sees %s at %v, want %v", c.w.Prev(size).Start().Format("15:04:05"), i, buyer, got, want)
		}
	}
}

func TestBadBuyerIsBlockedOnEveryPodAndRecovers(t *testing.T) {
	c := newCluster(t, 2)
	healthy := map[string]float64{"bad": 0.01, "good": 0.01}

	c.runWindow(1000, healthy)
	c.assertRate("bad", 0)

	// Outage: 90% failures across both pods.
	c.runWindow(1000, map[string]float64{"bad": 0.9, "good": 0.01})
	c.assertRate("bad", 0.95)
	c.assertRate("good", 0)

	// Still failing while blocked: only ~5% of traffic reaches it, which is
	// still > min sample, so it stays blocked.
	c.runWindow(2000, map[string]float64{"bad": 0.9, "good": 0.01})
	c.assertRate("bad", 0.95)

	// Healthy again: one step per window down to 0.
	for _, want := range []float64{0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05, 0} {
		c.runWindow(2000, healthy)
		c.assertRate("bad", want)
	}
}

func TestRelapseMidRecoverySnapsBack(t *testing.T) {
	c := newCluster(t, 3)
	c.runWindow(1000, map[string]float64{"b": 0.95})
	c.runWindow(2000, map[string]float64{"b": 0})
	c.runWindow(2000, map[string]float64{"b": 0})
	c.assertRate("b", 0.75)

	c.runWindow(2000, map[string]float64{"b": 0.95})
	c.assertRate("b", 0.95)
}

func TestLowVolumeBuyerIsNeverBlocked(t *testing.T) {
	c := newCluster(t, 2)
	for range 3 {
		c.runWindow(40, map[string]float64{"tiny": 1}) // 80 requests per window < 100
		c.assertRate("tiny", 0)
	}
}

func TestSkippedRequestsMatchThrottleRate(t *testing.T) {
	c := newCluster(t, 2)
	c.runWindow(1000, map[string]float64{"b": 1})
	c.assertRate("b", 0.95)

	called := 0
	const n = 20_000
	for range n {
		if c.pods[0].ShouldCall("b") {
			called++
		}
	}
	if frac := float64(called) / n; frac < 0.04 || frac > 0.06 {
		t.Fatalf("pod called a buyer at 0.95 for %.3f of requests, want about 0.05", frac)
	}
}
