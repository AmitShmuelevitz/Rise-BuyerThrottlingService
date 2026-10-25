package throttler

import (
	"context"
	"strconv"
	"testing"

	"github.com/amit/buyer-throttling/internal/store"

	"github.com/amit/buyer-throttling/internal/policy"
	"github.com/amit/buyer-throttling/internal/store/memstore"
)

// benchThrottler has 200 buyers, a quarter of them throttled.
func benchThrottler(b *testing.B) (*Throttler, []string) {
	s := memstore.New(nil)
	buyers := make([]string, 200)
	var states []policy.State
	for i := range buyers {
		buyers[i] = "buyer_" + strconv.Itoa(i)
		rate := 0.0
		if i%4 == 0 {
			rate = 0.95
		}
		states = append(states, policy.State{BuyerID: buyers[i], ThrottleRate: rate})
	}
	recs := make([]store.Record, len(states))
	for i, st := range states {
		recs[i] = store.Record{State: st}
	}
	if _, _, err := s.CommitState(context.Background(), 60, recs); err != nil {
		b.Fatal(err)
	}
	th := New(s, s, Options{})
	if err := th.Sync(context.Background()); err != nil {
		b.Fatal(err)
	}
	return th, buyers
}

func BenchmarkShouldCall(b *testing.B) {
	th, buyers := benchThrottler(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			th.ShouldCall(buyers[i%len(buyers)])
			i++
		}
	})
}

func BenchmarkRecord(b *testing.B) {
	th, buyers := benchThrottler(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			th.Record(buyers[i%len(buyers)], Result(i%3))
			i++
		}
	})
}
