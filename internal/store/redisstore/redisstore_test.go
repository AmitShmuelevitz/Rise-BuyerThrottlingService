package redisstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/redisstore"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/storetest"
)

// TestConformance runs the suite against miniredis, an in-process Redis
// that executes the same Lua scripts. It always runs.
func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (store.Store, func(time.Duration)) {
		mr := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		return redisstore.New(rdb, redisstore.Options{}), mr.FastForward
	})
}

// TestConformanceRealRedis runs the suite against a real Redis when
// REDIS_ADDR is set (make test-integration and CI do this). It flushes that
// Redis, so it must not run alongside the e2e package against the same server
// (both targets use -p 1).
func TestConformanceRealRedis(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	storetest.Run(t, func(t *testing.T) (store.Store, func(time.Duration)) {
		rdb := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = rdb.Close() })
		if err := rdb.FlushDB(context.Background()).Err(); err != nil {
			t.Fatalf("flush redis at %s: %v", addr, err)
		}
		return redisstore.New(rdb, redisstore.Options{}), nil
	})
}

func TestStatsKeyExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := redisstore.New(rdb, redisstore.Options{StatsTTL: time.Minute})
	ctx := context.Background()

	if err := s.AddStats(ctx, 60, map[string]policy.WindowStats{"a": {Total: 1}}); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("{throttle}:stats:60"); ttl != time.Minute {
		t.Fatalf("ttl = %v, want 1m", ttl)
	}
	mr.FastForward(time.Minute + time.Second)
	got, err := s.WindowStats(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("stats after expiry = %v, want empty", got)
	}
}

func TestLoadStateRejectsCorruptRecord(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := redisstore.New(rdb, redisstore.Options{})
	mr.HSet("{throttle}:state", "a", "{not json")

	if _, err := s.LoadState(context.Background()); err == nil {
		t.Fatal("expected an error for a corrupt record")
	}
}

func TestStoreErrorsWhenRedisIsDown(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	s := redisstore.New(rdb, redisstore.Options{})
	mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.AddStats(ctx, 60, map[string]policy.WindowStats{"a": {Total: 1}}); err == nil {
		t.Error("AddStats: expected an error")
	}
	if _, err := s.LoadState(ctx); err == nil {
		t.Error("LoadState: expected an error")
	}
	if _, _, err := s.CommitState(ctx, 60, nil); err == nil {
		t.Error("CommitState: expected an error")
	}
}
