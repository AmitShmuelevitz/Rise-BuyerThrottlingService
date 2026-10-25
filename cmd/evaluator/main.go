// Command evaluator runs the throttle evaluator: once per window it reads the
// buyers' aggregated stats from Redis, applies the throttling policy and
// publishes the new throttle state. Run several replicas for availability;
// a lease in Redis keeps exactly one active.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"golang.org/x/sync/errgroup"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/app"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/config"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/evaluator"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/httpapi"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/metrics"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/redisstore"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "evaluator:", err)
		os.Exit(1)
	}
}

func run() error {
	env := config.FromOS()
	logger := env.Logger("evaluator")
	pol := env.Policy()
	host, _ := os.Hostname()
	var (
		redisAddr = env.String("REDIS_ADDR", "localhost:6379")
		httpAddr  = env.String("HTTP_ADDR", ":8080")
		owner     = env.String("EVALUATOR_ID", host)
		grace     = env.Duration("EVALUATOR_GRACE", 5*time.Second)
		leaseTTL  = env.Duration("EVALUATOR_LEASE_TTL", 15*time.Second)
		tick      = env.Duration("EVALUATOR_TICK", time.Second)
	)
	if err := env.Err(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, stop := app.SignalContext()
	defer stop()

	rdb, err := app.ConnectRedis(ctx, redisAddr, logger)
	if err != nil {
		return err
	}
	defer rdb.Close()
	st := redisstore.New(rdb, redisstore.Options{StatsTTL: 10 * pol.Window})

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	svc, err := evaluator.New(st, st, st, evaluator.Options{
		Policy:       pol,
		Grace:        grace,
		LeaseTTL:     leaseTTL,
		TickInterval: tick,
		Owner:        owner,
		Logger:       logger,
		Metrics:      metrics.NewEvaluator(reg),
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	httpapi.Ops(mux, func(ctx context.Context) (map[string]store.Record, uint64, error) {
		snap, err := st.LoadState(ctx)
		return snap.States, snap.Version, err
	}, nil, reg, logger)

	logger.Info("evaluator starting", "owner", owner, "policy", fmt.Sprintf("%+v", pol), "grace", grace, "lease_ttl", leaseTTL)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return svc.Run(gctx) })
	g.Go(func() error { return app.ServeHTTP(gctx, app.NewServer(httpAddr, mux), logger) })
	return g.Wait()
}
