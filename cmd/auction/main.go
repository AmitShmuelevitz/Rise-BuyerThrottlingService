// Command auction is a demo auction pod. It serves POST /auction, fanning
// each ad request out to the eligible buyers through the throttler, and
// exposes its local view of the throttle state on GET /state.
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
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/auction"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/config"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/httpapi"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/metrics"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store/redisstore"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/throttler"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "auction:", err)
		os.Exit(1)
	}
}

func run() error {
	env := config.FromOS()
	logger := env.Logger("auction")
	pol := env.Policy() // only the window is used here; it must match the evaluator's
	host, _ := os.Hostname()
	logger = logger.With("pod", env.String("POD_ID", host))
	var (
		redisAddr     = env.String("REDIS_ADDR", "localhost:6379")
		httpAddr      = env.String("HTTP_ADDR", ":8080")
		buyerURL      = env.String("BUYER_BASE_URL", "http://localhost:9000")
		auctionTO     = env.Duration("AUCTION_TIMEOUT", 150*time.Millisecond)
		flushInterval = env.Duration("THROTTLE_FLUSH_INTERVAL", time.Second)
		syncInterval  = env.Duration("THROTTLE_SYNC_INTERVAL", time.Second)
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

	th := throttler.New(st, st, throttler.Options{
		Window:        pol.Window,
		FlushInterval: flushInterval,
		SyncInterval:  syncInterval,
		Logger:        logger,
		Metrics:       metrics.NewThrottler(reg),
	})

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 1024
	auc := &auction.Auction{
		Throttle: th,
		Client:   &auction.HTTPBuyerClient{BaseURL: buyerURL, Client: &http.Client{Transport: transport}},
		Timeout:  auctionTO,
		Logger:   logger,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /auction", auc.Handler())
	httpapi.Ops(mux, func(context.Context) (map[string]store.Record, uint64, error) {
		recs, v, _ := th.States()
		return recs, v, nil
	}, th.Ready, reg, logger)

	logger.Info("auction pod starting", "window", pol.Window, "flush_interval", flushInterval, "sync_interval", syncInterval, "auction_timeout", auctionTO)

	// Stop the throttler only after the HTTP server has drained, so results
	// of in-flight auctions make it into the final flush.
	thCtx, stopThrottler := context.WithCancel(context.WithoutCancel(ctx))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return th.Run(thCtx) })
	g.Go(func() error {
		defer stopThrottler()
		return app.ServeHTTP(gctx, app.NewServer(httpAddr, mux), logger)
	})
	return g.Wait()
}
