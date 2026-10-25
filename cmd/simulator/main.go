// Command simulator runs the demo's fake buyers (an HTTP server whose
// failure and timeout rates follow a scenario and can be changed at runtime)
// and a traffic generator that sends ad requests to the auction pods.
package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/amit/buyer-throttling/internal/app"
	"github.com/amit/buyer-throttling/internal/clock"
	"github.com/amit/buyer-throttling/internal/config"
	"github.com/amit/buyer-throttling/internal/simulator"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "simulator:", err)
		os.Exit(1)
	}
}

func run() error {
	env := config.FromOS()
	logger := env.Logger("simulator")
	var (
		scenarioPath = env.String("SCENARIO", "deploy/scenario.yaml")
		httpAddr     = env.String("HTTP_ADDR", ":9000")
		targets      = env.String("AUCTION_TARGETS", "http://localhost:8080")
		rps          = env.Int("RPS", 0)
		startDelay   = env.Duration("TRAFFIC_START_DELAY", 3*time.Second)
		reportEvery  = env.Duration("REPORT_EVERY", 10*time.Second)
	)
	if err := env.Err(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	sc, err := simulator.LoadScenario(scenarioPath)
	if err != nil {
		return err
	}
	if rps > 0 {
		sc.RPS = int(rps)
	}

	ctx, stop := app.SignalContext()
	defer stop()

	buyers := simulator.NewBuyers(sc, clock.Real{}, logger)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 1024
	traffic := &simulator.Traffic{
		Targets:     strings.Split(targets, ","),
		RPS:         sc.RPS,
		Buyers:      sc.Buyers,
		Client:      &http.Client{Transport: transport},
		Logger:      logger,
		ReportEvery: reportEvery,
	}

	logger.Info("simulator starting", "scenario", scenarioPath, "rps", sc.RPS, "targets", traffic.Targets, "buyers", len(sc.Buyers))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return app.ServeHTTP(gctx, app.NewServer(httpAddr, buyers.Handler()), logger) })
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return nil
		case <-time.After(startDelay): // let the pods come up
		}
		traffic.Run(gctx)
		return nil
	})
	return g.Wait()
}
