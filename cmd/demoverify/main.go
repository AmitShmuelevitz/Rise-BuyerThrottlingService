// Command demoverify checks the running docker-compose demo against the
// expectations in the scenario file: it polls the evaluator's GET /state
// until every buyer has done what its `expect` list says (blocked, snapped
// back, recovered, or never throttled), then checks that every auction pod
// has converged on the evaluator's state version. It exits non-zero on a
// violation or timeout, so CI can run the demo as an acceptance test.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/app"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/config"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/demoverify"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/httpapi"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/simulator"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo-verify: FAILED:", err)
		os.Exit(1)
	}
	fmt.Println("demo-verify: PASSED")
}

func run() error {
	env := config.FromOS()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	pol := env.Policy()
	var (
		evaluatorURL = env.String("EVALUATOR_URL", "http://localhost:8080")
		podURLs      = env.String("POD_URLS", "http://localhost:8091,http://localhost:8092,http://localhost:8093")
		scenarioPath = env.String("SCENARIO", "deploy/scenario.yaml")
		timeout      = env.Duration("VERIFY_TIMEOUT", 6*time.Minute)
		poll         = env.Duration("VERIFY_POLL", time.Second)
	)
	if err := env.Err(); err != nil {
		return err
	}
	sc, err := simulator.LoadScenario(scenarioPath)
	if err != nil {
		return err
	}

	ctx, stop := app.SignalContext()
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: 3 * time.Second}

	checker := demoverify.NewChecker(sc.Buyers, pol.BlockLevel)
	logger.Info("watching throttle state", "evaluator", evaluatorURL, "timeout", timeout, "pending", len(checker.Pending()))

	seen := map[string]demoverify.Observation{}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for !checker.Done() {
		if f := checker.Failed(); len(f) > 0 {
			return fmt.Errorf("expectation violated:\n  %s", strings.Join(f, "\n  "))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %v; still waiting for:\n  %s", timeout, strings.Join(checker.Pending(), "\n  "))
		case <-ticker.C:
		}
		st, err := fetchState(ctx, client, evaluatorURL)
		if err != nil {
			logger.Warn("evaluator not reachable yet", "err", err)
			continue
		}
		cur := make(map[string]demoverify.Observation, len(st.Buyers))
		for _, b := range st.Buyers {
			o := demoverify.Observation{Rate: b.ThrottleRate, Reason: b.Reason}
			cur[b.BuyerID] = o
			if prev, ok := seen[b.BuyerID]; !ok || prev != o {
				logger.Info("state change", "buyer", b.BuyerID, "rate", o.Rate, "reason", o.Reason, "version", st.Version)
				seen[b.BuyerID] = o
			}
		}
		checker.Observe(cur)
	}
	logger.Info("all buyer expectations met")
	return checkConvergence(ctx, client, evaluatorURL, strings.Split(podURLs, ","), logger)
}

// checkConvergence waits until every pod serves the evaluator's current
// state version, and reports how long that took.
func checkConvergence(ctx context.Context, client *http.Client, evaluatorURL string, pods []string, logger *slog.Logger) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		ev, err := fetchState(ctx, client, evaluatorURL)
		if err != nil {
			return err
		}
		lagging := []string{}
		for _, p := range pods {
			ps, err := fetchState(ctx, client, p)
			if err != nil || ps.Version < ev.Version {
				lagging = append(lagging, p)
			}
		}
		if len(lagging) == 0 {
			logger.Info("all pods converged", "version", ev.Version, "pods", len(pods))
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pods %v did not reach state version %d within 10s", lagging, ev.Version)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func fetchState(ctx context.Context, client *http.Client, base string) (httpapi.StateResponse, error) {
	var st httpapi.StateResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/state", nil)
	if err != nil {
		return st, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("%s/state: status %d", base, resp.StatusCode)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}
