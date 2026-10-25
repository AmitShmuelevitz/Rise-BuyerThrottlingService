// Package auction is a minimal auction used by the demo pods: it fans a
// request out to buyers in parallel, consulting the throttler before each
// call and recording each call's result.
package auction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/amit/buyer-throttling/internal/throttler"
)

// Throttle is the part of the throttler an auction needs.
type Throttle interface {
	ShouldCall(buyerID string) bool
	Record(buyerID string, r throttler.Result)
}

// BuyerClient asks one buyer for a bid.
type BuyerClient interface {
	Bid(ctx context.Context, buyerID string) error
}

// HTTPBuyerClient calls GET {BaseURL}/bid/{buyer}; any 2xx is a bid.
type HTTPBuyerClient struct {
	BaseURL string
	Client  *http.Client
}

func (c *HTTPBuyerClient) Bid(ctx context.Context, buyerID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/bid/"+url.PathEscape(buyerID), nil)
	if err != nil {
		return err
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection is reused
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("buyer %s: status %d", buyerID, resp.StatusCode)
	}
	return nil
}

// Auction runs auctions.
type Auction struct {
	Throttle Throttle
	Client   BuyerClient
	// Timeout is the per-auction deadline for buyer responses.
	Timeout time.Duration
	Logger  *slog.Logger
}

// Result summarizes one auction.
type Result struct {
	Called  []string `json:"called"`
	Skipped []string `json:"skipped"`
	Bids    int      `json:"bids"`
}

// Run calls every eligible buyer in parallel and waits for all of them or the
// deadline. A buyer that misses the deadline is recorded as a timeout. If
// the caller's own context ends first (the ad request was abandoned), nothing
// is recorded for the calls cut short: that is not the buyer's fault.
func (a *Auction) Run(ctx context.Context, buyers []string) Result {
	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()

	res := Result{Called: []string{}, Skipped: []string{}}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, b := range buyers {
		if !a.Throttle.ShouldCall(b) {
			res.Skipped = append(res.Skipped, b)
			continue
		}
		res.Called = append(res.Called, b)
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := a.Client.Bid(ctx, b)
			r, ok := classify(ctx, err)
			if !ok {
				return
			}
			a.Throttle.Record(b, r)
			if r == throttler.ResultOK {
				mu.Lock()
				res.Bids++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return res
}

// classify maps a buyer call error to a result. ok is false when the call
// was cut short by the caller rather than by the auction deadline.
func classify(ctx context.Context, err error) (r throttler.Result, ok bool) {
	if err == nil {
		return throttler.ResultOK, true
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return 0, false
	}
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return throttler.ResultTimeout, true
	}
	return throttler.ResultFailed, true
}

// Request is the POST /auction body: the buyers eligible for this ad request.
type Request struct {
	Buyers []string `json:"buyers"`
}

// Handler serves POST /auction.
func (a *Auction) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
			http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
			return
		}
		res := a.Run(r.Context(), req.Buyers)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	})
}
