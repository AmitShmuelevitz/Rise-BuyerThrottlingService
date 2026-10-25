package auction

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amit/buyer-throttling/internal/throttler"
)

type fakeThrottle struct {
	mu      sync.Mutex
	skip    map[string]bool
	results map[string]throttler.Result
}

func (f *fakeThrottle) ShouldCall(b string) bool { return !f.skip[b] }
func (f *fakeThrottle) Record(b string, r throttler.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[b] = r
}

type fakeClient map[string]func(ctx context.Context) error

func (f fakeClient) Bid(ctx context.Context, b string) error { return f[b](ctx) }

func TestRun_RecordsEachOutcome(t *testing.T) {
	th := &fakeThrottle{skip: map[string]bool{"throttled": true}, results: map[string]throttler.Result{}}
	client := fakeClient{
		"ok":     func(context.Context) error { return nil },
		"failed": func(context.Context) error { return errors.New("500") },
		"slow": func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	a := &Auction{Throttle: th, Client: client, Timeout: 20 * time.Millisecond}

	res := a.Run(context.Background(), []string{"ok", "failed", "slow", "throttled"})

	if res.Bids != 1 || len(res.Called) != 3 || len(res.Skipped) != 1 || res.Skipped[0] != "throttled" {
		t.Fatalf("result = %+v", res)
	}
	want := map[string]throttler.Result{"ok": throttler.ResultOK, "failed": throttler.ResultFailed, "slow": throttler.ResultTimeout}
	for b, r := range want {
		if th.results[b] != r {
			t.Errorf("%s recorded %v, want %v", b, th.results[b], r)
		}
	}
	if _, ok := th.results["throttled"]; ok {
		t.Error("a skipped buyer must not be recorded")
	}
}

func TestRun_CallerCancellationIsNotRecorded(t *testing.T) {
	th := &fakeThrottle{results: map[string]throttler.Result{}}
	client := fakeClient{"b": func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	a := &Auction{Throttle: th, Client: client, Timeout: time.Second}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	a.Run(ctx, []string{"b"})
	if len(th.results) != 0 {
		t.Fatalf("recorded %v for a call the caller abandoned", th.results)
	}
}

func TestHTTPBuyerClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bid/good":
			w.WriteHeader(200)
		case "/bid/slow":
			time.Sleep(100 * time.Millisecond)
		default:
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()

	th := &fakeThrottle{results: map[string]throttler.Result{}}
	a := &Auction{Throttle: th, Client: &HTTPBuyerClient{BaseURL: srv.URL, Client: srv.Client()}, Timeout: 30 * time.Millisecond}
	a.Run(context.Background(), []string{"good", "bad", "slow"})

	want := map[string]throttler.Result{"good": throttler.ResultOK, "bad": throttler.ResultFailed, "slow": throttler.ResultTimeout}
	for b, r := range want {
		if th.results[b] != r {
			t.Errorf("%s recorded %v, want %v", b, th.results[b], r)
		}
	}
}

func TestHandler(t *testing.T) {
	th := &fakeThrottle{results: map[string]throttler.Result{}}
	a := &Auction{Throttle: th, Client: fakeClient{"a": func(context.Context) error { return nil }}, Timeout: time.Second}

	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest("POST", "/auction", strings.NewReader(`{"buyers":["a"]}`)))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"bids":1`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest("POST", "/auction", strings.NewReader(`nope`)))
	if rec.Code != 400 {
		t.Fatalf("bad body status = %d, want 400", rec.Code)
	}
}
