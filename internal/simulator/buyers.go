package simulator

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
)

// Buyers serves the fake buyers' bid endpoint and the runtime overrides.
type Buyers struct {
	scenario Scenario
	start    time.Time
	clock    clock.Clock
	logger   *slog.Logger
	rand     func() float64
	sleep    func(r *http.Request, d time.Duration)

	mu        sync.RWMutex
	overrides map[string]Behavior
	specs     map[string]BuyerSpec
}

// NewBuyers starts the scenario clock now.
func NewBuyers(s Scenario, c clock.Clock, logger *slog.Logger) *Buyers {
	specs := make(map[string]BuyerSpec, len(s.Buyers))
	for _, b := range s.Buyers {
		specs[b.ID] = b
	}
	return &Buyers{
		scenario:  s,
		start:     c.Now(),
		clock:     c,
		logger:    logger,
		rand:      rand.Float64,
		sleep:     sleepCtx,
		overrides: make(map[string]Behavior),
		specs:     specs,
	}
}

func sleepCtx(r *http.Request, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.Context().Done():
	}
}

// Behavior returns a buyer's current behaviour and whether it is overridden.
// Unknown buyers are healthy.
func (b *Buyers) Behavior(id string) (Behavior, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if o, ok := b.overrides[id]; ok {
		return o, true
	}
	return b.specs[id].At(b.clock.Now().Sub(b.start)), false
}

// Handler serves:
//
//	GET    /bid/{buyer}     a bid, failure (500) or slow response per the behaviour
//	GET    /buyers          every buyer's current behaviour
//	PUT    /buyers/{buyer}  override a buyer: {"failure_rate":0.9,"timeout_rate":0}
//	DELETE /buyers/{buyer}  return a buyer to its scenario
func (b *Buyers) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bid/{buyer}", b.bid)
	mux.HandleFunc("GET /buyers", b.list)
	mux.HandleFunc("PUT /buyers/{buyer}", b.override)
	mux.HandleFunc("DELETE /buyers/{buyer}", b.clearOverride)
	return mux
}

func (b *Buyers) bid(w http.ResponseWriter, r *http.Request) {
	beh, _ := b.Behavior(r.PathValue("buyer"))
	u := b.rand()
	switch {
	case u < beh.FailureRate:
		b.sleep(r, b.scenario.Latency.D())
		http.Error(w, "buyer error", http.StatusInternalServerError)
	case u < beh.FailureRate+beh.TimeoutRate:
		b.sleep(r, b.scenario.TimeoutLatency.D())
		w.WriteHeader(http.StatusOK)
	default:
		// Jitter healthy latency between 0.5x and 1.5x.
		b.sleep(r, time.Duration(float64(b.scenario.Latency.D())*(0.5+b.rand())))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"bid":1.0}`)
	}
}

type buyerView struct {
	ID         string  `json:"buyer_id"`
	Share      float64 `json:"share"`
	Overridden bool    `json:"overridden"`
	Behavior
}

func (b *Buyers) list(w http.ResponseWriter, _ *http.Request) {
	out := make([]buyerView, 0, len(b.specs))
	for id, spec := range b.specs {
		beh, over := b.Behavior(id)
		out = append(out, buyerView{ID: id, Share: spec.ShareOrDefault(), Overridden: over, Behavior: beh})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	writeJSON(w, http.StatusOK, out)
}

func (b *Buyers) override(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("buyer")
	var beh Behavior
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&beh); err != nil {
		http.Error(w, "invalid body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := beh.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.overrides[id] = beh
	b.mu.Unlock()
	b.logger.Info("buyer behaviour overridden", "buyer", id, "failure_rate", beh.FailureRate, "timeout_rate", beh.TimeoutRate)
	writeJSON(w, http.StatusOK, buyerView{ID: id, Share: b.specs[id].ShareOrDefault(), Overridden: true, Behavior: beh})
}

func (b *Buyers) clearOverride(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("buyer")
	b.mu.Lock()
	delete(b.overrides, id)
	b.mu.Unlock()
	b.logger.Info("buyer override cleared", "buyer", id)
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
