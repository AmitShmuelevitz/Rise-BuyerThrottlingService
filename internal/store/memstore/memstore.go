// Package memstore is an in-memory store.Store for tests and local runs. It
// is safe for concurrent use but not shared between processes.
package memstore

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/clock"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/store"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

var _ store.Store = (*Store)(nil)

// Store is an in-memory implementation of store.Store.
type Store struct {
	clock clock.Clock

	mu          sync.Mutex
	stats       map[window.ID]map[string]policy.WindowStats
	states      map[string]store.Record
	version     uint64
	lastWindow  window.ID
	committed   bool
	subscribers map[chan uint64]struct{}
	leaseOwner  string
	leaseExpiry time.Time
}

// New returns an empty store. A nil clock uses the system clock.
func New(c clock.Clock) *Store {
	if c == nil {
		c = clock.Real{}
	}
	return &Store{
		clock:       c,
		stats:       make(map[window.ID]map[string]policy.WindowStats),
		states:      make(map[string]store.Record),
		subscribers: make(map[chan uint64]struct{}),
	}
}

func (s *Store) AddStats(_ context.Context, w window.ID, deltas map[string]policy.WindowStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	win, ok := s.stats[w]
	if !ok {
		win = make(map[string]policy.WindowStats)
		s.stats[w] = win
	}
	for buyer, d := range deltas {
		win[buyer] = win[buyer].Add(d)
	}
	return nil
}

func (s *Store) WindowStats(_ context.Context, w window.ID) (map[string]policy.WindowStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]policy.WindowStats, len(s.stats[w]))
	maps.Copy(out, s.stats[w])
	return out, nil
}

func (s *Store) LoadState(_ context.Context) (store.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return store.Snapshot{
		Version:    s.version,
		LastWindow: s.lastWindow,
		States:     maps.Clone(s.states),
	}, nil
}

func (s *Store) Version(_ context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, nil
}

func (s *Store) CommitState(_ context.Context, w window.ID, records []store.Record) (bool, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed && s.lastWindow >= w {
		return false, s.version, nil
	}
	for _, r := range records {
		s.states[r.BuyerID] = r
	}
	s.committed = true
	s.lastWindow = w
	s.version++
	for ch := range s.subscribers {
		select {
		case ch <- s.version:
		default: // slow subscriber: drop, it will catch up by polling
		}
	}
	return true, s.version, nil
}

func (s *Store) SubscribeUpdates(ctx context.Context) (<-chan uint64, error) {
	ch := make(chan uint64, 16)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.subscribers, ch)
		close(ch)
		s.mu.Unlock()
	}()
	return ch, nil
}

func (s *Store) TryAcquire(_ context.Context, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	if s.leaseOwner == "" || s.leaseOwner == owner || !now.Before(s.leaseExpiry) {
		s.leaseOwner, s.leaseExpiry = owner, now.Add(ttl)
		return true, nil
	}
	return false, nil
}

func (s *Store) Release(_ context.Context, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaseOwner == owner {
		s.leaseOwner, s.leaseExpiry = "", time.Time{}
	}
	return nil
}
