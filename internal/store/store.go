// Package store defines how throttle stats and throttle state move between
// the auction pods and the evaluator. Implementations live in sub-packages:
// redisstore for production and memstore for tests.
package store

import (
	"context"
	"time"

	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/policy"
	"github.com/AmitShmuelevitz/Rise-BuyerThrottlingService/internal/window"
)

// Snapshot is a consistent view of all buyers' throttle state.
type Snapshot struct {
	// Version increases by one on every committed evaluation.
	Version uint64
	// LastWindow is the most recent window the evaluator committed.
	LastWindow window.ID
	// States holds one entry per known buyer.
	States map[string]Record
}

// Record is one buyer's persisted state plus bookkeeping fields.
type Record struct {
	policy.State
	// Window is the window whose evaluation produced this record.
	Window window.ID `json:"window"`
	// UpdatedAt is when the throttle rate or reason last changed.
	UpdatedAt time.Time `json:"updated_at"`
}

// StatsSink receives per-buyer stat deltas from auction pods. Deltas from
// many pods for the same window are summed.
type StatsSink interface {
	AddStats(ctx context.Context, w window.ID, deltas map[string]policy.WindowStats) error
}

// StatsSource returns the summed stats of one window.
type StatsSource interface {
	WindowStats(ctx context.Context, w window.ID) (map[string]policy.WindowStats, error)
}

// StateReader is how auction pods read throttle state.
type StateReader interface {
	LoadState(ctx context.Context) (Snapshot, error)
	// SubscribeUpdates delivers the new version after each commit. Delivery
	// is best effort: callers must still poll LoadState periodically.
	// The channel is closed when ctx is done or the subscription fails.
	SubscribeUpdates(ctx context.Context) (<-chan uint64, error)
}

// StateWriter is how the evaluator persists throttle state.
type StateWriter interface {
	LoadState(ctx context.Context) (Snapshot, error)
	// CommitState atomically stores records for window w, bumps the version
	// and notifies subscribers. It is a no-op returning applied=false when a
	// window >= w was already committed, so re-running a window (for example
	// after a leader failover) can never apply it twice.
	CommitState(ctx context.Context, w window.ID, records []Record) (applied bool, version uint64, err error)
}

// Leaser is a lease used to elect a single active evaluator.
type Leaser interface {
	// TryAcquire takes the lease if free or renews it if owner already holds
	// it. It returns whether owner holds the lease afterwards.
	TryAcquire(ctx context.Context, owner string, ttl time.Duration) (bool, error)
	// Release gives up the lease if owner holds it.
	Release(ctx context.Context, owner string) error
}

// Store is everything a backend provides.
type Store interface {
	StatsSink
	StatsSource
	StateReader
	StateWriter
	Leaser
}
