// Package redisstore implements store.Store on Redis.
//
// Key layout (all keys share the {throttle} hash tag so the commit script stays
// valid on Redis Cluster):
//
//	{throttle}:stats:<window>  hash  "<buyer>:t|f|o" -> count   (TTL: several windows)
//	{throttle}:state           hash  "<buyer>" -> JSON store.Record
//	{throttle}:meta            hash  "version", "last_window"
//	{throttle}:leader          string owner id with a TTL (evaluator lease)
//	throttle:updates           pub/sub channel, message = new version
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/amit/buyer-throttling/internal/policy"
	"github.com/amit/buyer-throttling/internal/store"
	"github.com/amit/buyer-throttling/internal/window"
)

var _ store.Store = (*Store)(nil)

const (
	keyPrefix      = "{throttle}:"
	keyState       = keyPrefix + "state"
	keyMeta        = keyPrefix + "meta"
	keyLeader      = keyPrefix + "leader"
	updatesChannel = "throttle:updates"

	fieldTotal    = "t"
	fieldFailed   = "f"
	fieldTimeouts = "o"
)

// Options configures the store.
type Options struct {
	// StatsTTL is how long a window's stats hash is kept. It must comfortably
	// exceed the evaluation delay; a few windows is plenty.
	StatsTTL time.Duration
}

// Store is a Redis-backed store.Store.
type Store struct {
	rdb      redis.UniversalClient
	statsTTL time.Duration
}

// New wraps an existing client. The caller owns the client's lifecycle.
func New(rdb redis.UniversalClient, opts Options) *Store {
	if opts.StatsTTL <= 0 {
		opts.StatsTTL = 10 * time.Minute
	}
	return &Store{rdb: rdb, statsTTL: opts.StatsTTL}
}

func statsKey(w window.ID) string { return keyPrefix + "stats:" + w.String() }

// AddStats adds the deltas to the window's hash in one pipelined round trip.
// HINCRBY is atomic, so concurrent pods never lose increments.
func (s *Store) AddStats(ctx context.Context, w window.ID, deltas map[string]policy.WindowStats) error {
	if len(deltas) == 0 {
		return nil
	}
	key := statsKey(w)
	pipe := s.rdb.Pipeline()
	for buyer, d := range deltas {
		if d.Total != 0 {
			pipe.HIncrBy(ctx, key, buyer+":"+fieldTotal, d.Total)
		}
		if d.Failed != 0 {
			pipe.HIncrBy(ctx, key, buyer+":"+fieldFailed, d.Failed)
		}
		if d.Timeouts != 0 {
			pipe.HIncrBy(ctx, key, buyer+":"+fieldTimeouts, d.Timeouts)
		}
	}
	pipe.Expire(ctx, key, s.statsTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("add stats for window %s: %w", w, err)
	}
	return nil
}

// WindowStats reads every buyer's stats for w in a single HGETALL.
func (s *Store) WindowStats(ctx context.Context, w window.ID) (map[string]policy.WindowStats, error) {
	raw, err := s.rdb.HGetAll(ctx, statsKey(w)).Result()
	if err != nil {
		return nil, fmt.Errorf("read stats for window %s: %w", w, err)
	}
	out := make(map[string]policy.WindowStats)
	for field, val := range raw {
		// Split on the last colon: buyer IDs may contain colons, suffixes cannot.
		i := strings.LastIndexByte(field, ':')
		if i <= 0 {
			continue
		}
		buyer, kind := field[:i], field[i+1:]
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse stats field %q: %w", field, err)
		}
		st := out[buyer]
		switch kind {
		case fieldTotal:
			st.Total = n
		case fieldFailed:
			st.Failed = n
		case fieldTimeouts:
			st.Timeouts = n
		default:
			continue
		}
		out[buyer] = st
	}
	return out, nil
}

// LoadState reads state and metadata in one MULTI/EXEC so the version always
// matches the states returned with it.
func (s *Store) LoadState(ctx context.Context) (store.Snapshot, error) {
	var stateCmd *redis.MapStringStringCmd
	var metaCmd *redis.MapStringStringCmd
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		stateCmd = p.HGetAll(ctx, keyState)
		metaCmd = p.HGetAll(ctx, keyMeta)
		return nil
	})
	if err != nil {
		return store.Snapshot{}, fmt.Errorf("load state: %w", err)
	}

	snap := store.Snapshot{States: make(map[string]store.Record, len(stateCmd.Val()))}
	meta := metaCmd.Val()
	if v, ok := meta["version"]; ok {
		if snap.Version, err = strconv.ParseUint(v, 10, 64); err != nil {
			return store.Snapshot{}, fmt.Errorf("parse version %q: %w", v, err)
		}
	}
	if v, ok := meta["last_window"]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return store.Snapshot{}, fmt.Errorf("parse last_window %q: %w", v, err)
		}
		snap.LastWindow = window.ID(n)
	}
	for buyer, raw := range stateCmd.Val() {
		var r store.Record
		if err := json.Unmarshal([]byte(raw), &r); err != nil {
			return store.Snapshot{}, fmt.Errorf("decode state of buyer %q: %w", buyer, err)
		}
		r.BuyerID = buyer
		snap.States[buyer] = r
	}
	return snap, nil
}

// commitScript writes all records, advances last_window and the version, and
// publishes the new version, atomically. It refuses windows that are not newer
// than the last committed one.
//
// KEYS[1] state hash, KEYS[2] meta hash
// ARGV[1] window, ARGV[2] channel, ARGV[3..] buyer, json pairs
var commitScript = redis.NewScript(`
local last = redis.call('HGET', KEYS[2], 'last_window')
if last and tonumber(last) >= tonumber(ARGV[1]) then
  return {0, tonumber(redis.call('HGET', KEYS[2], 'version') or '0')}
end
for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('HSET', KEYS[2], 'last_window', ARGV[1])
local v = redis.call('HINCRBY', KEYS[2], 'version', 1)
redis.call('PUBLISH', ARGV[2], v)
return {1, v}
`)

func (s *Store) CommitState(ctx context.Context, w window.ID, records []store.Record) (bool, uint64, error) {
	args := make([]any, 0, 2+2*len(records))
	args = append(args, int64(w), updatesChannel)
	for _, r := range records {
		b, err := json.Marshal(r)
		if err != nil {
			return false, 0, fmt.Errorf("encode state of buyer %q: %w", r.BuyerID, err)
		}
		args = append(args, r.BuyerID, string(b))
	}
	res, err := commitScript.Run(ctx, s.rdb, []string{keyState, keyMeta}, args...).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("commit window %s: %w", w, err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("commit window %s: unexpected script result %v", w, res)
	}
	return res[0] == 1, uint64(res[1]), nil
}

// SubscribeUpdates forwards version numbers published on commit.
func (s *Store) SubscribeUpdates(ctx context.Context) (<-chan uint64, error) {
	sub := s.rdb.Subscribe(ctx, updatesChannel)
	// Wait for the subscription to be confirmed so no update is missed
	// between the caller's initial load and the subscription going live.
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("subscribe to %s: %w", updatesChannel, err)
	}
	out := make(chan uint64, 16)
	go func() {
		defer close(out)
		defer sub.Close()
		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-msgs:
				if !ok {
					return
				}
				v, err := strconv.ParseUint(m.Payload, 10, 64)
				if err != nil {
					continue
				}
				select {
				case out <- v:
				default: // reader is busy; it reloads the latest state anyway
				}
			}
		}
	}()
	return out, nil
}

// acquireScript takes the lease if free, or extends it if already ours.
var acquireScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if not cur then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`)

// releaseScript deletes the lease only if we still own it.
var releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

func (s *Store) TryAcquire(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	n, err := acquireScript.Run(ctx, s.rdb, []string{keyLeader}, owner, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("acquire lease: %w", err)
	}
	return n == 1, nil
}

func (s *Store) Release(ctx context.Context, owner string) error {
	if err := releaseScript.Run(ctx, s.rdb, []string{keyLeader}, owner).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}
