// Package store is the evaluator's Redis adapter: it reads window stats and
// a snapshot of buyer states, and writes new states. The pods' side is
// throttler/redisstore; both use the layout in internal/store/schema.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/evaluator"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
)

// Compile-time check that Store implements evaluator.Store.
var _ evaluator.Store = (*Store)(nil)

// Store is the evaluator's view of Redis. Safe for concurrent use.
type Store struct {
	client redis.UniversalClient
	log    *slog.Logger
}

// New returns a Store using client. The caller owns the client and closes it.
// logger reports malformed entries that are skipped; nil means slog.Default().
func New(client redis.UniversalClient, logger *slog.Logger) *Store {
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{client: client, log: logger}
}

// WindowStats returns every buyer's stats for the window starting at
// windowStart. A window nobody reported (or already expired) is empty.
// Malformed fields are logged and skipped.
func (s *Store) WindowStats(ctx context.Context, windowStart time.Time) (map[string]buyer.Stats, error) {
	key := schema.StatsKey(windowStart)
	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall %s: %w", key, err)
	}
	stats, invalid := schema.ParseStats(fields)
	for _, err := range invalid {
		s.log.Warn("skipping malformed stats field", "key", key, "err", err)
	}
	return stats, nil
}

// Snapshot reads every buyer's state and the last evaluated window in one
// MULTI/EXEC, so both come from the same evaluation. An entry that cannot be
// decoded is logged and skipped (that buyer starts from zero) so one corrupt
// buyer does not stop the evaluator.
func (s *Store) Snapshot(ctx context.Context) (evaluator.Snapshot, error) {
	var statesCmd *redis.MapStringStringCmd
	var lastCmd *redis.StringCmd
	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		statesCmd = p.HGetAll(ctx, schema.KeyState)
		lastCmd = p.Get(ctx, schema.KeyLastWindow)
		return nil
	})
	// A missing last-window key makes the transaction report redis.Nil;
	// check each command's own error instead.
	if err != nil && !errors.Is(err, redis.Nil) {
		return evaluator.Snapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	if err := statesCmd.Err(); err != nil {
		return evaluator.Snapshot{}, fmt.Errorf("hgetall %s: %w", schema.KeyState, err)
	}

	snap := evaluator.Snapshot{States: make(map[string]evaluator.State, len(statesCmd.Val()))}
	if unix, err := lastCmd.Int64(); err == nil {
		snap.LastWindow = time.Unix(unix, 0).UTC()
	} else if !errors.Is(err, redis.Nil) {
		return evaluator.Snapshot{}, fmt.Errorf("parse %s: %w", schema.KeyLastWindow, err)
	}
	for buyerID, raw := range statesCmd.Val() {
		var v schema.StateValue
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			s.log.Warn("skipping corrupt state entry", "key", schema.KeyState, "buyer", buyerID, "err", err)
			continue
		}
		snap.States[buyerID] = evaluator.State{
			ThrottleRate:       v.ThrottleRate,
			Reason:             evaluator.Reason(v.Reason),
			Accumulated:        v.Accumulated.Stats(),
			AccumulatedWindows: v.AccumulatedWindows,
		}
	}
	return snap, nil
}

// SaveStates writes the states produced by evaluating windowStart, bumps the
// state version and records windowStart as evaluated, all in one MULTI/EXEC:
// pods and a restarted evaluator never see half of an evaluation.
func (s *Store) SaveStates(ctx context.Context, windowStart time.Time, states map[string]evaluator.State) error {
	fields := make(map[string]any, len(states))
	for buyerID, st := range states {
		v, err := json.Marshal(schema.StateValue{
			ThrottleRate:       st.ThrottleRate,
			Reason:             string(st.Reason),
			Accumulated:        schema.ToStatsValue(st.Accumulated),
			AccumulatedWindows: st.AccumulatedWindows,
		})
		if err != nil {
			return fmt.Errorf("encode state of %s: %w", buyerID, err)
		}
		fields[buyerID] = v
	}

	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if len(fields) > 0 { // HSET with no fields is an error
			p.HSet(ctx, schema.KeyState, fields)
		}
		p.Incr(ctx, schema.KeyVersion)
		p.Set(ctx, schema.KeyLastWindow, strconv.FormatInt(windowStart.Unix(), 10), 0)
		return nil
	})
	if err != nil {
		return fmt.Errorf("save states for window %d: %w", windowStart.Unix(), err)
	}
	return nil
}
