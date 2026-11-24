// Package store is the evaluator's Redis adapter: it reads window stats and
// buyer states, and writes new states. The pods' side is
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

// States returns every known buyer's evaluator state, merged from the state
// hash (throttle rate, reason) and the eval hash (accumulation). Both are
// read in one MULTI/EXEC, so they come from the same write. An entry that
// cannot be decoded is logged and skipped (that part of the state starts
// from zero) so one corrupt buyer does not stop the evaluator.
func (s *Store) States(ctx context.Context) (map[string]evaluator.State, error) {
	var stateCmd, evalCmd *redis.MapStringStringCmd
	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		stateCmd = p.HGetAll(ctx, schema.KeyState)
		evalCmd = p.HGetAll(ctx, schema.KeyEval)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read states: %w", err)
	}

	out := make(map[string]evaluator.State)
	for buyerID, raw := range stateCmd.Val() {
		var v schema.StateValue
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			s.log.Warn("skipping corrupt state entry", "key", schema.KeyState, "buyer", buyerID, "err", err)
			continue
		}
		st := out[buyerID]
		st.ThrottleRate, st.Reason = v.ThrottleRate, evaluator.Reason(v.Reason)
		out[buyerID] = st
	}
	for buyerID, raw := range evalCmd.Val() {
		var v schema.EvalValue
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			s.log.Warn("skipping corrupt state entry", "key", schema.KeyEval, "buyer", buyerID, "err", err)
			continue
		}
		st := out[buyerID]
		st.Accumulated, st.AccumulatedWindows = v.Accumulated.Stats(), v.AccumulatedWindows
		out[buyerID] = st
	}
	return out, nil
}

// LastWindow returns the start of the last evaluated window; ok is false if
// no window was evaluated yet.
func (s *Store) LastWindow(ctx context.Context) (windowStart time.Time, ok bool, err error) {
	unix, err := s.client.Get(ctx, schema.KeyLastWindow).Int64()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("get %s: %w", schema.KeyLastWindow, err)
	}
	return time.Unix(unix, 0).UTC(), true, nil
}

// SaveStates writes the states produced by evaluating windowStart, bumps the
// state version and records windowStart as evaluated, all in one MULTI/EXEC:
// pods and a restarted evaluator never see half of an evaluation.
func (s *Store) SaveStates(ctx context.Context, windowStart time.Time, states map[string]evaluator.State) error {
	stateFields := make(map[string]any, len(states))
	evalFields := make(map[string]any, len(states))
	for buyerID, st := range states {
		sv, err := json.Marshal(schema.StateValue{ThrottleRate: st.ThrottleRate, Reason: string(st.Reason)})
		if err != nil {
			return fmt.Errorf("encode state of %s: %w", buyerID, err)
		}
		ev, err := json.Marshal(schema.EvalValue{
			Accumulated:        schema.ToStatsValue(st.Accumulated),
			AccumulatedWindows: st.AccumulatedWindows,
		})
		if err != nil {
			return fmt.Errorf("encode eval state of %s: %w", buyerID, err)
		}
		stateFields[buyerID], evalFields[buyerID] = sv, ev
	}

	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		if len(states) > 0 { // HSET with no fields is an error
			p.HSet(ctx, schema.KeyState, stateFields)
			p.HSet(ctx, schema.KeyEval, evalFields)
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
