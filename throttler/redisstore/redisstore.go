// Package redisstore implements throttler.Store on Redis. It lives apart
// from package throttler so the core library does not depend on a Redis
// client.
package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
	"github.com/ido-rosenberger/rise-assignment/throttler"
)

// Compile-time check that Store implements throttler.Store.
var _ throttler.Store = (*Store)(nil)

// Options configures a Store. Zero values take the defaults.
type Options struct {
	// EvaluationWindow must be the same value as throttler.Config's and the
	// evaluator's. It sets how long stats keys live
	// (schema.StatsRetentionWindows windows), which bounds how far back the
	// evaluator can catch up. Default 1m.
	EvaluationWindow time.Duration
	// Logger reports malformed entries that are skipped. Default slog.Default().
	Logger *slog.Logger
}

// Store is a throttler.Store backed by Redis. Safe for concurrent use.
type Store struct {
	client   redis.UniversalClient
	statsTTL time.Duration
	log      *slog.Logger
}

// New returns a Store using client. The caller owns the client and closes it.
func New(client redis.UniversalClient, opts Options) *Store {
	if opts.EvaluationWindow <= 0 {
		opts.EvaluationWindow = time.Minute
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Store{
		client:   client,
		statsTTL: schema.StatsRetentionWindows * opts.EvaluationWindow,
		log:      opts.Logger,
	}
}

// AddStats adds stats to the window's hash with HINCRBY, so every pod's
// counts sum up in Redis. Sent as one MULTI/EXEC: either all counters are
// added or none, so a retry after an error cannot double-count part of a
// batch. (If EXEC succeeds but the reply is lost, a retry double-counts that
// batch; accepted, it is rare and bounded to one flush.)
func (s *Store) AddStats(ctx context.Context, windowStart time.Time, stats map[string]buyer.Stats) error {
	if len(stats) == 0 {
		return nil
	}
	key := schema.StatsKey(windowStart)
	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for buyerID, st := range stats {
			if buyerID == "" { // would write ":total", unreadable by the evaluator
				s.log.Warn("skipping stats with empty buyer ID")
				continue
			}
			incr(ctx, p, key, schema.StatsField(buyerID, schema.FieldTotal), st.Total)
			incr(ctx, p, key, schema.StatsField(buyerID, schema.FieldFailed), st.Failed)
			incr(ctx, p, key, schema.StatsField(buyerID, schema.FieldTimeouts), st.Timeouts)
		}
		p.Expire(ctx, key, s.statsTTL)
		return nil
	})
	if err != nil {
		return fmt.Errorf("add stats to %s: %w", key, err)
	}
	return nil
}

func incr(ctx context.Context, p redis.Pipeliner, key, field string, n int64) {
	if n != 0 {
		p.HIncrBy(ctx, key, field, n)
	}
}

// StateVersion returns the state version; 0 if the evaluator never wrote.
func (s *Store) StateVersion(ctx context.Context) (int64, error) {
	v, err := s.client.Get(ctx, schema.KeyVersion).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", schema.KeyVersion, err)
	}
	return v, nil
}

// ThrottleRates reads every buyer's throttle rate and the version, in one
// MULTI/EXEC so the two are a consistent snapshot.
func (s *Store) ThrottleRates(ctx context.Context) (map[string]float64, int64, error) {
	var states *redis.MapStringStringCmd
	var version *redis.StringCmd
	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		states = p.HGetAll(ctx, schema.KeyState)
		version = p.Get(ctx, schema.KeyVersion)
		return nil
	})
	// A missing version key makes the transaction report redis.Nil; check
	// each command's own error instead.
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, 0, fmt.Errorf("read throttle state: %w", err)
	}
	if err := states.Err(); err != nil {
		return nil, 0, fmt.Errorf("hgetall %s: %w", schema.KeyState, err)
	}

	v, err := version.Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, 0, fmt.Errorf("parse %s: %w", schema.KeyVersion, err)
	}

	// A corrupt entry is skipped (that buyer reads as unknown, so open)
	// rather than failing the read and freezing every buyer's rate.
	rates := make(map[string]float64, len(states.Val()))
	for buyerID, raw := range states.Val() {
		var sv schema.StateValue
		if err := json.Unmarshal([]byte(raw), &sv); err != nil {
			s.log.Warn("skipping corrupt state entry", "key", schema.KeyState, "buyer", buyerID, "err", err)
			continue
		}
		rates[buyerID] = sv.ThrottleRate
	}
	return rates, v, nil
}
