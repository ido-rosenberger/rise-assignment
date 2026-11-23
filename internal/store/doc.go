// Package store defines how stats and throttle state are persisted and shared
// between auction pods and the evaluator, plus a Redis implementation.
// The throttler and evaluator depend on the interfaces here, not on Redis.
//
// Implemented in Phase 3.
package store
