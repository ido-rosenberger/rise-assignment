// Package throttler is the auction-side library imported by every auction pod.
// It exposes ShouldCall (hot path: local memory only) and Record (local atomic
// counters), and runs background loops that flush stats to the store and
// refresh throttle state from it.
//
// Implemented in Phase 2.
package throttler
