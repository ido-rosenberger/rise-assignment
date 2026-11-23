// Package evaluator holds the pure throttling rules: given a buyer's previous
// state and the stats of the window that just closed, it computes the buyer's
// next throttle state. It does no IO, so every rule is unit-testable.
//
// Implemented in Phase 1.
package evaluator
