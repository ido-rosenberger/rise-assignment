package evaluator

import (
	"math"

	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

// Stats are a buyer's request outcomes over one or more evaluation windows.
// Failed and Timeouts are disjoint: each request is ok, failed, or a timeout.
type Stats struct {
	Total    int64
	Failed   int64
	Timeouts int64
}

// Add returns the sum of two Stats.
func (s Stats) Add(o Stats) Stats {
	return Stats{
		Total:    s.Total + o.Total,
		Failed:   s.Failed + o.Failed,
		Timeouts: s.Timeouts + o.Timeouts,
	}
}

// Reason explains a buyer's current throttle rate. Values match the "reason"
// field of GET /state.
type Reason string

const (
	ReasonNone        Reason = ""             // fully open
	ReasonFailureRate Reason = "failure_rate" // blocked: failure rate reached threshold
	ReasonTimeoutRate Reason = "timeout_rate" // blocked: timeout rate reached threshold
	ReasonRecovering  Reason = "recovering"   // stepping down after a block
)

// State is a buyer's throttle state, carried from one evaluation to the next.
// The zero value is a fully open buyer with nothing accumulated, which is also
// the state of an unknown buyer.
type State struct {
	// ThrottleRate is the fraction of requests to skip, in [0, BlockLevel].
	ThrottleRate float64
	Reason       Reason
	// Accumulated holds the stats of recent windows that were below the
	// minimum sample size and have not been judged yet.
	Accumulated Stats
	// AccumulatedWindows is the number of windows merged into Accumulated.
	AccumulatedWindows int
}

// Evaluate computes a buyer's next state from its previous state and the
// stats of the window that just closed. It has no side effects.
//
// Rules, in order:
//  1. Pick the sample: a window that alone reaches MinSampleSize is judged
//     alone (older accumulated windows would only dilute or skew it); a
//     smaller window is merged with the accumulated windows.
//  2. Sample >= MinSampleSize: judge it. A violation blocks (throttle rate
//     jumps to BlockLevel, even mid-recovery); a clean sample steps down.
//  3. Too small, cap not reached: keep accumulating, throttle rate unchanged.
//  4. Too small, cap reached: never block. Step down only if the sample has
//     requests and is under both thresholds; otherwise hold. Accumulation resets.
func Evaluate(prev State, windowStats Stats, rules config.Rules) State {
	// The block level may have been lowered since the state was stored, or
	// the stored state may be corrupt.
	throttleRate := math.Max(0, math.Min(prev.ThrottleRate, rules.BlockLevel))

	sample, windows := windowStats, 1
	if windowStats.Total < rules.MinSampleSize {
		sample = prev.Accumulated.Add(windowStats)
		windows = prev.AccumulatedWindows + 1
	}

	if sample.Total >= rules.MinSampleSize {
		if reason := violation(sample, rules); reason != ReasonNone {
			return State{ThrottleRate: rules.BlockLevel, Reason: reason}
		}
		return stepDown(throttleRate, rules)
	}

	if windows < rules.MaxAccumulatedWindows {
		return State{
			ThrottleRate:       throttleRate,
			Reason:             prev.Reason,
			Accumulated:        sample,
			AccumulatedWindows: windows,
		}
	}

	// Cap reached, sample still below MinSampleSize.
	if sample.Total == 0 || violation(sample, rules) != ReasonNone {
		return State{ThrottleRate: throttleRate, Reason: prev.Reason} // hold
	}
	return stepDown(throttleRate, rules)
}

// violation reports which threshold the sample reaches, or ReasonNone.
// Failure rate is checked first, so it wins when both are reached.
func violation(s Stats, rules config.Rules) Reason {
	if s.Total == 0 {
		return ReasonNone
	}
	total := float64(s.Total)
	if float64(s.Failed)/total >= rules.FailureRateThreshold {
		return ReasonFailureRate
	}
	if float64(s.Timeouts)/total >= rules.TimeoutRateThreshold {
		return ReasonTimeoutRate
	}
	return ReasonNone
}

// stepDown lowers the throttle rate by one recovery step; 0 means fully open.
func stepDown(throttleRate float64, rules config.Rules) State {
	next := roundThrottleRate(throttleRate - rules.RecoveryStepSize)
	if next <= 0 {
		return State{}
	}
	return State{ThrottleRate: next, Reason: ReasonRecovering}
}

// roundThrottleRate removes float drift (0.95 - 0.1 = 0.8499999999999999).
func roundThrottleRate(r float64) float64 {
	return math.Round(r*config.ThrottleRateScale) / config.ThrottleRateScale
}
