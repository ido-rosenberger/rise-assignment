package evaluator

import (
	"math"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

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
	Accumulated buyer.Stats
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
func Evaluate(prev State, windowStats buyer.Stats, rules config.Rules) State {
	throttleRate := clampThrottleRate(prev.ThrottleRate, rules)
	sample, windows := pickSample(prev, windowStats, rules)

	switch {
	case sample.Total >= rules.MinSampleSize: // rule 2
		return judge(throttleRate, sample, rules)
	case windows < rules.MaxAccumulatedWindows: // rule 3
		return State{
			ThrottleRate:       throttleRate,
			Reason:             prev.Reason,
			Accumulated:        sample,
			AccumulatedWindows: windows,
		}
	default: // rule 4
		return capReached(throttleRate, prev.Reason, sample, rules)
	}
}

// clampThrottleRate keeps a stored rate inside [0, BlockLevel]: the block
// level may have been lowered since the state was stored, or the stored state
// may be corrupt.
func clampThrottleRate(rate float64, rules config.Rules) float64 {
	return math.Max(0, math.Min(rate, rules.BlockLevel))
}

// pickSample is rule 1. It returns the stats to judge and how many windows
// they span.
func pickSample(prev State, windowStats buyer.Stats, rules config.Rules) (buyer.Stats, int) {
	if windowStats.Total >= rules.MinSampleSize {
		return windowStats, 1
	}
	return prev.Accumulated.Add(windowStats), prev.AccumulatedWindows + 1
}

// judge is rule 2: block on a violation, otherwise step down.
func judge(throttleRate float64, sample buyer.Stats, rules config.Rules) State {
	if reason := violation(sample, rules); reason != ReasonNone {
		return State{ThrottleRate: rules.BlockLevel, Reason: reason}
	}
	return stepDown(throttleRate, rules)
}

// capReached is rule 4: the sample is still below MinSampleSize after
// MaxAccumulatedWindows windows. It never blocks; it steps down only if the
// sample has requests and is under both thresholds, otherwise it holds.
func capReached(throttleRate float64, reason Reason, sample buyer.Stats, rules config.Rules) State {
	if sample.Total == 0 || violation(sample, rules) != ReasonNone {
		return State{ThrottleRate: throttleRate, Reason: reason}
	}
	return stepDown(throttleRate, rules)
}

// violation reports which threshold the sample reaches, or ReasonNone.
// Failure rate is checked first, so it wins when both are reached.
func violation(s buyer.Stats, rules config.Rules) Reason {
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
