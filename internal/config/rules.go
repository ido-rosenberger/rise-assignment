// Package config holds the throttling rules and loads them from environment
// variables. Every value in the spec's "Default configuration" table is here.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Rules are the evaluator parameters. All are configurable via env vars.
type Rules struct {
	// FailureRateThreshold blocks a buyer when failed/total >= this value.
	FailureRateThreshold float64
	// TimeoutRateThreshold blocks a buyer when timeouts/total >= this value.
	TimeoutRateThreshold float64
	// MinSampleSize is the fewest requests a sample needs before its rates are
	// trusted. Smaller samples never trigger a block.
	MinSampleSize int64
	// BlockLevel is the throttle rate set on a block. Must stay below 1.0 so
	// traffic to a buyer is never cut to zero.
	BlockLevel float64
	// RecoveryStepSize is how much the throttle rate drops after each clean
	// window.
	RecoveryStepSize float64
	// EvaluationWindow is how often buyers are evaluated, and the span of
	// stats each evaluation reads.
	EvaluationWindow time.Duration
	// MaxAccumulatedWindows caps how many consecutive below-minimum windows
	// are merged before judging (not in the spec). 1 disables merging.
	MaxAccumulatedWindows int
}

// ThrottleRateScale is the precision throttle rates are rounded to (1e6 = 6
// decimal places). Recovery step sizes below 1/ThrottleRateScale would round
// away to nothing, so Validate rejects them.
const ThrottleRateScale = 1e6

// Environment variable names for Rules.
const (
	EnvFailureRateThreshold  = "THROTTLE_FAILURE_RATE_THRESHOLD"
	EnvTimeoutRateThreshold  = "THROTTLE_TIMEOUT_RATE_THRESHOLD"
	EnvMinSampleSize         = "THROTTLE_MIN_SAMPLE_SIZE"
	EnvBlockLevel            = "THROTTLE_BLOCK_LEVEL"
	EnvRecoveryStepSize      = "THROTTLE_RECOVERY_STEP_SIZE"
	EnvEvaluationWindow      = "THROTTLE_EVALUATION_WINDOW"
	EnvMaxAccumulatedWindows = "THROTTLE_MAX_ACCUMULATED_WINDOWS"
)

// DefaultRules returns the defaults from the spec.
func DefaultRules() Rules {
	return Rules{
		FailureRateThreshold:  0.7,
		TimeoutRateThreshold:  0.3,
		MinSampleSize:         100,
		BlockLevel:            0.95,
		RecoveryStepSize:      0.1,
		EvaluationWindow:      time.Minute,
		MaxAccumulatedWindows: 5,
	}
}

// LoadRules starts from DefaultRules, overrides any value set in the
// environment, and validates the result.
func LoadRules() (Rules, error) {
	return loadRules(os.LookupEnv)
}

// lookupFunc matches os.LookupEnv, so tests can pass a fake environment.
type lookupFunc func(key string) (string, bool)

func loadRules(lookup lookupFunc) (Rules, error) {
	r := DefaultRules()

	// Collect every parse error so a misconfigured deployment shows all
	// problems at once.
	var errs []error
	override(lookup, EnvFailureRateThreshold, parseFloat, &r.FailureRateThreshold, &errs)
	override(lookup, EnvTimeoutRateThreshold, parseFloat, &r.TimeoutRateThreshold, &errs)
	override(lookup, EnvMinSampleSize, parseInt64, &r.MinSampleSize, &errs)
	override(lookup, EnvBlockLevel, parseFloat, &r.BlockLevel, &errs)
	override(lookup, EnvRecoveryStepSize, parseFloat, &r.RecoveryStepSize, &errs)
	override(lookup, EnvEvaluationWindow, time.ParseDuration, &r.EvaluationWindow, &errs)
	override(lookup, EnvMaxAccumulatedWindows, strconv.Atoi, &r.MaxAccumulatedWindows, &errs)
	if err := errors.Join(errs...); err != nil {
		return Rules{}, err
	}

	if err := r.Validate(); err != nil {
		return Rules{}, err
	}
	return r, nil
}

// override replaces *dst with the parsed env value when key is set.
func override[T any](lookup lookupFunc, key string, parse func(string) (T, error), dst *T, errs *[]error) {
	raw, ok := lookup(key)
	if !ok {
		return
	}
	v, err := parse(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q: %w", key, raw, err))
		return
	}
	*dst = v
}

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }
func parseInt64(s string) (int64, error)   { return strconv.ParseInt(s, 10, 64) }

// Validate reports every rule that is out of range.
func (r Rules) Validate() error {
	var errs []error
	if r.FailureRateThreshold <= 0 || r.FailureRateThreshold > 1 {
		errs = append(errs, fmt.Errorf("failure rate threshold must be in (0, 1], got %v", r.FailureRateThreshold))
	}
	if r.TimeoutRateThreshold <= 0 || r.TimeoutRateThreshold > 1 {
		errs = append(errs, fmt.Errorf("timeout rate threshold must be in (0, 1], got %v", r.TimeoutRateThreshold))
	}
	if r.MinSampleSize < 1 {
		errs = append(errs, fmt.Errorf("min sample size must be >= 1, got %d", r.MinSampleSize))
	}
	// Traffic to a buyer is never cut to zero.
	if r.BlockLevel <= 0 || r.BlockLevel >= 1 {
		errs = append(errs, fmt.Errorf("block level must be in (0, 1), got %v", r.BlockLevel))
	}
	if r.RecoveryStepSize < 1/ThrottleRateScale || r.RecoveryStepSize > r.BlockLevel {
		errs = append(errs, fmt.Errorf("recovery step size must be in [%v, block level %v], got %v", 1/ThrottleRateScale, r.BlockLevel, r.RecoveryStepSize))
	}
	if r.EvaluationWindow <= 0 {
		errs = append(errs, fmt.Errorf("evaluation window must be > 0, got %v", r.EvaluationWindow))
	}
	if r.MaxAccumulatedWindows < 1 {
		errs = append(errs, fmt.Errorf("max accumulated windows must be >= 1, got %d", r.MaxAccumulatedWindows))
	}
	return errors.Join(errs...)
}
