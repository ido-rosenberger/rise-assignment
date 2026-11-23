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
	// MinSampleSize is the fewest requests a window needs before its rates
	// are trusted. Smaller windows never trigger a block on their own.
	MinSampleSize int64
	// BlockLevel is the throttle_rate set on a block. Must stay below 1.0 so
	// traffic to a buyer is never cut to zero.
	BlockLevel float64
	// RecoveryStep is how much throttle_rate drops after each clean window.
	RecoveryStep float64
	// Window is the evaluation window length.
	Window time.Duration
	// MaxAccumulatedWindows caps how many consecutive low-sample windows are
	// merged before judging (our decision, not in the spec). 1 means a
	// low-sample window is never merged with the next one.
	MaxAccumulatedWindows int
}

// Environment variable names for Rules.
const (
	EnvFailureRateThreshold  = "THROTTLE_FAILURE_RATE_THRESHOLD"
	EnvTimeoutRateThreshold  = "THROTTLE_TIMEOUT_RATE_THRESHOLD"
	EnvMinSampleSize         = "THROTTLE_MIN_SAMPLE_SIZE"
	EnvBlockLevel            = "THROTTLE_BLOCK_LEVEL"
	EnvRecoveryStep          = "THROTTLE_RECOVERY_STEP"
	EnvWindow                = "THROTTLE_WINDOW"
	EnvMaxAccumulatedWindows = "THROTTLE_MAX_ACCUMULATED_WINDOWS"
)

// DefaultRules returns the defaults from the spec.
func DefaultRules() Rules {
	return Rules{
		FailureRateThreshold:  0.7,
		TimeoutRateThreshold:  0.3,
		MinSampleSize:         100,
		BlockLevel:            0.95,
		RecoveryStep:          0.1,
		Window:                time.Minute,
		MaxAccumulatedWindows: 5,
	}
}

// LoadRules starts from DefaultRules, overrides any value set in the
// environment, and validates the result.
func LoadRules() (Rules, error) {
	return loadRules(os.LookupEnv)
}

// lookupFunc matches os.LookupEnv. Tests pass a fake so they never touch the
// real process environment.
type lookupFunc func(key string) (string, bool)

func loadRules(lookup lookupFunc) (Rules, error) {
	r := DefaultRules()

	// Collect every parse error instead of stopping at the first, so a
	// misconfigured deployment shows all problems at once.
	var errs []error
	override(lookup, EnvFailureRateThreshold, parseFloat, &r.FailureRateThreshold, &errs)
	override(lookup, EnvTimeoutRateThreshold, parseFloat, &r.TimeoutRateThreshold, &errs)
	override(lookup, EnvMinSampleSize, parseInt64, &r.MinSampleSize, &errs)
	override(lookup, EnvBlockLevel, parseFloat, &r.BlockLevel, &errs)
	override(lookup, EnvRecoveryStep, parseFloat, &r.RecoveryStep, &errs)
	override(lookup, EnvWindow, time.ParseDuration, &r.Window, &errs)
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
// T is a type parameter (generics): one helper works for float64, int64,
// int and time.Duration instead of four copies.
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
	// Spec: traffic is never cut to zero, so block level < 1.0.
	if r.BlockLevel <= 0 || r.BlockLevel >= 1 {
		errs = append(errs, fmt.Errorf("block level must be in (0, 1), got %v", r.BlockLevel))
	}
	if r.RecoveryStep <= 0 || r.RecoveryStep > r.BlockLevel {
		errs = append(errs, fmt.Errorf("recovery step must be in (0, block level %v], got %v", r.BlockLevel, r.RecoveryStep))
	}
	if r.Window <= 0 {
		errs = append(errs, fmt.Errorf("window must be > 0, got %v", r.Window))
	}
	if r.MaxAccumulatedWindows < 1 {
		errs = append(errs, fmt.Errorf("max accumulated windows must be >= 1, got %d", r.MaxAccumulatedWindows))
	}
	return errors.Join(errs...)
}
