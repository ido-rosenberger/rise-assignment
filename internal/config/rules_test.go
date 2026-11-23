package config

import (
	"strings"
	"testing"
	"time"
)

// fakeEnv turns a map into a lookupFunc.
func fakeEnv(m map[string]string) lookupFunc {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestDefaultRulesMatchSpec(t *testing.T) {
	r := DefaultRules()
	want := Rules{
		FailureRateThreshold:  0.7,
		TimeoutRateThreshold:  0.3,
		MinSampleSize:         100,
		BlockLevel:            0.95,
		RecoveryStepSize:      0.1,
		EvaluationWindow:      time.Minute,
		MaxAccumulatedWindows: 5,
	}
	if r != want {
		t.Fatalf("DefaultRules() = %+v, want %+v", r, want)
	}
	if err := r.Validate(); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
}

func TestLoadRulesNoEnvGivesDefaults(t *testing.T) {
	r, err := loadRules(fakeEnv(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r != DefaultRules() {
		t.Fatalf("got %+v, want defaults", r)
	}
}

func TestLoadRulesOverrides(t *testing.T) {
	r, err := loadRules(fakeEnv(map[string]string{
		EnvFailureRateThreshold:  "0.5",
		EnvTimeoutRateThreshold:  "0.2",
		EnvMinSampleSize:         "50",
		EnvBlockLevel:            "0.9",
		EnvRecoveryStepSize:      "0.3",
		EnvEvaluationWindow:      "10s",
		EnvMaxAccumulatedWindows: "3",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := Rules{
		FailureRateThreshold:  0.5,
		TimeoutRateThreshold:  0.2,
		MinSampleSize:         50,
		BlockLevel:            0.9,
		RecoveryStepSize:      0.3,
		EvaluationWindow:      10 * time.Second,
		MaxAccumulatedWindows: 3,
	}
	if r != want {
		t.Fatalf("got %+v, want %+v", r, want)
	}
}

func TestLoadRulesReportsAllParseErrors(t *testing.T) {
	_, err := loadRules(fakeEnv(map[string]string{
		EnvMinSampleSize:    "lots",
		EnvEvaluationWindow: "1 minute",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, key := range []string{EnvMinSampleSize, EnvEvaluationWindow} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error should mention %s, got: %v", key, err)
		}
	}
}

// Each case breaks one rule of otherwise valid defaults.
func TestValidateRejectsOutOfRange(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Rules)
	}{
		{"failure threshold zero", func(r *Rules) { r.FailureRateThreshold = 0 }},
		{"failure threshold above one", func(r *Rules) { r.FailureRateThreshold = 1.1 }},
		{"timeout threshold zero", func(r *Rules) { r.TimeoutRateThreshold = 0 }},
		{"min sample zero", func(r *Rules) { r.MinSampleSize = 0 }},
		{"block level one cuts traffic to zero", func(r *Rules) { r.BlockLevel = 1 }},
		{"block level zero", func(r *Rules) { r.BlockLevel = 0 }},
		{"recovery step size zero", func(r *Rules) { r.RecoveryStepSize = 0 }},
		{"recovery step size below rate precision", func(r *Rules) { r.RecoveryStepSize = 1e-7 }},
		{"recovery step size above block level", func(r *Rules) { r.RecoveryStepSize = 0.96 }},
		{"evaluation window zero", func(r *Rules) { r.EvaluationWindow = 0 }},
		{"max accumulated zero", func(r *Rules) { r.MaxAccumulatedWindows = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := DefaultRules()
			tc.mutate(&r)
			if err := r.Validate(); err == nil {
				t.Fatalf("expected validation error for %+v", r)
			}
		})
	}
}

func TestLoadRulesRejectsInvalidValue(t *testing.T) {
	_, err := loadRules(fakeEnv(map[string]string{EnvBlockLevel: "1.0"}))
	if err == nil {
		t.Fatal("block level 1.0 must be rejected")
	}
}
