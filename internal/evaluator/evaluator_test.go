package evaluator

import (
	"testing"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

// c builds Stats tersely: c(total, failed, timeouts).
func c(total, failed, timeouts int64) buyer.Stats {
	return buyer.Stats{Total: total, Failed: failed, Timeouts: timeouts}
}

// Shorthand states used across cases.
var (
	open     = State{}
	blockedF = State{ThrottleRate: 0.95, Reason: ReasonFailureRate}
	blockedT = State{ThrottleRate: 0.95, Reason: ReasonTimeoutRate}
)

func recovering(rate float64) State { return State{ThrottleRate: rate, Reason: ReasonRecovering} }

func TestEvaluate(t *testing.T) {
	rules := config.DefaultRules() // fail 0.7, timeout 0.3, min 100, block 0.95, step 0.1, cap 5

	cases := []struct {
		name        string
		prev        State
		windowStats buyer.Stats
		want        State
	}{
		// Block rules.
		{"healthy stays open", open, c(1000, 10, 10), open},
		{"failure rate at threshold blocks", open, c(100, 70, 0), blockedF},
		{"failure rate just below threshold", open, c(100, 69, 0), open},
		{"timeout rate at threshold blocks", open, c(100, 0, 30), blockedT},
		{"timeout rate just below threshold", open, c(100, 0, 29), open},
		{"both thresholds: failure reason wins", open, c(100, 70, 30), blockedF},
		{"no combined rate: 50% failed + 25% timeout is not a block", open, c(100, 50, 25), open},
		{"block while blocked stays at block level, reason updates", blockedF, c(100, 0, 40), blockedT},

		// Minimum sample.
		{"below min sample never blocks, starts accumulating", open, c(99, 99, 0),
			State{Accumulated: c(99, 99, 0), AccumulatedWindows: 1}},
		{"exactly min sample is judged", open, c(100, 100, 0), blockedF},
		{"empty window accumulates", open, c(0, 0, 0), State{AccumulatedWindows: 1}},

		// Recovery steps.
		{"first clean window after block", blockedF, c(100, 0, 0), recovering(0.85)},
		{"clean window mid recovery", recovering(0.35), c(500, 5, 5), recovering(0.25)},
		{"last step reaches fully open", recovering(0.05), c(100, 0, 0), open},
		{"step never goes below zero", recovering(0.04), c(100, 0, 0), open},

		// Snap-back.
		{"violation mid recovery snaps back to block", recovering(0.45), c(200, 150, 0), blockedF},
		{"timeout mid recovery snaps back to block", recovering(0.05), c(200, 0, 60), blockedT},

		// Accumulation of low-sample windows.
		{"small window while blocked: hold rate, keep accumulating", blockedF, c(50, 0, 0),
			State{ThrottleRate: 0.95, Reason: ReasonFailureRate, Accumulated: c(50, 0, 0), AccumulatedWindows: 1}},
		{"accumulated sample reaches min and is clean: step down, reset",
			State{ThrottleRate: 0.95, Reason: ReasonFailureRate, Accumulated: c(50, 0, 0), AccumulatedWindows: 1},
			c(50, 1, 0), recovering(0.85)},
		{"accumulated sample reaches min and violates: block, reset",
			State{ThrottleRate: 0.45, Reason: ReasonRecovering, Accumulated: c(50, 40, 0), AccumulatedWindows: 1},
			c(50, 40, 0), blockedF},
		{"full failing window is judged alone, not diluted by clean pending",
			State{ThrottleRate: 0.45, Reason: ReasonRecovering, Accumulated: c(90, 0, 0), AccumulatedWindows: 1},
			c(100, 70, 0), blockedF},
		{"full clean window is judged alone, not skewed by bad pending",
			State{ThrottleRate: 0.45, Reason: ReasonRecovering, Accumulated: c(99, 99, 0), AccumulatedWindows: 1},
			c(100, 45, 0), recovering(0.35)}, // merged would be 144/199 = 72% -> wrong block
		{"accumulated healthy buyer can be blocked",
			State{Accumulated: c(60, 50, 0), AccumulatedWindows: 2},
			c(40, 30, 0), blockedF},

		// Accumulation cap reached, sample still below min.
		{"cap reached, looks clean: step down, reset",
			State{ThrottleRate: 0.95, Reason: ReasonFailureRate, Accumulated: c(40, 0, 0), AccumulatedWindows: 4},
			c(10, 0, 0), recovering(0.85)},
		{"cap reached, looks bad: hold, no block, reset",
			State{ThrottleRate: 0.45, Reason: ReasonRecovering, Accumulated: c(40, 35, 0), AccumulatedWindows: 4},
			c(10, 10, 0), recovering(0.45)},
		{"cap reached, looks bad while open: stays open (small sample cannot block)",
			State{Accumulated: c(40, 40, 0), AccumulatedWindows: 4},
			c(10, 10, 0), open},
		{"cap reached, zero calls: hold, reset",
			State{ThrottleRate: 0.95, Reason: ReasonFailureRate, AccumulatedWindows: 4},
			c(0, 0, 0), blockedF},

		// Config changes between evaluations.
		{"rate above block level is clamped before stepping",
			State{ThrottleRate: 0.99, Reason: ReasonFailureRate}, c(100, 0, 0), recovering(0.85)},
		{"negative stored rate is clamped to zero",
			State{ThrottleRate: -0.2}, c(10, 0, 0), State{Accumulated: c(10, 0, 0), AccumulatedWindows: 1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(tc.prev, tc.windowStats, rules)
			if got != tc.want {
				t.Errorf("Evaluate(%+v, %+v)\n got: %+v\nwant: %+v", tc.prev, tc.windowStats, got, tc.want)
			}
		})
	}
}

// Full recovery from block to open, checking every step lands on an exact
// value (no float drift) and takes exactly 10 clean windows.
func TestFullRecoverySequence(t *testing.T) {
	rules := config.DefaultRules()
	clean := c(100, 0, 0)

	s := Evaluate(open, c(100, 80, 0), rules)
	want := []float64{0.95, 0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15, 0.05, 0}
	for i, w := range want {
		if s.ThrottleRate != w {
			t.Fatalf("step %d: rate = %v, want %v", i, s.ThrottleRate, w)
		}
		s = Evaluate(s, clean, rules)
	}
	if s != open {
		t.Fatalf("after full recovery: %+v, want open", s)
	}
}

// Minute-by-minute: block, recovery through accumulated small windows,
// snap-back, then accumulation starts again.
func TestRecoveryTimeline(t *testing.T) {
	rules := config.DefaultRules()
	steps := []struct {
		windowStats buyer.Stats
		want        float64
	}{
		{c(1000, 100, 0), 0},    // healthy
		{c(1000, 800, 0), 0.95}, // block
		{c(50, 2, 0), 0.95},     // 50 calls: below min, accumulate, hold
		{c(50, 1, 0), 0.85},     // 100 accumulated, clean: step
		{c(150, 3, 0), 0.75},    // clean: step
		{c(250, 190, 0), 0.95},  // snap-back
		{c(50, 0, 0), 0.95},     // accumulate again
	}
	s := open
	for i, st := range steps {
		s = Evaluate(s, st.windowStats, rules)
		if s.ThrottleRate != st.want {
			t.Fatalf("minute %d: rate = %v, want %v (state %+v)", i+1, s.ThrottleRate, st.want, s)
		}
	}
}

// Non-default rules: no accumulation (cap 1) and a custom step.
func TestCustomRules(t *testing.T) {
	rules := config.DefaultRules()
	rules.MaxAccumulatedWindows = 1
	rules.RecoveryStepSize = 0.3

	// Cap 1: a small window is judged by the cap rule right away.
	got := Evaluate(blockedF, c(10, 0, 0), rules)
	if want := recovering(0.65); got != want {
		t.Errorf("cap 1, small clean window: got %+v, want %+v", got, want)
	}

	// Step 0.3 from 0.95: 0.65, 0.35, 0.05, then open.
	s := blockedF
	for _, w := range []float64{0.65, 0.35, 0.05, 0} {
		s = Evaluate(s, c(100, 0, 0), rules)
		if s.ThrottleRate != w {
			t.Fatalf("rate = %v, want %v", s.ThrottleRate, w)
		}
	}
}
