package evaluator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

// Store is what the evaluator needs from shared storage. internal/store
// provides the Redis implementation.
type Store interface {
	// Snapshot returns every buyer's state and the window they came from,
	// read atomically.
	Snapshot(ctx context.Context) (Snapshot, error)
	// WindowStats returns every buyer's stats for one window (empty if none).
	WindowStats(ctx context.Context, windowStart time.Time) (map[string]buyer.Stats, error)
	// SaveStates atomically stores the states produced by evaluating
	// windowStart and records windowStart as the last evaluated window.
	SaveStates(ctx context.Context, windowStart time.Time, states map[string]State) error
}

// Snapshot is the stored evaluator state at one moment.
type Snapshot struct {
	States map[string]State
	// LastWindow is the start of the last evaluated window; zero if none.
	LastWindow time.Time
}

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	Rules config.Rules
	// Grace is how long after a window ends the Runner waits before
	// evaluating it, so the pods' last flushes for that window land first.
	// Should exceed the pods' flush interval plus store timeout.
	Grace time.Duration
	// MaxCatchUpWindows bounds catch-up after downtime: older windows'
	// stats have expired from the store anyway.
	MaxCatchUpWindows int
	Logger            *slog.Logger
	// Metrics, if set, is updated after every evaluated window.
	Metrics *Metrics
}

// Runner evaluates every buyer once per evaluation window: it reads the
// window's stats, applies Evaluate to the previous states, and saves the
// result. Only one Runner may run against a store at a time.
type Runner struct {
	cfg   RunnerConfig
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// NewRunner validates cfg and returns a Runner.
func NewRunner(cfg RunnerConfig, store Store) (*Runner, error) {
	if store == nil {
		return nil, errors.New("evaluator: store must not be nil")
	}
	if err := cfg.Rules.Validate(); err != nil {
		return nil, fmt.Errorf("evaluator rules: %w", err)
	}
	if cfg.Grace < 0 {
		return nil, fmt.Errorf("evaluator: grace must not be negative, got %v", cfg.Grace)
	}
	if cfg.MaxCatchUpWindows < 1 {
		return nil, fmt.Errorf("evaluator: max catch-up windows must be >= 1, got %d", cfg.MaxCatchUpWindows)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Runner{cfg: cfg, store: store, log: cfg.Logger, now: time.Now}, nil
}

// Run evaluates pending windows right away, then once per window (at each
// window end + Grace) until ctx is canceled. A failed evaluation is logged
// and retried on the next tick; it never stops the loop.
func (r *Runner) Run(ctx context.Context) {
	for {
		if err := r.EvaluatePending(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("evaluation failed, will retry next window", "err", err)
		}
		timer := time.NewTimer(r.nextRun().Sub(r.now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// nextRun is the next window end + Grace after now.
func (r *Runner) nextRun() time.Time {
	w := r.cfg.Rules.EvaluationWindow
	return r.now().Add(-r.cfg.Grace).Truncate(w).Add(w).Add(r.cfg.Grace)
}

// EvaluatePending evaluates every closed window not evaluated yet, oldest
// first. Each window's result is the next window's input, so the newest
// data is applied last.
func (r *Runner) EvaluatePending(ctx context.Context) error {
	snap, err := r.store.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	states := snap.States
	for _, windowStart := range r.windowsToEvaluate(snap.LastWindow) {
		stats, err := r.store.WindowStats(ctx, windowStart)
		if err != nil {
			return fmt.Errorf("read stats of window %s: %w", windowStart.Format(time.RFC3339), err)
		}
		next, changes := r.evaluateAll(states, stats)
		if err := r.store.SaveStates(ctx, windowStart, next); err != nil {
			return fmt.Errorf("save states of window %s: %w", windowStart.Format(time.RFC3339), err)
		}
		// Logs and metrics only after the save succeeded.
		r.logWindow(windowStart, len(next), changes)
		r.cfg.Metrics.observeWindow(windowStart, next, stats)
		states = next
	}
	return nil
}

// windowsToEvaluate returns the window starts to evaluate now, oldest first:
// every window after lastWindow whose end + Grace has passed, at most
// MaxCatchUpWindows of them. With no lastWindow (first start), only the
// latest closed window.
func (r *Runner) windowsToEvaluate(lastWindow time.Time) []time.Time {
	w := r.cfg.Rules.EvaluationWindow
	latest := r.now().Add(-r.cfg.Grace).Add(-w).Truncate(w)

	from := latest
	if !lastWindow.IsZero() {
		// Truncate first: if the window size changed since lastWindow was
		// stored, it may not sit on a boundary of the current window size.
		from = lastWindow.Truncate(w).Add(w)
	}
	if oldest := latest.Add(-time.Duration(r.cfg.MaxCatchUpWindows-1) * w); from.Before(oldest) {
		r.log.Warn("skipping windows older than catch-up limit", "skipped", int(oldest.Sub(from)/w), "from", oldest)
		from = oldest
	}

	var windows []time.Time
	for start := from; !start.After(latest); start = start.Add(w) {
		windows = append(windows, start)
	}
	return windows
}

// change is one buyer whose throttle rate or reason changed in a window.
type change struct {
	buyerID       string
	windowStats   buyer.Stats
	before, after State
}

// evaluateAll applies Evaluate to every buyer that has a state or stats for
// this window (a known buyer without stats gets an empty window). It returns
// the new states and the buyers whose throttle rate or reason changed.
func (r *Runner) evaluateAll(prev map[string]State, stats map[string]buyer.Stats) (map[string]State, []change) {
	next := make(map[string]State, len(prev)+len(stats))
	var changes []change
	for _, buyerID := range unionKeys(prev, stats) {
		before, windowStats := prev[buyerID], stats[buyerID]
		after := Evaluate(before, windowStats, r.cfg.Rules)
		next[buyerID] = after
		if after.ThrottleRate != before.ThrottleRate || after.Reason != before.Reason {
			changes = append(changes, change{buyerID, windowStats, before, after})
		}
	}
	return next, changes
}

// logWindow logs one line per changed buyer and a summary for the window.
func (r *Runner) logWindow(windowStart time.Time, buyers int, changes []change) {
	window := windowStart.Format(time.TimeOnly)
	for _, c := range changes {
		r.log.Info("throttle state changed",
			"buyer", c.buyerID,
			"window", window,
			"total", c.windowStats.Total,
			"failed", c.windowStats.Failed,
			"timeouts", c.windowStats.Timeouts,
			// The decision may use this window merged with earlier small ones.
			"accumulated_total", c.before.Accumulated.Total,
			"accumulated_windows", c.before.AccumulatedWindows,
			"from", c.before.ThrottleRate,
			"to", c.after.ThrottleRate,
			"reason", string(c.after.Reason))
	}
	r.log.Info("window evaluated", "window", window, "buyers", buyers, "changed", len(changes))
}

// unionKeys returns the keys of a and b, sorted so logs are stable.
func unionKeys[A, B any](a map[string]A, b map[string]B) []string {
	keys := make([]string, 0, len(a)+len(b))
	for k := range a {
		keys = append(keys, k)
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}
