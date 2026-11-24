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

// Store is what the Runner needs from shared storage. internal/store
// provides the Redis implementation.
type Store interface {
	// WindowStats returns every buyer's stats for one window (empty if none).
	WindowStats(ctx context.Context, windowStart time.Time) (map[string]buyer.Stats, error)
	// States returns every known buyer's state.
	States(ctx context.Context) (map[string]State, error)
	// LastWindow returns the last evaluated window start; ok is false if none.
	LastWindow(ctx context.Context) (windowStart time.Time, ok bool, err error)
	// SaveStates atomically stores the states produced by evaluating
	// windowStart and records windowStart as evaluated.
	SaveStates(ctx context.Context, windowStart time.Time, states map[string]State) error
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
}

// Runner evaluates every buyer once per evaluation window: it reads the
// window's stats and the previous states, applies Evaluate, and saves the
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
// first: each evaluation builds on the previous one's states, so the newest
// data must be applied last. On first start only the latest closed window is
// evaluated. A window is closed once its end + Grace has passed.
func (r *Runner) EvaluatePending(ctx context.Context) error {
	w := r.cfg.Rules.EvaluationWindow
	latest := r.now().Add(-r.cfg.Grace).Add(-w).Truncate(w)

	last, ok, err := r.store.LastWindow(ctx)
	if err != nil {
		return fmt.Errorf("read last window: %w", err)
	}
	from := latest
	if ok {
		// Truncate first: if the window size changed since last was stored,
		// last may not sit on a boundary of the current window size.
		from = last.Truncate(w).Add(w)
	}
	if oldest := latest.Add(-time.Duration(r.cfg.MaxCatchUpWindows-1) * w); from.Before(oldest) {
		skipped := int(oldest.Sub(from) / w)
		r.log.Warn("skipping windows older than catch-up limit", "skipped", skipped, "from", oldest)
		from = oldest
	}

	for start := from; !start.After(latest); start = start.Add(w) {
		if err := r.evaluateWindow(ctx, start); err != nil {
			return err
		}
	}
	return nil
}

// evaluateWindow applies Evaluate to every buyer that has a state or stats
// for this window. A known buyer without stats gets an empty window.
func (r *Runner) evaluateWindow(ctx context.Context, windowStart time.Time) error {
	stats, err := r.store.WindowStats(ctx, windowStart)
	if err != nil {
		return fmt.Errorf("read stats of window %s: %w", windowStart.Format(time.RFC3339), err)
	}
	prev, err := r.store.States(ctx)
	if err != nil {
		return fmt.Errorf("read states: %w", err)
	}

	next := make(map[string]State, len(prev)+len(stats))
	changed := 0
	for _, buyerID := range unionKeys(prev, stats) {
		before, windowStats := prev[buyerID], stats[buyerID]
		after := Evaluate(before, windowStats, r.cfg.Rules)
		next[buyerID] = after
		if after.ThrottleRate != before.ThrottleRate || after.Reason != before.Reason {
			changed++
			r.log.Info("throttle state changed",
				"buyer", buyerID,
				"window", windowStart.Format(time.TimeOnly),
				"total", windowStats.Total,
				"failed", windowStats.Failed,
				"timeouts", windowStats.Timeouts,
				// Decision may use this window merged with earlier small ones.
				"accumulated_total", before.Accumulated.Total,
				"accumulated_windows", before.AccumulatedWindows,
				"from", before.ThrottleRate,
				"to", after.ThrottleRate,
				"reason", string(after.Reason))
		}
	}

	if err := r.store.SaveStates(ctx, windowStart, next); err != nil {
		return fmt.Errorf("save states of window %s: %w", windowStart.Format(time.RFC3339), err)
	}
	r.log.Info("window evaluated", "window", windowStart.Format(time.TimeOnly), "buyers", len(next), "changed", changed)
	return nil
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
