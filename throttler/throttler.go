// Package throttler is the auction-side throttling library. Every auction pod
// creates one Throttler, asks ShouldCall before each buyer call and reports
// the outcome with Record. Both work on local memory only; Run is a
// background loop that sends stats to the shared store and pulls throttle
// rates from it.
package throttler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// Result is the outcome of one buyer call.
type Result int

const (
	ResultOK      Result = iota // the buyer answered (2xx)
	ResultFailed                // the buyer returned an error
	ResultTimeout               // the buyer missed the auction deadline
)

// String returns the result as used in logs: ok, failed or timeout.
func (r Result) String() string {
	switch r {
	case ResultOK:
		return "ok"
	case ResultFailed:
		return "failed"
	case ResultTimeout:
		return "timeout"
	default:
		return fmt.Sprintf("Result(%d)", int(r))
	}
}

// Store is what a Throttler needs from shared storage.
// throttler/redisstore provides a Redis implementation.
type Store interface {
	// AddStats adds per-buyer stats to the evaluation window that starts at
	// windowStart. It must be additive: every pod calls it for the same window.
	AddStats(ctx context.Context, windowStart time.Time, stats map[string]buyer.Stats) error
	// StateVersion returns a number that changes whenever throttle rates change.
	StateVersion(ctx context.Context) (int64, error)
	// ThrottleRates returns every buyer's throttle rate and its state version.
	ThrottleRates(ctx context.Context) (map[string]float64, int64, error)
}

// Config configures a Throttler. Zero values take the defaults.
type Config struct {
	// EvaluationWindow must match the evaluator's. Default 1m.
	EvaluationWindow time.Duration
	// FlushInterval is how often stats are sent to the store. Must divide
	// EvaluationWindow evenly. Default 1s.
	FlushInterval time.Duration
	// RefreshInterval is how often the state version is checked. Default 1s.
	RefreshInterval time.Duration
	// StoreTimeout bounds each store call. Default 1s.
	StoreTimeout time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.EvaluationWindow == 0 {
		c.EvaluationWindow = time.Minute
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = time.Second
	}
	if c.RefreshInterval == 0 {
		c.RefreshInterval = time.Second
	}
	if c.StoreTimeout == 0 {
		c.StoreTimeout = time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

func (c Config) validate() error {
	var errs []error
	if c.EvaluationWindow < 0 || c.FlushInterval < 0 || c.RefreshInterval < 0 || c.StoreTimeout < 0 {
		errs = append(errs, errors.New("durations must not be negative"))
	}
	// Flush ticks then land on every window boundary, so each flush interval
	// lies inside exactly one window.
	if c.FlushInterval > 0 && c.EvaluationWindow%c.FlushInterval != 0 {
		errs = append(errs, fmt.Errorf("evaluation window %v must be a multiple of flush interval %v", c.EvaluationWindow, c.FlushInterval))
	}
	return errors.Join(errs...)
}

// Throttler decides per request whether to call a buyer and collects the
// outcomes. ShouldCall and Record are safe for concurrent use; Run must be
// started once.
type Throttler struct {
	cfg   Config
	store Store
	log   *slog.Logger

	// Hot path. Both maps are immutable once stored: a change builds a new
	// map and swaps the pointer, so readers never lock.
	rates    atomic.Pointer[map[string]float64]   // buyer -> throttle rate
	counters atomic.Pointer[map[string]*counters] // buyer -> outcomes since last flush
	addMu    sync.Mutex                           // serializes adding a new buyer to counters

	// Owned by the Run goroutine.
	version     int64
	loaded      bool
	unsent      map[time.Time]map[string]buyer.Stats // window start -> stats not yet stored
	now         func() time.Time
	randomFloat func() float64
}

type counters struct {
	total, failed, timeouts atomic.Int64
}

// New returns a Throttler that calls every buyer until Run loads throttle
// rates from the store.
func New(cfg Config, store Store) (*Throttler, error) {
	if store == nil {
		return nil, errors.New("throttler: store must not be nil")
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("throttler config: %w", err)
	}
	t := &Throttler{
		cfg:         cfg,
		store:       store,
		log:         cfg.Logger,
		unsent:      make(map[time.Time]map[string]buyer.Stats),
		now:         time.Now,
		randomFloat: rand.Float64,
	}
	t.rates.Store(&map[string]float64{})
	t.counters.Store(&map[string]*counters{})
	return t, nil
}

// ShouldCall reports whether to send this request to the buyer. A buyer with
// throttle rate r is skipped with probability r. Unknown buyers are called.
func (t *Throttler) ShouldCall(buyerID string) bool {
	rate := (*t.rates.Load())[buyerID]
	if !(rate > 0) { // also true for NaN: never cut a buyer to zero by accident
		return true
	}
	return t.randomFloat() >= rate
}

// Record counts the outcome of one buyer call. Unknown Result values count as
// ok.
func (t *Throttler) Record(buyerID string, result Result) {
	c := t.countersFor(buyerID)
	// Total first; flush swaps outcomes first. Any outcome a flush sees then
	// has its total in the same or an earlier flush, so failed <= total.
	c.total.Add(1)
	switch result {
	case ResultFailed:
		c.failed.Add(1)
	case ResultTimeout:
		c.timeouts.Add(1)
	}
}

// countersFor returns the buyer's counters, creating them on first use.
// Creation copies the map (copy-on-write); it happens once per buyer.
func (t *Throttler) countersFor(buyerID string) *counters {
	if c, ok := (*t.counters.Load())[buyerID]; ok {
		return c
	}
	t.addMu.Lock()
	defer t.addMu.Unlock()
	current := *t.counters.Load()
	if c, ok := current[buyerID]; ok { // added while we waited for the lock
		return c
	}
	next := make(map[string]*counters, len(current)+1)
	maps.Copy(next, current)
	c := &counters{}
	next[buyerID] = c
	t.counters.Store(&next)
	return c
}
