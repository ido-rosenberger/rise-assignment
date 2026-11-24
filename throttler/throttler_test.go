package throttler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// fakeStore is an in-memory Store. Safe for concurrent use.
type fakeStore struct {
	mu          sync.Mutex
	added       map[time.Time]map[string]buyer.Stats // window start -> buyer -> stats
	addErr      error
	rates       map[string]float64
	version     int64
	versionErr  error
	ratesCalls  int
	versionCall int
}

func newFakeStore() *fakeStore {
	return &fakeStore{added: make(map[time.Time]map[string]buyer.Stats)}
}

func (f *fakeStore) AddStats(_ context.Context, windowStart time.Time, stats map[string]buyer.Stats) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	if f.added[windowStart] == nil {
		f.added[windowStart] = make(map[string]buyer.Stats)
	}
	for id, s := range stats {
		f.added[windowStart][id] = f.added[windowStart][id].Add(s)
	}
	return nil
}

func (f *fakeStore) StateVersion(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.versionCall++
	return f.version, f.versionErr
}

func (f *fakeStore) ThrottleRates(context.Context) (map[string]float64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ratesCalls++
	rates := make(map[string]float64, len(f.rates))
	for k, v := range f.rates {
		rates[k] = v
	}
	return rates, f.version, nil
}

func (f *fakeStore) setRates(version int64, rates map[string]float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.version, f.rates = version, rates
}

func (f *fakeStore) stats(window time.Time, buyerID string) buyer.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.added[window][buyerID]
}

func (f *fakeStore) setAddErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addErr = err
}

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestThrottler(t *testing.T, store Store) *Throttler {
	t.Helper()
	th, err := New(Config{Logger: quietLogger}, store)
	if err != nil {
		t.Fatal(err)
	}
	return th
}

// at parses an RFC 3339 time for readable test clocks.
func at(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func TestNewConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"zero config takes defaults", Config{}, false},
		{"window multiple of flush interval", Config{EvaluationWindow: 10 * time.Second, FlushInterval: 2 * time.Second}, false},
		{"window not multiple of flush interval", Config{EvaluationWindow: 10 * time.Second, FlushInterval: 3 * time.Second}, true},
		{"negative duration", Config{RefreshInterval: -time.Second}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.cfg, newFakeStore())
			if (err != nil) != tc.wantErr {
				t.Fatalf("New() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	t.Run("nil store", func(t *testing.T) {
		if _, err := New(Config{}, nil); err == nil {
			t.Fatal("want error for nil store")
		}
	})
}

func TestShouldCallDecision(t *testing.T) {
	cases := []struct {
		name   string
		rate   float64 // throttle rate stored for "b1"
		buyer  string
		random float64
		want   bool
	}{
		{"unknown buyer is called", 0.95, "other", 0.0, true},
		{"open buyer is called", 0, "b1", 0.0, true},
		{"random below rate: skip", 0.95, "b1", 0.94, false},
		{"random equal to rate: call", 0.95, "b1", 0.95, true},
		{"random above rate: call", 0.35, "b1", 0.5, true},
		{"NaN rate treated as open", math.NaN(), "b1", 0.0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			th := newTestThrottler(t, newFakeStore())
			th.rates.Store(&map[string]float64{"b1": tc.rate})
			th.randomFloat = func() float64 { return tc.random }
			if got := th.ShouldCall(tc.buyer); got != tc.want {
				t.Fatalf("ShouldCall(%q) = %v, want %v", tc.buyer, got, tc.want)
			}
		})
	}
}

// With the real random source, the share of calls matches 1 - throttle rate.
func TestShouldCallDistribution(t *testing.T) {
	const n = 200_000
	for _, rate := range []float64{0.95, 0.35, 0.05} {
		th := newTestThrottler(t, newFakeStore())
		th.rates.Store(&map[string]float64{"b1": rate})
		called := 0
		for range n {
			if th.ShouldCall("b1") {
				called++
			}
		}
		got, want := float64(called)/n, 1-rate
		if diff := got - want; diff > 0.01 || diff < -0.01 {
			t.Errorf("rate %v: called share %.4f, want %.2f ± 0.01", rate, got, want)
		}
	}
}

func TestRecordAndFlush(t *testing.T) {
	store := newFakeStore()
	th := newTestThrottler(t, store)
	th.now = func() time.Time { return at(t, "2026-01-01T12:00:30Z") }

	for range 7 {
		th.Record("b1", ResultOK)
	}
	for range 2 {
		th.Record("b1", ResultFailed)
	}
	th.Record("b1", ResultTimeout)
	th.Record("b2", ResultTimeout)

	// Interval starting 12:00:29 lies in the window starting 12:00:00.
	th.flush(context.Background(), at(t, "2026-01-01T12:00:29Z"))

	window := at(t, "2026-01-01T12:00:00Z")
	if got, want := store.stats(window, "b1"), (buyer.Stats{Total: 10, Failed: 2, Timeouts: 1}); got != want {
		t.Errorf("b1 = %+v, want %+v", got, want)
	}
	if got, want := store.stats(window, "b2"), (buyer.Stats{Total: 1, Timeouts: 1}); got != want {
		t.Errorf("b2 = %+v, want %+v", got, want)
	}

	// Counters were reset: a second flush adds nothing.
	th.flush(context.Background(), at(t, "2026-01-01T12:00:30Z"))
	if got, want := store.stats(window, "b1").Total, int64(10); got != want {
		t.Errorf("after second flush b1 total = %d, want %d", got, want)
	}
}

func TestFlushIntervalAtWindowEdges(t *testing.T) {
	store := newFakeStore()
	th := newTestThrottler(t, store)
	th.now = func() time.Time { return at(t, "2026-01-01T12:01:00Z") }

	// Last interval of a window: 12:00:59 - 12:01:00.
	th.Record("b1", ResultOK)
	th.flush(context.Background(), at(t, "2026-01-01T12:00:59Z"))
	// First interval of the next window: 12:01:00 - 12:01:01.
	th.Record("b1", ResultOK)
	th.flush(context.Background(), at(t, "2026-01-01T12:01:00Z"))

	for _, w := range []string{"2026-01-01T12:00:00Z", "2026-01-01T12:01:00Z"} {
		if got := store.stats(at(t, w), "b1").Total; got != 1 {
			t.Errorf("window %s total = %d, want 1", w, got)
		}
	}
}

func TestFailedFlushIsRetried(t *testing.T) {
	store := newFakeStore()
	th := newTestThrottler(t, store)
	th.now = func() time.Time { return at(t, "2026-01-01T12:00:10Z") }
	window := at(t, "2026-01-01T12:00:00Z")

	store.setAddErr(errors.New("redis down"))
	th.Record("b1", ResultFailed)
	th.flush(context.Background(), at(t, "2026-01-01T12:00:09Z"))
	if got := store.stats(window, "b1"); got != (buyer.Stats{}) {
		t.Fatalf("stats stored despite error: %+v", got)
	}

	// Store recovers; the retried stats merge with new ones for the same window.
	store.setAddErr(nil)
	th.Record("b1", ResultOK)
	th.flush(context.Background(), at(t, "2026-01-01T12:00:10Z"))
	if got, want := store.stats(window, "b1"), (buyer.Stats{Total: 2, Failed: 1}); got != want {
		t.Fatalf("after retry = %+v, want %+v", got, want)
	}
	if len(th.unsent) != 0 {
		t.Fatalf("unsent not cleared: %v", th.unsent)
	}
}

func TestUnsentStatsDroppedWhenTooOld(t *testing.T) {
	store := newFakeStore()
	store.setAddErr(errors.New("redis down"))
	th := newTestThrottler(t, store)

	th.now = func() time.Time { return at(t, "2026-01-01T12:00:10Z") }
	th.Record("b1", ResultOK)
	th.flush(context.Background(), at(t, "2026-01-01T12:00:09Z"))
	if len(th.unsent) != 1 {
		t.Fatalf("want 1 unsent window, got %d", len(th.unsent))
	}

	// Window 12:00 ended at 12:01; dropped once a full window past that.
	th.now = func() time.Time { return at(t, "2026-01-01T12:02:00Z") }
	th.flush(context.Background(), at(t, "2026-01-01T12:01:59Z"))
	if len(th.unsent) != 1 {
		t.Fatalf("dropped too early: %v", th.unsent)
	}
	th.now = func() time.Time { return at(t, "2026-01-01T12:02:01Z") }
	th.flush(context.Background(), at(t, "2026-01-01T12:02:00Z"))
	if len(th.unsent) != 0 {
		t.Fatalf("not dropped: %v", th.unsent)
	}
}

func TestRefresh(t *testing.T) {
	store := newFakeStore()
	th := newTestThrottler(t, store)
	th.randomFloat = func() float64 { return 0.5 }

	// First refresh loads even when no state was ever written (version 0).
	th.refresh(context.Background())
	if store.ratesCalls != 1 {
		t.Fatalf("first refresh: ThrottleRates calls = %d, want 1", store.ratesCalls)
	}

	// Same version: rates are not reloaded.
	th.refresh(context.Background())
	if store.ratesCalls != 1 {
		t.Fatalf("same version: ThrottleRates calls = %d, want 1", store.ratesCalls)
	}

	// New version: rates reloaded and used by ShouldCall.
	store.setRates(1, map[string]float64{"b1": 0.95})
	th.refresh(context.Background())
	if store.ratesCalls != 2 {
		t.Fatalf("new version: ThrottleRates calls = %d, want 2", store.ratesCalls)
	}
	if th.ShouldCall("b1") {
		t.Fatal("b1 at 0.95 with random 0.5 should be skipped")
	}

	// Store error: last known rates stay in use.
	store.mu.Lock()
	store.versionErr = errors.New("redis down")
	store.mu.Unlock()
	th.refresh(context.Background())
	if th.ShouldCall("b1") {
		t.Fatal("rates lost after store error")
	}
}

// Many goroutines call Record and ShouldCall while flush and refresh run.
// Run with -race. Every recorded request must reach the store exactly once.
func TestConcurrentUse(t *testing.T) {
	store := newFakeStore()
	store.setRates(1, map[string]float64{"b0": 0.5})
	th := newTestThrottler(t, store)
	th.now = func() time.Time { return at(t, "2026-01-01T12:00:30Z") }
	interval := at(t, "2026-01-01T12:00:29Z")

	const goroutines, perGoroutine, buyers = 32, 2000, 4
	buyerIDs := []string{"b0", "b1", "b2", "b3"}

	done := make(chan struct{})
	var background sync.WaitGroup
	background.Add(1)
	go func() { // stands in for Run: the only goroutine that flushes/refreshes
		defer background.Done()
		for {
			select {
			case <-done:
				return
			default:
				th.flush(context.Background(), interval)
				th.refresh(context.Background())
			}
		}
	}()

	var workers sync.WaitGroup
	for g := range goroutines {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range perGoroutine {
				id := buyerIDs[(g+i)%buyers]
				th.ShouldCall(id)
				th.Record(id, ResultFailed)
			}
		}()
	}
	workers.Wait()
	close(done)
	background.Wait()
	th.flush(context.Background(), interval)

	var total, failed int64
	for _, id := range buyerIDs {
		s := store.stats(at(t, "2026-01-01T12:00:00Z"), id)
		total += s.Total
		failed += s.Failed
	}
	if want := int64(goroutines * perGoroutine); total != want || failed != want {
		t.Fatalf("total = %d, failed = %d, want %d each", total, failed, want)
	}
}

// Run end to end with a real clock and short intervals.
func TestRun(t *testing.T) {
	store := newFakeStore()
	store.setRates(1, map[string]float64{"b1": 0.95})
	th, err := New(Config{
		EvaluationWindow: 100 * time.Millisecond,
		FlushInterval:    10 * time.Millisecond,
		RefreshInterval:  10 * time.Millisecond,
		Logger:           quietLogger,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	th.randomFloat = func() float64 { return 0.5 }

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		th.Run(ctx)
		close(stopped)
	}()

	waitFor(t, func() bool { return !th.ShouldCall("b1") }, "initial rates load")

	for range 50 {
		th.Record("b1", ResultOK)
	}
	store.setRates(2, map[string]float64{"b1": 0})
	waitFor(t, func() bool { return th.ShouldCall("b1") }, "refresh picks up new version")

	cancel()
	<-stopped

	var total int64
	store.mu.Lock()
	for _, perBuyer := range store.added {
		total += perBuyer["b1"].Total
	}
	store.mu.Unlock()
	if total != 50 {
		t.Fatalf("stored total = %d, want 50 (final flush must send everything)", total)
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func BenchmarkShouldCall(b *testing.B) {
	th, _ := New(Config{Logger: quietLogger}, newFakeStore())
	th.rates.Store(&map[string]float64{"b1": 0.5, "b2": 0})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			th.ShouldCall("b1")
		}
	})
}

func BenchmarkRecord(b *testing.B) {
	th, _ := New(Config{Logger: quietLogger}, newFakeStore())
	th.Record("b1", ResultOK) // counters exist, as in steady state
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			th.Record("b1", ResultFailed)
		}
	})
}
