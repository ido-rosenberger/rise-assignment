package evaluator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

// fakeStore is an in-memory Store that records saved windows in order.
type fakeStore struct {
	mu      sync.Mutex
	stats   map[time.Time]map[string]buyer.Stats
	states  map[string]State
	last    time.Time // zero: nothing evaluated yet
	saved   []time.Time
	saveErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{stats: map[time.Time]map[string]buyer.Stats{}, states: map[string]State{}}
}

func (f *fakeStore) WindowStats(_ context.Context, w time.Time) (map[string]buyer.Stats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats[w], nil
}

func (f *fakeStore) Snapshot(context.Context) (Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	states := make(map[string]State, len(f.states))
	for k, v := range f.states {
		states[k] = v
	}
	return Snapshot{States: states, LastWindow: f.last}, nil
}

func (f *fakeStore) SaveStates(_ context.Context, w time.Time, states map[string]State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.states, f.last = states, w
	f.saved = append(f.saved, w)
	return nil
}

func (f *fakeStore) savedWindows() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.saved...)
}

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// minute returns 12:mm:00 UTC on a fixed day.
func minute(mm int) time.Time {
	return time.Date(2026, 1, 1, 12, mm, 0, 0, time.UTC)
}

func newTestRunner(t *testing.T, store Store, now time.Time) *Runner {
	t.Helper()
	r, err := NewRunner(RunnerConfig{
		Rules:             config.DefaultRules(), // 1-minute windows
		Grace:             5 * time.Second,
		MaxCatchUpWindows: 10,
		Logger:            quietLogger,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	return r
}

func TestNewRunnerValidates(t *testing.T) {
	valid := RunnerConfig{Rules: config.DefaultRules(), Grace: time.Second, MaxCatchUpWindows: 1}
	cases := []struct {
		name   string
		mutate func(*RunnerConfig)
		store  Store
	}{
		{"nil store", func(*RunnerConfig) {}, nil},
		{"invalid rules", func(c *RunnerConfig) { c.Rules.BlockLevel = 1 }, newFakeStore()},
		{"negative grace", func(c *RunnerConfig) { c.Grace = -time.Second }, newFakeStore()},
		{"zero catch-up windows", func(c *RunnerConfig) { c.MaxCatchUpWindows = 0 }, newFakeStore()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := NewRunner(cfg, tc.store); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

// Which windows EvaluatePending evaluates, given the clock and last window.
func TestEvaluatePendingWindows(t *testing.T) {
	cases := []struct {
		name string
		now  time.Time
		last *time.Time
		want []time.Time
	}{
		{"first start: only latest closed window", minute(10).Add(6 * time.Second), nil,
			[]time.Time{minute(9)}},
		{"grace not passed: latest closed is one earlier", minute(10).Add(4 * time.Second), nil,
			[]time.Time{minute(8)}},
		{"up to date: nothing to do", minute(10).Add(6 * time.Second), ptr(minute(9)),
			nil},
		{"catch up oldest first", minute(10).Add(6 * time.Second), ptr(minute(6)),
			[]time.Time{minute(7), minute(8), minute(9)}},
		{"last window from an older, smaller window size: realign to boundaries", minute(10).Add(6 * time.Second), ptr(minute(7).Add(10 * time.Second)),
			[]time.Time{minute(8), minute(9)}},
		{"catch up bounded to 10 windows", minute(30).Add(6 * time.Second), ptr(minute(1)),
			[]time.Time{minute(20), minute(21), minute(22), minute(23), minute(24),
				minute(25), minute(26), minute(27), minute(28), minute(29)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			if tc.last != nil {
				store.last = *tc.last
			}
			r := newTestRunner(t, store, tc.now)
			if err := r.EvaluatePending(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := store.savedWindows()
			if len(got) != len(tc.want) {
				t.Fatalf("evaluated %v, want %v", got, tc.want)
			}
			for i := range got {
				if !got[i].Equal(tc.want[i]) {
					t.Fatalf("evaluated %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// Every buyer with a state or stats is evaluated; a known buyer without
// stats gets an empty window (it accumulates instead of being dropped).
func TestEvaluateWindowCoversAllBuyers(t *testing.T) {
	store := newFakeStore()
	store.stats[minute(9)] = map[string]buyer.Stats{
		"bad":   {Total: 200, Failed: 150},
		"fresh": {Total: 100},
	}
	store.states["quiet"] = State{ThrottleRate: 0.95, Reason: ReasonFailureRate}

	r := newTestRunner(t, store, minute(10).Add(6*time.Second))
	if err := r.EvaluatePending(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := map[string]State{
		"bad":   {ThrottleRate: 0.95, Reason: ReasonFailureRate},
		"fresh": {},
		"quiet": {ThrottleRate: 0.95, Reason: ReasonFailureRate, AccumulatedWindows: 1},
	}
	if len(store.states) != len(want) {
		t.Fatalf("states = %+v, want %+v", store.states, want)
	}
	for id, w := range want {
		if store.states[id] != w {
			t.Errorf("%s = %+v, want %+v", id, store.states[id], w)
		}
	}
}

// Catch-up applies windows in order, so each builds on the previous result.
func TestCatchUpChainsStates(t *testing.T) {
	store := newFakeStore()
	store.last = minute(5)
	store.stats[minute(6)] = map[string]buyer.Stats{"b1": {Total: 100, Failed: 80}} // block
	for m := 7; m <= 9; m++ {
		store.stats[minute(m)] = map[string]buyer.Stats{"b1": {Total: 100}} // 3 clean
	}

	r := newTestRunner(t, store, minute(10).Add(6*time.Second))
	if err := r.EvaluatePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := store.states["b1"], (State{ThrottleRate: 0.65, Reason: ReasonRecovering}); got != want {
		t.Fatalf("b1 = %+v, want %+v (block then 3 steps)", got, want)
	}
}

// A failed save leaves the last window unchanged, so the window is retried.
func TestFailedSaveIsRetried(t *testing.T) {
	store := newFakeStore()
	store.stats[minute(9)] = map[string]buyer.Stats{"b1": {Total: 100, Failed: 100}}
	store.saveErr = errors.New("redis down")
	r := newTestRunner(t, store, minute(10).Add(6*time.Second))

	if err := r.EvaluatePending(context.Background()); err == nil {
		t.Fatal("want error")
	}
	store.saveErr = nil
	if err := r.EvaluatePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.savedWindows(); len(got) != 1 || !got[0].Equal(minute(9)) {
		t.Fatalf("saved %v, want [12:09]", got)
	}
	if store.states["b1"].ThrottleRate != 0.95 {
		t.Fatalf("b1 not blocked after retry: %+v", store.states["b1"])
	}
}

func TestNextRun(t *testing.T) {
	cases := []struct {
		now, want time.Time
	}{
		{minute(10).Add(2 * time.Second), minute(10).Add(5 * time.Second)}, // inside grace of 12:10
		{minute(10).Add(5 * time.Second), minute(11).Add(5 * time.Second)}, // exactly at run time
		{minute(10).Add(30 * time.Second), minute(11).Add(5 * time.Second)},
	}
	for _, tc := range cases {
		r := newTestRunner(t, newFakeStore(), tc.now)
		if got := r.nextRun(); !got.Equal(tc.want) {
			t.Errorf("nextRun at %s = %s, want %s", tc.now.Format(time.TimeOnly), got.Format(time.TimeOnly), tc.want.Format(time.TimeOnly))
		}
	}
}

// Run with a real clock and short windows: it evaluates on its own and
// returns when ctx is canceled.
func TestRunLoop(t *testing.T) {
	store := newFakeStore()
	rules := config.DefaultRules()
	rules.EvaluationWindow = 50 * time.Millisecond
	r, err := NewRunner(RunnerConfig{Rules: rules, Grace: 5 * time.Millisecond, MaxCatchUpWindows: 10, Logger: quietLogger}, store)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { r.Run(ctx); close(stopped) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(store.savedWindows()) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d windows evaluated", len(store.savedWindows()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}

	// Windows are consecutive: none skipped, none repeated.
	saved := store.savedWindows()
	for i := 1; i < len(saved); i++ {
		if d := saved[i].Sub(saved[i-1]); d != rules.EvaluationWindow {
			t.Fatalf("windows %v not consecutive", saved)
		}
	}
}
