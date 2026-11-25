package store

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/evaluator"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
	"github.com/ido-rosenberger/rise-assignment/throttler"
	"github.com/ido-rosenberger/rise-assignment/throttler/redisstore"
)

func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1}) // no retries: fast "Redis down" tests
	t.Cleanup(func() { client.Close() })
	return mr, client
}

var (
	window      = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestWindowStats(t *testing.T) {
	mr, client := newRedis(t)
	s := New(client, quietLogger)
	ctx := context.Background()

	// Unknown window: empty, not an error.
	got, err := s.WindowStats(ctx, window)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty window = %v, %v", got, err)
	}

	key := schema.StatsKey(window)
	mr.HSet(key, "b1:total", "100")
	mr.HSet(key, "b1:failed", "70")
	got, err = s.WindowStats(ctx, window)
	if err != nil {
		t.Fatal(err)
	}
	if want := (buyer.Stats{Total: 100, Failed: 70}); got["b1"] != want {
		t.Fatalf("b1 = %+v, want %+v", got["b1"], want)
	}

	// A malformed field is skipped; the rest of the window is still read.
	mr.HSet(key, ":total", "1")
	got, err = s.WindowStats(ctx, window)
	if err != nil || got["b1"].Total != 100 {
		t.Fatalf("with malformed field: %v, %v", got, err)
	}
}

func TestSaveAndSnapshot(t *testing.T) {
	mr, client := newRedis(t)
	s := New(client, quietLogger)
	ctx := context.Background()

	snap, err := s.Snapshot(ctx)
	if err != nil || len(snap.States) != 0 || !snap.LastWindow.IsZero() {
		t.Fatalf("snapshot before any save = %+v, %v; want empty", snap, err)
	}

	states := map[string]evaluator.State{
		"b1": {ThrottleRate: 0.95, Reason: evaluator.ReasonFailureRate},
		"b2": {
			ThrottleRate:       0.45,
			Reason:             evaluator.ReasonRecovering,
			Accumulated:        buyer.Stats{Total: 50, Failed: 1},
			AccumulatedWindows: 1,
		},
		"b3": {}, // open
	}
	if err := s.SaveStates(ctx, window, states); err != nil {
		t.Fatal(err)
	}

	snap, err = s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.LastWindow.Equal(window) {
		t.Fatalf("LastWindow = %v, want %v", snap.LastWindow, window)
	}
	if len(snap.States) != len(states) {
		t.Fatalf("got %d states, want %d", len(snap.States), len(states))
	}
	for id, want := range states {
		if snap.States[id] != want {
			t.Errorf("%s = %+v, want %+v", id, snap.States[id], want)
		}
	}
	if v, _ := mr.Get(schema.KeyVersion); v != "1" {
		t.Fatalf("version = %q, want 1", v)
	}

	// An open buyer's JSON has no accumulation fields (omitzero).
	if got := mr.HGet(schema.KeyState, "b3"); got != `{"throttle_rate":0,"reason":""}` {
		t.Fatalf("open buyer stored as %s", got)
	}

	// A second save bumps the version again.
	if err := s.SaveStates(ctx, window.Add(time.Minute), states); err != nil {
		t.Fatal(err)
	}
	if v, _ := mr.Get(schema.KeyVersion); v != "2" {
		t.Fatalf("version = %q, want 2", v)
	}
}

func TestSnapshotSkipsCorruptEntries(t *testing.T) {
	mr, client := newRedis(t)
	s := New(client, quietLogger)
	mr.HSet(schema.KeyState, "bad", "{")
	mr.HSet(schema.KeyState, "b1", `{"throttle_rate":0.85,"reason":"recovering"}`)

	snap, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := (evaluator.State{ThrottleRate: 0.85, Reason: evaluator.ReasonRecovering}); snap.States["b1"] != want || len(snap.States) != 1 {
		t.Fatalf("got %+v, want only b1 = %+v", snap.States, want)
	}
}

func TestSaveStatesWithNoBuyers(t *testing.T) {
	_, client := newRedis(t)
	s := New(client, quietLogger)
	ctx := context.Background()
	if err := s.SaveStates(ctx, window, nil); err != nil {
		t.Fatal(err)
	}
	if snap, _ := s.Snapshot(ctx); !snap.LastWindow.Equal(window) {
		t.Fatalf("LastWindow = %v, want %v", snap.LastWindow, window)
	}
}

func TestStoreErrorsWhenRedisDown(t *testing.T) {
	mr, client := newRedis(t)
	s := New(client, quietLogger)
	mr.Close()
	ctx := context.Background()
	if _, err := s.WindowStats(ctx, window); err == nil {
		t.Error("WindowStats: want error")
	}
	if _, err := s.Snapshot(ctx); err == nil {
		t.Error("Snapshot: want error")
	}
	if err := s.SaveStates(ctx, window, map[string]evaluator.State{"b1": {}}); err == nil {
		t.Error("SaveStates: want error")
	}
}

// Contract test: the pod adapter and the evaluator adapter agree on the
// Redis layout in both directions.
func TestPodAndEvaluatorAdaptersAgree(t *testing.T) {
	_, client := newRedis(t)
	pod := redisstore.New(client, redisstore.Options{Logger: quietLogger})
	eval := New(client, quietLogger)
	ctx := context.Background()

	// Pods -> evaluator: stats from two flushes sum up.
	for range 2 {
		if err := pod.AddStats(ctx, window, map[string]buyer.Stats{"b1": {Total: 60, Failed: 40, Timeouts: 5}}); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := eval.WindowStats(ctx, window)
	if err != nil {
		t.Fatal(err)
	}
	if want := (buyer.Stats{Total: 120, Failed: 80, Timeouts: 10}); stats["b1"] != want {
		t.Fatalf("evaluator read %+v, want %+v", stats["b1"], want)
	}

	// Evaluator -> pods: saved throttle rates and a new version.
	if err := eval.SaveStates(ctx, window, map[string]evaluator.State{
		"b1": {ThrottleRate: 0.95, Reason: evaluator.ReasonFailureRate},
	}); err != nil {
		t.Fatal(err)
	}
	rates, version, err := pod.ThrottleRates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rates["b1"] != 0.95 || version != 1 {
		t.Fatalf("pod read rates %v version %d, want b1=0.95 version 1", rates, version)
	}
}

// End to end through the real throttler: Record in a pod, flush through
// redisstore, read back by the evaluator adapter.
func TestThrottlerFlushReachesEvaluator(t *testing.T) {
	_, client := newRedis(t)
	evalWindow := 200 * time.Millisecond
	th, err := throttler.New(throttler.Config{
		EvaluationWindow: evalWindow,
		FlushInterval:    10 * time.Millisecond,
		RefreshInterval:  10 * time.Millisecond,
		Logger:           quietLogger,
	}, redisstore.New(client, redisstore.Options{EvaluationWindow: evalWindow, Logger: quietLogger}))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { th.Run(ctx); close(stopped) }()

	start := time.Now()
	for range 30 {
		th.Record("b1", throttler.ResultTimeout)
	}
	cancel()
	<-stopped

	// Sum every window the records could have landed in.
	eval := New(client, quietLogger)
	var total, timeouts int64
	for w := start.Truncate(evalWindow); !w.After(time.Now()); w = w.Add(evalWindow) {
		stats, err := eval.WindowStats(context.Background(), w)
		if err != nil {
			t.Fatal(err)
		}
		total += stats["b1"].Total
		timeouts += stats["b1"].Timeouts
	}
	if total != 30 || timeouts != 30 {
		t.Fatalf("evaluator read total=%d timeouts=%d, want 30 each", total, timeouts)
	}
}
