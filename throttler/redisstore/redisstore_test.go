package redisstore

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
)

// newTestStore returns a Store on a fresh in-memory Redis.
func newTestStore(t *testing.T, opts Options) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1}) // no retries: fast "Redis down" tests
	t.Cleanup(func() { client.Close() })
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return New(client, opts), mr
}

var window = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func TestAddStatsSumsAcrossPods(t *testing.T) {
	podA, mr := newTestStore(t, Options{})
	clientB := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { clientB.Close() })
	podB := New(clientB, Options{})
	ctx := context.Background()

	if err := podA.AddStats(ctx, window, map[string]buyer.Stats{
		"b1": {Total: 10, Failed: 2},
		"b2": {Total: 3, Timeouts: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := podB.AddStats(ctx, window, map[string]buyer.Stats{
		"b1": {Total: 5, Failed: 1, Timeouts: 1},
	}); err != nil {
		t.Fatal(err)
	}

	key := schema.StatsKey(window)
	want := map[string]string{
		"b1:total": "15", "b1:failed": "3", "b1:timeouts": "1",
		"b2:total": "3", "b2:timeouts": "3", // zero counters are not written
	}
	got, err := mr.HKeys(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("fields = %v, want %v", got, want)
	}
	for field, v := range want {
		if g := mr.HGet(key, field); g != v {
			t.Errorf("%s = %q, want %q", field, g, v)
		}
	}
}

func TestAddStatsSetsTTL(t *testing.T) {
	s, mr := newTestStore(t, Options{EvaluationWindow: 10 * time.Second})
	if err := s.AddStats(context.Background(), window, map[string]buyer.Stats{"b1": {Total: 1}}); err != nil {
		t.Fatal(err)
	}
	if got, want := mr.TTL(schema.StatsKey(window)), schema.StatsRetentionWindows*10*time.Second; got != want {
		t.Fatalf("TTL = %v, want %v", got, want)
	}
}

func TestAddStatsEmptyIsNoop(t *testing.T) {
	s, mr := newTestStore(t, Options{})
	if err := s.AddStats(context.Background(), window, nil); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(schema.StatsKey(window)) {
		t.Fatal("empty AddStats created a key")
	}
}

func TestStateVersionAndRates(t *testing.T) {
	s, mr := newTestStore(t, Options{})
	ctx := context.Background()

	// Nothing written by the evaluator yet.
	v, err := s.StateVersion(ctx)
	if err != nil || v != 0 {
		t.Fatalf("StateVersion = %d, %v; want 0, nil", v, err)
	}
	rates, v, err := s.ThrottleRates(ctx)
	if err != nil || v != 0 || len(rates) != 0 {
		t.Fatalf("ThrottleRates = %v, %d, %v; want empty, 0, nil", rates, v, err)
	}

	mr.HSet(schema.KeyState, "b1", `{"throttle_rate":0.95,"reason":"failure_rate"}`)
	mr.HSet(schema.KeyState, "b2", `{"throttle_rate":0,"reason":""}`)
	mr.Set(schema.KeyVersion, "7")

	if v, err := s.StateVersion(ctx); err != nil || v != 7 {
		t.Fatalf("StateVersion = %d, %v; want 7, nil", v, err)
	}
	rates, v, err = s.ThrottleRates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v != 7 || rates["b1"] != 0.95 || rates["b2"] != 0 || len(rates) != 2 {
		t.Fatalf("ThrottleRates = %v, %d; want b1=0.95 b2=0, version 7", rates, v)
	}
}

// A corrupt entry is skipped; other buyers' rates still load.
func TestThrottleRatesSkipsCorruptState(t *testing.T) {
	s, mr := newTestStore(t, Options{})
	mr.HSet(schema.KeyState, "bad", "not json")
	mr.HSet(schema.KeyState, "b1", `{"throttle_rate":0.95,"reason":"failure_rate"}`)
	rates, _, err := s.ThrottleRates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rates) != 1 || rates["b1"] != 0.95 {
		t.Fatalf("rates = %v, want only b1 = 0.95", rates)
	}
}

func TestAddStatsSkipsEmptyBuyerID(t *testing.T) {
	s, mr := newTestStore(t, Options{})
	if err := s.AddStats(context.Background(), window, map[string]buyer.Stats{
		"":   {Total: 1},
		"b1": {Total: 2},
	}); err != nil {
		t.Fatal(err)
	}
	fields, _ := mr.HKeys(schema.StatsKey(window))
	if len(fields) != 1 || fields[0] != "b1:total" {
		t.Fatalf("fields = %v, want [b1:total]", fields)
	}
}

func TestErrorsWhenRedisDown(t *testing.T) {
	s, mr := newTestStore(t, Options{})
	mr.Close()
	ctx := context.Background()
	if err := s.AddStats(ctx, window, map[string]buyer.Stats{"b1": {Total: 1}}); err == nil {
		t.Error("AddStats: want error")
	}
	if _, err := s.StateVersion(ctx); err == nil {
		t.Error("StateVersion: want error")
	}
	if _, _, err := s.ThrottleRates(ctx); err == nil {
		t.Error("ThrottleRates: want error")
	}
}
