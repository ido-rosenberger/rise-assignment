package evaluator_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/internal/evaluator"
	"github.com/ido-rosenberger/rise-assignment/internal/store"
	"github.com/ido-rosenberger/rise-assignment/throttler"
	"github.com/ido-rosenberger/rise-assignment/throttler/redisstore"
)

// End to end on an in-memory Redis: two pods send a failing buyer's stats,
// the evaluator blocks it, both pods pick up the block; then the buyer is
// healthy and the pods see it recover step by step.
func TestEndToEndBlockAndRecover(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	window := 100 * time.Millisecond
	rules := config.DefaultRules()
	rules.EvaluationWindow = window
	rules.MinSampleSize = 20

	newPod := func() *throttler.Throttler {
		th, err := throttler.New(throttler.Config{
			EvaluationWindow: window,
			FlushInterval:    10 * time.Millisecond,
			RefreshInterval:  10 * time.Millisecond,
			Logger:           quiet,
		}, redisstore.New(client, redisstore.Options{EvaluationWindow: window, Logger: quiet}))
		if err != nil {
			t.Fatal(err)
		}
		return th
	}
	pods := []*throttler.Throttler{newPod(), newPod()}

	runner, err := evaluator.NewRunner(evaluator.RunnerConfig{
		Rules:             rules,
		Grace:             30 * time.Millisecond,
		MaxCatchUpWindows: 10,
		Logger:            quiet,
	}, store.New(client, quiet))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, p := range pods {
		go p.Run(ctx)
	}
	go runner.Run(ctx)

	// failing reports whether the simulated buyer fails right now.
	failing := true
	traffic := func(until func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !until() {
			if time.Now().After(deadline) {
				t.Fatal("timed out")
			}
			for _, p := range pods {
				for range 5 {
					if !p.ShouldCall("b1") {
						continue
					}
					result := throttler.ResultOK
					if failing {
						result = throttler.ResultFailed
					}
					p.Record("b1", result)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}

	called := func(p *throttler.Throttler) float64 {
		n := 0
		for range 2000 {
			if p.ShouldCall("b1") {
				n++
			}
		}
		return float64(n) / 2000
	}

	// Step 1: failing buyer gets blocked; both pods converge on ~5% calls.
	traffic(func() bool { return called(pods[0]) < 0.15 && called(pods[1]) < 0.15 })

	// Step 2: buyer is healthy again; pods see it fully open eventually.
	failing = false
	traffic(func() bool { return called(pods[0]) == 1 && called(pods[1]) == 1 })
}
