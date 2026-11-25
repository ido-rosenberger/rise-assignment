// Command evaluator runs the periodic evaluator: once per evaluation window
// it reads every buyer's aggregated stats from Redis, applies the throttling
// rules, and writes the new throttle states for the auction pods.
//
// Environment:
//
//	REDIS_ADDR                 Redis address (default localhost:6379)
//	LISTEN_ADDR                HTTP address for GET /state (default :8080)
//	EVALUATOR_GRACE            wait after a window ends before evaluating it (default 5s)
//	THROTTLE_*                 throttling rules, see internal/config
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/internal/evaluator"
	"github.com/ido-rosenberger/rise-assignment/internal/serve"
	"github.com/ido-rosenberger/rise-assignment/internal/store"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("evaluator stopped", "err", err)
		os.Exit(1)
	}
	log.Info("evaluator stopped")
}

func run(log *slog.Logger) error {
	rules, err := config.LoadRules()
	if err != nil {
		return err
	}
	grace, err := config.Duration("EVALUATOR_GRACE", 5*time.Second)
	if err != nil {
		return err
	}
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	listenAddr := config.String("LISTEN_ADDR", ":8080")

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()
	st := store.New(client, log)

	runner, err := evaluator.NewRunner(evaluator.RunnerConfig{
		Rules:             rules,
		Grace:             grace,
		MaxCatchUpWindows: schema.StatsRetentionWindows,
		Logger:            log,
	}, st)
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /state", evaluator.StateHandler(st))
	server := &http.Server{Addr: listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	// Stop cleanly on Ctrl+C or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var background sync.WaitGroup
	background.Go(func() { runner.Run(ctx) })

	log.Info("evaluator started",
		"redis", redisAddr,
		"listen", listenAddr,
		"window", rules.EvaluationWindow,
		"grace", grace,
		"failure_rate_threshold", rules.FailureRateThreshold,
		"timeout_rate_threshold", rules.TimeoutRateThreshold,
		"min_sample_size", rules.MinSampleSize,
		"block_level", rules.BlockLevel,
		"recovery_step_size", rules.RecoveryStepSize,
		"max_accumulated_windows", rules.MaxAccumulatedWindows)

	err = serve.Until(ctx, server)
	stop() // if the server failed, stop the runner too
	background.Wait()
	return err
}
