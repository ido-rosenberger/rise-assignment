// Command evaluator runs the periodic evaluator: once per evaluation window
// it reads every buyer's aggregated stats from Redis, applies the throttling
// rules, and writes the new throttle states for the auction pods.
//
// Environment:
//
//	REDIS_ADDR                 Redis address (default localhost:6379)
//	EVALUATOR_GRACE            wait after a window ends before evaluating it (default 5s)
//	THROTTLE_*                 throttling rules, see internal/config
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/internal/evaluator"
	"github.com/ido-rosenberger/rise-assignment/internal/store"
	"github.com/ido-rosenberger/rise-assignment/internal/store/schema"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("evaluator stopped", "err", err)
		os.Exit(1)
	}
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

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()

	runner, err := evaluator.NewRunner(evaluator.RunnerConfig{
		Rules:             rules,
		Grace:             grace,
		MaxCatchUpWindows: schema.StatsRetentionWindows,
		Logger:            log,
	}, store.New(client, log))
	if err != nil {
		return fmt.Errorf("create runner: %w", err)
	}

	// Stop cleanly on Ctrl+C or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("evaluator started",
		"redis", redisAddr,
		"window", rules.EvaluationWindow,
		"grace", grace,
		"failure_rate_threshold", rules.FailureRateThreshold,
		"timeout_rate_threshold", rules.TimeoutRateThreshold,
		"min_sample_size", rules.MinSampleSize,
		"block_level", rules.BlockLevel,
		"recovery_step_size", rules.RecoveryStepSize,
		"max_accumulated_windows", rules.MaxAccumulatedWindows)
	runner.Run(ctx)
	log.Info("evaluator stopped")
	return nil
}
