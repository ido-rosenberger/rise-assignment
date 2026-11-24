// Command auctionpod is a demo auction pod. It serves POST /auction: for
// each listed buyer it asks the throttler whether to call, calls the buyer
// over HTTP within the auction deadline, and records the result. A
// background loop shares stats and throttle rates through Redis.
//
// Environment:
//
//	REDIS_ADDR                  Redis address (default localhost:6379)
//	LISTEN_ADDR                 HTTP listen address (default :8080)
//	BUYER_BASE_URL              fake buyers' base URL (default http://localhost:9000)
//	BUYER_TIMEOUT               auction deadline for buyer calls (default 150ms)
//	POD_NAME                    name in logs (default hostname)
//	THROTTLE_EVALUATION_WINDOW  must match the evaluator (default 1m)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ido-rosenberger/rise-assignment/internal/auction"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/throttler"
	"github.com/ido-rosenberger/rise-assignment/throttler/redisstore"
)

func main() {
	hostname, _ := os.Hostname()
	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("pod", config.String("POD_NAME", hostname))
	if err := run(log); err != nil {
		log.Error("auction pod stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	window, err := config.Duration(config.EnvEvaluationWindow, time.Minute)
	if err != nil {
		return err
	}
	buyerTimeout, err := config.Duration("BUYER_TIMEOUT", 150*time.Millisecond)
	if err != nil {
		return err
	}
	redisAddr := config.String("REDIS_ADDR", "localhost:6379")
	listenAddr := config.String("LISTEN_ADDR", ":8080")
	buyerBaseURL := config.String("BUYER_BASE_URL", "http://localhost:9000")

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()

	th, err := throttler.New(throttler.Config{EvaluationWindow: window, Logger: log},
		redisstore.New(client, redisstore.Options{EvaluationWindow: window, Logger: log}))
	if err != nil {
		return err
	}

	traffic := &auction.Traffic{}
	auctioneer := &auction.Auctioneer{
		Throttler: th,
		Client: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000, // default 2 would reopen connections constantly
			IdleConnTimeout:     90 * time.Second,
		}},
		BuyerBaseURL: buyerBaseURL,
		Timeout:      buyerTimeout,
		Traffic:      traffic,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /auction", auctioneer.Handler())
	server := &http.Server{Addr: listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	throttlerDone := make(chan struct{})
	go func() { th.Run(ctx); close(throttlerDone) }()
	go logTraffic(ctx, traffic, window, log)

	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	log.Info("auction pod started", "listen", listenAddr, "redis", redisAddr, "buyers", buyerBaseURL,
		"buyer_timeout", buyerTimeout, "window", window)

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		stop()
		<-throttlerDone
		return fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "err", err)
	}
	<-throttlerDone // final stats flush
	log.Info("auction pod stopped")
	return nil
}

// logTraffic logs per-buyer traffic once per evaluation window.
func logTraffic(ctx context.Context, traffic *auction.Traffic, every time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			traffic.LogAndReset(log)
		}
	}
}
