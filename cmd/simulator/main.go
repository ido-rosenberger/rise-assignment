// Command simulator runs the demo's fake buyers and traffic generator.
//
// Environment:
//
//	SIM_CONFIG    path to the JSON config (default /config/simulator.json)
//	LISTEN_ADDR   HTTP listen address for buyers + control API (default :9000)
//	SUMMARY_EVERY interval of the "traffic sent" log line (default 10s)
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

	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/internal/simulator"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("simulator stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := simulator.LoadConfig(config.String("SIM_CONFIG", "/config/simulator.json"))
	if err != nil {
		return err
	}
	summaryEvery, err := config.Duration("SUMMARY_EVERY", 10*time.Second)
	if err != nil {
		return err
	}
	listenAddr := config.String("LISTEN_ADDR", ":9000")

	buyers := simulator.NewBuyers(cfg.Buyers, log)
	server := &http.Server{Addr: listenAddr, Handler: buyers.Handler(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	go buyers.LogIncidents(ctx)

	gen := &simulator.Generator{
		Pods:   cfg.Pods,
		RPS:    cfg.RPS,
		Buyers: buyers,
		Client: &http.Client{
			Timeout:   2 * time.Second,
			Transport: &http.Transport{MaxIdleConns: 1000, MaxIdleConnsPerHost: 1000, IdleConnTimeout: 90 * time.Second},
		},
		Log:         log,
		MaxInFlight: 1000,
	}
	go gen.Run(ctx, summaryEvery)
	log.Info("simulator started", "listen", listenAddr, "rps", cfg.RPS, "pods", cfg.Pods, "buyers", len(cfg.Buyers))

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		return fmt.Errorf("http server: %w", err)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("http shutdown", "err", err)
	}
	log.Info("simulator stopped")
	return nil
}
