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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ido-rosenberger/rise-assignment/internal/config"
	"github.com/ido-rosenberger/rise-assignment/internal/serve"
	"github.com/ido-rosenberger/rise-assignment/internal/simulator"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("simulator stopped", "err", err)
		os.Exit(1)
	}
	log.Info("simulator stopped")
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

	var background sync.WaitGroup
	background.Go(func() { buyers.LogIncidents(ctx) })
	background.Go(func() { gen.Run(ctx, summaryEvery) })

	log.Info("simulator started", "listen", listenAddr, "rps", cfg.RPS, "pods", cfg.Pods, "buyers", len(cfg.Buyers))
	err = serve.Until(ctx, server)
	stop() // if the server failed, stop traffic too
	background.Wait()
	return err
}
