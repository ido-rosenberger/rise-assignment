package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Generator sends RPS auctions per second, round-robin over Pods.
type Generator struct {
	Pods   []string
	RPS    int
	Buyers *Buyers
	Client *http.Client
	Log    *slog.Logger
	// MaxInFlight caps concurrent auction requests; extra ticks are dropped.
	MaxInFlight int

	sent, failed, dropped atomic.Int64
}

// Run sends auctions until ctx is canceled and logs a summary every
// summaryEvery.
func (g *Generator) Run(ctx context.Context, summaryEvery time.Duration) {
	tick := time.NewTicker(time.Second / time.Duration(g.RPS))
	defer tick.Stop()
	summary := time.NewTicker(summaryEvery)
	defer summary.Stop()

	inFlight := make(chan struct{}, g.MaxInFlight)
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-summary.C:
			g.Log.Info("traffic sent", "auctions", g.sent.Swap(0), "failed", g.failed.Swap(0), "dropped", g.dropped.Swap(0))
		case <-tick.C:
			buyers := g.Buyers.Pick()
			if len(buyers) == 0 {
				continue
			}
			select {
			case inFlight <- struct{}{}:
			default:
				g.dropped.Add(1)
				continue
			}
			pod := g.Pods[i%len(g.Pods)]
			go func() {
				defer func() { <-inFlight }()
				g.send(ctx, pod, buyers)
			}()
		}
	}
}

func (g *Generator) send(ctx context.Context, pod string, buyers []string) {
	body, _ := json.Marshal(map[string][]string{"buyers": buyers})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pod+"/auction", bytes.NewReader(body))
	if err != nil {
		g.failed.Add(1)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.Client.Do(req)
	if err != nil {
		g.failed.Add(1)
		return
	}
	io.Copy(io.Discard, resp.Body) // drain so the connection is reused
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		g.failed.Add(1)
		return
	}
	g.sent.Add(1)
}
