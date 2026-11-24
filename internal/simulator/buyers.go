package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// Buyers serves the fake buyers' bid endpoint and the live control API.
type Buyers struct {
	cfg   []BuyerConfig
	byID  map[string]BuyerConfig
	start time.Time
	log   *slog.Logger

	// TimeoutDelay is how long a "timeout" bid stalls; longer than the
	// pods' auction deadline. Latency is the delay of a normal bid.
	TimeoutDelay time.Duration
	Latency      time.Duration

	mu        sync.RWMutex
	overrides map[string]Rates // set via PUT /buyers/{id}

	now    func() time.Time
	random func() float64
}

// NewBuyers returns fake buyers whose incident schedule starts now.
func NewBuyers(cfg []BuyerConfig, log *slog.Logger) *Buyers {
	b := &Buyers{
		cfg:          cfg,
		byID:         make(map[string]BuyerConfig, len(cfg)),
		log:          log,
		TimeoutDelay: 300 * time.Millisecond,
		Latency:      10 * time.Millisecond,
		overrides:    map[string]Rates{},
		now:          time.Now,
		random:       rand.Float64,
	}
	for _, c := range cfg {
		b.byID[c.ID] = c
	}
	b.start = b.now()
	return b
}

// Effective is a buyer's current rates and where they come from.
type Effective struct {
	ID            string  `json:"id"`
	Participation float64 `json:"participation"`
	Rates
	Source string `json:"source"` // "base", "incident" or "override"
}

func (b *Buyers) effective(c BuyerConfig) Effective {
	e := Effective{ID: c.ID, Participation: c.Participation, Rates: c.Rates, Source: "base"}
	if in := c.Incident; in != nil {
		elapsed := b.now().Sub(b.start)
		if elapsed >= time.Duration(in.Start) && elapsed < time.Duration(in.End) {
			e.Rates, e.Source = in.Rates, "incident"
		}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if o, ok := b.overrides[c.ID]; ok {
		e.Rates, e.Source = o, "override"
	}
	return e
}

// Pick returns the buyers invited to one auction.
func (b *Buyers) Pick() []string {
	var ids []string
	for _, c := range b.cfg {
		if b.random() < c.Participation {
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// Handler serves:
//
//	GET    /bid/{id}     one bid request (fails or stalls per current rates)
//	GET    /buyers       every buyer's current rates
//	PUT    /buyers/{id}  override rates: {"failure_rate":0.9,"timeout_rate":0}
//	DELETE /buyers/{id}  remove the override
func (b *Buyers) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /bid/{id}", b.bid)
	mux.HandleFunc("GET /buyers", b.list)
	mux.HandleFunc("PUT /buyers/{id}", b.setOverride)
	mux.HandleFunc("DELETE /buyers/{id}", b.clearOverride)
	return mux
}

func (b *Buyers) bid(w http.ResponseWriter, r *http.Request) {
	c, ok := b.byID[r.PathValue("id")]
	if !ok {
		http.Error(w, "unknown buyer", http.StatusNotFound)
		return
	}
	e := b.effective(c)
	x := b.random()
	switch {
	case x < e.TimeoutRate:
		sleep(r.Context(), b.TimeoutDelay)
	case x < e.TimeoutRate+e.FailureRate:
		http.Error(w, "simulated failure", http.StatusInternalServerError)
		return
	default:
		sleep(r.Context(), b.Latency)
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"price":%.4f}`, 0.1+b.random())
}

// sleep waits d or until the caller gives up.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (b *Buyers) list(w http.ResponseWriter, _ *http.Request) {
	out := make([]Effective, 0, len(b.cfg))
	for _, c := range b.cfg {
		out = append(out, b.effective(c))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (b *Buyers) setOverride(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := b.byID[id]; !ok {
		http.Error(w, "unknown buyer", http.StatusNotFound)
		return
	}
	var rates Rates
	if err := json.NewDecoder(r.Body).Decode(&rates); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := rates.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b.mu.Lock()
	b.overrides[id] = rates
	b.mu.Unlock()
	b.log.Info("buyer override set", "buyer", id, "failure_rate", rates.FailureRate, "timeout_rate", rates.TimeoutRate)
	w.WriteHeader(http.StatusNoContent)
}

func (b *Buyers) clearOverride(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := b.byID[id]; !ok {
		http.Error(w, "unknown buyer", http.StatusNotFound)
		return
	}
	b.mu.Lock()
	delete(b.overrides, id)
	b.mu.Unlock()
	b.log.Info("buyer override cleared", "buyer", id)
	w.WriteHeader(http.StatusNoContent)
}

// LogIncidents logs when a buyer's scheduled incident starts and ends, so
// the evaluator's reactions can be matched to them in the logs.
func (b *Buyers) LogIncidents(ctx context.Context) {
	last := map[string]string{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		for _, c := range b.cfg {
			e := b.effective(c)
			if e.Source == last[c.ID] {
				continue
			}
			if last[c.ID] != "" || e.Source != "base" {
				b.log.Info("buyer behavior changed", "buyer", c.ID, "source", e.Source,
					"failure_rate", e.FailureRate, "timeout_rate", e.TimeoutRate)
			}
			last[c.ID] = e.Source
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
