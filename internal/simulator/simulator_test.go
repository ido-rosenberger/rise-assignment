package simulator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func validConfig() Config {
	return Config{
		RPS:  10,
		Pods: []string{"http://pod"},
		Buyers: []BuyerConfig{{
			ID: "b1", Participation: 1, Rates: Rates{FailureRate: 0.1},
			Incident: &Incident{Start: Duration(10 * time.Second), End: Duration(20 * time.Second), Rates: Rates{FailureRate: 0.9}},
		}},
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"zero rps", func(c *Config) { c.RPS = 0 }, true},
		{"no pods", func(c *Config) { c.Pods = nil }, true},
		{"no buyers", func(c *Config) { c.Buyers = nil }, true},
		{"empty id", func(c *Config) { c.Buyers[0].ID = "" }, true},
		{"duplicate id", func(c *Config) { c.Buyers = append(c.Buyers, c.Buyers[0]) }, true},
		{"zero participation", func(c *Config) { c.Buyers[0].Participation = 0 }, true},
		{"rates sum above one", func(c *Config) { c.Buyers[0].Rates = Rates{FailureRate: 0.6, TimeoutRate: 0.6} }, true},
		{"incident ends before start", func(c *Config) { c.Buyers[0].Incident.End = 0 }, true},
		{"incident bad rates", func(c *Config) { c.Buyers[0].Incident.Rates.TimeoutRate = -1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			if err := c.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLoadConfigParsesDurations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sim.json")
	os.WriteFile(path, []byte(`{"rps":5,"pods":["http://p"],"buyers":[{"id":"b","participation":1,
		"incident":{"start":"30s","end":"1m30s","failure_rate":0.9}}]}`), 0o600)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	in := c.Buyers[0].Incident
	if time.Duration(in.Start) != 30*time.Second || time.Duration(in.End) != 90*time.Second || in.FailureRate != 0.9 {
		t.Fatalf("incident = %+v", in)
	}
}

func TestEffectiveRates(t *testing.T) {
	b := NewBuyers(validConfig().Buyers, quietLogger)
	start := b.start
	cases := []struct {
		name     string
		elapsed  time.Duration
		override *Rates
		want     Effective
	}{
		{"before incident", 5 * time.Second, nil, Effective{ID: "b1", Participation: 1, Rates: Rates{FailureRate: 0.1}, Source: "base"}},
		{"during incident", 10 * time.Second, nil, Effective{ID: "b1", Participation: 1, Rates: Rates{FailureRate: 0.9}, Source: "incident"}},
		{"after incident", 20 * time.Second, nil, Effective{ID: "b1", Participation: 1, Rates: Rates{FailureRate: 0.1}, Source: "base"}},
		{"override beats incident", 15 * time.Second, &Rates{TimeoutRate: 0.5}, Effective{ID: "b1", Participation: 1, Rates: Rates{TimeoutRate: 0.5}, Source: "override"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b.now = func() time.Time { return start.Add(tc.elapsed) }
			b.overrides = map[string]Rates{}
			if tc.override != nil {
				b.overrides["b1"] = *tc.override
			}
			if got := b.effective(b.byID["b1"]); got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestBidOutcomes(t *testing.T) {
	b := NewBuyers([]BuyerConfig{{ID: "b1", Participation: 1, Rates: Rates{FailureRate: 0.3, TimeoutRate: 0.2}}}, quietLogger)
	b.TimeoutDelay, b.Latency = 50*time.Millisecond, 0
	cases := []struct {
		name       string
		random     float64
		wantStatus int
		wantSlow   bool
	}{
		{"timeout band [0, 0.2)", 0.1, http.StatusOK, true},
		{"failure band [0.2, 0.5)", 0.3, http.StatusInternalServerError, false},
		{"ok band [0.5, 1)", 0.7, http.StatusOK, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b.random = func() float64 { return tc.random }
			rec := httptest.NewRecorder()
			start := time.Now()
			b.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bid/b1", nil))
			slow := time.Since(start) >= b.TimeoutDelay
			if rec.Code != tc.wantStatus || slow != tc.wantSlow {
				t.Fatalf("status %d slow %v, want %d slow %v", rec.Code, slow, tc.wantStatus, tc.wantSlow)
			}
		})
	}

	rec := httptest.NewRecorder()
	b.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bid/nobody", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown buyer: status %d, want 404", rec.Code)
	}
}

func TestControlAPI(t *testing.T) {
	b := NewBuyers(validConfig().Buyers, quietLogger)
	h := b.Handler()
	do := func(method, path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
		return rec
	}

	if rec := do(http.MethodPut, "/buyers/b1", `{"failure_rate":0.8,"timeout_rate":0.1}`); rec.Code != http.StatusNoContent {
		t.Fatalf("PUT status %d", rec.Code)
	}
	var list []Effective
	json.NewDecoder(do(http.MethodGet, "/buyers", "").Body).Decode(&list)
	if len(list) != 1 || list[0].Source != "override" || list[0].FailureRate != 0.8 {
		t.Fatalf("after PUT: %+v", list)
	}

	if rec := do(http.MethodDelete, "/buyers/b1", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE status %d", rec.Code)
	}
	json.NewDecoder(do(http.MethodGet, "/buyers", "").Body).Decode(&list)
	if list[0].Source != "base" {
		t.Fatalf("after DELETE: %+v", list)
	}

	if rec := do(http.MethodPut, "/buyers/b1", `{"failure_rate":0.8,"timeout_rate":0.8}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid rates: status %d, want 400", rec.Code)
	}
	if rec := do(http.MethodPut, "/buyers/nobody", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("PUT unknown buyer: status %d, want 404", rec.Code)
	}
	if rec := do(http.MethodDelete, "/buyers/nobody", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE unknown buyer: status %d, want 404", rec.Code)
	}
}

func TestPickUsesParticipation(t *testing.T) {
	b := NewBuyers([]BuyerConfig{
		{ID: "always", Participation: 1},
		{ID: "rare", Participation: 0.1},
	}, quietLogger)
	b.random = func() float64 { return 0.5 }
	if got := b.Pick(); len(got) != 1 || got[0] != "always" {
		t.Fatalf("Pick() = %v, want [always]", got)
	}
}

func TestGeneratorSendsRoundRobin(t *testing.T) {
	var hitsA, hitsB atomic.Int64
	pod := func(hits *atomic.Int64) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct{ Buyers []string }
			if r.URL.Path != "/auction" || json.NewDecoder(r.Body).Decode(&req) != nil || len(req.Buyers) != 1 {
				http.Error(w, "bad", http.StatusBadRequest)
				return
			}
			hits.Add(1)
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	a, b := pod(&hitsA), pod(&hitsB)

	g := &Generator{
		Pods:        []string{a.URL, b.URL},
		RPS:         200,
		Buyers:      NewBuyers([]BuyerConfig{{ID: "b1", Participation: 1}}, quietLogger),
		Client:      &http.Client{},
		Log:         quietLogger,
		MaxInFlight: 100,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	g.Run(ctx, time.Hour)

	if hitsA.Load() < 10 || hitsB.Load() < 10 {
		t.Fatalf("hits a=%d b=%d, want both >= 10", hitsA.Load(), hitsB.Load())
	}
	if d := hitsA.Load() - hitsB.Load(); d > 2 || d < -2 {
		t.Fatalf("not round-robin: a=%d b=%d", hitsA.Load(), hitsB.Load())
	}
}
