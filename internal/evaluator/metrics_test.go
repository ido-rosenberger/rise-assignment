package evaluator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ido-rosenberger/rise-assignment/buyer"
	"github.com/ido-rosenberger/rise-assignment/internal/config"
)

func newRunnerWithMetrics(t *testing.T, store Store, now time.Time) (*Runner, *Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	r, err := NewRunner(RunnerConfig{
		Rules:             config.DefaultRules(),
		Grace:             5 * time.Second,
		MaxCatchUpWindows: 10,
		Logger:            quietLogger,
		Metrics:           m,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return now }
	return r, m, reg
}

// After a window is evaluated, the metrics show each buyer's throttle rate,
// error rates and request counts.
func TestMetricsAfterWindow(t *testing.T) {
	store := newFakeStore()
	store.last = minute(8)
	store.stats[minute(9)] = map[string]buyer.Stats{
		"bad":  {Total: 200, Failed: 150, Timeouts: 10},
		"good": {Total: 100, Timeouts: 1},
	}
	r, m, _ := newRunnerWithMetrics(t, store, minute(10).Add(6*time.Second))
	if err := r.EvaluatePending(context.Background()); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"bad throttle rate", testutil.ToFloat64(m.throttleRate.WithLabelValues("bad")), 0.95},
		{"good throttle rate", testutil.ToFloat64(m.throttleRate.WithLabelValues("good")), 0},
		{"bad failure rate", testutil.ToFloat64(m.failureRate.WithLabelValues("bad")), 0.75},
		{"bad timeout rate", testutil.ToFloat64(m.timeoutRate.WithLabelValues("bad")), 0.05},
		{"good timeout rate", testutil.ToFloat64(m.timeoutRate.WithLabelValues("good")), 0.01},
		{"bad ok requests", testutil.ToFloat64(m.requests.WithLabelValues("bad", "ok")), 40},
		{"bad failed requests", testutil.ToFloat64(m.requests.WithLabelValues("bad", "failed")), 150},
		{"bad timeout requests", testutil.ToFloat64(m.requests.WithLabelValues("bad", "timeout")), 10},
		{"windows evaluated", testutil.ToFloat64(m.windows), 1},
		{"last window", testutil.ToFloat64(m.lastWindow), float64(minute(9).Unix())},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// Request counters add up across windows (catch-up evaluates two here).
func TestMetricsCountersAccumulate(t *testing.T) {
	store := newFakeStore()
	store.last = minute(7)
	store.stats[minute(8)] = map[string]buyer.Stats{"b1": {Total: 100, Failed: 5}}
	store.stats[minute(9)] = map[string]buyer.Stats{"b1": {Total: 50, Failed: 1}}
	r, m, _ := newRunnerWithMetrics(t, store, minute(10).Add(6*time.Second))
	if err := r.EvaluatePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("b1", "failed")); got != 6 {
		t.Errorf("failed total = %v, want 6", got)
	}
	if got := testutil.ToFloat64(m.windows); got != 2 {
		t.Errorf("windows = %v, want 2", got)
	}
	// Window 12:09 had 50 requests: error rate is from that window only.
	if got := testutil.ToFloat64(m.failureRate.WithLabelValues("b1")); got != 0.02 {
		t.Errorf("failure rate = %v, want 0.02", got)
	}
}

// A failed save records no metrics, so a retried window is not counted twice.
func TestMetricsNotRecordedOnFailedSave(t *testing.T) {
	store := newFakeStore()
	store.stats[minute(9)] = map[string]buyer.Stats{"b1": {Total: 100, Failed: 100}}
	store.saveErr = context.DeadlineExceeded
	r, m, _ := newRunnerWithMetrics(t, store, minute(10).Add(6*time.Second))
	if err := r.EvaluatePending(context.Background()); err == nil {
		t.Fatal("want error")
	}
	if got := testutil.ToFloat64(m.windows); got != 0 {
		t.Fatalf("windows = %v after failed save, want 0", got)
	}
}

// The exposition text uses the documented metric names.
func TestMetricsExposition(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.observeWindow(minute(9), map[string]State{"b1": {ThrottleRate: 0.85, Reason: ReasonRecovering}},
		map[string]buyer.Stats{"b1": {Total: 10, Failed: 1}})

	want := `
# HELP throttling_buyer_throttle_rate Fraction of the buyer's requests skipped (0 = open, block level = blocked).
# TYPE throttling_buyer_throttle_rate gauge
throttling_buyer_throttle_rate{buyer="b1"} 0.85
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "throttling_buyer_throttle_rate"); err != nil {
		t.Fatal(err)
	}
}

// A nil *Metrics is valid: the Runner works without metrics.
func TestNilMetrics(t *testing.T) {
	var m *Metrics
	m.observeWindow(minute(9), map[string]State{"b1": {}}, nil) // must not panic
}
