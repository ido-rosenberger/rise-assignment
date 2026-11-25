package evaluator

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// Metrics are the evaluator's Prometheus metrics, updated after every
// evaluated window. A nil *Metrics records nothing.
type Metrics struct {
	throttleRate *prometheus.GaugeVec   // buyer
	failureRate  *prometheus.GaugeVec   // buyer, last evaluated window
	timeoutRate  *prometheus.GaugeVec   // buyer, last evaluated window
	requests     *prometheus.CounterVec // buyer, result: all windows so far
	windows      prometheus.Counter
	lastWindow   prometheus.Gauge
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "throttling"
	m := &Metrics{
		throttleRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "buyer_throttle_rate",
			Help: "Fraction of the buyer's requests skipped (0 = open, block level = blocked).",
		}, []string{"buyer"}),
		failureRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "buyer_failure_rate",
			Help: "failed / total in the last evaluated window (0 when the window had no requests).",
		}, []string{"buyer"}),
		timeoutRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Name: "buyer_timeout_rate",
			Help: "timeouts / total in the last evaluated window (0 when the window had no requests).",
		}, []string{"buyer"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "buyer_requests_total",
			Help: "Requests sent to the buyer, by result, summed over all pods and evaluated windows.",
		}, []string{"buyer", "result"}),
		windows: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "evaluator_windows_total",
			Help: "Evaluation windows evaluated and saved.",
		}),
		lastWindow: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "evaluator_last_window_timestamp_seconds",
			Help: "Start of the last evaluated window, unix seconds.",
		}),
	}
	reg.MustRegister(m.throttleRate, m.failureRate, m.timeoutRate, m.requests, m.windows, m.lastWindow)
	return m
}

// observeWindow records one evaluated window: every buyer's new throttle
// rate, its error rates in the window, and its request counts.
func (m *Metrics) observeWindow(windowStart time.Time, states map[string]State, stats map[string]buyer.Stats) {
	if m == nil {
		return
	}
	for buyerID, st := range states {
		s := stats[buyerID]
		m.throttleRate.WithLabelValues(buyerID).Set(st.ThrottleRate)
		failureRate, timeoutRate := errorRates(s)
		m.failureRate.WithLabelValues(buyerID).Set(failureRate)
		m.timeoutRate.WithLabelValues(buyerID).Set(timeoutRate)
		// max guards against inconsistent stats: Counter.Add panics on < 0.
		m.requests.WithLabelValues(buyerID, "ok").Add(float64(max(0, s.Total-s.Failed-s.Timeouts)))
		m.requests.WithLabelValues(buyerID, "failed").Add(float64(s.Failed))
		m.requests.WithLabelValues(buyerID, "timeout").Add(float64(s.Timeouts))
	}
	m.windows.Inc()
	m.lastWindow.Set(float64(windowStart.Unix()))
}

// errorRates returns failed/total and timeouts/total, or zeros for an empty
// window.
func errorRates(s buyer.Stats) (failureRate, timeoutRate float64) {
	if s.Total == 0 {
		return 0, 0
	}
	return float64(s.Failed) / float64(s.Total), float64(s.Timeouts) / float64(s.Total)
}
