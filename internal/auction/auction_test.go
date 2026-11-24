package auction

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ido-rosenberger/rise-assignment/throttler"
)

// fakeThrottler skips the buyers in skip and records every result.
type fakeThrottler struct {
	skip    map[string]bool
	mu      sync.Mutex
	results map[string]throttler.Result
}

func (f *fakeThrottler) ShouldCall(id string) bool { return !f.skip[id] }

func (f *fakeThrottler) Record(id string, r throttler.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results[id] = r
}

// buyerServer serves /bid/{id}: "ok-<price>" bids, "fail" returns 500,
// "slow" stalls past the deadline, "garbage" returns invalid JSON.
func buyerServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/bid/")
		switch {
		case id == "fail":
			http.Error(w, "boom", http.StatusInternalServerError)
		case id == "slow":
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
		case id == "garbage":
			io.WriteString(w, "{not json")
		case id == "ok-low":
			io.WriteString(w, `{"price":0.5}`)
		case id == "ok-high":
			io.WriteString(w, `{"price":0.9}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newAuctioneer(t *testing.T, ft *fakeThrottler) *Auctioneer {
	return &Auctioneer{
		Throttler:    ft,
		Client:       &http.Client{},
		BuyerBaseURL: buyerServer(t).URL,
		Timeout:      100 * time.Millisecond,
		Traffic:      &Traffic{},
	}
}

func TestRunClassifiesResults(t *testing.T) {
	ft := &fakeThrottler{skip: map[string]bool{"skipped": true}, results: map[string]throttler.Result{}}
	a := newAuctioneer(t, ft)

	out := a.Run(context.Background(), []string{"ok-low", "ok-high", "fail", "slow", "garbage", "skipped"})

	want := map[string]throttler.Result{
		"ok-low":  throttler.ResultOK,
		"ok-high": throttler.ResultOK,
		"fail":    throttler.ResultFailed,
		"slow":    throttler.ResultTimeout,
		"garbage": throttler.ResultFailed,
	}
	for id, w := range want {
		if got, ok := ft.results[id]; !ok || got != w {
			t.Errorf("%s recorded %v (present %v), want %v", id, got, ok, w)
		}
	}
	if _, ok := ft.results["skipped"]; ok {
		t.Error("skipped buyer must not be recorded")
	}
	if out.Called != 5 || out.Skipped != 1 {
		t.Errorf("called %d skipped %d, want 5 and 1", out.Called, out.Skipped)
	}
	if out.Winner == nil || out.Winner.BuyerID != "ok-high" || out.Winner.Price != 0.9 {
		t.Errorf("winner = %+v, want ok-high at 0.9", out.Winner)
	}
}

func TestRunRespectsDeadline(t *testing.T) {
	ft := &fakeThrottler{results: map[string]throttler.Result{}}
	a := newAuctioneer(t, ft)
	start := time.Now()
	a.Run(context.Background(), []string{"slow", "slow2-unknown"})
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("auction took %v, deadline is %v", elapsed, a.Timeout)
	}
}

// A canceled auction (client gone, shutdown) is not the buyers' fault.
func TestCanceledAuctionRecordsNothing(t *testing.T) {
	ft := &fakeThrottler{results: map[string]throttler.Result{}}
	a := newAuctioneer(t, ft)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a.Run(ctx, []string{"ok-low", "slow"})
	if len(ft.results) != 0 {
		t.Fatalf("recorded %v for a canceled auction", ft.results)
	}
}

func TestHandler(t *testing.T) {
	ft := &fakeThrottler{results: map[string]throttler.Result{}}
	a := newAuctioneer(t, ft)

	body, _ := json.Marshal(Request{Buyers: []string{"ok-low"}})
	rec := httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/auction", bytes.NewReader(body)))
	var out Outcome
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || out.Winner == nil || out.Winner.BuyerID != "ok-low" {
		t.Fatalf("status %d, outcome %+v", rec.Code, out)
	}

	rec = httptest.NewRecorder()
	a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/auction", strings.NewReader("nope")))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad body: status %d, want 400", rec.Code)
	}
}

func TestTrafficLogAndReset(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	tr := &Traffic{}
	tr.add("b1", outcomeSkipped)
	tr.add("b1", outcomeOK)
	tr.add("b1", outcomeFailed)
	tr.add("b1", outcomeTimeout)

	tr.LogAndReset(log)
	line := buf.String()
	for _, want := range []string{"buyer=b1", "attempted=4", "called=3", "skipped=1", "ok=1", "failed=1", "timeouts=1"} {
		if !strings.Contains(line, want) {
			t.Errorf("log %q missing %q", line, want)
		}
	}

	buf.Reset()
	tr.LogAndReset(log)
	if buf.Len() != 0 {
		t.Errorf("after reset, idle buyer still logged: %q", buf.String())
	}

	var nilTraffic *Traffic
	nilTraffic.add("b1", outcomeOK) // must not panic
}
