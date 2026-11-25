package evaluator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

func getState(t *testing.T, store Store) (int, StateResponse) {
	t.Helper()
	rec := httptest.NewRecorder()
	StateHandler(store).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/state", nil))
	var resp StateResponse
	if rec.Code == http.StatusOK {
		if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
			t.Fatal(err)
		}
	}
	return rec.Code, resp
}

func TestStateHandler(t *testing.T) {
	store := newFakeStore()
	store.states = map[string]State{
		"buyer_789": {},
		"buyer_123": {ThrottleRate: 0.95, Reason: ReasonFailureRate},
		"buyer_456": {ThrottleRate: 0.35, Reason: ReasonRecovering},
	}
	store.last = minute(9)
	store.stats[minute(9)] = map[string]buyer.Stats{
		"buyer_123": {Total: 200, Failed: 150, Timeouts: 10},
		"buyer_456": {Total: 3, Timeouts: 1},
	}

	code, resp := getState(t, store)
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	want := []BuyerState{
		{BuyerID: "buyer_123", ThrottleRate: 0.95, Reason: "failure_rate",
			LastWindow: &WindowInfo{Start: minute(9), Total: 200, Failed: 150, Timeouts: 10, FailureRate: 0.75, TimeoutRate: 0.05}},
		{BuyerID: "buyer_456", ThrottleRate: 0.35, Reason: "recovering",
			LastWindow: &WindowInfo{Start: minute(9), Total: 3, Timeouts: 1, TimeoutRate: 0.3333}},
		{BuyerID: "buyer_789", ThrottleRate: 0, Reason: "",
			LastWindow: &WindowInfo{Start: minute(9)}}, // no requests: rates 0, no division by zero
	}
	if len(resp.Buyers) != len(want) {
		t.Fatalf("got %d buyers, want %d", len(resp.Buyers), len(want))
	}
	for i, w := range want {
		g := resp.Buyers[i]
		if g.BuyerID != w.BuyerID || g.ThrottleRate != w.ThrottleRate || g.Reason != w.Reason {
			t.Errorf("buyer %d = %+v, want %+v", i, g, w)
			continue
		}
		gw, ww := *g.LastWindow, *w.LastWindow
		if !gw.Start.Equal(ww.Start) {
			t.Errorf("%s last_window.start = %v, want %v", w.BuyerID, gw.Start, ww.Start)
		}
		gw.Start = ww.Start // compared with Equal above (time zone may differ)
		if gw != ww {
			t.Errorf("%s last_window = %+v, want %+v", w.BuyerID, gw, ww)
		}
	}
}

func TestStateHandlerBeforeFirstEvaluation(t *testing.T) {
	code, resp := getState(t, newFakeStore())
	if code != http.StatusOK || resp.Buyers == nil || len(resp.Buyers) != 0 {
		t.Fatalf("status %d, buyers %v; want 200 and an empty list", code, resp.Buyers)
	}
}

// failingStore fails reading the snapshot.
type failingStore struct{ fakeStore }

func (*failingStore) Snapshot(context.Context) (Snapshot, error) {
	return Snapshot{}, errors.New("redis down")
}

func TestStateHandlerStoreError(t *testing.T) {
	if code, _ := getState(t, &failingStore{}); code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", code)
	}
}

// The response keeps the spec's field names.
func TestStateHandlerJSONFieldNames(t *testing.T) {
	store := newFakeStore()
	store.states["b1"] = State{ThrottleRate: 0.95, Reason: ReasonTimeoutRate}
	rec := httptest.NewRecorder()
	StateHandler(store).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/state", nil))

	var raw map[string][]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	b := raw["buyers"][0]
	if b["buyer_id"] != "b1" || b["throttle_rate"] != 0.95 || b["reason"] != "timeout_rate" {
		t.Fatalf("fields = %v", b)
	}
	if _, ok := b["last_window"]; ok {
		t.Fatal("last_window must be omitted before any evaluation")
	}
}
