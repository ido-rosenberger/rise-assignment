package evaluator

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// StateResponse is the body of GET /state.
type StateResponse struct {
	Buyers []BuyerState `json:"buyers"`
}

// BuyerState is one buyer's throttle state plus the stats of the last
// evaluated window (the per-buyer metrics).
type BuyerState struct {
	BuyerID      string      `json:"buyer_id"`
	ThrottleRate float64     `json:"throttle_rate"`
	Reason       string      `json:"reason"`
	LastWindow   *WindowInfo `json:"last_window,omitempty"`
}

// WindowInfo is a buyer's request counts and error rates in one window.
type WindowInfo struct {
	Start       time.Time `json:"start"`
	Total       int64     `json:"total"`
	Failed      int64     `json:"failed"`
	Timeouts    int64     `json:"timeouts"`
	FailureRate float64   `json:"failure_rate"`
	TimeoutRate float64   `json:"timeout_rate"`
}

// StateHandler serves GET /state from the store: every known buyer's
// throttle rate and reason, and its stats in the last evaluated window.
func StateHandler(store Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		states, last, ok, err := consistentStates(r.Context(), store)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var stats map[string]buyer.Stats
		if ok {
			if stats, err = store.WindowStats(r.Context(), last); err != nil {
				http.Error(w, "read window stats: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		resp := StateResponse{Buyers: make([]BuyerState, 0, len(states))}
		for id, st := range states {
			bs := BuyerState{BuyerID: id, ThrottleRate: st.ThrottleRate, Reason: string(st.Reason)}
			if ok {
				bs.LastWindow = windowInfo(last, stats[id])
			}
			resp.Buyers = append(resp.Buyers, bs)
		}
		slices.SortFunc(resp.Buyers, func(a, b BuyerState) int { return cmp.Compare(a.BuyerID, b.BuyerID) })

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(resp)
	})
}

// consistentStates reads the states and the window they came from. The two
// are separate reads, so it re-reads when an evaluation landed in between.
func consistentStates(ctx context.Context, store Store) (map[string]State, time.Time, bool, error) {
	for attempt := 0; ; attempt++ {
		before, okBefore, err := store.LastWindow(ctx)
		if err != nil {
			return nil, time.Time{}, false, fmt.Errorf("read last window: %w", err)
		}
		states, err := store.States(ctx)
		if err != nil {
			return nil, time.Time{}, false, fmt.Errorf("read states: %w", err)
		}
		after, okAfter, err := store.LastWindow(ctx)
		if err != nil {
			return nil, time.Time{}, false, fmt.Errorf("read last window: %w", err)
		}
		if (okBefore == okAfter && before.Equal(after)) || attempt == 2 {
			return states, after, okAfter, nil
		}
	}
}

func windowInfo(start time.Time, s buyer.Stats) *WindowInfo {
	info := &WindowInfo{Start: start, Total: s.Total, Failed: s.Failed, Timeouts: s.Timeouts}
	if s.Total > 0 {
		info.FailureRate = round4(float64(s.Failed) / float64(s.Total))
		info.TimeoutRate = round4(float64(s.Timeouts) / float64(s.Total))
	}
	return info
}

func round4(x float64) float64 { return math.Round(x*1e4) / 1e4 }
