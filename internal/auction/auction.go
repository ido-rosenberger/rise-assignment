// Package auction runs one auction: it calls every eligible buyer in
// parallel within a deadline, using the throttler to skip unhealthy buyers
// and to report each call's result.
package auction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ido-rosenberger/rise-assignment/throttler"
)

// Throttler is the part of *throttler.Throttler an auction needs.
type Throttler interface {
	ShouldCall(buyerID string) bool
	Record(buyerID string, result throttler.Result)
}

// Auctioneer calls buyers at BuyerBaseURL + "/bid/{buyerID}".
type Auctioneer struct {
	Throttler    Throttler
	Client       *http.Client
	BuyerBaseURL string
	// Timeout is the auction deadline for all buyer calls (e.g. 150ms).
	Timeout time.Duration
	// Traffic, if set, counts every buyer decision and result.
	Traffic *Traffic
}

// Bid is a buyer's answer.
type Bid struct {
	BuyerID string  `json:"buyer_id"`
	Price   float64 `json:"price"`
}

// Outcome is the auction result: the winning bid (nil if no bid) and how
// many buyers were called and skipped.
type Outcome struct {
	Winner  *Bid `json:"winner"`
	Called  int  `json:"called"`
	Skipped int  `json:"skipped"`
}

// Run holds one auction among buyerIDs: skipped buyers are counted, the
// others are called in parallel under one deadline, and the highest bid wins.
func (a *Auctioneer) Run(parent context.Context, buyerIDs []string) Outcome {
	ctx, cancel := context.WithTimeout(parent, a.Timeout)
	defer cancel()

	var (
		out Outcome
		wg  sync.WaitGroup
		mu  sync.Mutex // guards out.Winner
	)
	for _, id := range buyerIDs {
		if !a.Throttler.ShouldCall(id) {
			out.Skipped++
			a.Traffic.add(id, outcomeSkipped)
			continue
		}
		out.Called++
		wg.Add(1)
		go func() {
			defer wg.Done()
			bid := a.callAndRecord(parent, ctx, id)
			if bid == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if out.Winner == nil || bid.Price > out.Winner.Price {
				out.Winner = bid
			}
		}()
	}
	wg.Wait()
	return out
}

// callAndRecord calls one buyer and records the result with the throttler.
// If the auction itself was canceled (client gone, shutdown), the result is
// not the buyer's fault and is not recorded.
func (a *Auctioneer) callAndRecord(parent, ctx context.Context, buyerID string) *Bid {
	bid, result := a.call(ctx, buyerID)
	if parent.Err() != nil {
		return nil
	}
	a.Throttler.Record(buyerID, result)
	a.Traffic.add(buyerID, outcomeFor(result))
	return bid
}

// call asks one buyer for a bid and classifies the result: a missed
// deadline is a timeout, any other error or a non-2xx status is a failure.
func (a *Auctioneer) call(ctx context.Context, buyerID string) (*Bid, throttler.Result) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.BuyerBaseURL+"/bid/"+url.PathEscape(buyerID), nil)
	if err != nil {
		return nil, throttler.ResultFailed
	}
	resp, err := a.Client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, throttler.ResultTimeout
		}
		return nil, throttler.ResultFailed
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Read the (small) body so the connection can be reused.
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return nil, throttler.ResultFailed
	}
	var bid Bid
	if err := json.NewDecoder(resp.Body).Decode(&bid); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, throttler.ResultTimeout
		}
		return nil, throttler.ResultFailed
	}
	bid.BuyerID = buyerID
	return &bid, throttler.ResultOK
}

// Request is the body of POST /auction.
type Request struct {
	Buyers []string `json:"buyers"`
}

// Handler serves POST /auction.
func (a *Auctioneer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.Run(r.Context(), req.Buyers))
	})
}
