// Package buyer holds domain types shared by the auction-side throttler, the
// store and the evaluator.
package buyer

// Stats are a buyer's request outcomes over one or more evaluation windows.
// Failed and Timeouts are disjoint: each request is ok, failed, or a timeout.
type Stats struct {
	Total    int64
	Failed   int64
	Timeouts int64
}

// Add returns the sum of two Stats.
func (s Stats) Add(o Stats) Stats {
	return Stats{
		Total:    s.Total + o.Total,
		Failed:   s.Failed + o.Failed,
		Timeouts: s.Timeouts + o.Timeouts,
	}
}
