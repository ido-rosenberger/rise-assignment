package auction

import (
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/ido-rosenberger/rise-assignment/throttler"
)

type outcome int

const (
	outcomeSkipped outcome = iota
	outcomeOK
	outcomeFailed
	outcomeTimeout
	numOutcomes
)

func outcomeFor(r throttler.Result) outcome {
	switch r {
	case throttler.ResultFailed:
		return outcomeFailed
	case throttler.ResultTimeout:
		return outcomeTimeout
	default:
		return outcomeOK
	}
}

// Traffic counts per-buyer decisions and results for the pod's periodic log
// line. It is observability only; throttling decisions never read it.
// A nil *Traffic is valid and counts nothing.
type Traffic struct {
	buyers sync.Map // buyerID -> *[numOutcomes]atomic.Int64
}

func (t *Traffic) add(buyerID string, o outcome) {
	if t == nil {
		return
	}
	v, ok := t.buyers.Load(buyerID)
	if !ok {
		v, _ = t.buyers.LoadOrStore(buyerID, new([numOutcomes]atomic.Int64))
	}
	v.(*[numOutcomes]atomic.Int64)[o].Add(1)
}

// LogAndReset logs one line per buyer with counts since the last call, then
// resets them.
func (t *Traffic) LogAndReset(log *slog.Logger) {
	var ids []string
	t.buyers.Range(func(k, _ any) bool {
		ids = append(ids, k.(string))
		return true
	})
	slices.Sort(ids)
	for _, id := range ids {
		v, _ := t.buyers.Load(id)
		c := v.(*[numOutcomes]atomic.Int64)
		var n [numOutcomes]int64
		for i := range n {
			n[i] = c[i].Swap(0)
		}
		called := n[outcomeOK] + n[outcomeFailed] + n[outcomeTimeout]
		attempted := called + n[outcomeSkipped]
		if attempted == 0 {
			continue
		}
		log.Info("buyer traffic",
			"buyer", id,
			"attempted", attempted,
			"called", called,
			"skipped", n[outcomeSkipped],
			"ok", n[outcomeOK],
			"failed", n[outcomeFailed],
			"timeouts", n[outcomeTimeout])
	}
}
