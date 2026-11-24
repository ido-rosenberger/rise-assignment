package throttler

import (
	"context"
	"time"

	"github.com/ido-rosenberger/rise-assignment/buyer"
)

// Run loads throttle rates, then flushes stats and refreshes rates until ctx
// is canceled. On cancel it makes a final flush and returns. Store errors are
// logged, never returned: ShouldCall keeps the last known rates and unsent
// stats are retried.
func (t *Throttler) Run(ctx context.Context) {
	t.refresh(ctx)

	refreshTicker := time.NewTicker(t.cfg.RefreshInterval)
	defer refreshTicker.Stop()

	// Flush ticks are aligned to multiples of FlushInterval, so a window
	// boundary is always a tick and no flush interval spans two windows.
	nextFlush := t.nextFlushTick()
	flushTimer := time.NewTimer(nextFlush.Sub(t.now()))
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			// ctx is canceled; give the final flush its own deadline.
			t.flush(context.WithoutCancel(ctx), t.now())
			return
		case <-flushTimer.C:
			t.flush(ctx, nextFlush.Add(-t.cfg.FlushInterval))
			nextFlush = t.nextFlushTick()
			flushTimer.Reset(nextFlush.Sub(t.now()))
		case <-refreshTicker.C:
			t.refresh(ctx)
		}
	}
}

func (t *Throttler) nextFlushTick() time.Time {
	return t.now().Truncate(t.cfg.FlushInterval).Add(t.cfg.FlushInterval)
}

// flush moves the counters into the unsent stats of the window containing
// intervalStart, then sends every unsent window. A window that fails to send
// is kept and retried on the next flush, until it is a full evaluation window
// past its end; by then the evaluator has judged it, so it is dropped.
func (t *Throttler) flush(ctx context.Context, intervalStart time.Time) {
	window := intervalStart.Truncate(t.cfg.EvaluationWindow)
	for buyerID, c := range *t.counters.Load() {
		// Outcomes before total: the reverse of Record's order.
		var s buyer.Stats
		s.Failed = c.failed.Swap(0)
		s.Timeouts = c.timeouts.Swap(0)
		s.Total = c.total.Swap(0)
		if s == (buyer.Stats{}) {
			continue
		}
		if t.unsent[window] == nil {
			t.unsent[window] = make(map[string]buyer.Stats)
		}
		t.unsent[window][buyerID] = t.unsent[window][buyerID].Add(s)
	}

	// One deadline for all sends, so a store outage delays the next flush
	// tick by at most StoreTimeout, however many windows are unsent.
	ctx, cancel := context.WithTimeout(ctx, t.cfg.StoreTimeout)
	defer cancel()
	for start, stats := range t.unsent {
		err := t.store.AddStats(ctx, start, stats)
		switch {
		case err == nil:
			delete(t.unsent, start)
		case t.now().After(start.Add(2 * t.cfg.EvaluationWindow)):
			t.log.Error("dropping unsent stats", "window", start, "buyers", len(stats), "err", err)
			delete(t.unsent, start)
		default:
			t.log.Warn("flush failed, will retry", "window", start, "buyers", len(stats), "err", err)
		}
	}
}

// refresh reloads throttle rates when the store's state version changed (or
// on the first successful call). On error the current rates stay in use.
func (t *Throttler) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, t.cfg.StoreTimeout)
	defer cancel()

	version, err := t.store.StateVersion(ctx)
	if err != nil {
		t.log.Warn("state version check failed, keeping current throttle rates", "err", err)
		return
	}
	if t.loaded && version == t.version {
		return
	}

	rates, version, err := t.store.ThrottleRates(ctx)
	if err != nil {
		t.log.Warn("throttle rates load failed, keeping current throttle rates", "err", err)
		return
	}
	if rates == nil {
		rates = map[string]float64{}
	}
	t.rates.Store(&rates)
	t.version, t.loaded = version, true
	t.log.Info("throttle rates loaded", "version", version, "buyers", len(rates))
}
