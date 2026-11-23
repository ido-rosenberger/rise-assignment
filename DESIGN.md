# Buyer Throttling - Design

> **Status: living draft.** General direction agreed before implementation. Details may change while building; this file is finalized in Phase 7 to match the code.

## Components

```
                 +-------------------- Redis --------------------+
                 |  stats:{minute}   (hash: "{buyer}:total|failed|timeout" -> count, TTL)
                 |  throttle:state   (hash: buyer -> {rate, reason})   read by pods
                 |  throttle:eval    (hash: buyer -> accumulation)     evaluator-only
                 |  throttle:version (counter, +1 on every state write)
                 +-----------------------------------------------+
          HINCRBY every ~1s ^      | HGETALL every ~1s     ^ read closed minute
                            |      v                       | write new state
  +---------------------------------------+       +-------------------+
  | auction pod (x N)                     |       | evaluator (x 1)   |
  |  throttler lib:                       |       |  every minute:    |
  |   ShouldCall -> local snapshot + rand |       |  stats + prev     |
  |   Record     -> local atomic counters |       |  -> Evaluate()    |
  |   flush loop / refresh loop           |       |  -> new state     |
  +---------------------------------------+       +-------------------+
          | calls (when ShouldCall=true)
          v
     fake buyers (simulator, configurable fail/timeout rates)
```

- **Evaluator core** (`internal/evaluator`): pure function `Evaluate(prev, windowStats, rules) -> next`. No IO, so all spec rules are unit-tested directly.
- **Throttler** (`throttler/`, public package): in-process Go library inside each auction pod. Exposes `ShouldCall(buyerID) bool` and `Record(buyerID, result)`. Public because in production other services' repos would import it; it defines the small interfaces it needs from a store.
- **Store**: Redis implementation of those interfaces. The part pods use must be public too, since a public API cannot require internal types.
- **Evaluator service** (`cmd/evaluator`): ticker that runs the core for every known buyer once per window.
- **Auction pod / simulator** (`cmd/...`): demo binaries for docker-compose.

## Data flow

1. **Stats up.** `Record` increments in-memory atomic counters, bucketed by wall-clock minute. A flush loop (~1s) pushes deltas to Redis with pipelined `HINCRBY` on `stats:{minute}` (fields `{buyer}:total`, `{buyer}:failed`, `{buyer}:timeout`). Every pod writes the same key, so Redis does the cross-pod aggregation atomically. The key expires after a few windows.
2. **Decide.** After a minute closes (+ short grace for the last flushes), the evaluator reads the whole minute with one `HGETALL`, plus `throttle:state` and `throttle:eval`. It evaluates every buyer present in either (a buyer with no calls this minute still gets judged as an empty window), then writes both hashes in one `MULTI` so pods never see half an update.

**Why three keys:** they have different writers, readers and rates. Pods read only the small `throttle:state`; evaluator internals (`throttle:eval`) can change without touching the pod contract; per-window counters need TTL, state must not expire. One hash per window (not per buyer) means no buyer registry and one read per evaluation. Trade-off: under Redis Cluster a single key lives on one shard; per-buyer keys would spread load there.
3. **State down.** A refresh loop (~1s) in each pod reads `throttle:version`. Only when it differs from the last seen version (or on startup) does the pod read `throttle:state`, build a new immutable map `buyer -> rate`, and swap it in atomically. The evaluator increments the version in the same `MULTI` as the state write, so a new version always means new state is readable. Result: pods do one tiny `GET` per second and a full read about once per window, so Redis load stays flat as pods and buyers grow.

## Keeping `ShouldCall` fast

- No network, no locks: one atomic pointer load, one map lookup, one random number compare (`rand < rate` => skip).
- Rate `0.0` returns `true` without drawing a random number.
- `Record` only increments atomic counters; Redis IO happens in background goroutines.
- Probabilistic skipping needs no shared counter, so pods and goroutines never coordinate per request. Cost: 95% is exact on average, not per request.

## Consistency across pods

- **Single writer**: only the evaluator writes throttle state, so pods cannot disagree on decisions, only lag (by up to one refresh interval).
- **Shared input**: all pods' stats land in the same Redis keys, so the evaluator sees global health, not one pod's view.
- **Optional leader lock** (`SET NX` with TTL) makes a second evaluator instance safe.
- Pod clock skew smaller than the grace period does not affect which minute a stat lands in.

## Decisions on ambiguous / unspecified cases

- **Low-sample window during recovery: accumulate.** A blocked buyer gets only 5% of traffic, so its windows are often below min sample. Treating them as clean would unblock a buyer we never measured; freezing would leave a low-traffic buyer blocked forever. Instead, a low-sample window is merged with the following ones until the merged sample reaches min sample, then judged as one window.
- **Accumulation is capped** (default 5 windows), so old data does not decide current health.
- **Accumulated samples >= min sample may block.** The spec forbids blocking on too-small samples; a merged sample of 100+ is no longer too small.
- **Unknown buyer => open (`0.0`).** Buyers are revenue; not throttled until measured.
- **Redis unreachable => pods keep last known state** (fail-open); the hot path never waits on Redis.
- **Failed and timeout rates are checked independently**, as the spec defines. Consequence: 50% failed + 25% timeout is not a block; no combined rate is invented.
- **Thresholds use `>=`** ("reaches its threshold").
- **Step rounding:** each recovery step is rounded and clamped at `0.0`, so `0.95 -> 0.85 -> ... -> 0.05 -> 0.0` without float drift.

## Trade-offs

- **Polling vs push**: ~1s staleness accepted; evaluator only changes state once per minute. Pub/sub would cut lag but adds missed-message cases.
- **Stat loss**: a pod crash loses at most ~1s of unflushed counters. Acceptable for health estimation.
- **Redis as single dependency**: hot path never waits on it; outage freezes state instead of breaking auctions.
- **Library vs sidecar/service**: library ties consumers to Go but gives the lowest latency. Rejected: a sidecar (localhost round-trip on every check) and a central throttling service (network round-trip on the critical path, single point of failure, must itself absorb 100k+ rps).
