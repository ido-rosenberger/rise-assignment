# Buyer Throttling - Design

## Components

```
 simulator ──POST /auction──> auction pods (x N) ──GET /bid (150ms)──> buyers
                               │ throttler library:
                               │   ShouldCall: local snapshot + random number
                               │   Record:     local atomic counters
                               │   Run:        flush stats / refresh rates (1s)
                               ▼
              ┌─────────────────── Redis ───────────────────┐
              │ stats:{window}       hash  {buyer}:total|failed|timeouts, TTL 10 windows
              │ throttle:state       hash  buyer -> {throttle_rate, reason, accumulation}
              │ throttle:version     int   +1 on every evaluation
              │ throttle:last_window int   start of the last evaluated window
              └─────────────────────────────────────────────┘
                               ▲
                               │ once per window: read stats + states, Evaluate, save
                         evaluator (x 1) ── GET /state
```

- **`throttler/`** (public library, used inside every auction pod): `ShouldCall(buyerID)`, `Record(buyerID, result)`, and `Run`, a background loop. It defines the small `Store` interface it needs; `throttler/redisstore` implements it.
- **`internal/evaluator`**: `Evaluate(prev, windowStats, rules) -> next` holds every rule as a pure function. `Runner` calls it for every buyer once per window. `GET /state` serves the result.
- **`internal/store`**: the evaluator's Redis adapter. `internal/store/schema` is the Redis layout, shared by both adapters.
- **Demo**: `internal/auction` (pod fan-out over HTTP), `internal/simulator` (traffic + fake buyers), `cmd/*` binaries, docker-compose.

## Code map

| What | Where |
|---|---|
| Spec rules (pure function) | `Evaluate` [evaluator.go:47](internal/evaluator/evaluator.go#L47): `pickSample` [evaluator.go:75](internal/evaluator/evaluator.go#L75), `judge` [evaluator.go:83](internal/evaluator/evaluator.go#L83), `capReached` [evaluator.go:93](internal/evaluator/evaluator.go#L93), `violation` [evaluator.go:102](internal/evaluator/evaluator.go#L102), `stepDown` [evaluator.go:117](internal/evaluator/evaluator.go#L117); tests in [evaluator_test.go](internal/evaluator/evaluator_test.go#L1) |
| Configurable defaults | `DefaultRules` [rules.go:52](internal/config/rules.go#L52), `Validate` [rules.go:103](internal/config/rules.go#L103) |
| Per-request check | `ShouldCall` [throttler.go:155](throttler/throttler.go#L155), `Record` [throttler.go:165](throttler/throttler.go#L165) |
| Pod background loop | `Run` [run.go:14](throttler/run.go#L14): `collectCounters` [run.go:55](throttler/run.go#L55), `sendUnsent` [run.go:75](throttler/run.go#L75), `refresh` [run.go:96](throttler/run.go#L96) |
| Pod Redis adapter | `AddStats` [redisstore.go:62](throttler/redisstore/redisstore.go#L62), `StateVersion` [redisstore.go:93](throttler/redisstore/redisstore.go#L93), `ThrottleRates` [redisstore.go:106](throttler/redisstore/redisstore.go#L106) |
| Evaluator loop | `EvaluatePending` [runner.go:105](internal/evaluator/runner.go#L105), `windowsToEvaluate` [runner.go:130](internal/evaluator/runner.go#L130), `evaluateAll` [runner.go:162](internal/evaluator/runner.go#L162) |
| Evaluator Redis adapter | `Snapshot` [store.go:60](internal/store/store.go#L60), `SaveStates` [store.go:102](internal/store/store.go#L102); key layout [schema.go:20](internal/store/schema/schema.go#L20) |
| `GET /state` | `StateHandler` [state_http.go:40](internal/evaluator/state_http.go#L40) |
| Demo auction fan-out | `Auctioneer.Run` [auction.go:53](internal/auction/auction.go#L53), result classification `call` [auction.go:102](internal/auction/auction.go#L102) |
| End-to-end test (2 pods + evaluator) | [e2e_test.go](internal/evaluator/e2e_test.go#L1) |

## How stats and state flow

1. **Stats up.** `Record` increments in-memory counters per buyer. Every second, flush swaps them to zero and adds them to `stats:{window}` with `HINCRBY` in one `MULTI`. Every pod adds into the same hash, so Redis sums them across pods. Flush ticks fall on whole seconds and the window is a multiple of a second, so each flush lands entirely in one window and `Record` never reads the clock (cost: requests in the few ms between a window boundary and the flush wake-up count in the previous window). A failed send is kept and retried, and dropped once the evaluator has already judged that window.
2. **Decide.** The evaluator wakes at each window end + grace (5s; the last flushes land first). It reads a snapshot (states + last evaluated window, one `MULTI`) and evaluates every window after the last one, oldest first: read the window's stats, `Evaluate` every buyer that has a state or stats, then save the states, bump `throttle:version` and set `throttle:last_window` in one `MULTI`. Each window's result is the next one's input, so catch-up after downtime gives exactly what a never-stopped evaluator would have produced (bounded by the 10-window stats TTL). Because `last_window` is saved with the states, a restart never evaluates a window twice.
3. **State down.** Every second, each pod reads `throttle:version` (one small `GET`). Only when it changed does the pod read `throttle:state`, build a new immutable `buyer -> throttle rate` map, and swap it in.

## Keeping `ShouldCall` fast

- No network, no locks: one atomic pointer load, one map lookup, `rand < rate` means skip. Rate 0 skips the random draw.
- Both hot-path maps (rates, counters) are copy-on-write: a change builds a new map and swaps the pointer, so readers never wait. A new buyer takes a mutex once.
- `Record` does two atomic increments. All Redis IO happens in one background goroutine.
- Skipping is probabilistic, so pods never coordinate per request (95% holds on average, not per request).
- Measured: `ShouldCall` ~1 ns, `Record` ~40 ns with 20 goroutines on one buyer, 0 allocations.

## Consistency across pods

- **One writer.** Only the evaluator writes throttle state, so pods can lag (up to one refresh, ~1s) but never disagree on a decision.
- **Global input.** All pods' stats land in the same Redis keys, so decisions use global health, not one pod's view.
- **Atomic writes.** Every multi-key write is one `MULTI/EXEC`, so readers never see half of a flush or half of an evaluation.
- **One evaluator instance.** Running two would apply a window twice (see trade-offs).

## Decisions on unspecified cases

- **Thresholds use `>=`** ("reaches"). Failure and timeout rates are checked separately, as specified: 50% failed + 25% timeouts is not a block. If both thresholds are reached, the reason is `failure_rate`.
- **Small windows are accumulated.** A blocked buyer gets only 5% of its traffic, so its windows are often below the minimum sample. Counting them as clean would unblock a buyer nobody measured; freezing would keep a low-traffic buyer blocked forever. So a window below the minimum is merged with the next ones until the merged sample reaches the minimum, then judged as one sample. A merged sample of 100+ may block.
- **A window that alone reaches the minimum is judged alone.** Merging it with older small windows could hide a failing window or block a healthy one.
- **Accumulation is capped at 5 windows.** If even 5 windows stay below the minimum (very low traffic), the sample can never block (spec). It steps down if it has requests and is under both thresholds; otherwise the rate holds. With zero requests the rate holds: a buyer with no traffic is unaffected by its rate, and a stats outage cannot silently unblock buyers.
- **Unknown buyers are open** (`0.0`): buyers are revenue; not throttled until measured.
- **Redis unreachable**: pods keep their last rates (fail-open) and retry stats; the hot path never waits. Malformed Redis entries are logged and skipped, never block every buyer.
- **Rates are rounded** to 6 decimals each step, so `0.95 -> 0.85 -> ... -> 0.05 -> 0` is exact. A stored rate above the block level (after a config change) is clamped.
- **Missed windows are caught up oldest first**, so the newest data is applied last. If the window size changes, catch-up realigns to the new boundaries.

## Observability

- **`GET /state`** (evaluator): every buyer's `buyer_id`, `throttle_rate`, `reason` (spec format), plus `last_window` with total, failed, timeouts and both error rates: the per-buyer metrics. Read from Redis in one snapshot, so state and stats belong to the same window.
- **Logs**: the evaluator logs each throttle state change (window stats, accumulated stats, old -> new rate, reason). Pods log per-buyer traffic per window: attempted, called, skipped, ok, failed, timeouts.

## Trade-offs

- **Library, not a service.** An in-process library gives the lowest latency but ties pods to Go. Rejected: a sidecar (a localhost round trip on every check) and a central throttling service (a network round trip on the critical path, a single point of failure, and it would itself need to absorb 100k+ requests per second).
- **Polling, not push.** About 1s staleness is fine when state changes once per window; the version key keeps polling cheap. Pub/sub would cut the lag but adds missed-message cases.
- **Stats loss.** A pod crash loses at most ~1s of counters. A `MULTI` that succeeds but whose reply is lost is retried and double-counts one flush. Both are acceptable for health estimates.
- **One hash per window** means no buyer registry and one read per evaluation, but under Redis Cluster that key lives on one shard; per-buyer keys would spread the load.
- **One evaluator.** Simple and correct as long as only one runs. Hardening: `WATCH throttle:last_window` so a second instance's save aborts.
- **Snapshot metrics.** `/state` has no history or alerting; production would add Prometheus `/metrics`.
