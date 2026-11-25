# Buyer Throttling - Design

## Components

```
 simulator ──POST /auction──> auction pods (x N) ──GET /bid (150ms)──> buyers
                               │ throttler library: ShouldCall, Record, Run (1s loop)
                               ▼
              ┌─────────────────── Redis ───────────────────┐
              │ stats:{window}       {buyer}:total|failed|timeouts, TTL 10 windows
              │ throttle:state       buyer -> {throttle_rate, reason, accumulation}
              │ throttle:version     +1 on every evaluation
              │ throttle:last_window start of the last evaluated window
              └─────────────────────────────────────────────┘
                               ▲ once per window: stats + states -> Evaluate -> save
                         evaluator (x 1) ── GET /state, GET /metrics
```

- **`throttler/`**: public library inside every auction pod. `ShouldCall` and `Record` touch only local memory; `Run` syncs with Redis in the background (adapter: `throttler/redisstore`).
- **`internal/evaluator`**: `Evaluate(prev, windowStats, rules) -> next` is every spec rule as one pure function; `Runner` applies it to every buyer once per window; `GET /state` returns each buyer's state and last-window counts and error rates; `GET /metrics` exposes throttle rates, error rates and request counters to Prometheus.
- **`internal/store`**: the evaluator's Redis adapter; `schema/` defines the Redis layout for both sides. Function-level links: README, "Code map".

## Data flow

1. **Stats up.** `Record` increments per-buyer atomic counters. Each second the pod swaps them to zero and `HINCRBY`s them into `stats:{window}` (one `MULTI`); all pods add into the same hash, so Redis sums across pods. Flush ticks land on whole seconds and the window is a multiple of a second, so each flush belongs to exactly one window and `Record` never reads the clock. Failed sends are retried until the window is judged.
2. **Decide.** At each window end + grace (5s, for the last flushes), the evaluator reads states and `last_window` atomically, then evaluates every window since, oldest first: each result feeds the next, so catch-up after downtime equals never stopping (bounded by the stats TTL). It saves states, bumps the version and sets `last_window` in one `MULTI`, so no window is evaluated twice.
3. **State down.** Each second a pod `GET`s `throttle:version`; only when it changed does it read `throttle:state` and swap in a new immutable `buyer -> rate` map.

## Fast per-request check

`ShouldCall` is one atomic pointer load, one map lookup and `rand < rate`: no network, no locks, 0 allocations (~1 ns). Both hot-path maps are copy-on-write; a new buyer takes a mutex once. `Record` is two atomic adds (~40 ns under contention). Skipping is probabilistic, so pods never coordinate per request.

## Consistency across pods

Only the evaluator writes state, so pods may lag (~1s) but never disagree. All pods' stats meet in the same keys, so decisions use global health. Every multi-key write is one `MULTI/EXEC`, so no reader sees half an update.

## Decisions on unspecified cases

- **`>=` thresholds**; failure and timeout rates are checked separately (50% failed + 25% timeouts is no block); both reached means reason `failure_rate`.
- **Small windows accumulate.** A blocked buyer gets 5% of its traffic, so its windows are often below the minimum. Counting them as clean would unblock on no evidence; freezing would block a low-traffic buyer forever. They are merged until the sample reaches the minimum, then judged; a merged 100+ may block.
- **A window that alone reaches the minimum is judged alone**, so older small windows cannot hide or fake a failure.
- **Cap of 5 merged windows.** Still too small: never block; step down if it has requests and is under both thresholds, else hold. Zero requests hold, so a stats outage cannot unblock buyers.
- **Unknown buyer: open.** **Redis down: pods keep their last rates** (fail-open). Malformed entries are skipped and logged.
- **Rates rounded to 6 decimals** (exact `0.95 -> 0.85 -> ... -> 0`); a rate above a lowered block level is clamped. Catch-up realigns if the window size changes.

## Trade-offs

- **Library, not a service**: lowest latency, but Go-only. A sidecar adds a localhost hop per check; a central service adds a network hop, a single point of failure and 100k+ rps of its own.
- **Polling, not push**: ~1s lag is fine for once-per-window changes; the version key keeps it cheap.
- **Stats can be lost or doubled slightly**: ~1s on a pod crash; one flush if a `MULTI` reply is lost and retried. Fine for health estimates.
- **One hash per window**: no buyer registry, one read per evaluation; on Redis Cluster per-buyer keys would spread load.
- **One evaluator**: a second one (e.g. overlapping in a rolling deploy) cannot apply a window twice, since states and `last_window` are always read and written together, but the slower one's save wins and state may briefly step back a window. Run one replica; `WATCH throttle:last_window` would make extra ones no-ops.
- **Metrics only from the evaluator**: it already has every number per window, so pods stay metrics-free; skipped-call counts exist only in pods and are in pod logs, not metrics.
