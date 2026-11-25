# Buyer Throttling Service

Monitors the health of external buyers in a real-time auction platform and decides how much traffic each one receives. A bad buyer is blocked (95% of its requests skipped) and then recovers step by step. Written in Go, with Redis as the shared store.

Design and decisions: [DESIGN.md](DESIGN.md).

## Run the demo

Requirements: Docker with Compose.

```sh
docker compose up --build      # or, with the older standalone tool: docker-compose up --build
```

This starts Redis, the evaluator, two auction pods and a simulator (traffic + fake buyers). The demo uses **10-second evaluation windows** (spec default: 1 minute), so one block-and-recovery cycle takes about 2 minutes.

### What happens

The simulator (`simulator.json`) sends 200 auctions/s to the two pods. Each pod calls the fake buyers over HTTP with a 150ms deadline. Times are measured from simulator start:

| Buyer | Behavior | What to expect |
|---|---|---|
| `buyer_steady` | 2% failures, 1% timeouts | Never throttled |
| `buyer_flaky` | 90% failures from 30s to 90s | Blocked about one window after 30s; recovers 0.85 -> 0.75 -> ... -> 0 after 90s |
| `buyer_slow` | 60% timeouts from 60s to 120s | Blocked with reason `timeout_rate`, then recovers |
| `buyer_tiny` | Low traffic (2% of auctions), failing from 30s to 90s | Below the minimum sample: windows are accumulated before judging, so it is blocked and recovers more slowly |

### Watch it

Decisions (evaluator) next to the incidents that caused them (simulator):

```sh
docker compose logs -f evaluator simulator
```

```
simulator  | msg="buyer behavior changed" buyer=buyer_flaky source=incident failure_rate=0.9
evaluator  | msg="throttle state changed" buyer=buyer_flaky window=13:22:10 total=1999 failed=1815 ... from=0 to=0.95 reason=failure_rate
simulator  | msg="buyer behavior changed" buyer=buyer_flaky source=base failure_rate=0.02
evaluator  | msg="throttle state changed" buyer=buyer_flaky window=13:23:10 total=102 failed=3 ... from=0.95 to=0.85 reason=recovering
evaluator  | msg="throttle state changed" buyer=buyer_flaky window=13:23:20 total=206 failed=4 ... from=0.85 to=0.75 reason=recovering
...
evaluator  | msg="throttle state changed" buyer=buyer_flaky window=13:24:40 total=1841 failed=29 ... from=0.05 to=0 reason=""
```

The throttle at work inside a pod (requests skipped per window):

```sh
docker compose logs -f pod-1 | grep buyer_flaky
```

```
pod-1 | msg="buyer traffic" buyer=buyer_flaky attempted=1002 called=1002 skipped=0 ok=102 failed=900
pod-1 | msg="buyer traffic" buyer=buyer_flaky attempted=1000 called=52 skipped=948 ok=4 failed=48
pod-1 | msg="buyer traffic" buyer=buyer_flaky attempted=1000 called=175 skipped=824 ok=173 failed=2
```

Current state of all buyers, with last-window request counts and error rates:

```sh
curl localhost:8088/state
```

```json
{
  "buyers": [
    {
      "buyer_id": "buyer_flaky",
      "throttle_rate": 0.95,
      "reason": "failure_rate",
      "last_window": { "start": "...", "total": 2000, "failed": 1811, "timeouts": 0,
                       "failure_rate": 0.9055, "timeout_rate": 0 }
    }
  ]
}
```

### Break a buyer yourself

The simulator has a live control API (port 9000):

```sh
# make buyer_steady fail 90% of the time
curl -X PUT localhost:9000/buyers/buyer_steady -d '{"failure_rate":0.9,"timeout_rate":0}'
# back to its configured behavior
curl -X DELETE localhost:9000/buyers/buyer_steady
# every buyer's current rates and where they come from (base / incident / override)
curl localhost:9000/buyers
```

Stop with `Ctrl+C`, then `docker compose down`.

## Configuration

All values are environment variables, set in `docker-compose.yml`. One shared block (`x-throttle-env`) feeds every service, so the evaluator and the pods always agree on the window.

**Throttling rules** (evaluator; spec defaults):

| Variable | Default | Meaning |
|---|---|---|
| `THROTTLE_FAILURE_RATE_THRESHOLD` | `0.7` | Block when failed / total reaches this |
| `THROTTLE_TIMEOUT_RATE_THRESHOLD` | `0.3` | Block when timeouts / total reaches this |
| `THROTTLE_MIN_SAMPLE_SIZE` | `100` | Fewer requests never trigger a block on their own |
| `THROTTLE_BLOCK_LEVEL` | `0.95` | Throttle rate on block (must be < 1: traffic never drops to zero) |
| `THROTTLE_RECOVERY_STEP_SIZE` | `0.1` | Drop per clean window |
| `THROTTLE_EVALUATION_WINDOW` | `1m` (demo `10s`) | Window length; **also read by pods** |
| `THROTTLE_MAX_ACCUMULATED_WINDOWS` | `5` | Max below-minimum windows merged before judging |

**Services:**

| Service | Variable | Default |
|---|---|---|
| all | `REDIS_ADDR` | `localhost:6379` |
| evaluator | `EVALUATOR_GRACE` (wait after a window ends, so pods' last flushes land) | `5s` (demo `3s`) |
| evaluator, pod, simulator | `LISTEN_ADDR` | `:8080` (simulator `:9000`) |
| pod | `BUYER_BASE_URL`, `BUYER_TIMEOUT`, `POD_NAME` | `http://localhost:9000`, `150ms`, hostname |
| simulator | `SIM_CONFIG`, `SUMMARY_EVERY` | `/config/simulator.json`, `10s` |
| compose host ports | `STATE_PORT`, `SIMULATOR_PORT` | `8088`, `9000` |

**Fake buyers**: edit `simulator.json` (auctions per second, pods, and per buyer: `participation`, `failure_rate`, `timeout_rate`, optional `incident` with `start`, `end` and rates).

To run with the spec's 1-minute windows: set `THROTTLE_EVALUATION_WINDOW: 1m` in `docker-compose.yml` (full recovery then takes about 12 minutes).

## Tests

```sh
go test ./...
go test -race ./...    # needs cgo; without a C compiler (e.g. Windows):
docker run --rm -v "$PWD:/src" -w /src golang:1.27 go test -race ./...
```

No Docker or Redis is needed for the unit tests: Redis is replaced by an in-memory implementation ([miniredis](https://github.com/alicebob/miniredis)). Coverage highlights:

- **Every evaluator rule**: block on failure and timeout rates (at and below threshold), minimum sample, each recovery step, snap-back, accumulation, cap, exact float steps (`internal/evaluator/evaluator_test.go`).
- **Throttler**: skip probability, counts, window attribution, retries, concurrent use under `-race`, benchmarks.
- **End to end**: two pods and the evaluator on one in-memory Redis; a failing buyer is blocked on both pods, then recovers to fully open (`internal/evaluator/e2e_test.go`).

## Layout

```
buyer/                  shared domain type: buyer.Stats
throttler/              public library for auction pods: ShouldCall, Record, Run
throttler/redisstore/   its Redis adapter
internal/evaluator/     Evaluate (pure rules), Runner (per-window loop), GET /state
internal/store/         evaluator's Redis adapter; schema/ = Redis layout
internal/config/        rules + env vars
internal/auction/       demo pod: POST /auction fan-out to buyers
internal/simulator/     demo traffic + fake buyers
internal/serve/         run an HTTP server until shutdown
cmd/                    evaluator, auctionpod, simulator binaries
```

## Bonuses and extras

- **Go.**
- **`GET /state`** on the evaluator, in the spec's format.
- **Per-buyer metrics** (throttle rate, error rates, request counts) are in `GET /state` under `last_window`. Kept as JSON read from Redis for simplicity: no metrics stack to run.
- **Version key** (`throttle:version`): pods check one small number each second and read the full state only when it changed. Redis load stays flat as pods and buyers grow.
- **Catch-up after evaluator downtime**: missed windows are evaluated oldest first, so a restart gives the same result as never stopping, and the same window is never evaluated twice.
- **Accumulation of small windows**: a blocked buyer receives only 5% of its traffic, so its windows are often below the minimum sample. They are merged until there is enough data, so the buyer neither recovers on no evidence nor stays blocked forever (details in DESIGN.md).
- **Fail-open everywhere**: if Redis is down, auctions keep running with the last known rates, and stats are retried.
- **Live control API** in the simulator, to break and heal buyers while the demo runs.

## What I would add for production

- **Prometheus `/metrics`** on the evaluator and pods, for history, dashboards and alerts (skipped-call counts only exist in pods).
- **A guard for two evaluators**: `WATCH throttle:last_window` around the save, so a second instance's write aborts instead of applying a window twice.
- **Per-buyer rules** (for example, different thresholds for small buyers).
- **Push updates** (Redis pub/sub) next to polling, to cut the ~1s propagation lag.
