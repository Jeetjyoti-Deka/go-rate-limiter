# go-rate-limiter

A rate limiter that is provably correct on one machine becomes silently incorrect the
moment you run two copies of it.

This repository walks that failure into the open — starting from a mutex and a counter,
ending at a Redis-coordinated limiter that leases quota — and measures every step. Each
stage is a working implementation with benchmarks, and the multi-instance bug is
expressed as a *failing test* rather than a paragraph of prose:

```
$ go test -tags brokenbydesign ./distributed/
--- FAIL: TestAggregateLimitHolds/tokenbucket
    3 instances admitted 300 requests against a configured limit of 100:
    the limit is enforced per instance, not per service
```

All five algorithms fail it identically, because the defect belongs to none of them.

> **Status: in progress.** Phases 0–5 of 8 are complete — the `Limiter` contract, the
> clock abstraction, the conformance suite every implementation is graded against, five
> algorithms, measured comparisons of both synchronisation strategy and algorithm choice,
> and the multi-instance break. See [`PLAN.md`](PLAN.md) for the full roadmap.

Given the same configuration — 100 requests per minute — and the same test, run at a
window boundary:

| Implementation | Admitted within 1 ms | |
|---|---|---|
| `fixedwindow` | **199** | 2.0× the configured limit |
| `tokenbucket` | 100 | its burst, and no more |
| `windowlog` | 100 | exact |
| `windowcounter` | 99 | approximate, erring strict here |
| `gcra` | 100 | its burst |

All five pass the identical conformance suite. The fixed window is not buggy; its
guarantee was "100 per aligned window", which is not the guarantee anyone thought they
were configuring. See [docs/01](docs/01-mutex-and-fixed-windows.md) and
[docs/04](docs/04-algorithm-comparison.md).

They differ in what that correctness costs. Being exactly right means storing every
timestamp — **24.7 KB per key** at a limit of 1000, against 67 bytes for a token bucket.
And on a saturated key, GCRA is the only one that gets *faster* with more cores (3.53×
from one to four) because rejecting a request takes no lock and writes nothing, while
every mutex-based algorithm slows down by ~15%.

## Why this exists

Rate limiting is a well-worn topic, so the intent here is depth rather than coverage:

- **Measured, not asserted** — every performance claim has a `go test -bench` number
  checked into the repo, with machine specs.
- **Failure demonstrated** — the over-admission bug that appears when a service scales
  horizontally is a test that fails, not a diagram.
- **Deterministic time** — no `time.Sleep` in any test. A `Clock` interface means
  window-boundary behaviour is verified exactly.
- **One conformance suite** — every algorithm is held to the same observable behaviour,
  which is what makes comparing them meaningful.

## The contract

```go
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
	AllowN(ctx context.Context, key string, n int) (Decision, error)
}
```

`Decision` carries `Allowed`, `Limit`, `Remaining`, `RetryAfter`, and `ResetAfter` —
enough to populate rate limit response headers without asking twice.

The `context` and `error` are unused by the local implementations and load-bearing for
the distributed one; [`docs/00-foundations.md`](docs/00-foundations.md) explains why they
are in the interface from the start.

## Roadmap

| Phase | Subject | Status |
|---|---|---|
| 0 | Contract, clock abstraction, conformance suite | ✅ |
| 1 | Mutex + counter — and the fixed-window boundary burst | ✅ |
| 2 | Token bucket — lazy refill, burst vs sustained rate | ✅ |
| 3 | Concurrency: global mutex → sharded → per-key, benchmarked; idle-key eviction | ✅ |
| 4 | Sliding window (log and counter), GCRA — a comparison with numbers | ✅ |
| 5 | The break: three instances, one limit, triple the traffic admitted | ✅ |
| 6 | Redis-backed, atomically — why `GET`/`SET` is not enough | |
| 7 | Quota leasing and degradation — fail-open vs fail-closed | |
| 8 | HTTP middleware, docs, write-up | |

## Running the tests

```bash
go test -race ./...

# the multi-instance break, as a test that fails on purpose
go test -tags brokenbydesign ./distributed/
```

Requires Go 1.25+. Redis (via Docker) becomes a dependency at Phase 6; everything before
that runs with no external services.

### Seeing the break over real HTTP

Three servers, each told to allow 100 requests per minute, and a load generator
round-robining across them the way a load balancer would:

```bash
go run ./cmd/demo-server -addr :8081 -limit 100 -window 1m   # and :8082, :8083
go run ./cmd/loadgen -n 1000 -limit 100
```

```
3 instances, each configured for 100 requests
sent      1000 in 42ms
admitted  300
rejected  700

effective limit: 300 (3.0x the configured 100)
```

Use a window long enough to outlast the run. With the 1-second default, windows roll
mid-flight and instance-count multiplication becomes indistinguishable from replenishment.

## Documentation

Design notes are written during each phase rather than after, and live in
[`docs/`](docs/):

- [00 — Foundations: designing the contract before the implementation](docs/00-foundations.md)
- [01 — Mutex and fixed windows: correct, and still wrong](docs/01-mutex-and-fixed-windows.md)
- [02 — Token bucket: making burst a decision instead of an accident](docs/02-token-bucket.md)
- [03 — Concurrency: three strategies, measured](docs/03-concurrency.md)
  · [benchmark results](benchmarks/RESULTS.md)
- [04 — Algorithm comparison: what you buy with memory](docs/04-algorithm-comparison.md)
  · [benchmark results](benchmarks/RESULTS.md#phase-4--algorithms)
- [05 — Your rate limiter has no idea it has siblings](docs/05-the-multi-instance-break.md)
