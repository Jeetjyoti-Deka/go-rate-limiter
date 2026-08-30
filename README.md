# go-rate-limiter

A rate limiter that is provably correct on one machine becomes silently incorrect the
moment you run two copies of it.

This repository walks that failure into the open — starting from a mutex and a counter,
ending at a Redis-coordinated limiter that leases quota — and measures every step. Each
stage is a working implementation with benchmarks, and the multi-instance bug is
expressed as a *failing test* rather than a paragraph of prose.

> **Status: in progress.** Phases 0–3 of 8 are complete — the `Limiter` contract, the
> clock abstraction, the conformance suite every implementation is graded against, four
> limiters, and a measured comparison of three synchronisation strategies. See
> [`PLAN.md`](PLAN.md) for the full roadmap.

Given the same configuration — 100 requests per minute — and the same test, run at a
window boundary:

| Implementation | Admitted within 1 ms | |
|---|---|---|
| `fixedwindow` | **199** | 2.0× the configured limit |
| `tokenbucket` | **100** | 1.0× — its burst, and no more |

Both pass the identical conformance suite. The fixed window is not buggy; its guarantee
was "100 per aligned window", which is not the guarantee anyone thought they were
configuring. See [docs/01](docs/01-mutex-and-fixed-windows.md) and
[docs/02](docs/02-token-bucket.md).

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
| 4 | Sliding window (log and counter), GCRA — a comparison with numbers | |
| 5 | The break: three instances, one limit, triple the traffic admitted | |
| 6 | Redis-backed, atomically — why `GET`/`SET` is not enough | |
| 7 | Quota leasing and degradation — fail-open vs fail-closed | |
| 8 | HTTP middleware, docs, write-up | |

## Running the tests

```bash
go test -race ./...
```

Requires Go 1.25+. Redis (via Docker) becomes a dependency at Phase 6; everything before
that runs with no external services.

## Documentation

Design notes are written during each phase rather than after, and live in
[`docs/`](docs/):

- [00 — Foundations: designing the contract before the implementation](docs/00-foundations.md)
- [01 — Mutex and fixed windows: correct, and still wrong](docs/01-mutex-and-fixed-windows.md)
- [02 — Token bucket: making burst a decision instead of an accident](docs/02-token-bucket.md)
- [03 — Concurrency: three strategies, measured](docs/03-concurrency.md)
  · [benchmark results](benchmarks/RESULTS.md)
