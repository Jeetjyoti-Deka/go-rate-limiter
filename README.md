# go-rate-limiter

A rate limiter that is provably correct on one machine becomes silently incorrect the
moment you run two copies of it.

This repository walks that failure into the open — starting from a mutex and a counter,
ending at a Redis-coordinated limiter that leases quota — and measures every step. Each
stage is a working implementation with benchmarks, and the multi-instance bug is
expressed as a *failing test* rather than a paragraph of prose.

> **Status: early.** Phase 0 of 8 is complete — the `Limiter` contract, the clock
> abstraction, and the conformance suite every implementation is graded against. No
> limiter is implemented yet. See [`PLAN.md`](PLAN.md) for the full roadmap.

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
| 1 | Mutex + counter — and the fixed-window boundary burst | |
| 2 | Token bucket — lazy refill, burst vs sustained rate | |
| 3 | Concurrency: global mutex → sharded → per-key atomics, benchmarked | |
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
