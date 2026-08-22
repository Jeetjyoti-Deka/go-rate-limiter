# go-rate-limiter — Project Plan

> **Thesis:** A rate limiter that is provably correct on one machine becomes silently
> incorrect the moment you run two copies of it. This repo walks that failure into the
> open, measures it, and then fixes it — with the tradeoffs written down.

**Companion blog series:** *"Building a Rate Limiter in Go: From Mutex to Distributed Rate Limiting"*

---

## 1. What makes this not-a-tutorial

The topic is well-worn. The differentiator is **rigor**, in four specific forms:

| Differentiator | Concretely |
|---|---|
| **Measured, not asserted** | Every claim about performance has a `go test -bench` number behind it, checked into the repo. |
| **Failure demonstrated by test** | The multi-instance over-admission bug is a *failing test*, not a paragraph of prose. |
| **Deterministic time** | No `time.Sleep` in tests. A `Clock` interface means window-boundary behaviour is tested exactly, not approximately. |
| **Conformance suite** | One shared test suite that *every* implementation must pass, so the algorithms are genuinely swappable and genuinely comparable. |

Anti-goal: breadth. Five well-tested limiters with real benchmarks beat twelve half-finished ones.

---

## 2. Scope

### In scope
- Core `Limiter` interface + shared conformance test suite
- Five algorithms: fixed window, token bucket, sliding window log, sliding window counter, GCRA
- Concurrency hardening: global mutex → sharded → per-key atomics, benchmarked at each step
- Idle-key eviction (unbounded-memory bug most tutorials ship)
- Multi-instance failure demonstration
- Redis-backed limiter via atomic Lua script
- Hybrid leased limiter (local bucket + Redis quota lease) with degradation modes
- `net/http` middleware with `X-RateLimit-*` / `Retry-After`
- README as primary deliverable

### Explicitly NOT in scope
Stated up front so the README can say so:
- Not a production library — no semver promises, no gRPC interceptor, no OTel wiring
- No Redis Cluster / hash-tag sharding
- No consensus-based limiting (Raft) — the point is to explain why leasing beats it here
- No distributed-systems formal verification (TLA+); reasoning is prose + tests
- No admin UI, no web dashboard

---

## 3. Repository layout

```
go-rate-limiter/
├── README.md                  # primary deliverable — 90% of visitors read only this
├── PLAN.md                    # this file
├── go.mod                     # module github.com/Jeetjyoti-Deka/go-rate-limiter
│
├── ratelimit.go               # package ratelimit — Limiter, Decision, errors
├── clock.go                   # Clock, realClock, fakeClock
│
├── ratelimittest/             # shared conformance suite (importable by each impl)
│   └── conformance.go
│
├── fixedwindow/               # Stage 1 — mutex + counter
├── tokenbucket/               # Stage 2 — lazy refill, burst
├── slidingwindow/             # Stage 4 — log + counter variants
├── gcra/                      # Stage 4 — leaky bucket / virtual scheduling
├── redisstore/                # Stage 6 — atomic Lua
├── leased/                    # Stage 7 — hybrid local + Redis lease
├── middleware/                # net/http middleware
│
├── cmd/
│   ├── demo-server/           # one instance; -limiter and -port flags
│   └── loadgen/               # concurrent client, reports admitted vs rejected
│
├── docs/
│   ├── 01-mutex-and-fixed-windows.md
│   ├── 02-token-bucket.md
│   ├── 03-concurrency.md
│   ├── 04-algorithm-comparison.md
│   ├── 05-the-multi-instance-break.md
│   ├── 06-redis-atomicity.md
│   └── 07-leasing-and-degradation.md
│
└── benchmarks/
    └── RESULTS.md             # checked-in benchmark output, with machine specs
```

---

## 4. The core contract

Designed once, in Phase 0, and deliberately not changed afterwards — every later stage
must fit behind it. If an algorithm doesn't fit, that's a finding worth writing about.

```go
package ratelimit

// Decision is the full result of a limit check — enough to populate response headers.
type Decision struct {
    Allowed    bool
    Limit      int           // configured ceiling for this key
    Remaining  int           // tokens/slots left after this decision
    RetryAfter time.Duration // 0 when Allowed
    ResetAfter time.Duration // until the key is fully replenished
}

type Limiter interface {
    Allow(ctx context.Context, key string) (Decision, error)
    AllowN(ctx context.Context, key string, n int) (Decision, error)
}
```

Design notes to defend in the blog:
- **`error` in the signature** — local limiters never fail, but Redis-backed ones do. The
  interface must admit failure or the distributed implementation can't be honest. This is
  the first place local and distributed diverge.
- **`context.Context`** — same reason: network calls need cancellation and deadlines.
- **`Decision` struct over a bare `bool`** — headers need `Remaining` and `Retry-After`;
  returning them forces each algorithm to actually know its own state.
- **No `Wait()`** — blocking belongs to the caller, not the limiter. Keeps the surface small.

**`Clock` abstraction** (`clock.go`) is the quiet hero of this repo: it's what makes
window-boundary tests exact instead of flaky.

---

## 5. Phases

Each phase ends with: code + tests passing under `-race` + a `docs/` note + benchmark
numbers where relevant. Tag each phase (`v0.1-fixed-window`, …) so the blog can link to
the exact tree it describes.

### Phase 0 — Foundations
- `go mod init`, `ratelimit.go`, `clock.go` (`realClock` + `fakeClock`)
- `ratelimittest/conformance.go` — table-driven suite any `Limiter` must pass:
  allows up to limit, rejects past it, replenishes over time, isolates keys,
  survives concurrent access, reports coherent `Remaining`/`RetryAfter`
- GitHub Actions: `go vet`, `go test -race ./...`, `staticcheck`
- **Deliverable:** an empty-but-tested skeleton. Nothing works yet; everything is verifiable.

### Phase 1 — Mutex + counter (fixed window)
- `sync.Mutex` + `map[string]*window`
- **The interesting part:** a test proving the boundary burst — 2× the limit admitted
  across a window edge. Not a bug in the code; a property of the algorithm.
- **Blog:** why the obvious implementation is *correct* and still *wrong*.

### Phase 2 — Token bucket
- Lazy refill (no background goroutine — compute tokens from elapsed time on read)
- Burst vs sustained rate as separate knobs; `AllowN` for weighted requests
- Monotonic clock discussion: why `time.Since` and not wall-clock subtraction
- **Blog:** smoothing, and what "burst" actually costs downstream.

### Phase 3 — Concurrency hardening ⭐
The Go-craft centerpiece. Three implementations, same conformance suite, measured:
1. Single global `sync.Mutex` (baseline)
2. Sharded map — `N` shards, `fnv(key) % N`
3. Per-key state via `atomic` CAS loop / `sync.Map`

- Benchmarks: `go test -bench=. -benchmem -cpu=1,2,4,8,16`, run under `-race` separately
- `-blockprofile` / `-mutexprofile` to *show* the contention, not just assert it
- **Idle-key eviction:** unbounded `map` growth is a real memory leak. Implement a
  sweeper or LRU and benchmark the tradeoff.
- **Deliverable:** `benchmarks/RESULTS.md` with a table and machine specs.
- **Blog:** contention profiles, false sharing, why sharding wins and where it stops winning.

### Phase 4 — Algorithm comparison
- Sliding window log (exact, memory-hungry) vs sliding window counter (approximate, cheap)
- GCRA / leaky bucket — virtual scheduling time, O(1) state
- **Deliverable:** comparison table — accuracy, memory per key, burst behaviour, ops cost
- **Blog:** the actual engineering choice, with the fixed-window burst from Phase 1 as motive.

### Phase 5 — The break ⭐⭐
The pivot the whole project exists for.
- `cmd/demo-server` × 3 instances, `cmd/loadgen` firing at a shared key
- Configured limit 100/s; observed admissions ≈ 300/s
- **Written as a failing test**, not a demo script — a test that asserts the aggregate
  limit holds, and fails, is far more convincing than a paragraph
- **Blog:** *"Your rate limiter has no idea it has siblings."* State locality as the root
  cause; enumerate the escape routes (sticky routing, limit ÷ N, shared state) and why the
  first two are bad.

### Phase 6 — Redis-backed, atomically
- **First, do it wrong:** `GET` → check → `SET`. Write the test that catches the race.
- **Then right:** token bucket as a Lua script, one round-trip, atomic by Redis's execution model
- Redis `TIME` for the clock — clock skew across instances is a real failure mode
- Key TTL so idle keys expire server-side (the Redis answer to Phase 3's eviction problem)
- Measure the cost: ~1 network RTT added to *every* request, and Redis is now a SPOF
- **Blog:** atomicity as the whole ballgame; what you bought and what you paid.

### Phase 7 — Leasing + degradation ⭐⭐
The section that makes this a systems project rather than a tutorial.
- Each instance keeps a local token bucket, periodically **leases** quota from Redis
- Tunable: lease size, refresh interval → knob between accuracy and chattiness
- **Degradation policy:** what happens when Redis dies — fail-open (availability, risk
  overload) vs fail-closed (safety, self-inflicted outage). Make it configurable and
  argue for a default.
- Benchmark against Phase 6: p50/p99 latency, Redis ops/sec at the same request rate
- Quantify the accuracy loss so the tradeoff is a number, not a vibe
- **Blog:** coordination is expensive; buy less of it. Why approximate-and-fast beats
  exact-and-slow at the edge.

### Phase 8 — Polish
- `middleware/` — `net/http` middleware, correct `X-RateLimit-*` and `Retry-After`,
  keying by IP / API key / custom extractor
- **README** — treat as primary deliverable: the thesis in three sentences, one diagram,
  the benchmark table, the "why multi-instance breaks it" hook, quickstart
- `docs/` notes finalised; blog series drafted from them

---

## 6. Blog series

Three posts, not one 6000-word wall. Each cites real output from the tagged repo.

1. **"A Rate Limiter That's Correct — and Fast"** — Phases 1–4. Mutex → token bucket →
   contention benchmarks → algorithm comparison. Sells the Go-concurrency skills.
2. **"Your Rate Limiter Has No Idea It Has Siblings"** — Phases 5–6. The failing test, the
   root cause, naive Redis, the race, the Lua fix. The one people will share.
3. **"Buying Less Coordination"** — Phase 7. Leasing, degradation, the accuracy/latency
   tradeoff with numbers. The one that reads as senior.

Write `docs/*.md` *during* each phase while the reasoning is fresh; the posts are then an
edit, not an archaeology project.

---

## 7. Environment prep

Verified 2026-08-22: **Go 1.25.0 present. Redis and Docker both absent.**

Decisions made:
- **Module path:** `github.com/Jeetjyoti-Deka/go-rate-limiter` (fixed in Phase 0)
- **Redis via Docker** — enable Docker Desktop's WSL integration for this distro.
  Chosen over native `apt` because Phase 7 benefits from spinning Redis up and down
  (and killing it) to exercise the degradation paths.

Still needed:
- Docker Desktop WSL integration enabled (`docker --version` currently fails)
- `staticcheck` — `go install honnef.co/go/tools/cmd/staticcheck@latest`
- GitHub repo created at the module path above

---

## 8. Definition of done

- `go test -race ./...` green; every limiter passes the same conformance suite
- `benchmarks/RESULTS.md` has real numbers with machine specs
- The Phase 5 over-admission test exists and demonstrably fails without shared state
- README explains the thesis in under a minute of reading
- Three blog posts drafted, each linked to a repo tag
- Every performance and correctness claim traceable to a test or benchmark in the repo
