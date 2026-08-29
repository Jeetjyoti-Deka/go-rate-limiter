# 03 — Concurrency: three strategies, measured

> Both limiters so far serialise every request through one mutex. That is correct and it
> does not scale. This phase builds two alternatives, measures all three rather than
> reasoning about them, and then fixes the memory leak both have been carrying since
> Phase 1.
>
> **Numbers live in [`benchmarks/RESULTS.md`](../benchmarks/RESULTS.md)**, generated from a
> real run with machine specs attached. This document is the reasoning; that file is the
> evidence.

---

## Separating arithmetic from synchronisation

Comparing three concurrency strategies is only meaningful if they are running *identical*
algorithms. Three hand-copied token buckets would leave every measured difference open to
the objection that the maths drifted.

So the bucket arithmetic moves to `internal/bucket`, which holds the state, the refill
formula, and the accrual maths — and no synchronisation whatsoever:

```go
// no lock, no atomics, not safe for concurrent use
func (p Params) TryTake(s *State, now time.Time, n int) ratelimit.Decision
```

Each limiter package supplies the locking and calls into it. Whatever the benchmarks show
is then attributable to synchronisation alone, because the code doing the work is literally
the same function.

`internal/` is doing real work here: the package is importable within this module and
invisible outside it, so the extraction is a pure refactor with no public API surface added.
`tokenbucket`'s exported API is unchanged, and its Phase 2 tests pass untouched — which is
the evidence that the refactor preserved behaviour.

---

## The three strategies

### 1. One global mutex — `tokenbucket`

```
                    ┌──────────────────────────────┐
  all requests ───► │ mutex → map[string]*State    │
                    └──────────────────────────────┘
```

Every request for every key serialises through one lock. Simple, obviously correct, and
the throughput ceiling is one critical section at a time no matter how many cores you own.

This is the baseline. Not a straw man — for most services it is genuinely enough, and
knowing *where* it stops being enough is the point of measuring.

### 2. Sharded — `sharded`

```
  hash(key) & mask
        │
        ├─► shard 0:  mutex → map[string]*State
        ├─► shard 1:  mutex → map[string]*State
        └─► shard N:  mutex → map[string]*State
```

Split the map into `N` independently locked shards, choose one by hashing the key. Two
requests contend only if their keys land in the same shard, so with `N` shards and
well-spread keys, expected contention drops by roughly `N`.

Three details that matter more than the idea:

**Power-of-two shard count.** `hash & (n-1)` replaces `hash % n`, removing a division from
the hot path.

**`hash/maphash`, not `hash/fnv`.** `maphash.String` is the runtime's own string hasher —
faster than a hand-rolled FNV loop and allocation-free. The seed is created once per limiter,
so shard assignment is stable within a process and differs between processes, which is
harmless here.

**Cache-line padding.** A shard is a mutex plus a map header — 16 bytes. Without padding,
four shards share a 64-byte cache line, so a core taking shard 0's lock invalidates the
line holding shards 1–3 in every other core's cache. The shards are logically independent
and physically not. This is false sharing, it is measurable, and the padding that fixes it
is the clearest demonstration in the repo that "independent locks" is a claim about memory
layout as much as about code.

**Where sharding does nothing.** All of this helps only when keys are spread across shards.
One hot key — a single abusive client, a shared global quota — hashes to exactly one shard,
and the sharded limiter degenerates to the global-mutex case plus a hash computation. The
benchmark suite tests this case deliberately, because a benchmark that only measures the
flattering scenario is marketing.

### 3. Per-key locks — `perkey`

```
  sync.Map[string] ──► entry{ mutex, State }   ← lock is per key
```

Give every key its own mutex. Two requests contend only if they are for the *same* key,
which is the theoretical floor: state is per-key, so same-key requests genuinely must
serialise.

`sync.Map` is a good fit for a reason worth stating precisely: the mutations here are to
the *values* (token levels), not to the map's key set. The map itself is read-mostly after
warm-up, which is exactly the access pattern `sync.Map` is optimised for. A plain
`map` + `RWMutex` would take a write lock it doesn't need.

The costs are real:

- **Memory per key.** An `entry` is a mutex plus the state (~40 bytes), on top of
  `sync.Map`'s own per-key overhead — interface boxing, and its read/dirty double
  bookkeeping. Materially more than a shard-map entry.
- **Interface assertions** on every access, since `sync.Map` is untyped.
- **No `Len()`, and weakly-consistent `Range`.** This makes eviction harder — see below.

### The invariant that changes shape

Phase 1 established: read the clock inside the lock, so observed time is monotonically
non-decreasing in lock-acquisition order.

Under sharding and per-key locking that guarantee becomes *per shard* and *per key*
respectively. Two goroutines touching different keys can observe time out of order relative
to each other. This is fine, and worth being explicit about rather than discovering later:
the invariant only ever needed to hold for accesses to the *same state*, and it still does.
Global time ordering was never required — the single mutex just happened to provide it.

---

## What the benchmarks have to measure

The harness lives in `benchmarks/` and drives all three through the `ratelimit.Limiter`
interface, so the comparison is apples to apples.

**Hot key** — every goroutine hammers one key. Predicted: all three roughly equal, with
sharded and per-key slightly *worse* than the baseline from the extra hash or map
indirection. **This held.** All three land within ~15%, and per-key is worst at four and
eight goroutines — it pays `sync.Map`'s lookup for a partition of one.

**Distinct keys, varying cardinality** (16 / 1024 / 65536) — the case sharding exists for.
Predicted: the baseline flattens as cores increase while the others scale, and per-key
overtakes sharded as cardinality grows past the shard count. **This was half wrong, in
both directions.** The baseline does not flatten — it *regresses*, running 0.54× as fast on
four cores as on one. And per-key beats sharded at every cardinality measured, not only
past the shard count.

**Memory per key** — added after the first run, because the speed benchmarks could not see
it. `BenchmarkDistinctKeys` reports zero allocations per operation, but only because it
runs in steady state with every key already present. The per-key *footprint* is the axis
that decides whether an attacker minting distinct keys can exhaust the heap, and it is
where the ranking inverts: per-key locking is fastest and costs 2.57× the memory.

Which produced the phase's actual conclusion — sharded as the default, per-key as an
opt-in for bounded key spaces — and it is the opposite of what the latency tables alone
would have suggested. A benchmark that measures one dimension will confidently recommend
the wrong thing.

Two things the harness must get right or the numbers are worthless:

**Limit set absurdly high.** The deny path does less work than the admit path, so a
benchmark whose bucket runs dry is measuring rejections, not the machinery. Set the limit
high enough that every request is admitted.

**Real clock, not `FakeClock`.** `FakeClock` guards its field with a `RWMutex`, so using it
under `RunParallel` introduces a *second* contention point and pollutes the measurement of
the first. Deterministic time is a testing tool; benchmarks need the real thing.

And contention gets *shown*, not asserted:

```bash
go test -run '^$' -bench BenchmarkHotKey -cpu 8 -mutexprofile mutex.out ./benchmarks/
go tool pprof -top -nodecount=10 mutex.out
```

A profile naming the exact lock, with wait time attached, is the difference between
"sharding reduces contention" and knowing how much of the wall clock was spent blocked.

---

## Eviction, and why it is free

Both limiters have carried a `TODO(phase-3)` since Phase 1: the key map only grows. Keyed
by IP, an attacker with a /64 of IPv6 can mint effectively unbounded keys, each costing a
map entry, a string, and a struct — turning the component added to protect the service into
the cheapest way to exhaust its memory.

The fix is usually framed as a tradeoff: bound the memory, accept some loss of accuracy.
**For these algorithms it is not a tradeoff at all.**

Consider a token bucket idle for longer than `burst / rate` — the time to refill from empty
to full. On its next request, the refill computes `min(burst, tokens + elapsed × rate)`,
and since `elapsed ≥ burst / rate`, that is `burst` regardless of what `tokens` was. The
state is now *identical* to a fresh bucket.

So deleting a key idle for at least `burst / rate` cannot change any future decision. Not
approximately — exactly. The same argument holds for a fixed window idle longer than one
window: it rolls to `count = 0, start = now`, which is precisely a fresh key.

That deserves a test rather than a paragraph: run an identical request sequence through a
limiter with and without eviction, and assert every `Decision` matches.

The only real cost is the sweep itself, and there are three ways to pay it:

| Strategy | Cost | Notes |
|---|---|---|
| **Periodic full sweep** | O(keys) per interval | Simple; holds locks while scanning. Shardable — sweep one shard at a time so the pause is 1/N. |
| **Random sampling** | O(k) per interval | Redis's approach. No full scan, probabilistic bound, never blocks long. |
| **LRU with a hard cap** | O(1) per request | Guarantees a memory ceiling rather than an idle policy; costs a linked-list update on every request, in the hot path. |

This phase implements the periodic sweep, per shard, because its cost is the easiest to
measure honestly — and measuring it is the point.

### The lifecycle problem Phase 0 deferred

A background sweeper is a goroutine, and a goroutine that outlives its owner is a leak. So
`Close()` finally has to exist — the method Phase 0 explicitly declined to add on the
grounds that nothing yet owned a resource.

It stays off the `Limiter` interface. Most implementations need nothing torn down, and
widening the interface would force every one of them to carry a no-op. Callers that care
type-assert to `io.Closer`, which is the idiom Go already has for exactly this.

The sweep policy is also kept separate from its scheduling: an unexported
`sweep(now time.Time) int` does the work and is called directly by tests against
`FakeClock`, while the goroutine is a trivial ticker loop calling it. Deterministic tests
for the policy, nothing clever to test in the glue.

---

## What this phase does not settle

Per-key locking still allocates a mutex per key. Compressing a bucket's state into a single
64-bit word would allow a lock-free `atomic.CompareAndSwap` with no per-key mutex and no
allocation at all — but a token bucket's state is a float and a timestamp, which does not
fit. The algorithm that *does* fit is GCRA, whose entire state is one instant, and that is
Phase 4.

Which is the useful shape of the result: the ceiling here is not the synchronisation
strategy, it is the size of the state the algorithm needs.

---

**Next:** [04 — Algorithm comparison](04-algorithm-comparison.md) — sliding window log
versus counter, and GCRA, whose one-word state is what finally makes lock-free viable.
