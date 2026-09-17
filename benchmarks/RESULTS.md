# Benchmark results

Four independent comparisons, deliberately kept apart so each has exactly one variable:

- **[Phase 3](#phase-3--synchronisation)** — three synchronisation strategies wrapping an
  identical token bucket. The arithmetic lives in `internal/bucket` and is called by all
  three, so every difference is attributable to locking alone.
  ([`docs/03-concurrency.md`](../docs/03-concurrency.md))
- **[Phase 4](#phase-4--algorithms)** — five algorithms, each in its natural
  implementation. ([`docs/04-algorithm-comparison.md`](../docs/04-algorithm-comparison.md))
- **[Phase 6](#phase-6--what-coordination-costs)** — one algorithm, GCRA, with its state
  in this process and then in Redis.
  ([`docs/06-redis-atomicity.md`](../docs/06-redis-atomicity.md))
- **[Phase 7](#phase-7--what-leasing-bought-back)** — the Redis limiter asked for blocks of
  quota rather than single units, swept across block sizes.
  ([`docs/07-leasing-and-degradation.md`](../docs/07-leasing-and-degradation.md))

All run on the environment described immediately below. Absolute latencies drift by as much
as 1.6× between runs on this machine, so **compare only within a single table** — the
Phase 6 and Phase 7 sections each measured the same unleased Redis limiter and got 289 µs
and 183 µs respectively.

---

# Phase 3 — synchronisation

---

## Environment

```
AMD Ryzen 5 5600H with Radeon Graphics
4 CPUs visible          go1.25.0  linux/amd64  (WSL2)
```

**Read these numbers as relative, not absolute.** WSL2 sits under the Windows scheduler,
so absolute ns/op carries more noise than bare metal would. The ordering and the scaling
shapes are stable across runs; the third decimal place is not.

### Methodology

- **`-4` is the scaling column.** The machine has four cores.
- **`-8` is oversubscription, not more parallelism.** `RunParallel` spawns GOMAXPROCS
  goroutines, so `-8` means eight goroutines timesharing four cores — a normal condition
  for a loaded Go server, and read here as behaviour under scheduling pressure.
- **Shard count pinned to 64.** `sharded`'s default is `4*GOMAXPROCS`, which would have
  moved with `-cpu` and confounded partition count with goroutine count. 64 is above every
  goroutine count in the sweep, so shards are never the binding constraint.
- **Limit set to 2³⁰.** The deny path does less work than the admit path; a benchmark whose
  bucket runs dry measures rejections instead of synchronisation.
- **Real clock, not `FakeClock`.** `FakeClock` guards its field with an `RWMutex` and would
  add a second contention point.
- **No `-race`.** The detector adds 5–20× overhead and changes contention behaviour.
  Correctness runs use it; timing runs must not.

---

## One hot key

Every goroutine contends for a single key. This is the case sharding cannot help with, and
it is included precisely because omitting it would flatter the alternatives.

ns/op, lower is better:

| | -1 | -2 | -4 | -8 |
|---|---|---|---|---|
| GlobalMutex | 167.8 | 187.6 | **215.3** | 235.0 |
| Sharded | 175.9 | 193.7 | 220.3 | 239.9 |
| PerKey | 173.6 | 185.5 | 245.4 | 268.6 |

All three degrade as goroutines are added, and all three land within ~15% of each other.
Neither strategy can help: one key means one lock, whichever scheme selected it. `PerKey`
is the *worst* at 4 and 8 goroutines — it pays `sync.Map`'s lookup and interface assertion
for a partition of one.

**A rate limiter has no scaling story against a single abusive client.** Every strategy
here serialises, and the only real defences are upstream: reject earlier, or shard the
identity itself.

---

## Distinct keys

The case sharding exists for. Each parallel goroutine walks its own offset through a
shared key pool.

**keys = 16**

| | -1 | -2 | -4 | -8 |
|---|---|---|---|---|
| GlobalMutex | 183.0 | 162.2 | 228.9 | 246.6 |
| Sharded | 171.8 | 78.58 | 72.11 | 73.86 |
| PerKey | 128.6 | 82.83 | **63.09** | 67.17 |

**keys = 1024**

| | -1 | -2 | -4 | -8 |
|---|---|---|---|---|
| GlobalMutex | 140.3 | 164.1 | 245.3 | 271.4 |
| Sharded | 191.7 | 81.76 | 61.96 | 59.29 |
| PerKey | 132.1 | 71.67 | **52.19** | 52.56 |

**keys = 65536**

| | -1 | -2 | -4 | -8 |
|---|---|---|---|---|
| GlobalMutex | 159.0 | 195.5 | 292.0 | 312.2 |
| Sharded | 225.6 | 90.04 | 69.71 | 64.25 |
| PerKey | 163.6 | 84.95 | **47.64** | 49.04 |

### Scaling, 1 → 4 cores

| | keys=16 | keys=1024 | keys=65536 |
|---|---|---|---|
| GlobalMutex | **0.80×** | **0.57×** | **0.54×** |
| Sharded | 2.38× | 3.09× | 3.24× |
| PerKey | 2.04× | 2.53× | **3.43×** |

---

## Memory per key

`BenchmarkDistinctKeys` reports `0 allocs/op` because it runs in steady state, after every
key already exists. That says nothing about what a key *costs*, which is the axis that
decides whether a flood of distinct keys exhausts the heap.

200,000 distinct keys, `-cpu=1`:

| | retained | churn | allocs | cold insert |
|---|---|---|---|---|
| GlobalMutex | **66.95 B/key** | 101 B/op | 1 | 695.9 ns |
| Sharded | **66.95 B/key** | 101 B/op | 1 | 666.9 ns |
| PerKey | **172.3 B/key** | 172 B/op | 3 | 1173 ns |

`GlobalMutex` and `Sharded` are identical to the significant figure, as they must be:
the same `map[string]*bucket.State` holding the same 32-byte state and 16-byte string
header. Sharding changes *which* map an entry lands in, not what the entry costs.

`PerKey` costs **2.57× the memory**, three allocations instead of one, and 1.76× the
first-touch latency. `sync.Map` boxes each value in an interface, wraps it in its own
entry type, and maintains read and dirty maps that both reference live entries; on top of
that our `entry` carries a mutex alongside the state.

Two details worth reading carefully:

- For the map-based strategies, **churn (101 B) exceeds retained (67 B)**: map regrowth
  allocates fresh bucket arrays and frees the old ones. Allocation rate and footprint are
  different quantities.
- For `PerKey` they are **effectively equal** (172 vs 172.3) — almost nothing it allocates
  is ever released.

At a million tracked keys that is 67 MB versus 172 MB, for the same limiter.

---

## What eviction costs

`sharded` sweeps idle keys on a ticker. The recurring price is a full scan of every key,
paid each interval whether or not anything is reclaimed, so that worst case is what gets
measured: 100,000 live keys, none of them evictable.

```
BenchmarkSweepScan    895    1339948 ns/op    0 B/op    0 allocs/op
```

**1.34 ms to scan 100,000 keys — about 13.4 ns each, with zero allocations.**

Put in context: at a one-minute sweep interval that is 0.002% of a core. Even sweeping
every second it is 0.13%. The scan is effectively free, which is the expected result for
a map walk comparing one `time.Time` per entry and allocating nothing.

The number that matters more than the total is the *pause*. Shards are locked one at a
time, so no request ever waits behind a whole scan — only behind its own shard's slice of
it, around 21 µs at 64 shards. Sharding pays off twice: once for throughput, again for
keeping the sweep from becoming a stop-the-world event.

Eviction is therefore not a performance tradeoff in either direction. It costs almost
nothing to run, and because a bucket idle past its recovery time is indistinguishable from
a fresh one, it costs nothing in accuracy either.

---

## Contention, shown

```
go test -run '^$' -bench 'BenchmarkHotKey/GlobalMutex' -cpu 4 -benchtime=5s \
  -mutexprofile mutex.out -o bench.test ./benchmarks/
go tool pprof -top -nodecount=15 bench.test mutex.out
```

```
Type: delay
Showing nodes accounting for 6.41s, 99.80% of 6.42s total
      flat  flat%   sum%        cum   cum%
     6.41s 99.80% 99.80%      6.42s   100%  sync.(*Mutex).Unlock
         0     0% 99.80%      6.42s   100%  benchmarks.BenchmarkHotKey.func1.1
         0     0% 99.80%      6.42s   100%  tokenbucket.(*Limiter).Allow
         0     0% 99.80%      6.42s   100%  tokenbucket.(*Limiter).AllowN
         0     0% 99.80%      6.42s   100%  testing.(*B).RunParallel.func1
```

99.8% of all blocking in the process is one mutex, named, with the call path to it.

Two things that trip people up here. The profile attributes delay to `Unlock`, not `Lock`,
because Go records the wait it *imposes on others* at the moment the holder releases.
And 6.42s of delay inside a 5s run is not a contradiction: the figure is summed across
four goroutines, so it exceeds wall-clock time whenever more than one is blocked at once.

A first attempt at this profile captured only `runtime.unlock` and `gcMarkTermination` —
runtime-internal locks, 285µs total, our mutex absent entirely. The cause was that
`-bench BenchmarkHotKey` matched nothing, so no benchmark code ran and the profile
recorded GC and scheduler housekeeping. A profile that names no code of yours is usually
measuring an empty run.

---

## Findings

**1. The global mutex does not merely fail to scale — it goes backwards.** At 65536 keys it
runs 159 ns/op on one core and 312 ns/op on eight. Adding cores makes it *twice as slow*.
The work per request never changes, so all of the added time is coordination: contenders
parking and unparking on one futex. In throughput, that is ~3.2M ops/sec against ~21M for
per-key locking on the same four cores.

**2. Sharding is not free, and you pay first.** At `-1` it is the *slowest* option at both
1024 keys (191.7 vs 140.3) and 65536 (225.6 vs 159.0). The hash and the extra indirection
are pure overhead when there is no contention to relieve. It only pays back under
parallelism — and then it pays back well, 3.24× at high cardinality.

**3. Per-key locking is fastest on distinct keys and slowest on a hot one.** It wins every
distinct-key cell at 2 or more goroutines, peaking at 47.64 ns/op, but is worst in the
hot-key table (245.4 ns at `-4`). Its advantage is exactly proportional to how well the
traffic spreads.

**4. Speed and memory point in opposite directions.** Per-key is ~1.46× faster than sharded
at 65536 keys and `-4`, and costs 2.57× the memory. There is no cell in this data where one
strategy wins on both.

**5. A hypothesis, confirmed by fixing the confound.** The first run showed
`Sharded/keys=16` regressing at `-8` (83 → 113 ns), which could have been oversubscription
or shard-count artefact. Pinning shards to 64 removed it entirely (72.11 → 73.86, flat).
It was the default `4*GOMAXPROCS` producing 32 shards for 16 keys, not scheduling pressure.

---

## Recommendation

**Sharded is the default. Per-key is a deliberate opt-in.**

That is the opposite of what a speed-only reading suggests, and the memory column is why.
Rate limiter keys are frequently attacker-controlled — an IP, a token, anything a client
supplies — and the strategy that costs 2.57× per key is the wrong default for a component
whose job is to survive being flooded. Sharding takes ~1.46× the latency at high
cardinality to remove that exposure, while still delivering 3.24× scaling over the
baseline.

Choose per-key when key cardinality is *bounded and known* — a fixed tenant list, an
internal service mesh — and the traffic genuinely spreads across those keys.

Choose the global mutex when the service is single-core, or when peak throughput is far
enough below 5M ops/sec that the simplest correct thing is the right thing. Which, for most
services, it is.

None of them helps against one hot key. That needs a different layer.

---

## Reproducing

```bash
go test -run '^$' -bench 'BenchmarkDistinctKeys|BenchmarkHotKey' -benchmem \
  -cpu=1,2,4,8 -benchtime=2s ./benchmarks/

go test -run '^$' -bench BenchmarkMemoryPerKey -benchtime=200000x -cpu=1 ./benchmarks/

go test -run '^$' -bench 'BenchmarkHotKey/GlobalMutex' -cpu 4 -benchtime=5s \
  -mutexprofile mutex.out -o bench.test ./benchmarks/
go tool pprof -top -nodecount=15 bench.test mutex.out
```

`BenchmarkMemoryPerKey` needs an explicit iteration count: at Go's adaptively chosen `b.N`
the three strategies would be measured at different key counts, and small `b.N` makes the
per-key figure noise.

---

# Phase 4 — algorithms

Five algorithms, each in its natural implementation. Four use a global mutex; GCRA is
lock-free, which is a consequence of its state fitting in one word rather than a thumb on
the scale.

Kept in a separate harness (`algorithm_bench_test.go`) from the Phase 3 one. Folding both
into a single list of implementations would vary algorithm and synchronisation at once and
make every difference ambiguous.

## Burst at a window boundary

The Phase 1 test, structurally unchanged, run against every algorithm. 100 per minute,
measured across a 1 ms span containing a boundary:

| | admitted | |
|---|---|---|
| `fixedwindow` | **199** | 2.0× the limit |
| `tokenbucket` | 100 | its burst |
| `windowlog` | 100 | exact |
| `windowcounter` | 99 | approximate, erring strict here |
| `gcra` | 100 | its burst |

The fixed window is alone in exceeding its configured limit, and it is not a bug: its
guarantee was per aligned window, which is not the guarantee anyone thought they were
setting.

## The overload path

One hot key, drained before timing starts, so every measured request is rejected. This is
the case a limiter faces when it matters most.

ns/op, lower is better:

| | -1 | -2 | -4 | 1→4 cores |
|---|---|---|---|---|
| FixedWindow | 70.94 | 81.09 | 85.89 | 0.83× |
| TokenBucket | 102.7 | 117.0 | 119.8 | 0.86× |
| WindowLog | 95.33 | 106.2 | 109.5 | 0.87× |
| WindowCounter | 85.20 | 93.43 | 100.6 | 0.85× |
| **GCRA** | 72.97 | 37.30 | **20.70** | **3.53×** |

Zero allocations per operation throughout.

**GCRA is the only limiter in this repository that gets faster as cores are added.** The
four mutex-based algorithms all degrade by roughly the same 15%, because a rejection still
takes the lock — the identical negative-scaling signature Phase 3 found. GCRA's rejection
path is an atomic load, a comparison, and a return, with nothing written and nothing
contended, so four cores do four cores' worth of work.

At four cores: **48.3M rejections/sec against the token bucket's 8.3M**, a 5.8× gap.

The qualification matters as much as the result. On a single core GCRA is unremarkable —
72.97 ns against the fixed window's 70.94. Nothing about its arithmetic is faster. The
entire advantage is not serialising, and it appears only when something contends.

There is deliberately **no sustained-admit counterpart** to this benchmark. A sliding
window log physically cannot admit more than its limit within a window, and raising the
limit enough to survive millions of iterations would allocate a ring of that many
timestamps. Cold-path admission cost is captured below as the memory benchmark's ns/op.

## Memory per key

Retained bytes per key, swept across the limit because one algorithm's footprint depends
on it. 50,000 distinct keys, `-cpu=1`:

| | limit=10 | limit=100 | limit=1000 | allocs |
|---|---|---|---|---|
| FixedWindow | 66.95 | 66.95 | 66.95 | 1 |
| TokenBucket | 66.95 | 66.95 | 66.95 | 1 |
| **WindowLog** | 322.9 | 2,771 | **24,659** | 2 |
| WindowCounter | 82.95 | 82.95 | 82.95 | 1 |
| GCRA | 127.4 | 127.2 | 127.2 | 3 |

Cold first-touch admission, ns/op:

| | limit=10 | limit=100 | limit=1000 |
|---|---|---|---|
| FixedWindow | 348.8 | 320.4 | 331.5 |
| TokenBucket | 307.3 | 330.0 | 336.9 |
| **WindowLog** | 798.7 | 4,586 | **12,373** |
| WindowCounter | 326.8 | 269.3 | 260.1 |
| GCRA | 327.9 | 356.7 | 299.0 |

**The sliding window log scales exactly as theory predicts.** Subtract the container
overhead and it is 24.6 bytes per stored timestamp — the size of a `time.Time`. At
limit = 1000 that is 24.7 KB per key, 368× a token bucket, and at a million keys it is
24 GB. Its insertion cost scales too, since allocating and zeroing the ring is O(limit).

That is the price of being exactly correct, stated plainly. It is entirely reasonable at
limit = 10 (323 bytes) and disqualifying at limit = 1000.

## The result that contradicted the prediction

GCRA's state is 8 bytes against the token bucket's 32. Its measured footprint is **127
bytes per key against the token bucket's 67** — nearly double, in three allocations
rather than one.

The state shrank and the container grew. Lock-freedom requires a lock-free container,
which here is `sync.Map`: it boxes every value in an interface and maintains read and
dirty maps that both reference live entries. Those are the same three allocations that
made `perkey` cost 172 B/key in Phase 3. A token bucket in a plain mutex-guarded map pays
for one allocation and no boxing.

So the defensible claim is narrower than "GCRA is cheap": **a small state only buys a
small footprint when the container is cheap too, and lock-freedom rules out the cheap
container.** GCRA makes the same memory-for-concurrency trade `perkey` made, and simply
gets much more for it — 3.53× scaling rather than a mutex, at 127 bytes rather than 172.

This is the second time measurement has contradicted a sound-looking inference. The first
was Phase 3, where per-key locking won every latency benchmark and was still the wrong
default.

## Reproducing

```bash
go test -count=1 -run 'Boundary' -v \
  ./fixedwindow/ ./tokenbucket/ ./windowlog/ ./windowcounter/ ./gcra/ | grep admitted

go test -run '^$' -bench BenchmarkAlgorithmDeny -benchmem -cpu=1,2,4 \
  -benchtime=2s ./benchmarks/

go test -run '^$' -bench BenchmarkAlgorithmMemoryPerKey -benchtime=50000x \
  -cpu=1 ./benchmarks/
```

Do not raise `50000x` on the memory sweep: `WindowLog/limit=1000` allocates a
1000-timestamp ring per key, so 50,000 keys is already about 1.2 GB of rings.

---

# Phase 6 — what coordination costs

The same algorithm, GCRA, once in this process and once with its state in Redis. The limit
is set high enough that every request is admitted, so both measure the machinery rather
than the rejection path, and both use the same window so the emission interval matches.

```
BenchmarkAllow/InProcessGCRA-4    52133564      73.49 ns/op       0 B/op    0 allocs/op
BenchmarkAllow/Redis-4               12660     289000   ns/op     648 B/op   17 allocs/op
```

| | ns/op | allocs |
|---|---|---|
| In-process GCRA | **73.49** | 0 |
| Redis GCRA | **289,000** | 17 |

**Correctness across instances costs roughly 3,930× the latency of a local decision.**

Two qualifications, both of which make the real figure worse rather than better.

**This is loopback.** Redis was running in a container on the same machine, so the
measurement contains no network hop at all — only syscalls, serialisation, and Redis's own
execution. A deployment with Redis across a datacentre adds a real round trip; across an
availability zone, several. Read 3,930× as a floor.

**The 17 allocations belong to the client, not the limiter.** They are go-redis encoding
the command and parsing the reply. Nothing in `redisstore` allocates per call. Worth
naming so that nobody optimises the wrong layer: the cost here is the round trip, and no
amount of tuning the Go side moves it.

## Correctness, which is what the latency bought

| test | load | admitted | limit |
|---|---|---|---|
| `TestNaiveOverAdmits` | 100 concurrent, one process | **100** | 10 |
| `TestAtomicHoldsUnderConcurrency` | 100 concurrent, one process | **10** | 10 |
| `TestSharedAcrossInstances` | 1000 requests, 3 instances | **100** | 100 |

The first two differ only in whether the read-modify-write happens in one place or three.
Same arithmetic, same key, same Redis, same load — and one of them admits ten times its
limit while the other is exact.

The third is the answer to Phase 5. Three independently constructed limiters, sharing
nothing in-process, pointed at one Redis keyspace, admit exactly the configured limit
between them. `distributed/aggregate_test.go` asserts precisely this and fails for all five
in-process algorithms.

## Reproducing

```bash
docker run -d --name ratelimit-redis -p 6379:6379 redis:7-alpine

go test -run '^$' -bench BenchmarkAllow -benchmem -benchtime=3s ./redisstore/
go test -race -count=1 ./redisstore/
```

Redis tests skip when no server is reachable, so `go test ./...` stays green without one —
which also means a broken Redis setup looks identical to a passing run. Check for `ok`
rather than the absence of `FAIL`.

---

# Phase 7 — what leasing bought back

Phase 6 priced correctness across instances. This prices getting most of the latency back
by asking the shared limiter for blocks of quota instead of single units.

## Latency, and the dial

One key, admit path, limit high enough that nothing is denied. `redis/op` is a custom
metric: round trips divided by requests.

```
BenchmarkLeaseSize/RedisDirect-4     12998   183453    ns/op                    640 B/op  18 allocs/op
BenchmarkLeaseSize/Leased/1-4        13099   184296    ns/op  1.000    redis/op 640 B/op  18 allocs/op
BenchmarkLeaseSize/Leased/10-4      127579    18979    ns/op  0.1000   redis/op  64 B/op   1 allocs/op
BenchmarkLeaseSize/Leased/100-4    1263475     1882    ns/op  0.01000  redis/op   6 B/op   0 allocs/op
BenchmarkLeaseSize/Leased/1000-4   8808986      272.1  ns/op  0.001000 redis/op   0 B/op   0 allocs/op
```

| lease size | ns/op | vs direct | redis/op | allocs |
|---|---|---|---|---|
| — (direct) | 183,453 | 1× | — | 18 |
| 1 | 184,296 | 1.00× | **1.000** | 18 |
| 10 | 18,979 | **9.7×** | **0.1000** | 1 |
| 100 | 1,882 | **97×** | **0.01000** | 0 |
| 1000 | 272.1 | **674×** | **0.001000** | 0 |

`redis/op` lands on exactly `1/leaseSize` at every step. The amortisation does precisely
what the arithmetic says it should.

**Leasing at size 1 costs 0.46%.** That matters more than it looks: the dial can be turned
down to 1 and the behaviour is Phase 6 again, at no meaningful cost. It is a knob, not a
different code path with its own risks.

**Allocations fall to zero.** Past lease size 100 the hot path allocates nothing, because
go-redis's command encoding has disappeared from 99% of requests.

### The returns diminish faster than the costs grow

| step | latency saved |
|---|---|
| 1 → 10 | 165,317 ns |
| 10 → 100 | 17,097 ns |
| 100 → 1000 | 1,610 ns |

Each 10× increase in lease size saves 10× less latency while costing 10× more accuracy.
The benefit-to-cost ratio drops by **100× per step**, so the useful range is narrow and it
sits at the low end.

The floor is visible in the last row. At lease size 1000 the amortised Redis cost is
183 ns per request but the measured total is 272 ns — roughly **90 ns is the local path
itself** (a `sync.Map` lookup, a mutex, and some arithmetic). Beyond that point, larger
leases buy accuracy loss in exchange for latency that is no longer there to recover.

## Accuracy across a fleet

Three instances, each a `leased.Limiter` over its own `redisstore.Limiter`, sharing one
Redis keyspace. Limit 100, 1000 attempts.

**Uniform load** — round-robin across the three:

| lease | admitted | redis calls | per request |
|---|---|---|---|
| 1 | **100 / 100** | 103 | 0.103 |
| 5 | **100 / 100** | 26 | 0.026 |
| 20 | **100 / 100** | 11 | 0.011 |

Leasing is **exactly correct** here at every lease size. Instances can only spend what the
shared limiter granted, and under even load they spend all of it.

The 103 calls at lease size 1 is the denial cache made visible: 100 admissions at one round
trip each, plus one refusal per instance that every subsequent rejection is served from.
Without caching it would be 1000. **The limiter got roughly 10× quieter under overload**,
which is the property the design claims and this is the measurement of it.

**Skewed load** — two instances receive five requests each and then go quiet; the rest
lands on the third:

| lease | admitted | shortfall | bound |
|---|---|---|---|
| 1 | 100 / 100 | 0 | 3 |
| 5 | 100 / 100 | 0 | 15 |
| 20 | **70 / 100** | **30** | 60 |

### The bound is not the error

The design bounds fleet-wide drift at `instances × leaseSize`. It holds, and it is loose.

The mechanism is exact and worth stating precisely. Each quiet instance received five
requests. At lease size 5 it leased five and spent five: **nothing stranded**. At lease size
20 it leased twenty and spent five: **fifteen stranded**, twice over, for a shortfall of
exactly 30.

So the realised error is not `instances × leaseSize` but

```
Σ over instances of  max(0, leaseSize − requests that instance received)
```

Real traffic rarely approaches the worst case, in which every instance takes a full lease
and serves nothing. But the worst case is what a configuration has to be safe against,
which gives a sizing rule the design did not state:

**`instances × leaseSize` must stay well under `limit`, or there is no guarantee left.** At
limit 100 across three instances, lease size 100 gives a bound of 300 — the possible error
exceeds the quantity being limited. Even lease size 20 gives a 60% bound and produced a 30%
shortfall in practice. Something near `leaseSize ≤ limit / (4 × instances)` keeps the worst
case under a quarter.

## A note on comparing across runs

`RedisDirect` measures **183 µs** here and **289 µs** in the Phase 6 table. Same code, same
machine, different run — container state, page cache, and the Windows scheduler under WSL2
account for roughly 1.6× of run-to-run variance on absolute latency.

**Only within-run comparisons are valid.** `Leased/100` being 97× faster than `RedisDirect`
is sound because both were measured in the same process, seconds apart. Reading 1,882 ns
against Phase 6's 289,000 ns would silently inflate the result by that same 1.6×.

## Reproducing

```bash
docker run -d --name ratelimit-redis -p 6379:6379 redis:7-alpine

go test -run '^$' -bench BenchmarkLeaseSize -benchmem -benchtime=2s ./leased/
go test -count=1 -v -run TestFleetAccuracy ./leased/
```
