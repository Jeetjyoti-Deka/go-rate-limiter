# Benchmark results — Phase 3

Measured comparison of three synchronisation strategies wrapping an identical token
bucket. The arithmetic lives in `internal/bucket` and is called by all three, so every
difference below is attributable to locking alone.

Reasoning and design: [`docs/03-concurrency.md`](../docs/03-concurrency.md).

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
