# 06 — Redis, atomically

> Phase 5 established that the state has to move out of the process. This phase moves it,
> and discovers that the check-then-act bug from Phase 1 has been waiting there the whole
> time — now with a network round trip where the mutex used to be.

---

## What "shared state" actually costs

Moving the counters into Redis is the obvious fix, and obvious fixes deserve their price
tags written down before they are applied:

| | in-process | Redis | |
|---|---|---|---|
| `Allow`, measured | **73.5 ns** | **289,000 ns** | ~3,930× |
| Allocations | 0 | 17 | client encoding and reply parsing |
| Failure mode | cannot fail | connection refused, timeout, `LOADING` | |
| Availability | same as the process | a new dependency that can take the service down | |
| Concurrency | sharded across 64 locks | one key, one server, single-threaded | |

Every one of those is a regression. The only thing bought is correctness — which happens
to be the thing that was missing, so the trade is worth making. But it is a trade, and
Phase 7 exists because paying it on *every* request turns out to be unnecessary.

The latency figure deserves one caveat: it was measured against Redis on **loopback, on the
same machine**, so it contains no network hop at all. It is a floor. A deployment where
Redis lives across a datacentre is worse, and one where it lives across an availability
zone is worse again.

---

## The naive version is the Phase 1 bug, wearing a network

The direct translation of a limiter into Redis reads the current state, decides, and
writes the result:

```go
tat, _ := client.Get(ctx, key).Int64()   // read
// ... compute the decision ...
client.Set(ctx, key, newTat, ttl)        // write
```

This is `check-then-act`, the same defect described in
[docs/01](01-mutex-and-fixed-windows.md): two instances both read 95, both decide there is
room, both write 96. Two requests admitted for one slot.

What is different is the size of the window. In one process the gap between the read and
the write is a few nanoseconds, and you need genuine parallelism to land inside it — which
is why `ConcurrentNeverOverAdmits` needed 1,280 goroutines to catch it. Across a network
the gap is a full round trip, hundreds of microseconds, and **every concurrent request in
that window reads the same stale value.**

The bug is not more subtle in the distributed case. It is approximately ten thousand times
easier to hit.

There is also no mutex to reach for. `sync.Mutex` excludes goroutines in one address space;
it has nothing to say about another process on another machine. The tool that fixed this
in Phase 1 is simply not available.

`redisstore.NewNaive` implements exactly this. `TestNaiveIsCorrectWhenSerial` admits
exactly ten, establishing that the arithmetic is sound. `TestNaiveOverAdmits` then fires a
hundred concurrent requests at a fresh key with the same limit of ten and admits **all one
hundred** — not one of them observed another's write.

Phase 5 needed three processes to exceed the limit by 3×. This needs one process and
concurrency to exceed it by 10×. Both tests are kept in the repository rather than
described, for the same reason the boundary-burst test is: a defect you can run is more
convincing than a defect you can read about.

---

## Lua is the mutex you do not have

Redis executes commands on a single thread, and a script submitted with `EVAL` runs to
completion before any other command is served. That is the whole mechanism: a script is a
critical section, and Redis is the lock.

So the read, the decision, and the write move into the server:

```lua
local tat = tonumber(redis.call('GET', key)) or now
if tat < now then tat = now end

local newTat  = tat + cost
local allowAt = newTat - tolerance

if allowAt > now then
  return { 0, ... }              -- denied, nothing written
end

redis.call('SET', key, newTat, 'PX', ttl)
return { 1, ... }
```

Nothing can interleave between the `GET` and the `SET`, because nothing runs at all until
the script returns. The check-then-act window does not shrink — it stops existing.

One round trip, not two. The latency halves as a side effect of fixing the correctness
problem, which is a pleasant but secondary benefit.

The script is loaded once and invoked by hash with `EVALSHA`, so the body is not shipped
on every request. `go-redis`'s `Script.Run` does this and falls back to `EVAL` on
`NOSCRIPT`, which matters because Redis forgets its script cache on restart and after
`SCRIPT FLUSH`.

### The cost of a long script

Redis being single-threaded is what makes this work and also what makes it dangerous. A
script that takes a millisecond blocks *every* client for that millisecond. The GCRA script
is a handful of arithmetic operations on one key and runs in microseconds, which is the
only reason this is acceptable. A script that looped over a sliding-window log of a
thousand timestamps would not be.

Which is the first place the Phase 4 algorithm comparison pays off in a way that was not
obvious at the time.

---

## Why GCRA, specifically

Phase 4 found that GCRA's entire state is one instant, and cashed that in for a lock-free
compare-and-swap. In Redis the same property pays off three more times:

- **One key, one value.** The whole state is a single integer, so it is `GET`/`SET` rather
  than a hash with multiple fields or several keys that would need to be updated together.
- **A script short enough to be safe.** Five lines of arithmetic, no loops, no iteration
  over stored entries. It cannot block the server meaningfully.
- **Nothing to serialise.** No encoding, no decoding, no format to version.

Compare the alternatives. A token bucket needs a token count *and* a timestamp — two
values that must be updated atomically, so a hash and a longer script. A sliding window log
needs every timestamp: a sorted set, `ZREMRANGEBYSCORE` on every request, memory in Redis
proportional to the limit times the number of keys, and a script whose cost grows with the
limit.

The state size that bought concurrency in Phase 4 buys *network payload, script duration,
and atomicity scope* here. Same property, three different currencies — and the argument for
small state gets stronger the further the state travels.

---

## Whose clock?

The in-process limiters read time from an injected `Clock`, and Phase 2 was careful that
every comparison be a `time.Time` subtraction so the monotonic reading is preserved.

None of that survives the move. There are three instances now, each with its own clock,
and they disagree. Not by much — NTP keeps datacentre machines within milliseconds — but a
limiter doing arithmetic on timestamps from three different sources will produce a TAT that
jumps backwards whenever a request lands on the instance whose clock runs slow.

The fix is to stop asking the instances. The script calls Redis's own `TIME`:

```lua
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
```

One authoritative clock, shared by everyone, read inside the critical section. Instance
clock skew stops being a factor because instance clocks are no longer consulted.

Three things worth knowing about this:

**It requires effects replication.** `TIME` is non-deterministic, and older Redis refused
to run non-deterministic commands in scripts because replicas replayed the script verbatim.
Since Redis 5.0 scripts replicate by their effects — the resulting `SET`, not the script —
so `TIME` is allowed. On Redis 7 this needs no configuration.

**It is a wall clock, not a monotonic one.** Redis has no monotonic clock to offer, so the
careful monotonic discipline from Phase 2 does not survive this boundary. An NTP step on
the Redis host moves every limiter's notion of now. This is a genuine regression and the
mitigation is operational — run NTP with slew rather than step — rather than something the
code can fix.

**A failover changes clocks.** Promote a replica and `TIME` now comes from a different
machine. If that machine's clock is behind, TATs are suddenly in the future relative to it,
and clients are throttled until it catches up. Worth knowing before it happens at 3am.

**Resolution drops to microseconds.** `TIME` returns seconds and microseconds; the
in-process limiters work in nanoseconds. For rate limiting this is irrelevant — but it does
mean a configured rate finer than one unit per microsecond cannot be represented, and the
constructor rejects it rather than silently rounding to zero.

---

## Eviction, for free again

Phase 3b established that a key idle longer than its full-recovery time has state
indistinguishable from a fresh one, so deleting it changes no future decision. That
argument does not depend on where the state lives.

In Redis it needs no sweeper at all — the `SET` carries `PX`, and the server expires the
key on its own:

```lua
redis.call('SET', key, newTat, 'PX', ttl)
```

An allowed request always leaves `newTat - now ≤ tolerance`, so a TTL of `tolerance` is
always sufficient: once it elapses, the TAT is in the past and the key is equivalent to
absent. No goroutine, no `Close`, no sweep cost, no full scan.

This is the one dimension on which the distributed implementation is strictly *better* than
the in-process one. Phase 3b spent a background goroutine, a lifecycle method, and 1.34 ms
per sweep to achieve what `PX` does as a side effect of the write that was happening
anyway.

---

## The conformance suite meets a server-side clock

An awkward collision, and worth recording rather than working around quietly.

`ratelimittest.Run` requires the limiter to honour an injected `Clock` — `ReplenishesOverTime`
advances a `FakeClock` and demands the limiter notice. But the whole argument above says
the limiter must read time from Redis and ignore its caller.

Both cannot be true at once. The resolution is that `Config.Clock` is **nil in production
and set only by tests**: when nil, the script calls `TIME`; when set, the Go side passes
`now` as an argument and the script uses it. One branch in Lua, and the distributed limiter
is held to exactly the same conformance suite as the five in-process ones.

The cost is honest and should be stated: the tested path is not byte-for-byte the
production path. The arithmetic is identical and the atomicity is identical — only the
source of `now` differs. That is a real gap, and it is smaller than the alternative of
exempting the most important implementation from the suite that grades every other one.

It is also the third time the Phase 0 contract has met a case it was not designed for,
after `RetryAfter ≤ window` and `Close`. Each time the assumption was invisible until an
implementation violated it.

---

## What this does not fix

The limiter is now correct across instances, and three problems have been created:

- **Latency.** Every request now costs a round trip. The measured comparison against the
  in-process GCRA is in [`benchmarks/RESULTS.md`](../benchmarks/RESULTS.md).
- **A single point of failure.** When Redis is unreachable, every request gets an error and
  the caller has to decide what that means. There is no right answer, only a policy.
- **A bottleneck.** Every request from every instance converges on one key, on one
  single-threaded server. Phase 3 spent considerable effort sharding away exactly this, and
  it has come back in a place sharding cannot reach.

Phase 7 takes all three seriously, and its answer is not a better distributed algorithm.
It is to buy less coordination: keep a local bucket, lease quota from Redis in bulk, and
accept a bounded, measured inaccuracy in exchange for a hot path that touches the network
rarely and survives Redis being gone.

---

**Next:** [07 — Leasing and degradation](07-leasing-and-degradation.md) — coordination is
expensive, so buy less of it.
