# 07 — Buying less coordination

> Phase 6 made the limiter correct across instances and charged **3,930×** the latency for
> it, on loopback, plus a single point of failure.
>
> That is the right answer to the wrong question. The question is not "how do I share
> state" — it is "how much coordination does this actually need?" The answer is: far less
> than once per request.

---

## The observation

A rate limiter asking Redis on every request is asking the same question over and over and
getting an answer that barely changes. Ninety-nine times out of a hundred the answer is
"yes, you have quota" — and it will still be yes for the next ninety-nine.

So stop asking per request. Ask for a **block** of quota, then serve from it locally until
it runs out.

```
without leasing                    with leasing (lease = 20)

req 1  ──► Redis  ──► yes          req 1  ──► Redis ──► yes, take 20
req 2  ──► Redis  ──► yes          req 2  ──► local
req 3  ──► Redis  ──► yes          req 3  ──► local
  …                                  …
req 20 ──► Redis  ──► yes          req 20 ──► local
req 21 ──► Redis  ──► yes          req 21 ──► Redis ──► yes, take 20

20 round trips                     1 round trip
```

The coordination is not eliminated, it is **amortised**. One round trip buys twenty
decisions, and the twenty-first pays for the next twenty.

This is the same move as buffered I/O, batched database writes, or a CPU fetching a cache
line instead of a byte. None of those invented a cleverer algorithm; they noticed the unit
of work was smaller than the unit of cost.

---

## The mechanism

Each instance holds, per key, a small amount of state:

```go
type lease struct {
    remaining int       // units claimed from the shared limiter, not yet served
    expires   time.Time // when unspent units rot
    denyUntil time.Time // suppress lease attempts after a refusal
}
```

On a request:

1. **Local quota available?** Decrement it and admit. No network.
2. **Cached denial still valid?** Deny. No network.
3. **Otherwise, lease.** Call the shared limiter with `AllowN(key, leaseSize)`. If it
   grants, the block is now this instance's to spend.

The shared limiter is unchanged — it is exactly the Phase 6 Redis limiter, used as a
component. Leasing is not a new limiting algorithm; it is a caching layer over the one
that already works. `leased.Limiter` takes any `ratelimit.Limiter` as its remote, which is
the Phase 0 interface finally paying off in a way that was not anticipated when it was
written.

### The tail

A lease of 20 against a global limit of 100 works out evenly. A lease of 30 does not: after
three leases the global quota has 10 left, and a request for 30 is refused even though the
caller only wanted one unit.

So a refused lease falls back to asking for exactly what the request needs:

```go
d, err := remote.AllowN(ctx, key, leaseSize)
if !d.Allowed && leaseSize > need {
    d, err = remote.AllowN(ctx, key, need)   // take what is actually left
}
```

Two round trips in that case rather than one, and only near the limit. Without it the
limiter systematically under-admits by up to `leaseSize - 1` units per window, which would
fail the conformance suite's `AllowsUpToLimit` — and did, before the fallback existed.

### Why unspent quota must rot

An instance that leases 20 and serves 3 before its traffic moves elsewhere is holding 17
units the rest of the fleet cannot use. The shared limiter already counted all 20 as spent.

Left alone this is worse than wasteful, it is **wrong in the other direction**: the global
limiter regenerates those 20 units over time, and if the instance still holds its 17 it can
spend them *later*, on top of freshly regenerated quota. Hoarding converts a temporary
under-admission into a permanent over-admission.

So a lease expires. The natural TTL is the time the global rate takes to regenerate a
lease's worth of quota — `leaseSize × window / limit` — because at exactly that moment the
units being discarded have been replaced upstream. The books balance.

### The accuracy bound

Because the shared limiter is exact and instances can only spend what it granted, the
error is bounded by how much is in flight:

```
|admitted − limit|  ≤  instances × leaseSize
```

Over-admission when instances spend blocks leased just before a window boundary;
under-admission when they hold blocks they never spend. Both are capped by the same
quantity, and that quantity is **a number you choose**.

The bound holds, and measuring it showed it is loose enough to be misleading on its own.
Three instances, limit 100, 1000 attempts:

| load | lease 1 | lease 5 | lease 20 |
|---|---|---|---|
| **uniform** (round-robin) | 100/100 | 100/100 | 100/100 |
| **skewed** (two instances get 5 requests, then go quiet) | 100/100 | 100/100 | **70/100** |

Under even load leasing is **exactly correct** at every lease size — instances can only
spend what the shared limiter granted, and even load means they spend all of it. The error
appears only when an instance takes a block and then stops receiving traffic.

The mechanism is precise. Each quiet instance received five requests. At lease size 5 it
leased five and spent five, stranding nothing. At lease size 20 it leased twenty and spent
five, stranding fifteen — twice over, for a shortfall of exactly 30. So the realised error
is not `instances × leaseSize` but

```
Σ over instances of  max(0, leaseSize − requests that instance received)
```

Real traffic rarely approaches the worst case, where every instance takes a full lease and
serves nothing. But that is still what a configuration must be safe against, which is what
the sizing rule below is for.

That is the whole trade, and it is a dial rather than a compromise. Measured against real
Redis, one key, admit path:

| lease size | ns/op | vs unleased | Redis calls per request | worst case, 3 instances |
|---|---|---|---|---|
| — (unleased) | 183,453 | 1× | 1 | 0 |
| 1 | 184,296 | 1.00× | **1.000** | 0 — Phase 6 with 0.46% overhead |
| 10 | 18,979 | **9.7×** | **0.1000** | ±30 |
| 100 | 1,882 | **97×** | **0.01000** | ±300 |
| 1000 | 272.1 | **674×** | **0.001000** | ±3000 |

Calls per request land on exactly `1/leaseSize` at every step.

At `leaseSize = 1` the leased limiter *is* the Redis limiter, with the local path never
taken, and it costs less than half a percent to have the option. Everything above 1 trades
a bounded inaccuracy for a proportional reduction in coordination.

### The useful range is narrower than the table suggests

| step | latency saved |
|---|---|
| 1 → 10 | 165,317 ns |
| 10 → 100 | 17,097 ns |
| 100 → 1000 | 1,610 ns |

Each 10× increase saves 10× less latency while costing 10× more accuracy, so the
benefit-to-cost ratio falls by **100× per step**. The last row of the table is a bad trade
dressed up as a big number: 674× faster, and a worst-case error thirty times the limit
itself.

The floor is visible in the measurements. At lease size 1000 the amortised Redis cost is
183 ns per request against a measured 272 ns — roughly **90 ns is the local path itself**,
a `sync.Map` lookup, a mutex, and some arithmetic. Past that, larger leases buy accuracy
loss in exchange for latency that is no longer there to recover.

### Sizing it

The worst case has to stay well under the thing being limited, or the guarantee is empty.
At limit 100 across three instances, `leaseSize = 100` gives a bound of ±300: the possible
error exceeds the limit. Even 20 gives ±60.

A serviceable rule:

```
leaseSize  ≤  limit / (4 × instances)
```

which keeps the worst case under a quarter of the limit. For 100 requests/minute across
three instances that is a lease of 8 — which the table above says still removes about 90%
of the coordination.

---

## Two details that matter more than the idea

### The lock is held across the network call, deliberately

The obvious objection to this code is that it holds a mutex while making a Redis call,
blocking every other goroutine wanting that key for a full round trip.

That is the point. Without it, a hundred goroutines arriving at an exhausted lease would
each independently decide to lease, and fire a hundred simultaneous Redis calls for a key
that needs one. The thundering herd would arrive at the moment the system is busiest, and
the amortisation this phase exists for would evaporate exactly when it was needed.

Holding the lock means **one lease request in flight per key**, and every goroutine behind
it gets served from the block that arrives. The cost is one round trip of latency for
those goroutines — which they would have paid anyway, individually, without leasing.

The lock is per key, so unrelated keys never wait on each other.

### Denials are cached, or overload makes it chattier

When the shared limiter refuses a lease, the naive implementation asks again on the very
next request. Under sustained overload — when every request is being rejected — that is a
Redis round trip per request, which is *worse* than not leasing at all.

The failure mode is perverse enough to be worth stating plainly: without denial caching,
the optimisation works when you do not need it and stops working when you do.

So a refusal is remembered until the `RetryAfter` the shared limiter advertised. During
that interval rejections are served locally, at local speed, with no network traffic at
all. The system gets *quieter* under overload rather than louder.

The effect is visible in the fleet measurements. At `leaseSize = 1` — where leasing itself
does nothing, since every admission still costs a round trip — 1000 attempts against a
limit of 100 cost **103 Redis calls**, not 1000: one hundred admissions, plus a single
refusal per instance that every subsequent rejection is served from. Nine hundred rejected
requests generated no network traffic at all.

The cache is keyed by request size: a refusal of 30 units says nothing about whether one
unit would be granted, so it only suppresses requests at least as large as the one refused.

---

## When Redis is gone

Leasing softens this — an instance with unspent quota keeps serving — but a lease runs out
eventually, and then a decision has to be made with no information.

There is no correct answer, only a policy, and the two options are both bad in different
directions:

**Fail open** — admit. The service keeps working, unlimited. If the limiter was protecting
a fragile downstream, that downstream is now unprotected during an incident that is already
underway.

**Fail closed** — deny. The limit is enforced, absolutely, by rejecting everything. A Redis
outage has become a total outage of your service, caused by a component that exists to
*improve* reliability.

### The default is fail open, and here is the argument

A rate limiter is almost never the primary function of the service it sits in front of. It
is a safety mechanism. Turning the failure of a safety mechanism into a full outage
inverts the reason it was installed — you have built a component whose breakage is
strictly worse than its absence.

Fail open also matches the blast radius. Redis being down is one failure; the service being
down is a bigger one. Choosing fail closed means choosing the bigger failure every time.

This is not a novel position: Envoy's rate limit filter ships with `failure_mode_deny`
false for the same reason.

**Flip it when the thing being protected is worse to lose than the service itself.** A
limiter in front of a payments API, an expensive inference endpoint, or anything where
unbounded traffic costs real money or corrupts state should fail closed. The question to
ask is: *if this limiter vanished entirely for ten minutes, what would happen?* If the
answer is "a large bill" or "data corruption", fail closed. If it is "elevated load", fail
open.

### On swallowing the error

When degradation applies, `leased.Limiter` returns a `Decision` with the policy's verdict
and a **nil error**.

That is a deliberate and slightly uncomfortable choice. Phase 0 put `error` in the
interface precisely so a distributed limiter could say "I don't know", and this is a
limiter that knows it doesn't know and answers anyway.

The justification is that a policy configured once is better than a decision re-derived at
every call site. Propagating the error means every caller writes the same `if err != nil {
allow anyway }` block, and they will not all write it the same way — some will fail closed
by accident simply because that is what an early `return err` does.

But Phase 5's lesson was that the dangerous failures are the silent ones, and a limiter
that quietly stops limiting is exactly that. So degradation is *loud by construction*:

```go
Config{
    OnRemoteFailure: leased.FailOpen,
    OnDegraded:      func(err error) { degradedTotal.Inc() },
}
```

`Stats()` also counts degraded decisions. Wiring neither is how you end up unlimited for a
week without noticing, and the documentation says so in those words.

---

## What this still does not solve

**A single hot key.** Every instance leasing the same key still converges on one Redis key
on one single-threaded server. Leasing reduces the rate by `leaseSize`, which is a large
constant factor and not a change in shape.

**Redis as a single point of failure.** Degradation is a policy for surviving it, not a fix.
Redis Sentinel or Cluster is the fix, and is out of scope here.

**Clock skew during degradation.** While Redis is unreachable each instance is on its own
clock again, with all of Phase 2's monotonic discipline and none of Phase 6's shared time
source.

**A smarter fallback.** The interesting thing this phase does not build: instead of a binary
open/closed, degrade to a *local* limiter configured at `limit / expected_instances`. That
keeps approximate enforcement during an outage rather than abandoning it, at the cost of
needing to know the instance count — which Phase 5 established is a number nobody reliably
has. It is the right next step and it is a genuinely harder problem than it looks.

---

## The shape of the answer

The arc of the last three phases is not "local is wrong, distributed is right". It is:

- **Phase 5** — local state is wrong across instances
- **Phase 6** — shared state is correct and costs 3,930×
- **Phase 7** — most of that cost was buying coordination nobody needed

Exact coordination on every request is almost always the wrong default. The useful question
is how much inexactness the problem tolerates, because that number converts directly into
throughput, latency, and resilience. A rate limiter that admits 103 instead of 100 has not
failed at anything anyone cares about; one that adds 289 µs to every request, and stops the
service when its datastore blinks, has.

Measured results — calls per request against lease size, latency against Phase 6, and the
observed accuracy across three instances — are in
[`benchmarks/RESULTS.md`](../benchmarks/RESULTS.md).

---

**Next:** [08 — Middleware and write-up](08-middleware.md) — turning a `Decision` into an
HTTP response, and the headers that tell a client what just happened.
