# 04 — Algorithm comparison: what you buy with memory

> Phase 1 showed a fixed window admitting twice its limit at a boundary. Phase 2 fixed
> that with a token bucket. This phase asks the question those two skipped: given that
> several algorithms are *correct*, what actually distinguishes them?
>
> The answer is a trade between memory, exactness, and the shape of the burst you tolerate
> — and one of the four turns out to have a property none of the others do.

---

## The independent variable is the algorithm

Phase 3 measured three synchronisation strategies against one algorithm. This phase does
the reverse: several algorithms, one synchronisation strategy.

So the limiters here use a plain global mutex, not the sharded store that Phase 3
recommended. That is deliberate. Sharding is orthogonal — it can be bolted onto any of
these, and Phase 3 already measured what it buys. Mixing both variables into one
comparison would leave every difference ambiguous, which is the same discipline that put
the token bucket arithmetic into `internal/bucket` in the first place.

The exception is GCRA, whose whole point is that it needs no lock at all.

---

## Sliding window log — exactness, priced honestly

Keep every request's timestamp. On each request, drop the ones older than the window and
count what remains.

```
window = 60s, limit = 5, now = 12:00:30

  12:00:29 ┐
  12:00:22 │
  12:00:11 ├── 4 timestamps inside the trailing 60s → admit, store now
  11:59:48 ┘
  11:59:20 ×── outside, dropped
```

This is the only algorithm here that is **exactly** correct: over any interval of length
`window`, never more than `limit`. No boundary burst, no estimation error, nothing to
argue about. It is what the other algorithms are approximating.

The price is memory proportional to the limit. At 1000 requests/minute that is 1000
timestamps — **24 KB per key**, against 67 bytes for a token bucket. Multiply by a million
keys and the exactness costs 24 GB.

Implemented here as a fixed-capacity ring buffer of exactly `limit` entries, so the
footprint is bounded by construction rather than by hoping the eviction keeps up. Nothing
can push a key past its ceiling because there is nowhere to put it.

**Use it when the limit is small and being exactly right matters** — a login endpoint at
5 attempts per hour, a payment API. At those numbers the memory is irrelevant and the
exactness is worth having.

---

## Sliding window counter — the pragmatic approximation

Keep two integers: how many requests landed in the current window, and how many landed in
the previous one. Estimate the trailing count by weighting the previous window by how much
of it still overlaps.

```
estimate = prev × (1 − elapsed/window) + current
```

Thirty seconds into a sixty-second window, the previous window's count counts for half.

State is two counters and a timestamp — about 40 bytes, independent of the limit. It has no
boundary burst, because the estimate falls continuously rather than resetting.

It is **approximate**, and the direction of the error is worth knowing: it assumes requests
were spread evenly through the previous window. Traffic clustered at the end of that window
gets *undercounted*, so a burst can slip through; traffic clustered at the start gets
overcounted, so legitimate requests get refused. Cloudflare, who popularised this
algorithm, measured 0.003% of requests wrongly admitted on real traffic.

**Use it when limits are large and keys are many** — the default choice for a public API.
Constant memory at any limit is the entire argument.

---

## GCRA — one instant, and no lock

The Generic Cell Rate Algorithm, borrowed from ATM networking, tracks a single value: the
**theoretical arrival time** (TAT), the instant at which the bucket would next be empty.

```
T   = window / limit          emission interval, time per request
τ   = burst × T               how far ahead of schedule a client may run

if tat < now:  tat = now
newTat  = tat + n×T
allowAt = newTat − τ
admit if allowAt ≤ now, and if so, tat = newTat
```

Behaviourally this is a token bucket. `burst` requests may arrive at once, then one every
`T` thereafter. It produces the same decisions.

What differs is the state: **one instant, where the token bucket needs a float and a
timestamp.** That is 8 bytes instead of 32, and — the part that matters — it fits in a
single 64-bit word.

### Which is what makes it lock-free

Phase 3 ended on a specific note: per-key locking needed a mutex per key because a token
bucket's state is too large to update atomically. Two fields cannot be swapped as one, so
they need a lock around them.

GCRA's state is one `int64`. It can be updated with `atomic.CompareAndSwap`:

```go
for {
    old := tat.Load()
    // ... compute the decision from old ...
    if !allowed {
        return decision          // no write at all
    }
    if tat.CompareAndSwap(old, newTat) {
        return decision
    }
    // lost the race; another goroutine moved the TAT — recompute
}
```

No mutex, no per-key lock allocation, no blocking. And a property that falls out for free:
**denied requests perform no write.** They load, compute, and return. Under overload —
precisely when contention is worst and most requests are being rejected — the limiter
degenerates to a wait-free read.

That inverts the usual failure mode. A mutex-based limiter contends hardest exactly when
it is rejecting the most traffic, because every rejection still takes the lock.

It is also visible in the measurements, more starkly than expected. On a drained hot key,
**GCRA is the only limiter in this repository that gets faster as cores are added** —
3.53× from one core to four, while every mutex-based algorithm slows down by the ~15% that
lock handoff costs. At four cores it rejects in 20.7 ns against the token bucket's 119.8,
which is 48 million rejections per second against 8 million.

One honest qualification: on a **single** core GCRA is unremarkable, at 72.97 ns against
the fixed window's 70.94. There is no clever arithmetic here making it intrinsically
faster. The entire advantage is that it does not serialise, so it only appears when
something is contending.

The Phase 3 ceiling turns out not to have been about synchronisation strategy at all. It
was about the size of the state the algorithm needs — though, as the comparison below
shows, small state buys concurrency rather than memory.

---

## What implementing a fifth algorithm did to the conformance suite

Two assertions written in Phase 0 turned out to encode assumptions that only held because
the first four implementations were all variations on the same family.

### `RetryAfter ≤ window` was not universal

The suite asserted that a denied request never advertises a retry longer than the window.
True for fixed windows and token buckets. **False for a sliding window counter.**

Exhaust the limit at the very start of a window. The estimate stays at `limit` for the
whole of that window, and when the window rolls, the exhausted count becomes `prev` with a
weight of 1.0 — so the estimate is *still* `limit`. It only decays as the new window
progresses. The earliest a request can succeed is a little over one full window away, and
the honest `RetryAfter` exceeds `window`.

The limiter was right; the assertion was wrong. It has been replaced with a bound that
holds for every algorithm:

```go
if d.RetryAfter > d.ResetAfter {
```

`ResetAfter` is time until fully replenished, `RetryAfter` is time until *this* request
could succeed, and the second can never exceed the first. That is a real invariant rather
than a coincidence of the fixed-window family, and it catches the same class of nonsense.

### "One idle window restores capacity" was not universal either

`ReplenishesOverTime` exhausted a key, advanced exactly one window, and required the next
request to be admitted. A sliding window counter denies it — correctly. One window after
exhausting your quota, the trailing window still contains that quota. That is what
*sliding* means.

The test now advances two windows and asserts the weaker, genuinely universal property:
a sufficiently long idle period restores capacity.

### The lesson, concretely

Phase 0 argued that a conformance suite is a floor rather than a ceiling — it encodes the
properties you thought to name. Phase 1 restated it. This is the first time it has been
*demonstrated*: two assertions that looked like universal truths were artefacts of a
sample size of four, and only a genuinely different algorithm could reveal it.

Worth noting what did **not** happen: no implementation was bent to satisfy a bad test. The
tests were wrong and they were changed, with the argument written down. The alternative —
special-casing the counter out of two assertions — would have preserved a green suite and
destroyed its meaning.

---

## Comparison

All figures measured; method and full tables in
[`benchmarks/RESULTS.md`](../benchmarks/RESULTS.md).

| | Fixed window | Sliding log | Sliding counter | Token bucket | GCRA |
|---|---|---|---|---|---|
| **Accuracy** | 2× at boundary | exact | ~0.003% error | exact to burst | exact to burst |
| **Admitted in 1 ms** | **199** | 100 | 99 | 100 | 100 |
| **Footprint/key** | 67 B | **24.7 KB** | 83 B | 67 B | 127 B |
| **Scales with limit** | no | **yes** | no | no | no |
| **Burst control** | none | none | none | explicit | explicit |
| **Denial, 4 cores** | 85.9 ns | 109.5 ns | 100.6 ns | 119.8 ns | **20.7 ns** |
| **Scaling, 1→4 cores** | 0.83× | 0.87× | 0.85× | 0.86× | **3.53×** |
| **Lock-free** | no | no | no | no | **yes** |
| **Denials write state** | yes | yes | yes | yes | **no** |

Footprints are at limit = 1000; the sliding log's is the only one that moves with the
limit, at 24.6 bytes per stored timestamp — the size of a `time.Time`.

### Small state does not mean small footprint

The comparison table contains a result worth stopping on, because it contradicts what the
section above would lead you to expect.

GCRA's state is 8 bytes against the token bucket's 32. Its measured footprint is **127
bytes per key against the token bucket's 67** — nearly double, in three allocations
rather than one.

The state shrank; the container grew. Being lock-free requires a container that is also
lock-free, which here means `sync.Map` — and `sync.Map` boxes every value in an interface
and maintains read and dirty maps that both hold live entries. That is the same
three-allocation overhead that made `perkey` cost 172 bytes per key in Phase 3. A token
bucket in a plain mutex-guarded map pays for one allocation and no boxing.

So the accurate claim is narrower than "GCRA is cheap": **a small state only buys a small
footprint when the container is cheap too, and lock-freedom rules out the cheap
container.** GCRA is making the same memory-for-concurrency trade `perkey` made. It simply
gets far more in return — 3.53× scaling instead of a mutex, at 127 bytes instead of 172.

This is the second time in the project that measuring contradicted a reasonable-sounding
inference from first principles. The first was Phase 3, where per-key locking won every
latency benchmark and was still the wrong default.

### Choosing

- **Small limit, exactness required** — sliding log. 5 logins per hour, 3 password resets
  per day. At limit = 10 it costs 323 bytes per key, which is nothing, and being exactly
  right is the whole point. Never use it with a large limit: the same algorithm at
  limit = 1000 costs 24.7 KB per key, and at a million keys that is 24 GB.
- **Large limit, many keys, public API** — sliding counter. 83 bytes per key at any limit,
  with an error small enough to be uninteresting.
- **You care about burst shape** — token bucket or GCRA, the only two that let you set
  sustained rate and instantaneous burst independently.
- **High contention, or overload is the common case** — GCRA. Not because its footprint is
  small (it is not — 127 bytes, nearly double a token bucket) but because rejections cost
  one atomic load and take no lock, so throughput rises with core count instead of
  falling.
- **Low concurrency** — anything. At one core the five are within 1.7× of each other, and
  the choice should be made on accuracy and memory alone.

The fixed window from Phase 1 is on this table for completeness. There is no workload where
it is the right answer: the sliding counter costs the same memory and does not have the
boundary flaw.

---

**Next:** [05 — The multi-instance break](05-the-multi-instance-break.md) — every algorithm
here is correct in one process, and every one of them silently stops being correct the
moment you run a second copy.
