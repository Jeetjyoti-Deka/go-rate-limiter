# 02 — Token bucket: making burst a decision instead of an accident

> The fixed window admitted 199 requests in a millisecond because its guarantee was
> "100 per bucket" and the operator heard "100 per minute". The token bucket fixes this
> not by tightening the limit but by *separating two things the fixed window had fused
> together* — how fast quota accrues, and how much of it can pile up.

---

## The model

A bucket holds tokens. Tokens are added continuously at `rate` per second, up to a
capacity of `burst`. A request costs `n` tokens and is admitted only if that many are
present.

```
        ┌─────────────┐
        │  ░░░░░░░░░  │  ← refills at `rate` tokens/sec,
        │  ░░░░░░░░░  │     never exceeds `burst`
        │  ░░░░░░░░░  │
        └──────┬──────┘
               │  each request removes n tokens
               ▼
```

Two knobs, and this is the whole point of the phase:

- **`rate`** — the sustained throughput you're willing to serve, long-run
- **`burst`** — how much unused quota may accumulate, i.e. the largest instantaneous
  spike you'll tolerate

A fixed window has only one knob. `limit` per `window` sets the sustained rate *and*
implicitly sets the burst to `2 × limit` — the value you get across a boundary, which
nobody chose. The token bucket makes that second number explicit.

---

## Lazy refill: no goroutine, no ticker

The naive reading of "tokens are added continuously" suggests a background goroutine per
bucket topping it up on a ticker. That design costs a goroutine and a timer per key, needs
shutdown coordination, and — with a million keys — spends most of its CPU waking up to add
tokens to buckets nobody is using.

Instead, compute the refill on read:

```
elapsed = now - last
tokens  = min(burst, tokens + elapsed × rate)
last    = now
```

The bucket is only ever updated when someone asks about it. An idle key costs exactly its
stored state and zero CPU. This is why `Clock` needed only a `Now()` method — a design
decision from Phase 0 that this phase cashes in.

Two details that are easy to get wrong:

**Advance `last` on denial too.** Refill is a function of elapsed time, not of the
decision. A limiter that only updates `last` when it admits will under-refill a bucket
that is being hammered — every denied request discards the time since the previous one.

**Cap before spending, not after.** `min(burst, ...)` must be applied when refilling. If
you let `tokens` exceed `burst` and clamp later, a bucket idle for a week accumulates a
week of quota and releases it all at once — reintroducing an unbounded burst, which is
the exact failure this algorithm exists to prevent.

---

## The burst dissolves

The Phase 1 test is worth re-running against this implementation, unchanged in structure:
anchor a window, sit until 1 ms before the boundary, drain the quota, cross the boundary,
try to drain again.

| | Fixed window | Token bucket |
|---|---|---|
| Admitted in that 1 ms span | **199** | **100** |
| Guarantee | ≤ limit per aligned bucket | ≤ burst instantaneously, ≤ rate sustained |

There is no boundary to cross, because there are no windows. The general bound is:

```
max admitted over any interval T  =  burst + rate × T
```

As `T → 0` this converges on `burst` — a number you set deliberately. Compare the fixed
window, where the same limit as `T → 0` converges on `2 × limit`, a number that is a
side effect of the data structure.

This is what "structurally correct" means here. The fixed window wasn't buggy; it was
answering a different question than the one being asked.

---

## Fractional tokens, and why `RetryAfter` must round up

Tokens accrue continuously, so the count is fractional. Storing it as `float64` is the
pragmatic choice: a request costs an integer number of tokens, but the bucket's level
between requests is real-valued.

Two consequences worth naming.

**Drift.** Repeated float addition accumulates rounding error — around 1e-16 relative per
operation. After a billion operations that is ~1e-7 tokens. It is not worth a redesign,
but it *is* worth knowing that the exact-arithmetic alternative exists: store a single
"theoretical arrival time" instead of a token count and do all the maths in `time.Time`.
That is GCRA, and it arrives in Phase 4.

**`RetryAfter` must round up, not truncate.** The time until `n` tokens are available is
`(n - tokens) / rate`, which almost never lands on a whole nanosecond. Truncating means
telling the client to retry at an instant when the bucket is a hair short — they retry,
get denied, and the `Retry-After` header becomes a lie that costs a round trip every time.

```go
retryAfter := time.Duration(math.Ceil(deficit / rate * float64(time.Second)))
```

`math.Ceil` guarantees the advice is actionable: retry then and it will succeed. Rounding
in the client's favour would be a correctness bug in the limiter's own contract.

---

## An asymmetry inherited from Phase 1

`AllowN(key, n)` where `n > burst` can never succeed — no amount of waiting accumulates
more than `burst` tokens. So what should `RetryAfter` be?

The fixed-window implementation returns the window reset time, which is misleading:
retrying then fails identically. The token bucket has the same shape of problem and less
excuse, because the impossibility is structural rather than incidental.

The honest answer is `RetryAfter: 0` on an unsatisfiable request, documented as "no retry
will succeed" — but that collides with the convention that `RetryAfter == 0` means
"allowed". Reporting a duration that is knowably useless is worse than reporting none, so
this repository chooses zero and documents it, and the conformance suite does not assert
on the unsatisfiable case precisely because there is no defensible universal answer.

Flagging it rather than smoothing it over: this is a genuine wart in the `Decision`
contract designed in Phase 0, discovered by implementing the second algorithm. It is the
kind of thing an interface designed against one implementation would have missed.

---

## Choosing burst in practice

Because it is now a real knob, it needs a real answer:

- **`burst == limit`** (the default here) reproduces the intuitive reading of "100 per
  minute" — up to 100 at once after an idle period, then a steady drip. Good for user-facing
  APIs where clients batch.
- **`burst == 1`** removes burst entirely and spaces requests evenly at `1/rate`. This is a
  traffic shaper, not really a limiter, and it is what you want in front of a fragile
  downstream that cares about instantaneous concurrency rather than volume.
- **`burst > limit`** deliberately tolerates spikes larger than the sustained rate — sane
  when the protected resource has real headroom and you would rather absorb a burst than
  reject legitimate traffic.

The thing to internalise is that a fixed window forces the middle option's *rate* with the
worst option's *burst*, and gives you no way to say otherwise.

---

## What still isn't fixed

The key map still only grows — Phase 1's deferred problem is unchanged, and now there are
two implementations carrying it. Phase 3 addresses eviction and measures what the sweep
costs.

Contention is also unchanged: one mutex guards every key. The token bucket does slightly
more work inside the critical section than the fixed window did (a multiply and a `min`
instead of a comparison), which makes it the more interesting subject for Phase 3's
sharding benchmarks.

---

**Next:** [03 — Concurrency](03-concurrency.md) — three strategies for the same limiter,
profiled rather than argued about, plus the eviction that both implementations have been
deferring.
