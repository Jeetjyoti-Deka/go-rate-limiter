# 01 — Mutex and fixed windows: correct, and still wrong

> The obvious rate limiter is a counter, a timestamp, and a mutex. It passes every
> assertion in the conformance suite. It also lets a client send twice its configured
> limit in the space of a millisecond, and that is not a bug in the code.

---

## The algorithm

Keep, per key, the instant the current window began and how many requests have landed in
it. On each request: if the window has expired, start a new one and zero the counter;
then admit if the counter plus the request size fits under the limit.

```
key "alice"  →  { start: 12:00:00.000, count: 47 }
```

That is the whole thing. Two fields, one comparison, one increment. It is the right place
to start not because it is simple but because *it is correct in the ways that are easy to
test and wrong in a way that is hard to see* — which makes it the ideal object for a
conformance suite to fail to catch.

---

## What the mutex is actually protecting

The naive version of this code has a bug that survives casual review:

```go
count := l.counts[key]      // read
if count < l.limit {        // decide
    l.counts[key] = count+1 // act
}
```

Two goroutines both read 99 against a limit of 100, both decide there is room, both write
100. Two requests admitted for one slot. This is check-then-act, and it is the single most
common concurrency bug in rate limiting code.

The fix is not "add a mutex somewhere" — it is that **the read, the decision, and the
write must all happen inside one critical section.** A mutex held only across the write is
useless; the decision was already made on stale data. `ConcurrentNeverOverAdmits` in the
conformance suite exists to catch exactly this, and it asserts an *exact* count rather
than an upper bound because a lost-update race makes the limiter admit **fewer**, not
more — an upper bound never sees it.

Worth internalising now, because in Phase 6 this identical bug reappears as
`GET` → compare → `SET` against Redis, where there is no mutex to reach for and the gap
between the calls is a network round trip.

### The clock read belongs inside the lock too

This is subtler and easy to get wrong. The tempting shape is:

```go
now := l.clock.Now()   // outside
l.mu.Lock()
defer l.mu.Unlock()
// ...use now...
```

Reading the clock is expensive-ish and stateless, so hoisting it out of the critical
section looks like a free optimisation. It is not, because it lets two goroutines observe
time *out of order relative to the lock*:

1. Goroutine A reads `now = T1`
2. Goroutine B reads `now = T2`, where `T2 > T1`
3. B acquires the lock first, sees the window has expired, sets `start = T2`
4. A acquires the lock and computes `elapsed = T1.Sub(T2)` — **negative**

A negative elapsed time means no window roll (correct, by luck) but
`ResetAfter = window - elapsed` is now *larger than the window*. A denied client is told
to retry after longer than the window it is waiting on, which violates the invariant
`RetryAfterOnlyWhenDenied` asserts.

The tests never catch this, because `FakeClock` is frozen for the duration of the
concurrency test — every goroutine reads the same instant, so the inversion cannot occur.
It only appears in production, rarely, under contention. That is worth sitting with: **the
conformance suite is strong on the properties it encodes and blind to this one.**

Reading the clock inside the lock makes observed time monotonically non-decreasing in lock
order, and removes the class of bug entirely. It widens the critical section by one
`time.Now()`, which Phase 3 will measure rather than guess at.

---

## The flaw: boundary burst

Fixed windows admit up to **twice the limit** across a window boundary, in an arbitrarily
short span of real time.

```
limit = 100 per minute

window 1 [00:00 ── 01:00)                window 2 [01:00 ── 02:00)
                    ▲                    ▲
                    │                    │
              100 requests         100 requests
              at 00:59.999         at 01:00.000

              └──────── 200 requests in 1 ms ────────┘
```

Nothing is broken. Each window independently admitted exactly 100. The limiter's contract
— "at most 100 per window" — held perfectly. But the contract the *operator* believed they
were buying was "at most 100 in any 60-second span", and that was never what a fixed window
provides. The guarantee is 100 per *aligned bucket*, and an attacker who knows where the
boundary falls gets 200 in any span containing it.

For a limiter protecting a database connection pool, a 2× spike is the difference between
headroom and saturation.

### The test that shows it

Phase 1's most valuable artifact is a test that *passes* while demonstrating this, in
`fixedwindow/burst_test.go`. It is not a conformance test — a fixed window admitting a
boundary burst is a property of the algorithm, not a failure of the implementation — so it
belongs to the package, not to the shared suite. That distinction is the reason the suite
was split the way it was in Phase 0.

The mechanism is worth noting: because the window is anchored to the first request (see
below), the test anchors it with a single request, advances to just before the boundary,
drains the remaining quota, then advances one millisecond and drains a full fresh limit.
The result is ~2× the limit inside a 1 ms span, asserted rather than asserted-about.

---

## Anchored vs aligned windows

There are two flavours of "fixed window", and most writing on the topic conflates them.

**Anchored** (implemented here): the window starts when the key's first request arrives.
Every key has its own phase.

**Aligned** (calendar windows): boundaries fall on absolute time — `now.Truncate(window)`.
Every key resets at the top of the minute, together.

| | Anchored | Aligned |
|---|---|---|
| Boundary position | Per key, unpredictable from outside | Global, trivially predictable |
| Burst exploitability | Attacker must probe to locate the boundary | Attacker reads a clock |
| Reset semantics for users | "a minute after you started" | "at the top of the minute" |
| Thundering herd | Spread across keys | All keys reset simultaneously |

Aligned windows are what most public API gateways implement, because "your quota resets at
00:00 UTC" is explainable to users in a way "60 seconds after whenever you happened to
start" is not. The cost is that the 2× burst stops being a statistical curiosity and
becomes a *reliably reproducible attack*: the boundary is public information.

Anchored windows are chosen here because they make the burst harder to exploit without
making it any less real — which keeps the demonstration honest. The implementation
difference is roughly one line, and either way the flaw is structural.

---

## Caveats this phase deliberately leaves in place

### Unbounded memory growth

Every distinct key allocates a window struct that is never freed. The map only grows.

Key by API token and cardinality is bounded by your customer count. Key by IP — the
default in every tutorial — and an attacker with a /64 of IPv6 can mint 18 quintillion
distinct keys, each one costing a map entry, a string, and a struct. The rate limiter
becomes the cheapest available memory exhaustion vector against your own service. The
irony is worth stating plainly: *the component you added to protect the service is now the
easiest way to kill it.*

This is left unfixed until Phase 3, where eviction is implemented and the cost of the
sweep is measured. It is flagged in the code rather than silently carried, because an
acknowledged limitation is engineering and an unacknowledged one is a bug.

### `RetryAfter` invites a thundering herd

Every client denied inside the same window is told to retry at the same instant — the
window reset. They comply, simultaneously, and the first moment of the new window
receives a synchronised spike of exactly the traffic that was just rejected.

The limiter is right to report the true reset time; it does not know how many clients it
told. Jitter belongs on the client, or in the middleware that translates a `Decision` into
a `Retry-After` header (Phase 8). Naming it here so the header work later is a deliberate
decision rather than a default.

### `Remaining` on a partially-satisfiable denial

`AllowN(key, 4)` against a limit of 5 with 3 already used is denied, and reports
`Remaining: 2`. Both facts are true and they look contradictory: quota remains, and the
request failed. The alternative — reporting 0 because *this* request got nothing — makes
the header lie to every other client sharing the key. Truthful and confusing beats tidy
and wrong.

---

## What the conformance suite proved, and what it did not

Passing `ratelimittest.Run` establishes that the implementation admits its limit, rejects
past it, isolates keys, replenishes on schedule, reports coherent metadata, keeps `AllowN`
atomic, and does not over-admit under 1,280 concurrent attempts with the race detector on.

It does not establish that the limiter is *useful*. The boundary burst passes every
assertion. So does the unbounded map. So would a clock inversion that only manifests under
production contention.

That gap is the honest lesson of Phase 1, and it generalises: a conformance suite encodes
the properties you thought to name. It is a floor, not a ceiling — and its real value is
that the next four implementations get held to the same floor for free, so that the
per-algorithm tests can spend their attention on what actually differs.

---

**Next:** [02 — Token bucket](02-token-bucket.md) — replacing the discrete window with a
continuously replenishing one, which dissolves the boundary burst and introduces a
separate knob for how much burst you actually want.
