# 00 — Foundations: designing the contract before the implementation

> Phase 0 ships no rate limiter. It ships the definition of one, and a test suite
> strict enough that every implementation to come is graded rather than trusted.

This felt backwards while writing it. The payoff shows up five times, once per
algorithm, and again in Phase 6 when the distributed implementation has to satisfy
exactly the same assertions as the in-memory one.

---

## Why the test suite comes first

The normal order is: write the limiter, then write tests for the limiter. Do that five
times and you get five test suites that each encode their author's assumptions about
what "rate limited" means. They pass. They also aren't comparable, because they aren't
asking the same questions.

Inverting the order forces a different question up front: *what is true of every rate
limiter, regardless of algorithm?* That list turns out to be short and non-obvious:

- A fresh limiter admits exactly its configured limit, no more
- A denied request reports `Remaining == 0` and a positive `RetryAfter`
- One key's exhaustion never affects another key
- An idle period of one full window restores capacity
- `Remaining` never rises without time passing, never exceeds the limit, never goes negative
- An oversized `AllowN` consumes nothing on its way to failing
- Under concurrency, the admitted count is *exactly* the limit

Everything else — how replenishment is shaped, how much state a key costs, whether
bursts are smoothed — is algorithm-specific, and belongs in that algorithm's own tests.
The split is the useful artifact. `ratelimittest.Run` holds the universal properties;
each package's own test file holds the interesting ones.

A second effect, unintended and more valuable: writing the suite first is what surfaced
the `AllowN` atomicity requirement. Nothing about a fixed-window counter forces you to
think about partial consumption. Asking "what must be true of *all* of them" does.

---

## Three decisions in the interface that look wrong

```go
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
	AllowN(ctx context.Context, key string, n int) (Decision, error)
}
```

### `error`, on a function that cannot fail

Every limiter in Phases 1–4 is a map and a mutex. None of them can fail. Returning
`error` from them is dead weight — `return d, nil`, forever.

It stays because the interface has to be able to describe the Redis-backed limiter in
Phase 6, and that one fails constantly: connection refused, timeout, `LOADING Redis is
loading the dataset in memory`. An interface without `error` would force that
implementation to either panic or lie — silently allowing (or silently denying) on
network failure, with the caller unable to tell the difference between "you have quota"
and "I have no idea whether you have quota."

That distinction is the whole subject of Phase 7. A limiter that can't express "I don't
know" can't have a degradation policy, and a distributed limiter without a degradation
policy is a latent outage. So the `nil` returns in the local implementations are the
price of being able to tell the truth later.

`ErrBackendUnavailable` exists in Phase 0 for the same reason. Phase 7 needs to
distinguish *the store is unreachable* — where failing open may be correct — from *you
passed a negative n*, where it never is. Defining it now means the error taxonomy
doesn't churn when the distributed implementation lands.

### `context.Context`, on a function that never blocks

Same argument, shorter. Local limiters ignore it. Network calls need deadlines and
cancellation, and a `ctx` retrofitted later is a breaking change to every call site.

### `Decision`, not `bool`

A rate limiter that returns `bool` has to be asked twice: once for the verdict, once for
the headers. That second call races the first — quota can change in between, so the
`X-RateLimit-Remaining` you emit may not be the one your verdict was based on.

Returning a struct also forces each algorithm to actually *know* its own state. It's
easy to write a fixed-window counter that answers yes/no without ever computing when the
window resets. Requiring `ResetAfter` and `RetryAfter` in the return value makes that
impossible, and those are precisely the fields the HTTP middleware in Phase 8 needs.

### No `Wait()`

`golang.org/x/time/rate` offers `Wait(ctx)`, which blocks until quota is available.
Deliberately omitted here. Blocking is a caller policy — a server usually wants to
reject immediately with 429, a background job usually wants to wait, and a limiter that
decides this for you has to grow a second set of knobs to be told otherwise. Two methods
that answer one question is a smaller surface than four methods that answer two.

---

## The clock is the load-bearing abstraction

Rate limiting is arithmetic on time. Test it against the real clock and every assertion
about a window boundary becomes a race between the test and the scheduler — which is
resolved by sleeping, which makes the suite slow, and resolved *unreliably*, which makes
it flaky. A limiter test that sleeps for a second per case is a limiter test nobody runs.

```go
type Clock interface {
	Now() time.Time
}
```

One method, because of a design choice that runs through the whole repo: **replenishment
is computed lazily on read, never pushed by a background goroutine.** A limiter with a
`time.Ticker` per key needs `NewTicker` in the interface, needs a goroutine per key, and
needs shutdown semantics. A limiter that stores `lastRefill` and derives available quota
from `now.Sub(lastRefill)` needs none of that. It is also the reason `Clock` never grew a
second method.

### The monotonic clock trap

`time.Now()` returns a `time.Time` carrying both a wall-clock reading and a monotonic
one. Subtracting two such values uses the monotonic reading, so elapsed time stays
correct even when the wall clock jumps — NTP correction, a DST transition, an operator
running `date -s`.

That property is only preserved if you subtract `time.Time` values. Store a Unix
timestamp instead and compute `now.Unix() - stored`, and the monotonic reading is gone:
an NTP step backwards makes elapsed time negative, and a limiter that treats negative
elapsed time as "no refill" will deny every request until the clock catches up. Worse,
a step *forward* refills every bucket to full at once.

So the rule for every implementation in this repo: **elapsed time comes from
`time.Time.Sub`, never from integer timestamp arithmetic.** Phase 6 is where this stops
being free — Redis has no access to a Go process's monotonic clock, which is why the Lua
script uses Redis's own `TIME` rather than a timestamp shipped from the client.

### Why the fake clock deliberately has no monotonic reading

`FakeClock` is built from `time.Date(...)`, which produces a `time.Time` with a wall
reading only. This is not a limitation being tolerated; it is a property being exploited.

An implementation that correctly derives elapsed time by subtraction behaves identically
whether or not a monotonic reading is present. One that reaches for `time.Since` on a
stored value, or mixes injected time with `time.Now()`, will behave differently under the
fake clock than in production — and the `ReplenishesOverTime` test catches it, because
the fake clock advances by a full window while the real one hasn't moved.

Passing the suite is therefore evidence about *how* time is being read, not just that the
answer came out right.

---

## Two assertions doing more work than they look like

### `ConcurrentNeverOverAdmits` asserts an exact count

1,280 concurrent attempts against a limit of 100, with a one-hour window and a clock that
never advances. No replenishment is possible, so the admitted count must be exactly 100.

The bound most people write is `<= limit`. That version passes for a limiter with a
lost-update race, because losing an update makes it admit *fewer* — a race that
double-counts a decrement is invisible to an upper bound. It also passes for a limiter
that deadlocks after the first ten requests. `== limit` fails both.

Combined with `-race`, this is the assertion that Phase 3's three concurrency strategies
all have to survive. Sharding a map is easy; sharding it without changing the observable
admission count is the part worth measuring.

### `AllowNIsAtomic` is a Phase 6 test written early

Ask for `limit + 1` units. It must be denied — no surprise — and it must consume
*nothing*, so the next `limit` single requests all succeed.

Under a mutex this is nearly free: check and commit inside one critical section. The test
looks like a formality.

It stops being a formality against Redis, where the naive implementation is
`GET` → compare → `SET`, and the gap between those calls is where two instances both
read 95, both decide 5 more units fit, and both write. Same check-then-act bug, now
distributed across a network with no lock to hide behind. Phase 6 deliberately implements
the broken version first, and this test — written before any of that existed — is what
catches it.

That is the strongest argument for the inverted order. A property that seems trivial in
one context is the central difficulty in another, and you only notice by asking what's
true of every implementation rather than what's true of the one in front of you.

---

## What Phase 0 deliberately left out

- **No `Close()`** — nothing owns a goroutine or a connection yet. Phases 3 (key
  eviction) and 6 (Redis client) will need lifecycle management; adding it now would be
  guessing at its shape.
- **No `Reset(key)`** — useful for tests and admin tooling, not needed by any current
  caller.
- **No metrics or tracing hooks.** They belong behind a decorator that wraps a `Limiter`,
  not in the interface every implementation must satisfy.
- **No config struct in the root package.** Each algorithm has genuinely different
  parameters — a token bucket has separate rate and burst, a fixed window has neither —
  and flattening them into one shared struct would produce fields that are meaningless
  for most implementations.
- **No algorithm-specific tests in the shared suite.** The fixed-window boundary burst is
  a *property of fixed windows*, not a conformance failure. It belongs in
  `fixedwindow/`, and it is the subject of the next doc.

---

## Verifying the phase

```bash
gofmt -l .            # must print nothing
go vet ./...
go test -race -count=1 ./...
staticcheck ./...
go doc .              # package documentation must actually appear
go doc . Decision     # field comments must appear
```

The last two matter more than they look. A blank line between a doc comment and the
declaration below it detaches the comment — the code compiles, `gofmt` stays quiet, and
`go doc` silently shows nothing. In a repo whose reasoning lives largely in its comments,
that is a real defect, and `go doc` is the only thing that catches it.

Note the two-argument form. `go doc .Decision` looks like it should work and does not:
`go doc` reads it as a *package path*, fails to resolve it, and falls back to printing
the docs for package `.` — so it produces plausible output while never looking at the
symbol at all. Use `go doc . Decision`, or `go doc Decision` from inside the package
directory. A verification command that can't fail is worse than none, because it
manufactures confidence.

The suite also needs to be seen failing at least once. Breaking the stub's atomicity on
purpose:

```go
	s.counts[key] = s.cfg.Limit   // partially drain on the failure path
	d.RetryAfter = reset
	return d, nil
```

should produce exactly one failure, `AllowNIsAtomic`, with the partial-drain message. A
test suite never observed failing is a suite of unknown strength.

---

**Next:** [01 — Mutex and fixed windows](01-mutex-and-fixed-windows.md) — the obvious
implementation, why it is correct, and why it still admits twice the limit.
