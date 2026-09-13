# 05 — Your rate limiter has no idea it has siblings

> Five algorithms. A shared conformance suite. Benchmarks, contention profiles, memory
> measured per key, eviction proved not to change a single decision.
>
> All of it correct. All of it useless the moment you run a second copy of the process.

---

## Everything up to here assumed one process

Go back through the repository and the assumption is everywhere, never stated because it
never needed to be. `map[string]*bucket.State` is a map in *this* process's heap.
`sync.Mutex` excludes *this* process's goroutines. `atomic.CompareAndSwap` is atomic with
respect to *this* process's cores.

Every one of those is a statement about a single address space. None of them says anything
about the identical binary running on another machine.

So deploy three replicas behind a load balancer, configure 100 requests per minute, and
each replica independently admits 100. The service admits 300.

```
                       ┌──────────────┐
                  ┌───►│  instance A  │  admits 100  ─┐
                  │    └──────────────┘               │
   client ───► LB ┼───►┌──────────────┐               ├──► 300 admitted
                  │    │  instance B  │  admits 100  ─┤
                  │    └──────────────┘               │
                  └───►┌──────────────┐               │
                       │  instance C  │  admits 100  ─┘
                       └──────────────┘

            configured limit: 100        actual: 300
```

Nothing is broken. Each instance enforced exactly the limit it was given, against exactly
the requests it saw. The limiter's contract — *at most `limit` per `window`, per key, as
observed by me* — held perfectly in all three. The contract the operator thought they were
buying said nothing about "as observed by me", because in one process that clause is
vacuous.

---

## It is not an algorithm problem

The instinct is to look for the algorithm that survives this. None do, and the test in
`distributed/` runs all five to prove it rather than assert it:

| | configured | admitted across 3 instances |
|---|---|---|
| `fixedwindow` | 100 | 300 |
| `tokenbucket` | 100 | 300 |
| `windowlog` | 100 | 300 |
| `windowcounter` | 100 | 300 |
| `gcra` | 100 | 300 |

Identical, because the defect is not in any of them. It is in where the state lives. An
algorithm decides *what* to remember; it has no opinion about *where*. Swapping a fixed
window for GCRA changes the shape of the burst and the cost of a rejection, and changes
nothing at all about the fact that three processes keep three separate counters.

This is worth sitting with, because it is the moment the problem stops being a programming
problem and becomes a distributed systems problem. No amount of care inside one process
can fix it. The fix has to come from outside.

---

## The worst part is that it is silent

A bug that crashes is cheap. A bug that logs an error is cheap. This one does neither.

Every instance reports healthy. Every instance's metrics show it admitting at or under
its configured limit. Every `X-RateLimit-Remaining` header the service emits is accurate
*from the perspective of whichever instance happened to answer*. There is no error, no
log line, no alert, and no single place where the aggregate is visible — because no
component in the system computes the aggregate.

You find out when the database the limiter was protecting falls over, and the limiter's
own dashboards say it was doing its job.

### And the limit drifts with your deployment

The effective limit is `configured_limit × instance_count`. That second term is owned by
your autoscaler.

- Scale three replicas to ten under load: your rate limit silently becomes 1000/min,
  at exactly the moment the extra traffic made you scale.
- Run a rolling deploy: old and new pods overlap, instance count briefly doubles, and so
  does the limit.
- Run a canary: the canary has its own counters. Your limit is now `100 × (N + 1)`.

The rate limit — a number chosen deliberately, reviewed, written in an API contract, maybe
published to customers — is in practice a function of an autoscaling policy that nobody
thought of as security-relevant. It is highest precisely when load is highest, which is
the exact opposite of what you wanted.

---

## Three ways out, two of them bad

### Sticky routing

Hash each client to a fixed instance, so one key is only ever seen by one limiter. The
per-instance view becomes the whole view, and the existing code is correct again.

It fails on contact with reality. Instance counts change, and every rescale reshuffles the
hash — clients land on instances with no history and get a fresh full quota, so scaling
events become quota resets. Load distributes by key rather than by request, so one heavy
client pins an instance while others idle. And it cannot express a *global* limit at all:
"1000 requests per minute across all customers" has no single key to hash.

Worst, it couples your rate limiting correctness to your load balancer's configuration —
two systems usually owned by different teams, with the dependency written down nowhere.

### Divide the limit by N

Configure each instance with `limit / N`. Three instances, 33 each, aggregate 99.

This is arithmetically appealing and behaves badly. It assumes traffic spreads evenly
across instances; when it does not, a client whose requests happen to land on one instance
gets a third of the quota they are entitled to, while capacity sits unused elsewhere. It
requires every instance to know `N` accurately and immediately — so an autoscaling event
must reconfigure every replica, and during the gap the service is either over- or
under-limiting. A crashed instance permanently loses its share until something notices.

It converts a correctness problem into a capacity-utilisation problem and a configuration
problem, and solves neither.

### Shared state

Move the counters out of the process and into somewhere all instances can see: Redis, or
any store with atomic read-modify-write. One counter per key, incremented by whoever
handles the request.

This is the answer, and it is worth being clear-eyed about what it costs before Phase 6
implements it:

- **A network round trip on every request.** A map lookup is ~50 ns; a Redis call across a
  datacentre is ~500 µs. That is four orders of magnitude, added to every request the
  service handles.
- **A new single point of failure.** The component you added to keep the service up can
  now take it down. Phase 7 is entirely about what to do when it does.
- **A new bottleneck.** Every request from every instance now converges on one Redis key.
  The thing you were avoiding with sharding in Phase 3 is back, in a worse place.
- **Atomicity you have to earn.** `GET`, compare, `SET` is the Phase 1 check-then-act bug
  again, now with a network hop where the mutex used to be. Phase 6 implements it wrong
  first, on purpose, to show the race.

The trade is stark: correctness bought with latency, availability, and a new dependency.
Phase 7 asks whether the full price has to be paid on every request, and the answer is no.

---

## Written as a failing test

`distributed/` holds two tests, and the pairing is the point.

`TestInstancesOverAdmit` **passes**. It asserts the behaviour that actually occurs —
exactly `instances × limit` admissions — for all five algorithms. It is a regression test
for the defect, which sounds backwards until you notice it is the only thing that would
catch a Phase 6 implementation silently regressing to per-instance state.

`TestAggregateLimitHolds` **fails**. It asserts what an operator believes they configured:
that a service with a limit of 100 admits at most 100. It is guarded by a build tag so CI
stays green, and run deliberately:

```
$ go test -tags brokenbydesign ./distributed/
--- FAIL: TestAggregateLimitHolds/tokenbucket
    aggregate_test.go:52: 3 instances admitted 300 requests against a configured
        limit of 100: the limit is enforced per instance, not per service
FAIL
```

That output is the whole phase. It is more convincing than this document because it cannot
be argued with.

It also stays red permanently, because in-process state is what these five limiters *are*.
Its counterpart is `redisstore.TestSharedAcrossInstances`: the same scenario — three
independently constructed limiters, one shared key — run against state that lives outside
the process. From Phase 6 onward that one admits exactly the configured limit, which is
what the rest of this document is arguing has to happen.

There is also a runnable version for people who would rather see processes than
assertions: `cmd/demo-server` and `cmd/loadgen` do the same thing over real HTTP, three
servers and a load generator round-robining across them. Same number, more theatre.

---

## What this phase actually establishes

Not "use Redis". That is the boring half.

The useful half is the shape of the mistake: **a correctness property that holds in every
component can fail to hold in the system those components make up, and nothing inside any
component can detect it.** Each instance is individually correct and locally consistent.
The failure only exists at a level no instance can see.

That generalises well past rate limiting — it is the same shape as a cache each replica
invalidates separately, a "once per day" job on three replicas that runs three times, or a
uniqueness check done per-instance before an insert. In every case the code is right, the
tests pass, and the system is wrong.

The question to carry into Phase 6 is not "how do I share state" but the one underneath
it: **how much coordination does this actually need, and how little can I get away with?**
Phase 6 buys the expensive, exact version. Phase 7 works out what it costs and how much of
it can be given back.

---

**Next:** [06 — Redis, atomically](06-redis-atomicity.md) — why `GET`/`SET` is the Phase 1
bug with a network hop in the middle, and what a Lua script buys.
