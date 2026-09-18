# 08 — Telling the client what happened

> Seven phases produced a `Decision`. This one turns it into an HTTP response, which is
> where three of the fields designed in Phase 0 finally get used for the reason they were
> added — and where two decisions that look cosmetic turn out to be a security boundary and
> an interoperability trap.

---

## The contract, cashed in

Phase 0 argued for returning a struct rather than a bool:

> A rate limiter that returns `bool` has to be asked twice: once for the verdict, once for
> the headers. That second call races the first.

Eight phases later, here is the entire middleware body, and every field earns its place:

```go
h.Set("X-RateLimit-Limit", strconv.Itoa(d.Limit))
h.Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
h.Set("X-RateLimit-Reset", strconv.Itoa(seconds(d.ResetAfter)))

if !d.Allowed {
    h.Set("Retry-After", strconv.Itoa(max(seconds(d.RetryAfter), 1)))
    w.WriteHeader(http.StatusTooManyRequests)
    return
}
```

`Limit`, `Remaining`, `ResetAfter`, `RetryAfter`, `Allowed` — all five, from one call, all
consistent with each other because they describe the same instant. A `bool` API would have
needed a second call to populate the headers, and the quota could have moved in between:
the response would advertise a `Remaining` that the verdict was never based on.

Note also that the headers are set on **allowed** responses, not just rejections. That is
the difference between a client that paces itself and a client that discovers the limit by
hitting it.

---

## `X-RateLimit-Reset` means two incompatible things

There is no standard here, and the two conventions in the wild are not distinguishable by
inspection:

| | `X-RateLimit-Reset: 60` means |
|---|---|
| **delta seconds** | reset in 60 seconds |
| **epoch seconds** (GitHub, Twitter) | reset at 1970-01-01T00:01:00Z |

A client that assumes the wrong one either sleeps for fifty-six years or does not sleep at
all. Both have happened to real people.

This repository emits **delta seconds**, because it composes with `Retry-After` — which RFC
9110 defines as a delta — and because it does not require the client's clock to agree with
the server's, which Phase 6 spent a section explaining is not a safe assumption.

The IETF draft (`draft-ietf-httpapi-ratelimit-headers`) standardises unprefixed
`RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` with delta semantics. It is
still a draft and client support is thin, so the `X-`-prefixed names are what ship here.
Emitting both is a two-line change and a reasonable thing to do in a public API.

Whichever you choose: **write it down where clients will read it.** The header itself cannot
tell them.

---

## `Retry-After: 0` is a footgun

`Retry-After` is whole seconds. A `RetryAfter` of 200 ms truncates to `0`, which tells a
compliant client to retry immediately — and it will, and be rejected, and retry again. The
limiter has converted a rejection into a hot loop.

So: round up, and floor at 1.

```go
max(int(math.Ceil(d.RetryAfter.Seconds())), 1)
```

This is the same rounding argument as `tokenbucket.accrualTime` in
[docs/02](02-token-bucket.md), where truncating left the bucket a fraction of a token short
at the advertised instant. Different layer, identical mistake, identical fix: **advice that
is not actionable is worse than no advice.**

### Jitter is the client's job

Every client rejected inside the same window is told to retry at the same moment, complies,
and produces a synchronised spike of exactly the traffic that was just rejected — the
thundering herd from [docs/01](01-mutex-and-fixed-windows.md).

The middleware does not add jitter, and that is deliberate rather than an omission. The
server reports the true reset time; it has no idea how many clients it just told. Spreading
the retries requires knowing the size of the herd, which only the client population knows.
Well-behaved clients add jitter to `Retry-After`; the server that lies about the reset time
to compensate has made its headers useless for every other purpose.

---

## Key extraction is a security decision

Which field identifies "the client" is where a rate limiter is usually defeated.

```go
func ByIP(r *http.Request) string {
    host, _, err := net.SplitHostPort(r.RemoteAddr)
    ...
}
```

`RemoteAddr` is the TCP peer. It cannot be forged by the peer, because the connection
exists. **`X-Forwarded-For` can be forged by anyone**, because it is just a header — and a
limiter keyed on it is not a limiter at all. An attacker sends a different value per
request and gets one fresh bucket each time.

That failure is worse than it first appears, because it is *two* vulnerabilities:

1. The limit is bypassed entirely.
2. Every forged value allocates a key, which is the memory exhaustion vector
   [docs/01](01-mutex-and-fixed-windows.md) named and [Phase 3](03-concurrency.md) built
   eviction for. The attacker now has an unbounded supply of distinct keys.

`X-Forwarded-For` is only trustworthy when a proxy you control **overwrites** it — not
appends to it — and nothing can reach your service except through that proxy. If both hold,
key on it. If either is uncertain, key on `RemoteAddr` and accept that clients behind a
shared NAT share a bucket.

This repository ships `ByIP` and `ByHeader` and no `ByForwardedFor`, because the safe
version of that function depends on deployment topology the library cannot see. Writing it
is four lines; deciding it is correct is the hard part, and that decision belongs to whoever
knows the network.

### Anonymous traffic shares a bucket

`ByHeader("X-API-Key")` returns `""` for a request without the header, so every
unauthenticated request lands in the same bucket. That is usually what you want — a single
pool for anonymous traffic — but it is a policy, not a default to absorb by accident.
Composing gives the other behaviour:

```go
KeyFunc: middleware.FirstNonEmpty(
    middleware.ByHeader("X-API-Key"),
    middleware.ByIP,
)
```

Authenticated clients get per-key limits; everyone else falls back to per-IP.

---

## Weighted requests, at last

`AllowN` has been in the interface since Phase 0 and, outside the conformance suite, has
never been called with anything but 1. This is where it earns its place:

```go
CostFunc: func(r *http.Request) int {
    if r.URL.Path == "/search" {
        return 10
    }
    return 1
},
```

A rate limit is a proxy for resource consumption, and requests are not equally expensive. A
search costing ten units against the same quota is a far better approximation of load than
counting it as one — and the atomicity guarantee from
[docs/02](02-token-bucket.md) matters here for the first time outside a test: a weighted
request that cannot be satisfied must consume nothing, or an endpoint costing 10 against a
budget of 5 would drain the bucket every time it was refused.

---

## 429, not 503

`429 Too Many Requests` (RFC 6585) means *you* sent too many. `503 Service Unavailable`
means the server is in trouble regardless of who is asking.

The distinction matters because clients treat them differently and should: a 429 is the
client's problem to pace, a 503 is a server incident. Returning 503 for rate limiting tells
every client that you are broken, which affects their circuit breakers, their alerting, and
their willingness to retry at all.

---

## Errors at the edge

The middleware receives `(Decision, error)`. With a `leased.Limiter` the error is already
handled by its configured policy, but with a bare `redisstore.Limiter` it arrives here.

The default is **fail open**, for the reason argued in
[docs/07](07-leasing-and-degradation.md): a rate limiter is a safety mechanism, and turning
its failure into a total outage inverts the reason it was installed. `OnError` overrides
it, and — as with `OnDegraded` — the only way to know it is happening is to wire it to
something:

```go
OnError: func(w http.ResponseWriter, r *http.Request, err error) bool {
    limiterErrors.Inc()
    return true // admit
},
```

A limiter that has silently stopped limiting is the failure mode
[docs/05](05-the-multi-instance-break.md) is entirely about. Phase 5 produced it by
scaling out; Phase 8 produces it with an unwired callback.

---

## What the repository ends up being

Not a library. The README says so, and this document is where it becomes obvious why:
`ByForwardedFor` is missing because the correct version depends on topology, the header
names are a choice you should revisit, and the error policy is a decision no library can
make for you. A production rate limiter is mostly deployment context, and the interesting
part — the part these eight phases are actually about — is the reasoning underneath it.

What the repository is instead is the reasoning, with the measurements attached:

- A fixed window admits **199** requests in a millisecond against a limit of 100, and that
  is a property of the algorithm rather than a bug.
- A global mutex gets **slower** as cores are added — 0.54× on four cores versus one.
- Per-key locking wins every latency benchmark and is still the wrong default, because it
  costs **2.57×** the memory.
- GCRA is the only limiter here that speeds up under contention, **3.53×**, because
  rejecting a request writes nothing.
- Three instances of a correct limiter admit **300** against a limit of 100, and no
  component can see it.
- Fixing that costs **3,930×** the latency of a local decision.
- Most of that cost was coordination nobody needed: leasing recovers **97×** of it for a
  bounded, measurable error.

Every one of those started as an assumption and became a number.

---

**Next:** the write-up. `docs/` was written during each phase rather than after, which
means the blog series is an edit rather than an archaeology project — which was the whole
point of writing it that way.
