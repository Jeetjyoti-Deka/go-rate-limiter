// Package leased amortises coordination: it claims blocks of quota from a
// shared limiter and serves requests from them locally, so the network is
// touched once per lease rather than once per request.
//
// See docs/07-leasing-and-degradation.md.
package leased

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// FailureMode is what a limiter does when the shared limiter is unreachable.
type FailureMode int

const (
	// FailOpen admits the request. This is the default: a rate limiter is a
	// safety mechanism, and turning its failure into a total outage inverts
	// the reason it was installed.
	FailOpen FailureMode = iota

	// FailClosed denies the request. Choose it when losing the protected
	// resource is worse than losing the service — a payments API, an expensive
	// inference endpoint, anything where unbounded traffic costs real money or
	// corrupts state.
	FailClosed
)

// Config configures a Limiter.
type Config struct {
	// Remote is the shared limiter that grants leases. Required.
	//
	// Any ratelimit.Limiter will do; in practice it is a redisstore.Limiter.
	// Leasing is not a limiting algorithm, it is a caching layer over one.
	Remote ratelimit.Limiter

	// Limit and Window must match Remote's configuration. They derive how long
	// an unspent lease stays valid.
	Limit  int
	Window time.Duration

	// LeaseSize is how many units are claimed at once.
	//
	// Larger means fewer round trips and a looser limit: fleet-wide error is
	// bounded by instances * LeaseSize. Defaults to 1, at which point this
	// limiter is exactly Remote with extra steps.
	LeaseSize int

	// OnRemoteFailure is the degradation policy. Defaults to FailOpen.
	OnRemoteFailure FailureMode

	// OnDegraded is called whenever that policy is applied.
	//
	// Wire it to a metric. A limiter that has quietly stopped limiting is
	// precisely the silent failure docs/05-the-multi-instance-break.md is
	// about, and this is the only thing that will tell you.
	OnDegraded func(error)

	Clock ratelimit.Clock
}

// Stats counts what a Limiter has done. Safe to read concurrently.
type Stats struct {
	Requests    int64
	RemoteCalls int64
	Degraded    int64
}

// lease is one key's block of claimed quota.
type lease struct {
	// mu is held across the remote call on purpose: one lease request in
	// flight per key, rather than every waiting goroutine firing its own.
	mu sync.Mutex

	// remaining is unspent units; expires is when they rot. Hoarding unspent
	// quota is worse than wasting it — the shared limiter regenerates those
	// units, and an instance still holding them can spend both.
	remaining int
	expires   time.Time

	// remoteRemaining is what Remote reported having left at the last lease.
	// Added to remaining it estimates global headroom.
	remoteRemaining int

	// A refusal is remembered until denyUntil, so overload does not make the
	// limiter chattier. deniedFor is the size refused: a refusal of 30 units
	// says nothing about whether one would be granted.
	denyUntil time.Time
	resetAt   time.Time
	deniedFor int
}

// Limiter serves requests from leased blocks of a shared limiter's quota.
type Limiter struct {
	remote     ratelimit.Limiter
	limit      int
	leaseSize  int
	leaseTTL   time.Duration
	onFailure  FailureMode
	onDegraded func(error)
	clock      ratelimit.Clock

	leases sync.Map // map[string]*lease

	requests    atomic.Int64
	remoteCalls atomic.Int64
	degraded    atomic.Int64
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if the configuration is invalid.
func New(cfg Config) (*Limiter, error) {
	if cfg.Remote == nil {
		return nil, fmt.Errorf("%w: nil Remote", ratelimit.ErrInvalidConfig)
	}
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v",
			ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	size := cfg.LeaseSize
	if size == 0 {
		size = 1
	}
	if size < 0 || size > cfg.Limit {
		return nil, fmt.Errorf("%w: LeaseSize %d must be between 1 and Limit %d",
			ratelimit.ErrInvalidConfig, cfg.LeaseSize, cfg.Limit)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	emission := cfg.Window / time.Duration(cfg.Limit)

	return &Limiter{
		remote:    cfg.Remote,
		limit:     cfg.Limit,
		leaseSize: size,
		// A lease rots after the time the global rate needs to regenerate it,
		// so the units discarded have already been replaced upstream and the
		// books balance.
		leaseTTL:   emission * time.Duration(size),
		onFailure:  cfg.OnRemoteFailure,
		onDegraded: cfg.OnDegraded,
		clock:      clock,
	}, nil
}

// Stats reports counters since construction.
func (l *Limiter) Stats() Stats {
	return Stats{
		Requests:    l.requests.Load(),
		RemoteCalls: l.remoteCalls.Load(),
		Degraded:    l.degraded.Load(),
	}
}

// Allow implements ratelimit.Limiter.
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter.
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}
	l.requests.Add(1)

	ls := l.leaseFor(key)

	ls.mu.Lock()
	defer ls.mu.Unlock()

	now := l.clock.Now()

	// Unspent quota rots.
	if !ls.expires.After(now) {
		ls.remaining = 0
	}

	// The fast path: no network at all.
	if ls.remaining >= n {
		ls.remaining -= n
		return l.decide(ls, true, now), nil
	}

	// A remembered refusal, for a request at least as large as the one refused.
	if ls.denyUntil.After(now) && n >= ls.deniedFor {
		d := l.decide(ls, false, now)
		d.RetryAfter = ls.denyUntil.Sub(now)
		if d.ResetAfter < d.RetryAfter {
			d.ResetAfter = d.RetryAfter
		}
		return d, nil
	}

	need := n - ls.remaining
	size := max(l.leaseSize, need)

	rd, err := l.acquire(ctx, key, size)
	if err != nil {
		return l.degrade(err, ls, now), nil
	}

	// Not enough global quota for a whole lease? Take exactly what is needed.
	// Without this the limiter under-admits by up to LeaseSize-1 per window.
	if !rd.Allowed && size > need {
		size = need
		rd, err = l.acquire(ctx, key, size)
		if err != nil {
			return l.degrade(err, ls, now), nil
		}
	}

	if !rd.Allowed {
		ls.denyUntil = now.Add(rd.RetryAfter)
		ls.resetAt = now.Add(rd.ResetAfter)
		ls.deniedFor = n

		d := l.decide(ls, false, now)
		d.RetryAfter = rd.RetryAfter
		if d.ResetAfter < d.RetryAfter {
			d.ResetAfter = d.RetryAfter
		}
		return d, nil
	}

	ls.remaining += size
	ls.remoteRemaining = rd.Remaining
	ls.expires = now.Add(l.leaseTTL)
	ls.resetAt = now.Add(rd.ResetAfter)
	ls.denyUntil = time.Time{}
	ls.deniedFor = 0

	ls.remaining -= n
	return l.decide(ls, true, now), nil
}

func (l *Limiter) acquire(ctx context.Context, key string, size int) (ratelimit.Decision, error) {
	l.remoteCalls.Add(1)
	return l.remote.AllowN(ctx, key, size)
}

// decide builds a Decision from local state.
//
// Remaining estimates global headroom as what Remote last reported plus what
// this instance holds unspent. That stays monotonically non-increasing between
// leases and across them, which reporting the raw local balance would not.
func (l *Limiter) decide(ls *lease, allowed bool, now time.Time) ratelimit.Decision {
	r := ls.remoteRemaining + ls.remaining
	if r < 0 {
		r = 0
	}
	if r > l.limit {
		r = l.limit
	}
	return ratelimit.Decision{
		Allowed:    allowed,
		Limit:      l.limit,
		Remaining:  r,
		ResetAfter: max(ls.resetAt.Sub(now), 0),
	}
}

// degrade applies the configured policy when Remote is unreachable.
//
// It returns a nil error: a policy configured once beats the same decision
// re-derived at every call site, where some callers would fail closed by
// accident simply because that is what an early `return err` does. The cost is
// that the failure is silent unless OnDegraded is wired up, which is why the
// documentation insists on it.
func (l *Limiter) degrade(err error, ls *lease, now time.Time) ratelimit.Decision {
	l.degraded.Add(1)
	if l.onDegraded != nil {
		l.onDegraded(err)
	}

	if l.onFailure == FailOpen {
		return l.decide(ls, true, now)
	}

	d := l.decide(ls, false, now)
	// Nothing is known about when Remote returns. Advertise the lease TTL
	// rather than zero, which would invite a hot retry loop.
	d.RetryAfter = l.leaseTTL
	if d.ResetAfter < d.RetryAfter {
		d.ResetAfter = d.RetryAfter
	}
	return d
}

// leaseFor returns key's lease, creating it if absent. The zero lease is valid:
// no quota, already expired, no cached refusal.
func (l *Limiter) leaseFor(key string) *lease {
	if v, ok := l.leases.Load(key); ok {
		return v.(*lease)
	}
	v, _ := l.leases.LoadOrStore(key, &lease{})
	return v.(*lease)
}
