// Package bucket holds the token bucket arithmetic with no synchronisation of
// its own. Each limiter package supplies its own locking around it, which is
// what makes the Phase 3 benchmarks a comparison of synchronisation strategies
// rather than of arithmetic.
//
// Nothing here is safe for concurrent use.
package bucket

import (
	"fmt"
	"math"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// State is one key's bucket: its token level as of Last.
//
// The zero value is not a valid bucket; use Params.New.
type State struct {
	Tokens float64
	Last   time.Time
}

// Params is the configuration shared by every bucket in one limiter.
type Params struct {
	Limit int     // reported as Decision.Limit
	Burst float64 // capacity
	Rate  float64 // tokens per second
}

// NewParams validates a limiter configuration and derives the internal
// parameters. A zero burst defaults to limit.
func NewParams(limit int, window time.Duration, burst int) (Params, error) {
	if limit <= 0 || window <= 0 {
		return Params{}, fmt.Errorf("%w: limit=%d window=%v",
			ratelimit.ErrInvalidConfig, limit, window)
	}
	if burst < 0 {
		return Params{}, fmt.Errorf("%w: burst=%d", ratelimit.ErrInvalidConfig, burst)
	}
	if burst == 0 {
		burst = limit
	}

	return Params{
		Limit: limit,
		Burst: float64(burst),
		Rate:  float64(limit) / window.Seconds(),
	}, nil
}

// ClockOr returns c, or the system clock when c is nil.
func ClockOr(c ratelimit.Clock) ratelimit.Clock {
	if c == nil {
		return ratelimit.SystemClock{}
	}
	return c
}

// New returns the state of a fresh, full bucket as of now.
func (p Params) New(now time.Time) State {
	return State{Tokens: p.Burst, Last: now}
}

// TryTake refills s up to now and removes n tokens if they are available,
// mutating s in place.
func (p Params) TryTake(s *State, now time.Time, n int) ratelimit.Decision {
	// Refill unconditionally: accrual is a function of elapsed time, not of
	// the decision below. Capping at Burst here rather than later is what
	// stops a long-idle key from accumulating unbounded quota.
	if elapsed := now.Sub(s.Last); elapsed > 0 {
		s.Tokens = math.Min(p.Burst, s.Tokens+elapsed.Seconds()*p.Rate)
		s.Last = now
	}

	d := ratelimit.Decision{
		Limit:      p.Limit,
		Remaining:  int(s.Tokens),
		ResetAfter: p.AccrualTime(p.Burst - s.Tokens),
	}

	want := float64(n)
	if want <= s.Tokens {
		s.Tokens -= want
		d.Allowed = true
		d.Remaining = int(s.Tokens)
		d.ResetAfter = p.AccrualTime(p.Burst - s.Tokens)
		return d
	}

	// A request larger than Burst can never succeed, so no retry time is
	// meaningful. See docs/02-token-bucket.md.
	if want <= p.Burst {
		d.RetryAfter = p.AccrualTime(want - s.Tokens)
	}
	return d
}

// AccrualTime reports how long it takes to accrue the given number of tokens,
// rounded up so that waiting exactly this long is guaranteed sufficient.
func (p Params) AccrualTime(tokens float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(tokens / p.Rate * float64(time.Second)))
}

// IdleRecovery is how long a bucket must go untouched before its state becomes
// indistinguishable from a fresh one — the time to refill from empty to full.
//
// A key idle for at least this long can be deleted with no change to any future
// decision, which is what makes eviction exact rather than approximate. Used in
// Phase 3b.
func (p Params) IdleRecovery() time.Duration {
	return p.AccrualTime(p.Burst)
}
