package tokenbucket

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

type Config struct {
	// Limit tokens per Window is the sustained refill rate. Required.
	Limit int

	// Window is the period over which Limit accrues. Required.
	Window time.Duration

	// Burst is the bucket capacity: the largest number of tokens that may
	// accumulate, and therefore the largest instantaneous spike admitted.
	// Defaults to Limit.
	Burst int

	// Clock supplies the current time. Defaults to ratelimit.SystemClock.
	Clock ratelimit.Clock
}

// Limiter is a token bucket rate limiter, safe for concurrent use.
type Limiter struct {
	limit int     // reported as Decision.Limit
	burst float64 // capacity
	rate  float64 // tokens per second
	clock ratelimit.Clock

	mu      sync.Mutex
	buckets map[string]*bucket
}

// bucket is one key's state: its token level as of `last`.
type bucket struct {
	tokens float64
	last   time.Time
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if limit, window, or burst is invalid
func New(cfg Config) (*Limiter, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v", ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	burst := cfg.Burst
	if burst == 0 {
		burst = cfg.Limit
	}

	if burst < 0 {
		return nil, fmt.Errorf("%w: burst=%d", ratelimit.ErrInvalidConfig, cfg.Burst)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	return &Limiter{
		limit:   cfg.Limit,
		burst:   float64(burst),
		clock:   clock,
		rate:    float64(cfg.Limit) / cfg.Window.Seconds(),
		buckets: make(map[string]*bucket),
	}, nil
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

	l.mu.Lock()
	defer l.mu.Unlock()

	// Under the lock, for the same reason as fixedwindow: observed time must
	// be monotonically non-decreasing in lock-acquisition order.
	now := l.clock.Now()

	// TODO(phase-3): unbounded map growth, same as fixedwindow.
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}

	// Lazy refill. `last` advances unconditionally — refill is a function of
	// elapsed time, not of the decision made below. Capping at burst here
	// rather than later is what prevents a long-idle key from accumulating
	// unbounded quota.
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed.Seconds()*l.rate)
		b.last = now
	}

	d := ratelimit.Decision{
		Limit:      l.limit,
		Remaining:  int(b.tokens),
		ResetAfter: l.accrualTime(l.burst - b.tokens),
	}

	want := float64(n)
	if want <= b.tokens {
		b.tokens -= want
		d.Allowed = true
		d.Remaining = int(b.tokens)
		d.ResetAfter = l.accrualTime(l.burst - b.tokens)
		return d, nil
	}

	// A request larger than burst can never be satisfied, so no retry time is
	// meaningful; leave RetryAfter zero. See docs/02-token-bucket.md.
	if want <= l.burst {
		d.RetryAfter = l.accrualTime(want - b.tokens)
		return d, nil
	}

	return d, nil
}

// accrualTime reports how long it takes to accrue the given number of tokens.
//
// The result is rounded up: truncating would advertise an instant at which the
// bucket is a fraction of a token short, so a client obeying Retry-After to the
// nanosecond would be denied again and burn a round trip.
func (l *Limiter) accrualTime(tokens float64) time.Duration {
	if tokens <= 0 {
		return 0
	}
	return time.Duration(math.Ceil(tokens / l.rate * float64(time.Second)))
}
