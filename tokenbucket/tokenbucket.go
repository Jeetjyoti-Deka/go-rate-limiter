package tokenbucket

import (
	"context"
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/internal/bucket"
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
//
// A single mutex serialises every key. This is the baseline the Phase 3
// benchmarks measure the alternatives against.
type Limiter struct {
	params bucket.Params
	clock  ratelimit.Clock

	mu      sync.Mutex
	buckets map[string]*bucket.State
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if limit, window, or burst is invalid
func New(cfg Config) (*Limiter, error) {
	p, err := bucket.NewParams(cfg.Limit, cfg.Window, cfg.Burst)
	if err != nil {
		return nil, err
	}

	return &Limiter{
		params:  p,
		clock:   bucket.ClockOr(cfg.Clock),
		buckets: make(map[string]*bucket.State),
	}, nil
}

// Allow implements ratelimit.Limiter.
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter.
func (l *Limiter) AllowN(_ context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Under the lock: observed time must be monotonically non-decreasing in
	// lock-acquisition order.
	now := l.clock.Now()

	// TODO(phase-3b): unbounded map growth.
	s, ok := l.buckets[key]
	if !ok {
		st := l.params.New(now)
		s = &st
		l.buckets[key] = s
	}

	return l.params.TryTake(s, now, n), nil
}
