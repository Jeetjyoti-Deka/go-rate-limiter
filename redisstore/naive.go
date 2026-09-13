package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Naive implements the limiter as separate read and write round trips.
//
// It is broken, deliberately, and is kept in the repository because a defect
// you can run is more convincing than one you can read about.
//
// The read, the decision and the write are three separate operations with a
// network round trip between each. Two instances both read the same value, both
// decide there is room, and both write — the check-then-act bug from
// docs/01-mutex-and-fixed-windows.md, except the window between check and act
// is now a network round trip rather than a few nanoseconds, so every
// concurrent request inside it reads the same stale state.
//
// There is no mutex available to fix it: sync.Mutex excludes goroutines in one
// address space and says nothing about another process on another machine.
//
// Use Atomic. This type exists for TestNaiveOverAdmits.
type Naive struct {
	p params
}

var _ ratelimit.Limiter = (*Naive)(nil)

// NewNaive returns a deliberately racy limiter. See the type documentation.
func NewNaive(cfg Config) (*Naive, error) {
	p, err := newParams(cfg)
	if err != nil {
		return nil, err
	}
	return &Naive{p: p}, nil
}

// Allow implements ratelimit.Limiter.
func (l *Naive) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter, racily.
func (l *Naive) AllowN(ctx context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	// Round trip 1: what time is it?
	now, err := l.p.now(ctx)
	if err != nil {
		return ratelimit.Decision{}, err
	}

	rkey := l.p.redisKey(key)

	// Round trip 2: read the current state.
	stored, err := l.p.client.Get(ctx, rkey).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return ratelimit.Decision{}, fmt.Errorf("%w: %v", ratelimit.ErrBackendUnavailable, err)
	}
	// A missing key reads as 0, which max() below turns into a fresh bucket.

	tat := max(stored, now)
	cost := int64(n) * l.p.emission
	newTat := tat + cost
	allowAt := newTat - l.p.tolerance

	if allowAt > now {
		d := ratelimit.Decision{
			Limit:      l.p.limit,
			Remaining:  l.p.remaining(tat, now),
			ResetAfter: time.Duration(max(tat-now, 0)) * time.Microsecond,
		}
		if cost <= l.p.tolerance {
			d.RetryAfter = time.Duration(allowAt-now) * time.Microsecond
		}
		return d, nil
	}

	// Round trip 3: write. Everything that happened since round trip 2 is
	// silently discarded — this is the bug.
	if err := l.p.client.Set(ctx, rkey, newTat, l.p.ttl).Err(); err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: %v", ratelimit.ErrBackendUnavailable, err)
	}

	return ratelimit.Decision{
		Allowed:    true,
		Limit:      l.p.limit,
		Remaining:  l.p.remaining(newTat, now),
		ResetAfter: time.Duration(max(newTat-now, 0)) * time.Microsecond,
	}, nil
}
