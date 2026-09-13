package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// gcraScript is loaded once and invoked by SHA. go-redis issues EVALSHA and
// falls back to EVAL on NOSCRIPT, which matters because Redis forgets its
// script cache on restart and on SCRIPT FLUSH.
var gcraScript = redis.NewScript(gcraSource)

// Limiter is a GCRA limiter whose state lives in Redis, so every instance of a
// service shares one limit.
//
// GCRA is not an arbitrary choice. Its entire state is a single instant, which
// here means one key holding one integer, a script short enough to be safe on a
// single-threaded server, and nothing to serialise. A token bucket would need
// two values updated together; a sliding window log would need a sorted set and
// a script whose cost grows with the limit. See docs/06-redis-atomicity.md.
type Limiter struct {
	p params
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Redis-backed limiter, or ErrInvalidConfig if the configuration
// is invalid.
func New(cfg Config) (*Limiter, error) {
	p, err := newParams(cfg)
	if err != nil {
		return nil, err
	}
	return &Limiter{p: p}, nil
}

// Allow implements ratelimit.Limiter.
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter in a single round trip.
//
// Errors wrap ErrBackendUnavailable so callers can distinguish "the store is
// unreachable", where failing open may be the right policy, from a programming
// mistake, where it never is. Deciding what to do about it is Phase 7.
func (l *Limiter) AllowN(ctx context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	// Zero tells the script to read the server clock. A non-zero value is a
	// test affordance; see Config.Clock.
	var now int64
	if l.p.clock != nil {
		now = l.p.clock.Now().UnixMicro()
	}

	res, err := gcraScript.Run(ctx, l.p.client,
		[]string{l.p.redisKey(key)},
		l.p.emission,
		l.p.tolerance,
		int64(n)*l.p.emission,
		max(l.p.ttl.Milliseconds(), 1), // PX rejects 0
		now,
	).Slice()
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("%w: %v", ratelimit.ErrBackendUnavailable, err)
	}

	vals, err := int64s(res, 4)
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("ratelimit: malformed script reply: %w", err)
	}

	return ratelimit.Decision{
		Allowed:    vals[0] == 1,
		Limit:      l.p.limit,
		Remaining:  int(vals[1]),
		RetryAfter: time.Duration(vals[2]) * time.Microsecond,
		ResetAfter: time.Duration(vals[3]) * time.Microsecond,
	}, nil
}

// int64s converts a script reply to exactly n integers.
func int64s(res []any, n int) ([]int64, error) {
	if len(res) != n {
		return nil, fmt.Errorf("got %d values, want %d", len(res), n)
	}
	out := make([]int64, n)
	for i, v := range res {
		got, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("value %d has type %T, want int64", i, v)
		}
		out[i] = got
	}
	return out, nil
}
