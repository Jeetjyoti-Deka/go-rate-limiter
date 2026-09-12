// Package gcra implements the Generic Cell Rate Algorithm, a virtual
// scheduling limiter borrowed from ATM networking.
//
// Behaviourally it matches a token bucket: burst requests may arrive at once,
// then one every window/limit thereafter. What differs is the state. A token
// bucket needs a token count and a timestamp; GCRA needs a single instant, the
// theoretical arrival time at which the bucket would next be empty.
//
// One instant fits in one 64-bit word, which is what makes this the only
// limiter here that needs no lock: the update is an atomic compare-and-swap.
// See docs/04-algorithm-comparison.md.
package gcra

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config configures a Limiter.
type Config struct {
	// Limit units per Window is the sustained rate. Required.
	Limit int

	// Window is the period over which Limit applies. Required.
	Window time.Duration

	// Burst is how far ahead of schedule a client may run — the largest
	// instantaneous spike admitted. Defaults to Limit.
	Burst int

	// Clock supplies the current time. Defaults to ratelimit.SystemClock.
	Clock ratelimit.Clock
}

// Limiter is a GCRA rate limiter, safe for concurrent use and lock-free on the
// hot path.
type Limiter struct {
	limit     int
	burst     int
	emission  time.Duration // T: time per unit
	tolerance time.Duration // τ: burst × T
	clock     ratelimit.Clock

	// base is the origin for every stored TAT, captured once at construction.
	//
	// TATs are held as int64 nanosecond offsets from base so they fit in an
	// atomic word. Storing absolute Unix nanoseconds instead would discard
	// the monotonic clock reading and reintroduce the wall-clock bug
	// docs/02-token-bucket.md warns about; subtracting two time.Time values
	// that both carry a monotonic reading does not.
	base time.Time

	// tats is a map[string]*atomic.Int64 of nanosecond offsets from base.
	//
	// A zero offset is a valid fresh key — max(stored, now) clamps it — so
	// entries need no initialisation and there is no publish race to lose.
	tats sync.Map
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if the configuration is invalid.
func New(cfg Config) (*Limiter, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v",
			ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	burst := cfg.Burst
	if burst < 0 {
		return nil, fmt.Errorf("%w: burst=%d", ratelimit.ErrInvalidConfig, burst)
	}
	if burst == 0 {
		burst = cfg.Limit
	}

	emission := cfg.Window / time.Duration(cfg.Limit)
	if emission <= 0 {
		// The per-unit interval rounded to zero, so the configured rate is
		// finer than a nanosecond and cannot be represented.
		return nil, fmt.Errorf("%w: window %v over limit %d is below 1ns per unit",
			ratelimit.ErrInvalidConfig, cfg.Window, cfg.Limit)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	return &Limiter{
		limit:     cfg.Limit,
		burst:     burst,
		emission:  emission,
		tolerance: emission * time.Duration(burst),
		clock:     clock,
		base:      clock.Now(),
	}, nil
}

// Allow implements ratelimit.Limiter.
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter.
//
// The hot path takes no lock. A denied request performs no write at all: it
// loads, computes, and returns. Under overload — exactly when contention is
// worst — this degenerates to a wait-free read, which inverts the usual failure
// mode where a limiter contends hardest while rejecting the most traffic.
func (l *Limiter) AllowN(_ context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	slot := l.slotFor(key)

	// Read the clock once, outside the retry loop. Re-reading it on each
	// attempt would let a contended key drift forward indefinitely.
	now := int64(l.clock.Now().Sub(l.base))
	cost := int64(n) * int64(l.emission)

	for {
		stored := slot.Load()

		// A TAT in the past means the bucket has drained; start from now.
		tat := max(stored, now)
		newTat := tat + cost
		allowAt := newTat - int64(l.tolerance)

		if allowAt > now {
			d := ratelimit.Decision{
				Limit:      l.limit,
				Remaining:  l.remaining(tat, now),
				ResetAfter: time.Duration(max(tat-now, 0)),
			}
			// A request larger than the burst can never succeed, so it gets
			// no retry time — matching tokenbucket.
			if cost <= int64(l.tolerance) {
				d.RetryAfter = time.Duration(allowAt - now)
			}
			return d, nil
		}

		if slot.CompareAndSwap(stored, newTat) {
			return ratelimit.Decision{
				Allowed:    true,
				Limit:      l.limit,
				Remaining:  l.remaining(newTat, now),
				ResetAfter: time.Duration(max(newTat-now, 0)),
			}, nil
		}
		// Lost the race: another goroutine moved the TAT. Recompute against
		// the value it wrote.
	}
}

// slotFor returns key's TAT cell, creating it if absent.
func (l *Limiter) slotFor(key string) *atomic.Int64 {
	if v, ok := l.tats.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := l.tats.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64)
}

// remaining reports how many further units could be admitted at now, given tat.
func (l *Limiter) remaining(tat, now int64) int {
	avail := (now + int64(l.tolerance) - tat) / int64(l.emission)
	if avail < 0 {
		return 0
	}
	if avail > int64(l.burst) {
		return l.burst
	}
	return int(avail)
}
