// Package windowlog implements a sliding window log: it records the timestamp
// of every admitted request and counts those falling inside the trailing
// window.
//
// This is the only limiter here that is exactly correct — over any interval of
// one window, never more than the limit — and it pays for that with memory
// proportional to the limit. See docs/04-algorithm-comparison.md.
package windowlog

import (
	"context"
	"fmt"
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config configures a Limiter.
type Config struct {
	Limit  int
	Window time.Duration
	Clock  ratelimit.Clock
}

// ring is a fixed-capacity circular buffer of admission timestamps, oldest
// first.
//
// Capacity is exactly Limit, so a key's footprint is bounded by construction
// rather than by trusting eviction to keep up: there is physically nowhere to
// put a limit+1'th entry.
type ring struct {
	times []time.Time
	head  int // index of the oldest entry
	count int
}

func newRing(capacity int) *ring {
	return &ring{times: make([]time.Time, capacity)}
}

// at returns the i'th oldest entry; i must be less than count.
func (r *ring) at(i int) time.Time {
	return r.times[(r.head+i)%len(r.times)]
}

// push stores t as the newest entry. The caller must ensure there is room.
func (r *ring) push(t time.Time) {
	r.times[(r.head+r.count)%len(r.times)] = t
	r.count++
}

// dropBefore discards every entry at or before cutoff.
func (r *ring) dropBefore(cutoff time.Time) {
	for r.count > 0 && !r.at(0).After(cutoff) {
		r.head = (r.head + 1) % len(r.times)
		r.count--
	}
}

// Limiter is a sliding window log limiter, safe for concurrent use.
//
// A single mutex guards every key. Sharding is orthogonal and was measured in
// Phase 3; this phase varies the algorithm only.
type Limiter struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	mu   sync.Mutex
	keys map[string]*ring
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if limit or window is
// non-positive.
func New(cfg Config) (*Limiter, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v", ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	return &Limiter{
		limit:  cfg.Limit,
		window: cfg.Window,
		clock:  clock,
		keys:   make(map[string]*ring),
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

	now := l.clock.Now()

	r, ok := l.keys[key]
	if !ok {
		r = newRing(l.limit)
		l.keys[key] = r
	}

	r.dropBefore(now.Add(-l.window))

	d := ratelimit.Decision{
		Limit:      l.limit,
		Remaining:  l.limit - r.count,
		ResetAfter: l.expiryOf(r, r.count-1, now),
	}

	if r.count+n <= l.limit {
		for range n {
			r.push(now)
		}

		d.Allowed = true
		d.Remaining = l.limit - r.count
		d.ResetAfter = l.expiryOf(r, r.count-1, now)
		return d, nil
	}

	// Denied: n slots are wanted, limit-count are free, so the
	// (count+n-limit) oldest entries must age out first. A request larger
	// than the limit can never succeed and gets no retry time, matching
	// tokenbucket.
	if n <= l.limit {
		d.RetryAfter = l.expiryOf(r, r.count+n-l.limit-1, now)
	}
	return d, nil
}

// expiryOf reports how long until the i'th oldest entry leaves the window.
func (l *Limiter) expiryOf(r *ring, i int, now time.Time) time.Duration {
	if i < 0 || i >= r.count {
		return 0
	}
	if d := r.at(i).Add(l.window).Sub(now); d > 0 {
		return d
	}
	return 0
}
