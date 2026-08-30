// Package windowcounter implements a sliding window counter: it keeps a count
// for the current window and the previous one, and estimates the trailing
// count by weighting the previous window by how much of it still overlaps.
//
// Constant memory regardless of the limit, at the cost of being approximate.
// See docs/04-algorithm-comparison.md.
package windowcounter

import (
	"context"
	"fmt"
	"math"
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

// counters is one key's state: when the current window opened, how many units
// it has taken, and how many the immediately preceding window took.
type counters struct {
	start time.Time
	cur   int
	prev  int
}

// Limiter is a sliding window counter limiter, safe for concurrent use.
type Limiter struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	mu   sync.Mutex
	keys map[string]*counters
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if limit or window is
// non-positive.
func New(cfg Config) (*Limiter, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v",
			ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	return &Limiter{
		limit:  cfg.Limit,
		window: cfg.Window,
		clock:  clock,
		keys:   make(map[string]*counters),
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

	c, ok := l.keys[key]
	if !ok {
		c = &counters{start: now}
		l.keys[key] = c
	}

	l.roll(c, now)

	elapsed := now.Sub(c.start)
	// The previous window's weight falls linearly from 1 to 0 across this one.
	// This is the approximation: it assumes those requests were spread evenly.
	weight := 1 - float64(elapsed)/float64(l.window)
	estimate := float64(c.prev)*weight + float64(c.cur)

	d := ratelimit.Decision{
		Limit:      l.limit,
		Remaining:  remaining(l.limit, estimate),
		ResetAfter: l.resetAfter(c, elapsed),
	}

	if estimate+float64(n) <= float64(l.limit) {
		c.cur += n
		d.Allowed = true
		d.Remaining = remaining(l.limit, estimate+float64(n))
		d.ResetAfter = l.resetAfter(c, elapsed)
		return d, nil
	}

	if n <= l.limit {
		d.RetryAfter = l.retryAfter(c, n, elapsed)
	}

	return d, nil
}

// roll advances the window if now has moved past it.
//
// Windows are anchored to the key's first request and advanced by whole
// windows, so every comparison remains time.Time subtraction and keeps its
// monotonic reading. Truncating to absolute wall-clock boundaries — the usual
// formulation — would strip it, reintroducing exactly the clock-jump bug
// docs/02-token-bucket.md warns about.
func (l *Limiter) roll(c *counters, now time.Time) {
	elapsed := now.Sub(c.start)
	if elapsed < l.window {
		return
	}

	windows := int64(elapsed / l.window)
	if windows == 1 {
		c.prev = c.cur
	} else {
		// Two or more windows of silence: nothing carries over.
		c.prev = 0
	}
	c.cur = 0
	c.start = c.start.Add(l.window * time.Duration(windows))
}

// resetAfter reports how long until the estimate reaches zero.
func (l *Limiter) resetAfter(c *counters, elapsed time.Duration) time.Duration {
	rest := l.window - elapsed
	switch {
	case c.cur > 0:
		// This window's count becomes the next window's prev, which then
		// needs a further full window to decay away.
		return rest + l.window
	case c.prev > 0:
		return rest
	default:
		return 0
	}
}

// retryAfter reports how long until n units could be admitted.
//
// The estimate falls in two phases: linearly through the rest of the current
// window as prev decays, then — once the window rolls and cur becomes the new
// prev — linearly again through the next. The answer is whichever phase first
// brings the estimate low enough.
func (l *Limiter) retryAfter(c *counters, n int, elapsed time.Duration) time.Duration {
	w := float64(l.window)
	rest := l.window - elapsed

	// Phase 1: only prev is decaying.
	if headroom := float64(l.limit - c.cur - n); headroom >= 0 && c.prev > 0 {
		need := time.Duration(math.Ceil(w * (1 - headroom/float64(c.prev))))
		if d := need - elapsed; d <= rest {
			return max(d, 0)
		}
	}

	// Phase 2: after the roll, cur has become prev and decays in turn.
	headroom := float64(l.limit - n)
	if c.cur == 0 || headroom >= float64(c.cur) {
		return rest
	}
	return rest + time.Duration(math.Ceil(w*(1-headroom/float64(c.cur))))
}

// remaining clamps limit-estimate to a non-negative whole number of units.
func remaining(limit int, estimate float64) int {
	if r := float64(limit) - estimate; r > 0 {
		return int(r)
	}
	return 0
}
