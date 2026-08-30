// Package fixedwindow implements the simplest correct rate limiter: a counter
// and a window start timestamp per key, guarded by a single mutex.
//
// It is correct in the sense the conformance suite tests for, and structurally
// incorrect in a way the suite cannot see — see burst_test.go and
// docs/01-mutex-and-fixed-windows.md.
package fixedwindow

import (
	"context"
	"fmt"
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config configures Limiter
type Config struct {
	// Limit is the maximum number of units admitted per Window. Required.
	Limit int

	// Window is the period over which Limit applies. Required.
	Window time.Duration

	// Clock supplies the current time. Defaults to ratelimit.SystemClock.
	Clock ratelimit.Clock
}

// Limiter is a fixed-window rate limiter, safe for concurrent use.
//
// Windows are anchored to a key's first request rather than aligned to
// absolute time, so keys do not share boundaries. The tradeoff is discussed in
// docs/01-mutex-and-fixed-windows.md.
type Limiter struct {
	limit  int
	window time.Duration
	clock  ratelimit.Clock

	// mu guards windows and the contents of every window it holds. Every
	// read-decide-write sequence must complete inside a single acquisition:
	// splitting it is the check-then-act bug this type exists to avoid.
	mu      sync.Mutex
	windows map[string]*window
}

// window is one key's state: when its current window opened, and how many
// units have been admitted since.
type window struct {
	start time.Time
	count int
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if limit or window is non-positive
func New(cfg Config) (*Limiter, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return nil, fmt.Errorf("%w: limit=%d window=%v", ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = ratelimit.SystemClock{}
	}

	return &Limiter{
		limit:   cfg.Limit,
		window:  cfg.Window,
		clock:   clock,
		windows: make(map[string]*window),
	}, nil
}

// Allow implements ratelimit.Limiter
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter.
//
// The context is unused: this limiter never blocks and never performs I/O.
func (l *Limiter) AllowN(_ context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Read the clock under the lock, not before it. Doing it outside allows
	// two goroutines to observe time out of order relative to lock
	// acquisition, which can produce a negative elapsed and a ResetAfter
	// larger than the window.
	now := l.clock.Now()

	// This map only ever grows. Keyed by IP that is a memory exhaustion
	// vector, and it is left unfixed deliberately: this package is the Phase 1
	// exhibit, kept as it was written. sharded carries the eviction.
	w, ok := l.windows[key]
	if !ok {
		w = &window{start: now}
		l.windows[key] = w
	}

	// Roll into a fresh window if the current one has elapsed. Note this is a
	// jump to now, not an advance by one window: a key idle for an hour starts
	// a new window on its next request rather than replaying every window it
	// missed.
	if now.Sub(w.start) >= l.window {
		w.start = now
		w.count = 0
	}

	resetAfter := l.window - now.Sub(w.start)

	d := ratelimit.Decision{
		Limit:      l.limit,
		Remaining:  l.limit - w.count,
		ResetAfter: resetAfter,
	}

	// Decide and commit without releasing the lock.
	if w.count+n <= l.limit {
		w.count += n
		d.Allowed = true
		d.Remaining = l.limit - w.count
		return d, nil
	}

	d.RetryAfter = resetAfter
	return d, nil
}
