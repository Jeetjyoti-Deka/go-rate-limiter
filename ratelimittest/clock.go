// Package ratelimittest provides a shared conformance suite that every
// ratelimit.Limiter implementation in this repository must pass, plus the
// controllable clock those tests run against.
package ratelimittest

import (
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// FakeClock is a manually advanced Clock. It is safe for concurrent use, which
// matters because the concurrency conformance test reads it from many
// goroutines at once.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

// Compile-time proof that FakeClock satisfies the production interface.
var _ ratelimit.Clock = (*FakeClock)(nil)

// NewFakeClock returns a clock frozen at t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{now: t}
}

// Now implements ratelimit.Clock.
//
// Note that the returned time has no monotonic reading, unlike time.Now. That
// is intentional: a limiter that only ever subtracts stored time.Time values
// behaves identically either way, so passing the suite is evidence the
// implementation is not smuggling in wall clock arithmetic.
func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
