package ratelimit

import "time"

// Clock abstracts time so limiter behaviour at window boundaries can be
// tested exactly rather than approximately. Tests that sleep are slow and
// flaky; tests that advance a fake clock are neither.
//
// The interface is deliberately minimal — limiters in this repository compute
// elapsed time lazily on read rather than refilling from a background
// goroutine, so Now is all they need.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock, backed by time.Now.
//
// Values returned by time.Now carry a monotonic reading, so subtracting two
// of them measures elapsed time correctly even if the wall clock jumps
// (NTP correction, DST, an operator running date -s). Limiters must therefore
// derive elapsed time by subtracting stored time.Time values, never by
// comparing Unix timestamps — doing the latter reintroduces exactly the wall
// clock bug the monotonic reading exists to prevent.
type SystemClock struct{}

// Now implements Clock.
func (SystemClock) Now() time.Time { return time.Now() }
