// Package ratelimit defines the contract every rate limiter in this repository
// implements, along with the clock abstraction that makes their behaviour
// testable without sleeping.
//
// The import path ends in "go-rate-limiter" but the package is named
// "ratelimit"; import it explicitly:
//
//	import ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
package ratelimit

import (
	"context"
	"errors"
	"time"
)

// Decision is the complete result of a limit check. It carries enough
// information to populate RFC-style rate limit response headers without a
// second call into the limiter.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool

	// Limit is the configured ceiling for this key.
	Limit int

	// Remaining is the quota left after this decision. Never negative.
	Remaining int

	// RetryAfter is how long the caller should wait before retrying.
	// Zero when Allowed is true.
	RetryAfter time.Duration

	// ResetAfter is how long until the key is fully replenished.
	ResetAfter time.Duration
}

// Limiter decides whether a request identified by key may proceed.
//
// Implementations must be safe for concurrent use by multiple goroutines.
//
// The context and error return exist for implementations that talk to a
// network — a purely local limiter never fails, but a Redis-backed one can,
// and the interface has to admit that or the distributed implementation is
// forced to lie. Callers must decide what a non-nil error means for them:
// failing open trades correctness for availability, failing closed does the
// reverse. See docs/07-leasing-and-degradation.md.
type Limiter interface {
	// Allow is shorthand for AllowN(ctx, key, 1).
	Allow(ctx context.Context, key string) (Decision, error)

	// AllowN reports whether n units of quota are available for key, and
	// consumes them if so.
	//
	// AllowN is atomic: if the request is denied, no quota is consumed. A
	// request for more than the configured limit can never succeed and must
	// not partially drain the key.
	AllowN(ctx context.Context, key string, n int) (Decision, error)
}

var (
	// ErrInvalidN is returned by AllowN when n is negative.
	ErrInvalidN = errors.New("ratelimit: n must not be negative")

	// ErrInvalidConfig is returned by constructors given a non-positive
	// limit or window.
	ErrInvalidConfig = errors.New("ratelimit: invalid configuration")

	// ErrBackendUnavailable is returned when a limiter's backing store could
	// not be reached. It is deliberately distinct from other errors so
	// callers can implement a degradation policy that keys on exactly this
	// condition rather than on any failure.
	ErrBackendUnavailable = errors.New("ratelimit: backend unavailable")
)
