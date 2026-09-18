// Package middleware turns a ratelimit.Decision into an HTTP response: the
// X-RateLimit-* headers a client needs in order to pace itself, and a 429 with
// a Retry-After when it has not.
//
// See docs/08-middleware.md.
package middleware

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config configures the middleware.
type Config struct {
	// Limiter decides. Required.
	Limiter ratelimit.Limiter

	// KeyFunc identifies the client. Defaults to ByIP.
	//
	// This is a security decision rather than a detail: keying on anything the
	// client can forge hands an attacker unlimited buckets, which both bypasses
	// the limit and supplies an unbounded stream of keys to allocate. See
	// docs/08-middleware.md.
	KeyFunc func(*http.Request) string

	// CostFunc reports how many units a request consumes. Defaults to 1.
	//
	// A rate limit is a proxy for resource consumption, and requests are not
	// equally expensive.
	CostFunc func(*http.Request) int

	// OnLimited serves rejected requests. Defaults to a plain-text 429.
	//
	// The rate limit headers are already set when it runs.
	OnLimited http.Handler

	// OnError is called when the limiter itself fails, and reports whether to
	// admit the request anyway. Defaults to admitting it.
	//
	// It should not write to w — returning false produces a 503. The default is
	// fail open, for the reason argued in docs/07-leasing-and-degradation.md: a
	// rate limiter is a safety mechanism, and turning its failure into a total
	// outage inverts the reason it was installed.
	//
	// Wire it to a metric. A limiter that has silently stopped limiting is the
	// failure mode docs/05-the-multi-instance-break.md is about.
	OnError func(w http.ResponseWriter, r *http.Request, err error) bool
}

// New returns middleware for any net/http chain.
func New(cfg Config) (func(http.Handler) http.Handler, error) {
	if cfg.Limiter == nil {
		return nil, fmt.Errorf("%w: nil Limiter", ratelimit.ErrInvalidConfig)
	}

	keyFn := cfg.KeyFunc
	if keyFn == nil {
		keyFn = ByIP
	}

	costFn := cfg.CostFunc
	if costFn == nil {
		costFn = func(*http.Request) int { return 1 }
	}

	limited := cfg.OnLimited
	if limited == nil {
		limited = http.HandlerFunc(defaultLimited)
	}

	onErr := cfg.OnError
	if onErr == nil {
		onErr = func(http.ResponseWriter, *http.Request, error) bool { return true }
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d, err := cfg.Limiter.AllowN(r.Context(), keyFn(r), costFn(r))
			if err != nil {
				if onErr(w, r, err) {
					next.ServeHTTP(w, r)
					return
				}
				// 503, not 429: the server cannot determine whether to serve.
				// That is a server problem, and saying 429 would blame the
				// client for it.
				http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
				return
			}

			setHeaders(w.Header(), d)

			if !d.Allowed {
				limited.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

// setHeaders writes the rate limit headers.
//
// They go on allowed responses too. That is the difference between a client
// that paces itself and one that discovers the limit by hitting it.
func setHeaders(h http.Header, d ratelimit.Decision) {
	h.Set("X-RateLimit-Limit", strconv.Itoa(d.Limit))
	h.Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))

	// Delta seconds, not an epoch timestamp. Both conventions exist in the
	// wild and are indistinguishable by inspection, so a client guessing wrong
	// either sleeps for decades or not at all. See docs/08-middleware.md.
	h.Set("X-RateLimit-Reset", strconv.Itoa(seconds(d.ResetAfter)))

	if !d.Allowed {
		// Never zero. A compliant client told to retry immediately will
		// hot-loop, turning a rejection into a denial-of-service on yourself.
		h.Set("Retry-After", strconv.Itoa(max(seconds(d.RetryAfter), 1)))
	}
}

// seconds rounds a duration up to whole seconds. Rounding down would advertise
// an instant at which the request still fails — the same mistake, at a
// different layer, as truncating in tokenbucket.accrualTime.
func seconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}

// defaultLimited writes 429 Too Many Requests.
//
// 429 says the client sent too much; 503 says the server is in trouble
// regardless of who is asking. Clients treat them differently and should.
func defaultLimited(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
}
