package middleware_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/middleware"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// failingLimiter stands in for a limiter whose backing store is unreachable.
type failingLimiter struct{ err error }

func (f failingLimiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return f.AllowN(ctx, key, 1)
}
func (f failingLimiter) AllowN(context.Context, string, int) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, f.err
}

func serve(t *testing.T, cfg middleware.Config) http.Handler {
	t.Helper()
	mw, err := middleware.New(cfg)
	if err != nil {
		t.Fatalf("middleware.New: %v", err)
	}
	return mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
}

func get(h http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func newBucket(t *testing.T, limit int, window time.Duration, clk ratelimit.Clock) ratelimit.Limiter {
	t.Helper()
	l, err := tokenbucket.New(tokenbucket.Config{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("tokenbucket.New: %v", err)
	}
	return l
}

func TestAllowedSetsHeaders(t *testing.T) {
	const limit = 5
	h := serve(t, middleware.Config{
		Limiter: newBucket(t, limit, time.Minute, ratelimittest.NewFakeClock(epoch)),
	})

	w := get(h, "192.0.2.1:1234")

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if got := w.Header().Get("X-RateLimit-Limit"); got != "5" {
		t.Errorf("X-RateLimit-Limit = %q, want \"5\"", got)
	}
	if got := w.Header().Get("X-RateLimit-Remaining"); got != "4" {
		t.Errorf("X-RateLimit-Remaining = %q, want \"4\"", got)
	}
	// Headers on success are the point: a client can pace itself instead of
	// discovering the limit by hitting it.
	if w.Header().Get("X-RateLimit-Reset") == "" {
		t.Error("X-RateLimit-Reset missing on an allowed response")
	}
	if got := w.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q on an allowed response, want empty", got)
	}
}

func TestRejectedReturns429WithRetryAfter(t *testing.T) {
	const limit = 2
	h := serve(t, middleware.Config{
		Limiter: newBucket(t, limit, time.Minute, ratelimittest.NewFakeClock(epoch)),
	})

	for range limit {
		if w := get(h, "192.0.2.1:1234"); w.Code != http.StatusOK {
			t.Fatalf("status = %d during drain, want 200", w.Code)
		}
	}

	w := get(h, "192.0.2.1:1234")
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", w.Code)
	}
	if got := w.Header().Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q; a client told to retry immediately hot-loops", got)
	}
	if got := w.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want \"0\"", got)
	}
}

// TestRetryAfterNeverZero pins the rounding. At 10 per second the wait is
// 100ms, which truncates to 0 and must be reported as 1.
func TestRetryAfterNeverZero(t *testing.T) {
	const limit = 10
	h := serve(t, middleware.Config{
		Limiter: newBucket(t, limit, time.Second, ratelimittest.NewFakeClock(epoch)),
	})

	for range limit {
		get(h, "192.0.2.1:1234")
	}

	w := get(h, "192.0.2.1:1234")
	if got := w.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q for a sub-second wait, want \"1\"", got)
	}
}

func TestKeysAreIsolated(t *testing.T) {
	h := serve(t, middleware.Config{
		Limiter: newBucket(t, 1, time.Minute, ratelimittest.NewFakeClock(epoch)),
	})

	if w := get(h, "192.0.2.1:1111"); w.Code != http.StatusOK {
		t.Fatalf("first client: status = %d, want 200", w.Code)
	}
	if w := get(h, "192.0.2.1:2222"); w.Code != http.StatusTooManyRequests {
		t.Errorf("same IP, different port: status = %d, want 429 — ByIP must ignore the port",
			w.Code)
	}
	if w := get(h, "198.51.100.9:1111"); w.Code != http.StatusOK {
		t.Errorf("different IP: status = %d, want 200", w.Code)
	}
}

func TestCostFunc(t *testing.T) {
	const limit = 10
	h := serve(t, middleware.Config{
		Limiter:  newBucket(t, limit, time.Minute, ratelimittest.NewFakeClock(epoch)),
		CostFunc: func(*http.Request) int { return 4 },
	})

	// Two requests at 4 units each fit; the third does not.
	for i := range 2 {
		if w := get(h, "192.0.2.1:1234"); w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, w.Code)
		}
	}
	if w := get(h, "192.0.2.1:1234"); w.Code != http.StatusTooManyRequests {
		t.Errorf("third weighted request: status = %d, want 429", w.Code)
	}
}

func TestOnError(t *testing.T) {
	unreachable := fmt.Errorf("%w: dial tcp", ratelimit.ErrBackendUnavailable)

	t.Run("defaults to fail open", func(t *testing.T) {
		h := serve(t, middleware.Config{Limiter: failingLimiter{err: unreachable}})
		if w := get(h, "192.0.2.1:1234"); w.Code != http.StatusOK {
			t.Errorf("status = %d, want 200: the default policy is fail open", w.Code)
		}
	})

	t.Run("fail closed returns 503", func(t *testing.T) {
		var seen error
		h := serve(t, middleware.Config{
			Limiter: failingLimiter{err: unreachable},
			OnError: func(http.ResponseWriter, *http.Request, error) bool { return false },
		})
		_ = seen

		if w := get(h, "192.0.2.1:1234"); w.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503: the server cannot decide, which is not the "+
				"client's fault", w.Code)
		}
	})

	t.Run("observes the error", func(t *testing.T) {
		var seen error
		h := serve(t, middleware.Config{
			Limiter: failingLimiter{err: unreachable},
			OnError: func(_ http.ResponseWriter, _ *http.Request, err error) bool {
				seen = err
				return true
			},
		})
		get(h, "192.0.2.1:1234")

		if !errors.Is(seen, ratelimit.ErrBackendUnavailable) {
			t.Errorf("OnError saw %v, want an ErrBackendUnavailable", seen)
		}
	})
}

func TestFirstNonEmpty(t *testing.T) {
	key := middleware.FirstNonEmpty(
		middleware.ByHeader("X-API-Key"),
		middleware.ByIP,
	)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "192.0.2.1:1234"
	if got := key(r); got != "192.0.2.1" {
		t.Errorf("without the header, key = %q, want the IP", got)
	}

	r.Header.Set("X-API-Key", "tenant-7")
	if got := key(r); got != "tenant-7" {
		t.Errorf("with the header, key = %q, want \"tenant-7\"", got)
	}
}
