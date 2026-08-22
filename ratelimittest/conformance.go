package ratelimittest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config describes the limiter the conformance suite wants built.
//
// A Factory must honour every field. In particular, a limiter that ignores
// Clock and calls time.Now directly cannot pass this suite — that is the
// point.
type Config struct {
	Limit  int           // requests permitted per Window
	Window time.Duration // period over which Limit applies
	Clock  ratelimit.Clock
}

// Factory constructs the limiter under test.
type Factory func(t *testing.T, cfg Config) ratelimit.Limiter

// Run executes the full conformance suite against newLimiter.
//
// Every implementation in this repository calls this from its own package
// test, which is what makes the algorithms genuinely comparable: they are not
// merely swappable by interface, they are held to the same observable
// behaviour.
func Run(t *testing.T, newLimiter Factory) {
	t.Helper()

	cases := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"AllowsUpToLimit", testAllowsUpToLimit},
		{"RejectsPastLimit", testRejectsPastLimit},
		{"ReplenishesOverTime", testReplenishesOverTime},
		{"IsolatesKeys", testIsolatesKeys},
		{"RemainingIsCoherent", testRemainingIsCoherent},
		{"RetryAfterOnlyWhenDenied", testRetryAfterOnlyWhenDenied},
		{"AllowNIsAtomic", testAllowNIsAtomic},
		{"RejectsNegativeN", testRejectsNegativeN},
		{"ConcurrentNeverOverAdmits", testConcurrentNeverOverAdmits},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newLimiter)
		})
	}
}

// epoch is an arbitrary fixed instant. Tests must never depend on the real
// current time.
var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func fixture(t *testing.T, f Factory, limit int, window time.Duration) (ratelimit.Limiter, *FakeClock) {
	t.Helper()
	clk := NewFakeClock(epoch)
	return f(t, Config{Limit: limit, Window: window, Clock: clk}), clk
}

// mustAllow calls Allow and fails the test on a transport-level error
func mustAllow(t *testing.T, l ratelimit.Limiter, key string) ratelimit.Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow(%q) returned unexpected error: %v", key, err)
	}

	return d
}

func testAllowsUpToLimit(t *testing.T, f Factory) {
	const limit = 10
	l, _ := fixture(t, f, limit, time.Second)

	for i := range limit {
		if d := mustAllow(t, l, "k"); !d.Allowed {
			t.Fatalf("request %d/%d denied; a fresh limiter must admit its full limit", i+1, limit)
		}
	}
}

func testRejectsPastLimit(t *testing.T, f Factory) {
	const limit = 5
	l, _ := fixture(t, f, limit, time.Second)

	for range limit {
		mustAllow(t, l, "k")
	}

	d := mustAllow(t, l, "k")
	if d.Allowed {
		t.Fatal("request past the limit was allowed")
	}
	if d.Remaining != 0 {
		t.Errorf("Remaining = %d on a denied request, want 0", d.Remaining)
	}
}

func testReplenishesOverTime(t *testing.T, f Factory) {
	const limit = 5
	const window = time.Second
	l, clk := fixture(t, f, limit, window)

	for range limit {
		mustAllow(t, l, "k")
	}
	if d := mustAllow(t, l, "k"); d.Allowed {
		t.Fatal("limiter not exhausted; test precondition failed")
	}

	// A full idle window must restore capacity. Algorithms differ in *how*
	// they replenish (a fixed window resets, a token bucket drips), so the
	// suite only asserts the property they share.
	clk.Advance(window)

	if d := mustAllow(t, l, "k"); !d.Allowed {
		t.Fatal("limiter did not replenish after a full idle window; " +
			"it is probably calling time.Now instead of the injected Clock")
	}
}

func testIsolatesKeys(t *testing.T, f Factory) {
	const limit = 3
	l, _ := fixture(t, f, limit, time.Second)

	for range limit {
		mustAllow(t, l, "alice")
	}
	if d := mustAllow(t, l, "alice"); d.Allowed {
		t.Fatal("alice not exhausted; test precondition failed")
	}

	if d := mustAllow(t, l, "bob"); !d.Allowed {
		t.Fatal("exhausting one key denied another; keys must be independent")
	}
}

func testRemainingIsCoherent(t *testing.T, f Factory) {
	const limit = 8
	l, _ := fixture(t, f, limit, time.Second)

	prev := limit
	for i := range limit + 3 {
		d := mustAllow(t, l, "k")

		if d.Limit != limit {
			t.Errorf("call %d: Limit = %d, want %d", i, d.Limit, limit)
		}
		if d.Remaining < 0 {
			t.Errorf("call %d: Remaining = %d, must never be negative", i, d.Remaining)
		}
		if d.Remaining > limit {
			t.Errorf("call %d: Remaining = %d exceeds limit %d", i, d.Remaining, limit)
		}
		if d.Remaining > prev {
			t.Errorf("call %d: Remaining rose from %d to %d without time passing",
				i, prev, d.Remaining)
		}
		prev = d.Remaining
	}
}

func testRetryAfterOnlyWhenDenied(t *testing.T, f Factory) {
	const limit = 3
	const window = time.Second
	l, _ := fixture(t, f, limit, window)

	for i := range limit {
		if d := mustAllow(t, l, "k"); d.RetryAfter != 0 {
			t.Errorf("call %d: RetryAfter = %v on an allowed request, want 0", i, d.RetryAfter)
		}
	}

	d := mustAllow(t, l, "k")
	if d.Allowed {
		t.Fatal("limiter not exhausted; test precondition failed")
	}
	if d.RetryAfter <= 0 {
		t.Error("RetryAfter must be positive on a denied request; " +
			"a client told to retry after 0 will hot-loop")
	}
	if d.RetryAfter > window {
		t.Errorf("RetryAfter = %v exceeds the window %v", d.RetryAfter, window)
	}
}

func testAllowNIsAtomic(t *testing.T, f Factory) {
	const limit = 5
	l, _ := fixture(t, f, limit, time.Second)

	// A request larger than the limit can never be satisfied...
	d, err := l.AllowN(context.Background(), "k", limit+1)
	if err != nil {
		t.Fatalf("AllowN returned unexpected error: %v", err)
	}
	if d.Allowed {
		t.Fatal("AllowN allowed more than the configured limit")
	}

	// ...and, crucially, must not have consumed anything on its way to
	// failing. A partial drain here is the classic check-then-act bug.
	for i := range limit {
		if got := mustAllow(t, l, "k"); !got.Allowed {
			t.Fatalf("request %d denied: the oversized AllowN partially drained the key",
				i+1)
		}
	}
}

func testRejectsNegativeN(t *testing.T, f Factory) {
	l, _ := fixture(t, f, 5, time.Second)

	if _, err := l.AllowN(context.Background(), "k", -1); err == nil {
		t.Error("AllowN(-1) returned nil error, want ErrInvalidN")
	}
}

func testConcurrentNeverOverAdmits(t *testing.T, f Factory) {
	const (
		limit      = 100
		goroutines = 64
		perG       = 20
	)
	l, _ := fixture(t, f, limit, time.Hour)

	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // maximise the collision window
			for range perG {
				d, err := l.Allow(context.Background(), "hot")
				if err != nil {
					t.Errorf("Allow returned unexpected error: %v", err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}

	close(start)
	wg.Wait()

	// The window is an hour and the clock never advances, so no replenishment
	// can occur: the count must be exact, not merely bounded. Run under -race
	// to catch the data races that produce a wrong count here.
	if got := allowed.Load(); got != limit {
		t.Errorf("admitted %d of %d attempts, want exactly %d",
			got, goroutines*perG, limit)
	}
}
