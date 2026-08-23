package fixedwindow_test

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/fixedwindow"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func allow(t *testing.T, l ratelimit.Limiter, key string) ratelimit.Decision {
	t.Helper()
	d, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow(%q): %v", key, err)
	}
	return d
}

// drain issues up to max requests and reports how many were admitted.
func drain(t *testing.T, l ratelimit.Limiter, key string, max int) int {
	t.Helper()
	admitted := 0
	for range max {
		if allow(t, l, key).Allowed {
			admitted++
		}
	}
	return admitted
}

// TestBoundaryBurst demonstrates that a fixed window admits nearly twice its
// configured limit within an arbitrarily short span, provided that span
// contains a window boundary.
//
// This test passes. Nothing here is a bug in the implementation: each window
// independently admitted exactly its limit. The limit is per window, and
// "per window" is not the same guarantee as "per any interval of that length".
func TestBoundaryBurst(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
	)

	clk := ratelimittest.NewFakeClock(epoch)
	l, err := fixedwindow.New(fixedwindow.Config{
		Limit:  limit,
		Window: window,
		Clock:  clk,
	})
	if err != nil {
		t.Fatalf("fixedwindow.New: %v", err)
	}

	// Anchor the window with one request; it now runs for exactly `window`.
	if !allow(t, l, "k").Allowed {
		t.Fatal("first request denied")
	}

	// Wait until 1ms before expiry, then take the rest of the quota. All of
	// it lands inside window 1.
	clk.Advance(window - time.Millisecond)
	first := drain(t, l, "k", limit)
	if want := limit - 1; first != want {
		t.Fatalf("end of window 1: admitted %d, want %d", first, want)
	}

	// Cross the boundary. The counter resets.
	clk.Advance(time.Millisecond)
	second := drain(t, l, "k", limit)
	if second != limit {
		t.Fatalf("start of window 2: admitted %d, want %d", second, limit)
	}

	burst := first + second
	t.Logf("admitted %d requests within %v, against a limit of %d per %v (%.1fx)",
		burst, time.Millisecond, limit, window, float64(burst)/float64(limit))

	if burst < limit*2-1 {
		t.Errorf("burst = %d, expected ~%d", burst, limit*2-1)
	}
}
