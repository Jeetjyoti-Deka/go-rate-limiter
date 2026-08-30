package windowcounter_test

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowcounter"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := windowcounter.New(windowcounter.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("windowcounter.New: %v", err)
		}
		return l
	})
}

// TestNoBoundaryBurst is fixedwindow's TestBoundaryBurst, structurally
// unchanged. The fixed window admits 199 in the 1ms span; a sliding window
// admits at most its limit, because there is no boundary to straddle.
func TestNoBoundaryBurst(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
	)

	clk := ratelimittest.NewFakeClock(epoch)
	l, err := windowcounter.New(windowcounter.Config{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("windowcounter.New: %v", err)
	}

	ctx := context.Background()
	allow := func() bool {
		t.Helper()
		d, err := l.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		return d.Allowed
	}
	drain := func(max int) int {
		t.Helper()
		n := 0
		for range max {
			if allow() {
				n++
			}
		}
		return n
	}

	if !allow() {
		t.Fatal("first request denied")
	}

	clk.Advance(window - time.Millisecond)
	first := drain(limit)

	clk.Advance(time.Millisecond)
	second := drain(limit)

	burst := first + second
	t.Logf("admitted %d requests within %v, against a limit of %d per %v (%.1fx)",
		burst, time.Millisecond, limit, window, float64(burst)/float64(limit))

	if burst > limit {
		t.Errorf("admitted %d within %v, exceeding the limit of %d", burst, time.Millisecond, limit)
	}
}
