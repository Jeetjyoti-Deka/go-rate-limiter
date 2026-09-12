package gcra_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := gcra.New(gcra.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("gcra.New: %v", err)
		}
		return l
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := map[string]gcra.Config{
		"zero limit":       {Limit: 0, Window: time.Second},
		"zero window":      {Limit: 10, Window: 0},
		"negative burst":   {Limit: 10, Window: time.Second, Burst: -1},
		"sub-nanosecond T": {Limit: 1_000_000_000, Window: time.Millisecond},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := gcra.New(cfg); !errors.Is(err, ratelimit.ErrInvalidConfig) {
				t.Errorf("New(%+v) error = %v, want ErrInvalidConfig", cfg, err)
			}
		})
	}
}

func TestNoBoundaryBurst(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
	)

	clk := ratelimittest.NewFakeClock(epoch)
	l, err := gcra.New(gcra.Config{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("gcra.New: %v", err)
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
		t.Errorf("admitted %d within %v, exceeding the burst of %d", burst, time.Millisecond, limit)
	}
}
