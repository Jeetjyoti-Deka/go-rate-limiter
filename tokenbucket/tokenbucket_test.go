package tokenbucket_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
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

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := tokenbucket.New(tokenbucket.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("tokenbucket.New: %v", err)
		}
		return l
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := map[string]tokenbucket.Config{
		"zero limit":      {Limit: 0, Window: time.Second},
		"negative limit":  {Limit: -1, Window: time.Second},
		"zero window":     {Limit: 10, Window: 0},
		"negative window": {Limit: 10, Window: -time.Second},
		"negative burst":  {Limit: 10, Window: time.Second, Burst: -1},
	}

	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := tokenbucket.New(cfg); !errors.Is(err, ratelimit.ErrInvalidConfig) {
				t.Errorf("New(%+v) error = %v, want ErrInvalidConfig", cfg, err)
			}
		})
	}
}
