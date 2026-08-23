package fixedwindow_test

import (
	"errors"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/fixedwindow"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := fixedwindow.New(fixedwindow.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})

		if err != nil {
			t.Fatalf("fixedwindow.New: %v", err)
		}
		return l
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := map[string]fixedwindow.Config{
		"zero limit":      {Limit: 0, Window: time.Second},
		"negative limit":  {Limit: -1, Window: time.Second},
		"zero window":     {Limit: 10, Window: 0},
		"negative window": {Limit: 10, Window: -time.Second},
	}

	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := fixedwindow.New(cfg); !errors.Is(err, ratelimit.ErrInvalidConfig) {
				t.Errorf("New(%+v) error = %v, want ErrInvalidConfig", cfg, err)
			}
		})
	}
}

func TestNewDefaultsToSystemClock(t *testing.T) {
	l, err := fixedwindow.New(fixedwindow.Config{Limit: 1, Window: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d := allow(t, l, "k"); !d.Allowed {
		t.Error("first request denied with a defaulted clock")
	}
}
