package perkey_test

import (
	"testing"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/perkey"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := perkey.New(perkey.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("perkey.New: %v", err)
		}
		return l
	})
}
