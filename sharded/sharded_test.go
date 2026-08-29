package sharded_test

import (
	"testing"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/sharded"
)

func TestConformance(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := sharded.New(sharded.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("sharded.New: %v", err)
		}
		return l
	})
}

// TestConformanceSingleShard forces every key into one partition, exercising
// the degenerate case sharding is meant to avoid. Correctness must not depend
// on the shard count.
func TestConformanceSingleShard(t *testing.T) {
	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := sharded.New(sharded.Config{
			Limit:  cfg.Limit,
			Window: cfg.Window,
			Shards: 1,
			Clock:  cfg.Clock,
		})
		if err != nil {
			t.Fatalf("sharded.New: %v", err)
		}
		return l
	})
}
