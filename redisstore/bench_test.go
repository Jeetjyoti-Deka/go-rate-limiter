package redisstore

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
)

// BenchmarkAllow prices coordination: the same algorithm, once in this process
// and once across a network.
//
// The limit is set high enough that every request is admitted, so both measure
// the machinery rather than the rejection path. Both use the same window so the
// emission interval matches.
//
// Loopback Redis is the best case by a wide margin. A real deployment crosses a
// datacentre, and the gap this reports should be read as a floor.
func BenchmarkAllow(b *testing.B) {
	const (
		limit  = 1 << 30
		window = time.Hour
	)
	ctx := context.Background()

	b.Run("InProcessGCRA", func(b *testing.B) {
		l, err := gcra.New(gcra.Config{Limit: limit, Window: window})
		if err != nil {
			b.Fatalf("gcra.New: %v", err)
		}
		runAllow(b, l, ctx)
	})

	b.Run("Redis", func(b *testing.B) {
		l, err := New(Config{
			Client:    testClient(b),
			Limit:     limit,
			Window:    window,
			KeyPrefix: uniquePrefix(b),
		})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		runAllow(b, l, ctx)
	})
}

func runAllow(b *testing.B, l ratelimit.Limiter, ctx context.Context) {
	b.Helper()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := l.Allow(ctx, "k"); err != nil {
			b.Fatalf("Allow: %v", err)
		}
	}
}
