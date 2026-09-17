package leased_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/leased"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/redisstore"
)

// BenchmarkLeaseSize prices the dial: what one lease size costs in latency and
// what it costs in Redis traffic.
//
// The limit is set far above any achievable iteration count so nothing is
// denied — this measures the admit path, where the amortisation happens.
func BenchmarkLeaseSize(b *testing.B) {
	const (
		limit  = 1 << 24
		window = time.Hour
	)
	ctx := context.Background()

	newRemote := func(b *testing.B) ratelimit.Limiter {
		b.Helper()
		l, err := redisstore.New(redisstore.Config{
			Client:    redisClient(b),
			Limit:     limit,
			Window:    window,
			KeyPrefix: fmt.Sprintf("rlbench:%d", time.Now().UnixNano()),
		})
		if err != nil {
			b.Fatalf("redisstore.New: %v", err)
		}
		return l
	}

	b.Run("RedisDirect", func(b *testing.B) {
		l := newRemote(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := l.Allow(ctx, "k"); err != nil {
				b.Fatalf("Allow: %v", err)
			}
		}
	})

	for _, size := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("Leased/%d", size), func(b *testing.B) {
			l, err := leased.New(leased.Config{
				Remote: newRemote(b), Limit: limit, Window: window,
				LeaseSize: size,
			})
			if err != nil {
				b.Fatalf("leased.New: %v", err)
			}

			b.ReportAllocs()
			for b.Loop() {
				if _, err := l.Allow(ctx, "k"); err != nil {
					b.Fatalf("Allow: %v", err)
				}
			}
			b.StopTimer()

			st := l.Stats()
			b.ReportMetric(float64(st.RemoteCalls)/float64(st.Requests), "redis/op")
		})
	}
}
