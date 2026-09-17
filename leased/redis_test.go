package leased_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Jeetjyoti-Deka/go-rate-limiter/leased"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/redisstore"
)

func redisClient(tb testing.TB) *redis.Client {
	tb.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		tb.Skipf("redis unavailable at %s (%v)\n"+
			"start one with: docker run -d --name ratelimit-redis -p 6379:6379 redis:7-alpine",
			addr, err)
	}
	tb.Cleanup(func() { c.Close() })
	return c
}

// fleet builds n instances, each a leased limiter over its own redisstore
// limiter. They share only the Redis keyspace, which is what makes them one
// service rather than n services.
func fleet(tb testing.TB, n, limit, leaseSize int, window time.Duration) ([]*leased.Limiter, string) {
	tb.Helper()

	client := redisClient(tb)
	prefix := fmt.Sprintf("rltest:%s:%d", tb.Name(), time.Now().UnixNano())

	out := make([]*leased.Limiter, n)
	for i := range out {
		remote, err := redisstore.New(redisstore.Config{
			Client:    client,
			Limit:     limit,
			Window:    window,
			KeyPrefix: prefix,
		})
		if err != nil {
			tb.Fatalf("redisstore.New: %v", err)
		}
		l, err := leased.New(leased.Config{
			Remote:    remote,
			Limit:     limit,
			Window:    window,
			LeaseSize: leaseSize,
		})
		if err != nil {
			tb.Fatalf("leased.New: %v", err)
		}
		out[i] = l
	}
	return out, prefix
}

func totalRemoteCalls(fleet []*leased.Limiter) int64 {
	var n int64
	for _, l := range fleet {
		n += l.Stats().RemoteCalls
	}
	return n
}

// TestFleetAccuracyUniform is the flattering case: traffic spread evenly, so
// every leased block gets spent and nothing is stranded.
//
// Leasing is *exact* here. Instances can only spend what the shared limiter
// granted, and under even load they spend all of it.
func TestFleetAccuracyUniform(t *testing.T) {
	const (
		instances = 3
		limit     = 100
		attempts  = 1000
	)

	for _, leaseSize := range []int{1, 5, 20} {
		t.Run(fmt.Sprintf("leaseSize=%d", leaseSize), func(t *testing.T) {
			fl, _ := fleet(t, instances, limit, leaseSize, time.Minute)
			ctx := context.Background()

			admitted := 0
			for i := range attempts {
				d, err := fl[i%instances].Allow(ctx, "shared-tenant")
				if err != nil {
					t.Fatalf("Allow: %v", err)
				}
				if d.Allowed {
					admitted++
				}
			}

			calls := totalRemoteCalls(fl)
			t.Logf("lease %d: admitted %d/%d, %d redis calls (%.3f per request)",
				leaseSize, admitted, limit, calls, float64(calls)/float64(attempts))

			if admitted != limit {
				t.Errorf("admitted %d, want exactly %d under uniform load", admitted, limit)
			}
		})
	}
}

// TestFleetAccuracySkewed is where the bound actually binds.
//
// Two instances see a handful of requests each and then go quiet, each holding
// most of a lease it will never spend. That stranded quota is the price of
// leasing, and it is capped at instances * leaseSize.
func TestFleetAccuracySkewed(t *testing.T) {
	const (
		instances = 3
		limit     = 100
		quiet     = 5 // requests each of the two quiet instances receives
	)

	for _, leaseSize := range []int{1, 5, 20} {
		t.Run(fmt.Sprintf("leaseSize=%d", leaseSize), func(t *testing.T) {
			fl, _ := fleet(t, instances, limit, leaseSize, time.Minute)
			ctx := context.Background()

			// A fixed plan, so the result is reproducible rather than a
			// function of scheduling.
			plan := make([]int, 0, 1000)
			for range quiet {
				plan = append(plan, 1, 2)
			}
			for len(plan) < 1000 {
				plan = append(plan, 0)
			}

			admitted := 0
			for _, inst := range plan {
				d, err := fl[inst].Allow(ctx, "shared-tenant")
				if err != nil {
					t.Fatalf("Allow: %v", err)
				}
				if d.Allowed {
					admitted++
				}
			}

			floor := limit - instances*leaseSize
			t.Logf("lease %d: admitted %d/%d, shortfall %d, bound %d",
				leaseSize, admitted, limit, limit-admitted, instances*leaseSize)

			if admitted > limit {
				t.Errorf("admitted %d, exceeding the limit %d", admitted, limit)
			}
			if admitted < floor {
				t.Errorf("admitted %d, below the bound limit-instances*leaseSize = %d",
					admitted, floor)
			}
		})
	}
}
