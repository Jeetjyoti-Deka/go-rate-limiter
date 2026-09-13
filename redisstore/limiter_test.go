package redisstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

// TestConformance holds the distributed limiter to the same suite as the five
// in-process ones.
//
// It needs an injected clock, which is exactly what the production path avoids
// — so the tested path differs from the production path in where `now` comes
// from, and only there. See docs/06-redis-atomicity.md on why that gap is
// preferable to exempting this implementation from the suite.
func TestConformance(t *testing.T) {
	client := testClient(t)

	ratelimittest.Run(t, func(t *testing.T, cfg ratelimittest.Config) ratelimit.Limiter {
		l, err := New(Config{
			Client:    client,
			Limit:     cfg.Limit,
			Window:    cfg.Window,
			KeyPrefix: uniquePrefix(t),
			Clock:     cfg.Clock,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return l
	})
}

// TestAtomicHoldsUnderConcurrency is TestNaiveOverAdmits with the only
// difference being atomicity. Same load, same limit, same arithmetic: the naive
// limiter admits all 100, this one admits exactly 10.
func TestAtomicHoldsUnderConcurrency(t *testing.T) {
	const (
		limit      = 10
		goroutines = 100
	)

	l, err := New(Config{
		Client:    testClient(t),
		Limit:     limit,
		Window:    time.Minute,
		KeyPrefix: uniquePrefix(t),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	var admitted atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, err := l.Allow(ctx, "k")
			if err != nil {
				t.Errorf("Allow: %v", err)
				return
			}
			if d.Allowed {
				admitted.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := admitted.Load(); got != limit {
		t.Errorf("%d concurrent requests admitted %d, want exactly %d",
			goroutines, got, limit)
	}
}

// TestSharedAcrossInstances is the answer to Phase 5.
//
// Three independently constructed limiters stand in for three service
// instances. They share nothing in-process — separate structs, separate
// clients' worth of state — and are pointed at the same Redis keyspace. The
// aggregate limit holds, which is precisely what distributed/aggregate_test.go
// asserts and the in-process limiters cannot satisfy.
func TestSharedAcrossInstances(t *testing.T) {
	const (
		instances = 3
		limit     = 100
		attempts  = 1000
	)

	client := testClient(t)
	prefix := uniquePrefix(t)

	limiters := make([]*Limiter, instances)
	for i := range limiters {
		l, err := New(Config{
			Client:    client,
			Limit:     limit,
			Window:    time.Minute,
			KeyPrefix: prefix, // shared: this is what makes them one service
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		limiters[i] = l
	}

	ctx := context.Background()
	admitted := 0
	for i := range attempts {
		d, err := limiters[i%instances].Allow(ctx, "shared-tenant")
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if d.Allowed {
			admitted++
		}
	}

	t.Logf("%d instances, limit %d: admitted %d of %d attempts (%.1fx the limit)",
		instances, limit, admitted, attempts, float64(admitted)/float64(limit))

	if admitted != limit {
		t.Errorf("admitted %d across %d instances, want exactly %d", admitted, instances, limit)
	}
}

// TestKeyExpires checks that the server evicts idle keys on its own, which is
// the Phase 3b sweeper replaced by a TTL on the write that was happening anyway.
func TestKeyExpires(t *testing.T) {
	const limit = 10
	window := time.Minute

	client := testClient(t)
	prefix := uniquePrefix(t)

	l, err := New(Config{
		Client: client, Limit: limit, Window: window, KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	if d, err := l.Allow(ctx, "k"); err != nil || !d.Allowed {
		t.Fatalf("Allow: allowed=%v err=%v", d.Allowed, err)
	}

	ttl, err := client.PTTL(ctx, prefix+":k").Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("PTTL = %v; the key has no expiry and will leak", ttl)
	}
	// The invariant is that the key cannot outlive its recovery time: once
	// tolerance elapses the TAT is in the past and the key is equivalent to
	// absent. window is not the bound — with Burst > Limit, tolerance
	// legitimately exceeds it.
	if maxTTL := time.Duration(l.p.tolerance) * time.Microsecond; ttl > maxTTL {
		t.Errorf("PTTL = %v exceeds the recovery time %v", ttl, maxTTL)
	}
}

// TestUsesServerClock exercises the production path, where no clock is injected
// and the script reads Redis TIME.
func TestUsesServerClock(t *testing.T) {
	const limit = 5

	l, err := New(Config{
		Client:    testClient(t),
		Limit:     limit,
		Window:    time.Minute,
		KeyPrefix: uniquePrefix(t),
		// Clock deliberately nil.
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	admitted := 0
	for range limit * 3 {
		d, err := l.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if d.Allowed {
			admitted++
		}
	}

	if admitted != limit {
		t.Errorf("admitted %d against a server-clock limiter, want %d", admitted, limit)
	}
}
