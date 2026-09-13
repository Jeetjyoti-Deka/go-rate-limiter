package redisstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNaiveIsCorrectWhenSerial establishes that the arithmetic is right, so the
// failure in TestNaiveOverAdmits can only be about atomicity.
func TestNaiveIsCorrectWhenSerial(t *testing.T) {
	const limit = 10

	l, err := NewNaive(Config{
		Client:    testClient(t),
		Limit:     limit,
		Window:    time.Minute,
		KeyPrefix: uniquePrefix(t),
	})
	if err != nil {
		t.Fatalf("NewNaive: %v", err)
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
		t.Errorf("serial requests admitted %d, want exactly %d", admitted, limit)
	}
}

// TestNaiveOverAdmits fires many requests concurrently at one fresh key and
// watches the limit fail to hold.
//
// This is a race demonstration, so it is probabilistic by nature — it passes
// because the race reliably occurs, not because it must. With 100 concurrent
// requests each separated from its write by a full network round trip, nearly
// all of them read the same empty state and all decide they may proceed.
//
// If this ever stops over-admitting, the interesting question is what changed:
// a genuinely serialised client, or a limit large enough to hide it.
func TestNaiveOverAdmits(t *testing.T) {
	const (
		limit      = 10
		goroutines = 100
	)

	l, err := NewNaive(Config{
		Client:    testClient(t),
		Limit:     limit,
		Window:    time.Minute,
		KeyPrefix: uniquePrefix(t),
	})
	if err != nil {
		t.Fatalf("NewNaive: %v", err)
	}

	ctx := context.Background()
	var admitted atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release everyone into the same round trip window
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

	got := admitted.Load()
	t.Logf("%d concurrent requests against a limit of %d: admitted %d (%.1fx the limit)",
		goroutines, limit, got, float64(got)/float64(limit))

	if got <= limit {
		t.Errorf("admitted %d, expected the read-modify-write race to admit more than %d",
			got, limit)
	}
}
