package sharded

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	ratelimittest "github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

func countKeys(l *Limiter) int {
	n := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].buckets)
		l.shards[i].mu.Unlock()
	}
	return n
}

func TestNextPow2(t *testing.T) {
	for in, want := range map[int]int{
		0: 1, 1: 1, 2: 2, 3: 4, 4: 4, 5: 8, 255: 256, 256: 256, 257: 512,
	} {
		if got := nextPow2(in); got != want {
			t.Errorf("nextPow2(%d) = %d, want %d", in, got, want)
		}
	}
}

// TestKeysSpreadAcrossShards guards against a hashing mistake that would make
// sharding pointless by funnelling every key into one partition.
func TestKeysSpreadAcrossShards(t *testing.T) {
	const (
		shards = 16
		keys   = 10_000
	)

	l, err := New(Config{Limit: 1000, Window: time.Second, Shards: shards})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	for i := range keys {
		if _, err := l.Allow(ctx, fmt.Sprintf("tenant-%d", i)); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}

	for i := range l.shards {
		if n := len(l.shards[i].buckets); n == 0 {
			t.Errorf("shard %d holds no keys; hashing is not spreading", i)
		}
	}
}

// TestEvictionPreservesDecisions is the claim this phase rests on: evicting an
// idle key changes nothing observable. Two limiters run an identical request
// sequence, one swept and one not, and every Decision must match.
func TestEvictionPreservesDecisions(t *testing.T) {
	const (
		limit  = 10
		window = time.Second
	)
	epoch := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	clkA := ratelimittest.NewFakeClock(epoch)
	clkB := ratelimittest.NewFakeClock(epoch)

	swept, err := New(Config{Limit: limit, Window: window, Clock: clkA})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	kept, err := New(Config{Limit: limit, Window: window, Clock: clkB})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	keys := []string{"alice", "bob", "carol"}

	step := func(advance time.Duration, runSweep bool) {
		t.Helper()
		clkA.Advance(advance)
		clkB.Advance(advance)
		if runSweep {
			swept.sweep(clkA.Now())
		}
		for _, k := range keys {
			a, err := swept.Allow(ctx, k)
			if err != nil {
				t.Fatalf("swept.Allow(%q): %v", k, err)
			}
			b, err := kept.Allow(ctx, k)
			if err != nil {
				t.Fatalf("kept.Allow(%q): %v", k, err)
			}
			if a != b {
				t.Fatalf("key %q: swept limiter returned %+v, unswept returned %+v", k, a, b)
			}
		}
	}

	// Drain every key.
	for range limit {
		step(0, false)
	}

	// Idle past the recovery threshold, then evict. The swept limiter has
	// forgotten these keys entirely; the other still holds their drained state.
	step(swept.idleTimeout+time.Second, true)
	if n := countKeys(swept); n != len(keys) {
		t.Errorf("after sweep and one request per key, swept holds %d keys, want %d", n, len(keys))
	}

	// Everything from here must still agree.
	for range limit {
		step(0, false)
	}
}

// TestSweepEvictsOnlyIdleKeys guards the other direction: a sweep must not
// discard state that is still live.
func TestSweepEvictsOnlyIdleKeys(t *testing.T) {
	epoch := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clk := ratelimittest.NewFakeClock(epoch)

	l, err := New(Config{Limit: 10, Window: time.Second, Clock: clk})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	if _, err := l.Allow(ctx, "cold"); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	// Move past the threshold, then touch one key so it is freshly active.
	clk.Advance(l.idleTimeout + time.Second)
	if _, err := l.Allow(ctx, "hot"); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	if got := l.sweep(clk.Now()); got != 1 {
		t.Errorf("sweep evicted %d keys, want 1", got)
	}
	if got := countKeys(l); got != 1 {
		t.Errorf("%d keys remain, want 1 (the freshly touched one)", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	l, err := New(Config{Limit: 10, Window: time.Second, SweepInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestCloseStopsSweeper checks the goroutine actually exits.
//
// runtime.NumGoroutine is a blunt instrument — it is process-global and would
// be polluted by concurrently running tests — but it avoids taking a dependency
// on goleak in a repository that currently has none. The settle loop absorbs
// the scheduling delay between close(done) and the janitor returning.
func TestCloseStopsSweeper(t *testing.T) {
	before := runtime.NumGoroutine()

	l, err := New(Config{Limit: 10, Window: time.Second, SweepInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("%d goroutines after Close, started from %d: sweeper leaked", got, before)
	}
}

// BenchmarkSweepScan measures the recurring steady-state cost: a full scan over
// a large key set where nothing is evictable. That is the price paid every
// SweepInterval regardless of how much it actually reclaims.
func BenchmarkSweepScan(b *testing.B) {
	const nkeys = 100_000

	// A one-hour window makes idleTimeout an hour, so no key populated below
	// is anywhere near evictable.
	l, err := New(Config{Limit: 1000, Window: time.Hour, Shards: 64})
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	for i := range nkeys {
		if _, err := l.Allow(ctx, fmt.Sprintf("tenant-%d", i)); err != nil {
			b.Fatalf("Allow: %v", err)
		}
	}

	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := l.sweep(now); got != 0 {
			b.Fatalf("evicted %d keys, want 0", got)
		}
	}
}
