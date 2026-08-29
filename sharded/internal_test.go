package sharded

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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
