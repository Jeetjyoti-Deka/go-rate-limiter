// Package sharded implements a token bucket limiter whose key map is split
// into independently locked partitions, so requests for keys in different
// shards do not contend.
package sharded

import (
	"context"
	"hash/maphash"
	"math/bits"
	"runtime"
	"sync"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/internal/bucket"
)

// Config configures a Limiter.
type Config struct {
	Limit  int
	Window time.Duration
	Burst  int

	// Shards is the number of independently locked partitions, rounded up to
	// a power of two so shard selection is a mask rather than a division.
	// Defaults to the next power of two at or above 4*GOMAXPROCS.
	Shards int

	Clock ratelimit.Clock
}

// shard is one independently locked partition of the key space.
type Shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket.State

	// Padding keeps each shard on its own cache line. A mutex plus a map
	// header is 16 bytes, so without this four shards share a 64-byte line
	// and taking one shard's lock invalidates the others in every other
	// core's cache — false sharing between locks that are logically
	// independent and physically adjacent.
	_ [48]byte
}

// Limiter is a sharded token bucket limiter, safe for concurrent use.
type Limiter struct {
	params bucket.Params
	clock  ratelimit.Clock
	seed   maphash.Seed
	mask   uint64
	shards []Shard
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if the configuration is invalid
func New(cfg Config) (*Limiter, error) {
	p, err := bucket.NewParams(cfg.Limit, cfg.Window, cfg.Burst)
	if err != nil {
		return nil, err
	}

	n := cfg.Shards
	if n <= 0 {
		n = 4 * runtime.GOMAXPROCS(0)
	}

	n = nextPow2(n)
	l := &Limiter{
		params: p,
		clock:  bucket.ClockOr(cfg.Clock),
		seed:   maphash.MakeSeed(),
		mask:   uint64(n - 1),
		shards: make([]Shard, n),
	}
	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket.State)
	}
	return l, nil
}

// Allow implements ratelimit.Limiter.
func (l *Limiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return l.AllowN(ctx, key, 1)
}

// AllowN implements ratelimit.Limiter.
func (l *Limiter) AllowN(_ context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	sh := l.shardFor(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	// The Phase 1 clock-under-lock invariant now holds per shard rather than
	// globally. That is sufficient: it only ever needed to cover accesses to
	// the same state.
	now := l.clock.Now()

	// TODO(phase-3b): unbounded map growth.
	s, ok := sh.buckets[key]
	if !ok {
		st := l.params.New(now)
		s = &st
		sh.buckets[key] = s
	}

	return l.params.TryTake(s, now, n), nil
}

// shardFor selects the partition owning key.
//
// maphash is the runtime's own string hasher: faster than a hand-rolled FNV
// loop and allocation-free. The seed is fixed per Limiter, so assignment is
// stable within a process.
func (l *Limiter) shardFor(key string) *Shard {
	return &l.shards[maphash.String(l.seed, key)&l.mask]
}

// nextPow2 returns the smallest power of two at or above n.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
