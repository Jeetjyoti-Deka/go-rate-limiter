// Package sharded implements a token bucket limiter whose key map is split
// into independently locked partitions, so requests for keys in different
// shards do not contend.
package sharded

import (
	"context"
	"hash/maphash"
	"io"
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

	// SweepInterval is how often idle keys are evicted. Zero, the default,
	// disables eviction entirely: no goroutine is started, Close need not be
	// called, and the key map grows without bound.
	//
	// A non-zero value starts a background sweeper, and the caller MUST call
	// Close to stop it. Intended for use with the system clock; the sweep
	// schedule uses real time even when Clock is injected.
	SweepInterval time.Duration
}

// shard is one independently locked partition of the key space.
type shard struct {
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
	shards []shard

	// idleTimeout is how long a key may go untouched before eviction. It is
	// not configurable: bucket.IdleRecovery is the smallest value that
	// provably changes no decision, and a larger one would only cost memory
	// for no behavioural gain.
	idleTimeout time.Duration

	done      chan struct{}
	closeOnce sync.Once
}

var _ ratelimit.Limiter = (*Limiter)(nil)
var _ io.Closer = (*Limiter)(nil)

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
		shards: make([]shard, n),
	}

	l.idleTimeout = p.IdleRecovery()
	l.done = make(chan struct{})

	for i := range l.shards {
		l.shards[i].buckets = make(map[string]*bucket.State)
	}

	if cfg.SweepInterval > 0 {
		go l.janitor(cfg.SweepInterval)
	}

	return l, nil
}

// Close stops the background sweeper. It is safe to call more than once, and
// unnecessary when SweepInterval is zero.
//
// Close is deliberately not part of ratelimit.Limiter: most implementations own
// nothing that needs tearing down, and widening the interface would make every
// one of them carry a no-op. Callers that care assert to io.Closer.
func (l *Limiter) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}

// janitor runs sweep on a ticker until Close.
//
// The schedule uses real time rather than the injected Clock, which has no
// ticker by design. Keeping policy and scheduling apart is what lets sweep be
// tested deterministically against a FakeClock while this stays trivial glue.
func (l *Limiter) janitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			l.sweep(l.clock.Now())
		case <-l.done:
			return
		}
	}
}

// sweep evicts every key untouched for at least idleTimeout, returning how many
// were removed.
//
// A bucket idle that long has refilled to capacity, making its state identical
// to a fresh one — so removing it cannot change any future decision. See
// docs/03-concurrency.md.
//
// Shards are locked one at a time, so the pause this imposes on any single
// request is a 1/N slice of the full scan rather than the whole thing.
func (l *Limiter) sweep(now time.Time) int {
	cutoff := now.Add(-l.idleTimeout)
	evicted := 0

	for i := range l.shards {
		sh := &l.shards[i]

		sh.mu.Lock()
		for key, s := range sh.buckets {
			if s.Last.Before(cutoff) {
				delete(sh.buckets, key)
				evicted++
			}
		}
		sh.mu.Unlock()
	}

	return evicted
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

	// Growth here is bounded by the sweeper: see sweep and Config.SweepInterval.
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
func (l *Limiter) shardFor(key string) *shard {
	return &l.shards[maphash.String(l.seed, key)&l.mask]
}

// nextPow2 returns the smallest power of two at or above n.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
