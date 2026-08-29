// Package perkey implements a token bucket limiter that gives every key its own
// mutex, so requests contend only when they are for the same key.
package perkey

import (
	"context"
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
	Clock  ratelimit.Clock
}

// entry is one key's bucket together with the lock guarding it.
type entry struct {
	mu    sync.Mutex
	state bucket.State
}

// Limiter is a token bucket limiter with per-key locking, safe for concurrent
// use.
//
// sync.Map fits because the mutations here are to the values — token levels —
// not to the key set. The map itself is read-mostly after warm-up, which is the
// access pattern sync.Map is built for; a map guarded by an RWMutex would take
// a write lock it does not need.
type Limiter struct {
	params bucket.Params
	clock  ratelimit.Clock

	// entries is a map[string]*entry.
	entries sync.Map
}

var _ ratelimit.Limiter = (*Limiter)(nil)

// New returns a Limiter, or ErrInvalidConfig if the configuration is invalid.
func New(cfg Config) (*Limiter, error) {
	p, err := bucket.NewParams(cfg.Limit, cfg.Window, cfg.Burst)
	if err != nil {
		return nil, err
	}

	return &Limiter{
		params: p,
		clock:  bucket.ClockOr(cfg.Clock),
	}, nil
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

	e := l.entryFor(key)

	e.mu.Lock()
	defer e.mu.Unlock()

	return l.params.TryTake(&e.state, l.clock.Now(), n), nil
}

// entryFor returns key's entry, creating it if absent.
func (l *Limiter) entryFor(key string) *entry {
	if v, ok := l.entries.Load(key); ok {
		return v.(*entry)
	}

	// Fully initialise before publishing: a partially constructed entry must
	// never be observable by a concurrent Load. Losing the LoadOrStore race
	// wastes this allocation, which is cheaper than the alternative of
	// initialising after the store and racing a reader.
	fresh := &entry{state: l.params.New(l.clock.Now())}
	v, _ := l.entries.LoadOrStore(key, fresh)
	return v.(*entry)
}
