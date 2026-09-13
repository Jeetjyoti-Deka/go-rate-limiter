// Package redisstore implements rate limiters whose state lives in Redis, so
// that every instance of a horizontally scaled service enforces one shared
// limit rather than its own private copy.
//
// See docs/06-redis-atomicity.md.
package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// Config configures a Redis-backed limiter.
type Config struct {
	// Client is the Redis client. Required.
	Client redis.UniversalClient

	// Limit units per Window is the sustained rate. Required.
	Limit int

	// Window is the period over which Limit applies. Required.
	Window time.Duration

	// Burst is the largest instantaneous spike admitted. Defaults to Limit.
	Burst int

	// KeyPrefix namespaces this limiter's keys. Required: a Redis instance is
	// usually shared, and an unprefixed key called "alice" is an accident
	// waiting to happen.
	KeyPrefix string

	// Clock overrides the server-side clock when set.
	//
	// Leave nil in production. Reading the time from Redis is precisely what
	// removes instance clock skew from the arithmetic — see
	// docs/06-redis-atomicity.md. Tests set it so the shared conformance suite,
	// which requires an injectable clock, can run against this limiter.
	Clock ratelimit.Clock
}

// params is the validated, derived form of a Config.
//
// All times are microseconds: Redis TIME has microsecond resolution, and mixing
// units across the Go/Lua boundary is a good way to be wrong by a factor of a
// thousand.
type params struct {
	client    redis.UniversalClient
	clock     ratelimit.Clock
	prefix    string
	limit     int
	burst     int
	emission  int64 // T, microseconds per unit
	tolerance int64 // burst * T
	ttl       time.Duration
}

func newParams(cfg Config) (params, error) {
	if cfg.Client == nil {
		return params{}, fmt.Errorf("%w: nil Client", ratelimit.ErrInvalidConfig)
	}
	if cfg.KeyPrefix == "" {
		return params{}, fmt.Errorf("%w: empty KeyPrefix", ratelimit.ErrInvalidConfig)
	}
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		return params{}, fmt.Errorf("%w: limit=%d window=%v",
			ratelimit.ErrInvalidConfig, cfg.Limit, cfg.Window)
	}

	burst := cfg.Burst
	if burst < 0 {
		return params{}, fmt.Errorf("%w: burst=%d", ratelimit.ErrInvalidConfig, burst)
	}
	if burst == 0 {
		burst = cfg.Limit
	}

	emission := (cfg.Window / time.Duration(cfg.Limit)).Microseconds()
	if emission < 1 {
		return params{}, fmt.Errorf(
			"%w: window %v over limit %d is under 1µs per unit, and Redis TIME has "+
				"microsecond resolution", ratelimit.ErrInvalidConfig, cfg.Window, cfg.Limit)
	}

	tolerance := emission * int64(burst)

	return params{
		client:    cfg.Client,
		clock:     cfg.Clock,
		prefix:    cfg.KeyPrefix,
		limit:     cfg.Limit,
		burst:     burst,
		emission:  emission,
		tolerance: tolerance,
		// An admitted request always leaves newTat-now <= tolerance, so once
		// tolerance elapses the TAT is in the past and the key is equivalent
		// to absent. Expiring it changes no future decision — the Phase 3b
		// argument, enforced by the server instead of a sweeper.
		ttl: time.Duration(tolerance) * time.Microsecond,
	}, nil
}

func (p params) redisKey(key string) string {
	return p.prefix + ":" + key
}

// now returns the current time in microseconds, from Redis unless a Clock was
// injected.
func (p params) now(ctx context.Context) (int64, error) {
	if p.clock != nil {
		return p.clock.Now().UnixMicro(), nil
	}
	t, err := p.client.Time(ctx).Result()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ratelimit.ErrBackendUnavailable, err)
	}
	return t.UnixMicro(), nil
}

// remaining reports how many further units could be admitted at now, given tat.
func (p params) remaining(tat, now int64) int {
	avail := (now + p.tolerance - tat) / p.emission
	if avail < 0 {
		return 0
	}
	if avail > int64(p.burst) {
		return p.burst
	}
	return int(avail)
}
