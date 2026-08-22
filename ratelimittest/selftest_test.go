package ratelimittest

import (
	"context"
	"sync"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
)

// stubLimiter is a deliberately unsophisticated fixed-window limiter that
// exists only to prove the conformance suite is self-consistent. The real
// implementations live in their own packages; this one never ships.
type stubLimiter struct {
	cfg Config

	mu      sync.Mutex
	counts  map[string]int
	started map[string]time.Time
}

func newStub(cfg Config) *stubLimiter {
	return &stubLimiter{
		cfg:     cfg,
		counts:  make(map[string]int),
		started: make(map[string]time.Time),
	}
}

func (s *stubLimiter) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *stubLimiter) AllowN(_ context.Context, key string, n int) (ratelimit.Decision, error) {
	if n < 0 {
		return ratelimit.Decision{}, ratelimit.ErrInvalidN
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.cfg.Clock.Now()
	start, ok := s.started[key]
	if !ok || now.Sub(start) >= s.cfg.Window {
		start = now
		s.started[key] = start
		s.counts[key] = 0
	}

	elapsed := now.Sub(start)
	reset := s.cfg.Window - elapsed

	used := s.counts[key]
	d := ratelimit.Decision{
		Limit:      s.cfg.Limit,
		Remaining:  s.cfg.Limit - used,
		ResetAfter: reset,
	}

	if used+n <= s.cfg.Limit {
		s.counts[key] = used + n
		d.Allowed = true
		d.Remaining = s.cfg.Limit - (used + n)
		return d, nil
	}
	d.RetryAfter = reset
	return d, nil
}

func TestStubPassesConformance(t *testing.T) {
	Run(t, func(t *testing.T, cfg Config) ratelimit.Limiter {
		return newStub(cfg)
	})
}
