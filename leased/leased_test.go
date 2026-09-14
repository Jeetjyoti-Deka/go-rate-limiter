package leased_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/leased"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// stubRemote stands in for the shared limiter so the leasing logic can be
// tested without Redis, and so failure can be injected on demand.
type stubRemote struct {
	inner ratelimit.Limiter

	mu   sync.Mutex
	fail error
}

func (s *stubRemote) Allow(ctx context.Context, key string) (ratelimit.Decision, error) {
	return s.AllowN(ctx, key, 1)
}

func (s *stubRemote) AllowN(ctx context.Context, key string, n int) (ratelimit.Decision, error) {
	s.mu.Lock()
	failing := s.fail
	s.mu.Unlock()

	if failing != nil {
		return ratelimit.Decision{}, failing
	}
	return s.inner.AllowN(ctx, key, n)
}

func (s *stubRemote) setFail(err error) {
	s.mu.Lock()
	s.fail = err
	s.mu.Unlock()
}

func newStub(t testing.TB, limit int, window time.Duration, clk ratelimit.Clock) *stubRemote {
	t.Helper()
	inner, err := gcra.New(gcra.Config{Limit: limit, Window: window, Clock: clk})
	if err != nil {
		t.Fatalf("gcra.New: %v", err)
	}
	return &stubRemote{inner: inner}
}

// TestLeaseReducesRemoteCalls is the phase's central claim, as a number.
func TestLeaseReducesRemoteCalls(t *testing.T) {
	const (
		limit    = 1000
		requests = 500
	)

	for _, size := range []int{1, 10, 100} {
		t.Run(fmt.Sprintf("leaseSize=%d", size), func(t *testing.T) {
			clk := ratelimittest.NewFakeClock(epoch)
			remote := newStub(t, limit, time.Hour, clk)

			l, err := leased.New(leased.Config{
				Remote: remote, Limit: limit, Window: time.Hour,
				LeaseSize: size, Clock: clk,
			})
			if err != nil {
				t.Fatalf("leased.New: %v", err)
			}

			ctx := context.Background()
			for range requests {
				if d, err := l.Allow(ctx, "k"); err != nil || !d.Allowed {
					t.Fatalf("Allow: allowed=%v err=%v", d.Allowed, err)
				}
			}

			s := l.Stats()
			want := int64(requests / size)
			t.Logf("lease %d: %d requests, %d remote calls (%.3f per request)",
				size, s.Requests, s.RemoteCalls, float64(s.RemoteCalls)/float64(s.Requests))

			if s.RemoteCalls != want {
				t.Errorf("RemoteCalls = %d, want %d", s.RemoteCalls, want)
			}
		})
	}
}

// TestDenialIsCached checks that overload makes the limiter quieter, not
// louder. Without the cache, a fully rejected workload costs a round trip per
// request — the optimisation failing exactly when it is needed.
func TestDenialIsCached(t *testing.T) {
	const limit = 10

	clk := ratelimittest.NewFakeClock(epoch)
	remote := newStub(t, limit, time.Hour, clk)

	l, err := leased.New(leased.Config{
		Remote: remote, Limit: limit, Window: time.Hour,
		LeaseSize: 5, Clock: clk,
	})
	if err != nil {
		t.Fatalf("leased.New: %v", err)
	}

	ctx := context.Background()
	for range limit {
		if d, _ := l.Allow(ctx, "k"); !d.Allowed {
			t.Fatal("denied during drain")
		}
	}

	// First refusal: a full-size attempt, then the tail fallback.
	if d, _ := l.Allow(ctx, "k"); d.Allowed {
		t.Fatal("admitted past the limit")
	}
	after := l.Stats().RemoteCalls

	for range 99 {
		if d, _ := l.Allow(ctx, "k"); d.Allowed {
			t.Fatal("admitted past the limit")
		}
	}

	if got := l.Stats().RemoteCalls; got != after {
		t.Errorf("99 cached denials made %d remote calls, want 0", got-after)
	}
}

func TestUnspentLeaseExpires(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
		size   = 10
	)

	clk := ratelimittest.NewFakeClock(epoch)
	remote := newStub(t, limit, window, clk)

	l, err := leased.New(leased.Config{
		Remote: remote, Limit: limit, Window: window,
		LeaseSize: size, Clock: clk,
	})
	if err != nil {
		t.Fatalf("leased.New: %v", err)
	}

	ctx := context.Background()
	if d, _ := l.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("first request denied")
	}
	if got := l.Stats().RemoteCalls; got != 1 {
		t.Fatalf("RemoteCalls = %d, want 1", got)
	}

	// The lease holds 9 unspent units, valid for size * window/limit.
	clk.Advance(window/limit*size + time.Second)

	if d, _ := l.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("request after expiry denied")
	}
	if got := l.Stats().RemoteCalls; got != 2 {
		t.Errorf("RemoteCalls = %d after lease expiry, want 2: unspent quota did not rot", got)
	}
}

func TestDegradation(t *testing.T) {
	unreachable := fmt.Errorf("%w: dialing redis", ratelimit.ErrBackendUnavailable)

	cases := []struct {
		name string
		mode leased.FailureMode
		want bool
	}{
		{"FailOpen", leased.FailOpen, true},
		{"FailClosed", leased.FailClosed, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := ratelimittest.NewFakeClock(epoch)
			remote := newStub(t, 100, time.Minute, clk)

			var observed error
			l, err := leased.New(leased.Config{
				Remote: remote, Limit: 100, Window: time.Minute,
				LeaseSize:       1,
				OnRemoteFailure: tc.mode,
				OnDegraded:      func(err error) { observed = err },
				Clock:           clk,
			})
			if err != nil {
				t.Fatalf("leased.New: %v", err)
			}

			remote.setFail(unreachable)

			d, err := l.Allow(context.Background(), "k")
			if err != nil {
				t.Fatalf("Allow returned %v; degradation must apply the policy, not propagate", err)
			}
			if d.Allowed != tc.want {
				t.Errorf("Allowed = %v, want %v", d.Allowed, tc.want)
			}
			if !errors.Is(observed, ratelimit.ErrBackendUnavailable) {
				t.Errorf("OnDegraded saw %v, want an ErrBackendUnavailable", observed)
			}
			if got := l.Stats().Degraded; got != 1 {
				t.Errorf("Stats().Degraded = %d, want 1", got)
			}
			if !tc.want && d.RetryAfter <= 0 {
				t.Error("fail-closed denial must advertise a positive RetryAfter, or clients hot-loop")
			}
		})
	}
}
