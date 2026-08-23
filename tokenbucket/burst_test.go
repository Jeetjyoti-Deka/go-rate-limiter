package tokenbucket_test

import (
	"testing"
	"time"

	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
)

// TestNoBoundaryBurst is fixedwindow's TestBoundaryBurst, structurally
// unchanged, run against a token bucket. The fixed window admits 199 requests
// in the 1ms span; a token bucket admits its burst and no more, because it has
// no window boundary to straddle.
func TestNoBoundaryBurst(t *testing.T) {
	const (
		limit  = 100
		window = time.Minute
	)

	clk := ratelimittest.NewFakeClock(epoch)
	l, err := tokenbucket.New(tokenbucket.Config{
		Limit:  limit,
		Window: window,
		Clock:  clk,
	})
	if err != nil {
		t.Fatalf("tokenbucket.New: %v", err)
	}

	if !allow(t, l, "k").Allowed {
		t.Fatal("first request denied")
	}

	clk.Advance(window - time.Millisecond)
	first := drain(t, l, "k", limit)

	clk.Advance(time.Millisecond)
	second := drain(t, l, "k", limit)

	burst := first + second
	t.Logf("admitted %d requests within %v, against a limit of %d per %v (%.1fx)",
		burst, time.Millisecond, limit, window, float64(burst)/float64(limit))

	if burst > limit {
		t.Errorf("admitted %d within %v, exceeding the burst capacity of %d",
			burst, time.Millisecond, limit)
	}
}

// TestBurstIsIndependentOfRate exercises the knob a fixed window does not have:
// 60/minute sustained, but never more than 5 at once.
func TestBurstIsIndependentOfRate(t *testing.T) {
	clk := ratelimittest.NewFakeClock(epoch)
	l, err := tokenbucket.New(tokenbucket.Config{
		Limit:  60,
		Window: time.Minute,
		Burst:  5,
		Clock:  clk,
	})
	if err != nil {
		t.Fatalf("tokenbucket.New: %v", err)
	}

	// A fresh bucket holds Burst, not Limit.
	if got := drain(t, l, "k", 60); got != 5 {
		t.Errorf("admitted %d instantaneously, want 5 (the burst)", got)
	}

	// The sustained rate is untouched: one token per second.
	clk.Advance(time.Second)
	if got := drain(t, l, "k", 10); got != 1 {
		t.Errorf("admitted %d after 1s, want 1 (the rate)", got)
	}
}

// TestRetryAfterIsActionable pins the math.Ceil in accrualTime. Waiting exactly
// the advertised RetryAfter must succeed; truncating instead of rounding up
// leaves the bucket a fraction of a token short and this test fails.
func TestRetryAfterIsActionable(t *testing.T) {
	clk := ratelimittest.NewFakeClock(epoch)
	l, err := tokenbucket.New(tokenbucket.Config{
		Limit:  3,
		Window: time.Second,
		Clock:  clk,
	})
	if err != nil {
		t.Fatalf("tokenbucket.New: %v", err)
	}

	drain(t, l, "k", 3)

	d := allow(t, l, "k")
	if d.Allowed {
		t.Fatal("expected denial after draining the bucket")
	}
	if d.RetryAfter <= 0 {
		t.Fatalf("RetryAfter = %v on a denial, want positive", d.RetryAfter)
	}

	clk.Advance(d.RetryAfter)
	if !allow(t, l, "k").Allowed {
		t.Errorf("still denied after waiting the advertised RetryAfter of %v",
			d.RetryAfter)
	}
}
