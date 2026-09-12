package gcra

import (
	"context"
	"testing"
	"time"

	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

// TestDenialPerformsNoWrite pins the property that makes GCRA attractive under
// overload: a rejected request loads the TAT and returns without storing
// anything, so a limiter rejecting everything does no writes and no CAS
// retries at all.
func TestDenialPerformsNoWrite(t *testing.T) {
	clk := ratelimittest.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	l, err := New(Config{Limit: 5, Window: time.Second, Clock: clk})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	for range 5 {
		if d, err := l.Allow(ctx, "k"); err != nil || !d.Allowed {
			t.Fatalf("Allow during drain: allowed=%v err=%v", d.Allowed, err)
		}
	}

	before := l.slotFor("k").Load()

	for range 100 {
		d, err := l.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("Allow: %v", err)
		}
		if d.Allowed {
			t.Fatal("request admitted past the burst")
		}
	}

	if after := l.slotFor("k").Load(); after != before {
		t.Errorf("TAT moved from %d to %d across 100 denials; denials must not write", before, after)
	}
}
