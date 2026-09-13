package distributed

import (
	"testing"

	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

// TestInstancesOverAdmit demonstrates the defect this phase exists for: N
// independent instances admit N times the configured limit, because each one
// enforces its limit against only the requests it happened to see.
//
// It passes. The assertion encodes the broken behaviour deliberately, which
// sounds backwards until you notice what it buys: once Phase 7 lands, a
// distributed limiter that silently falls back to per-instance state will make
// this test keep passing when it should have started failing. It is the
// tripwire for a regression that would otherwise be invisible.
func TestInstancesOverAdmit(t *testing.T) {
	for _, a := range algorithms() {
		t.Run(a.name, func(t *testing.T) {
			clk := ratelimittest.NewFakeClock(epoch)
			c := newCluster(t, a, instances, clk)

			admitted := c.admit(t, sharedKey, attempts)

			t.Logf("limit %d/%v across %d instances: admitted %d of %d attempts (%.1fx the limit)",
				limit, window, instances, admitted, attempts,
				float64(admitted)/float64(limit))

			if want := instances * limit; admitted != want {
				t.Errorf("admitted %d, want %d (= %d instances x %d)",
					admitted, want, instances, limit)
			}
		})
	}
}
