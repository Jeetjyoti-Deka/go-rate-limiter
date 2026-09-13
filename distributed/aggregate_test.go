//go:build brokenbydesign

package distributed

import (
	"testing"

	"github.com/Jeetjyoti-Deka/go-rate-limiter/ratelimittest"
)

// TestAggregateLimitHolds asserts what an operator believes they configured:
// that a service with a limit of 100 admits at most 100.
//
// It fails, and is meant to. The build tag keeps it out of normal builds and
// CI; run it deliberately with
//
//	go test -tags brokenbydesign ./distributed/
//
// It will keep failing until Phase 7 completes the distributed limiter, at
// which point it stops being a demonstration and becomes the acceptance test.
func TestAggregateLimitHolds(t *testing.T) {
	for _, a := range algorithms() {
		t.Run(a.name, func(t *testing.T) {
			clk := ratelimittest.NewFakeClock(epoch)
			c := newCluster(t, a, instances, clk)

			admitted := c.admit(t, sharedKey, attempts)

			if admitted > limit {
				t.Errorf("%d instances admitted %d requests against a configured limit of %d: "+
					"the limit is enforced per instance, not per service",
					instances, admitted, limit)
			}
		})
	}
}
