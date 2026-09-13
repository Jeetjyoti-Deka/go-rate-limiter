package distributed

import (
	"context"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/fixedwindow"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowcounter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowlog"
)

const (
	instances = 3
	limit     = 100
	window    = time.Second
	attempts  = 1000
	sharedKey = "shared-tenant"
)

var epoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// algorithm is one limiter constructor.
type algorithm struct {
	name string
	new  func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter
}

// algorithms returns every limiter in the repository.
//
// All five are run because the defect belongs to none of them: an algorithm
// decides what to remember, not where it lives. Running one would invite the
// reading that a different one might survive.
func algorithms() []algorithm {
	return []algorithm{
		{"fixedwindow", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			l, err := fixedwindow.New(fixedwindow.Config{Limit: limit, Window: window, Clock: clk})
			if err != nil {
				t.Fatalf("fixedwindow.New: %v", err)
			}
			return l
		}},
		{"tokenbucket", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			l, err := tokenbucket.New(tokenbucket.Config{Limit: limit, Window: window, Clock: clk})
			if err != nil {
				t.Fatalf("tokenbucket.New: %v", err)
			}
			return l
		}},
		{"windowlog", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			l, err := windowlog.New(windowlog.Config{Limit: limit, Window: window, Clock: clk})
			if err != nil {
				t.Fatalf("windowlog.New: %v", err)
			}
			return l
		}},
		{"windowcounter", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			l, err := windowcounter.New(windowcounter.Config{Limit: limit, Window: window, Clock: clk})
			if err != nil {
				t.Fatalf("windowcounter.New: %v", err)
			}
			return l
		}},
		{"gcra", func(t *testing.T, clk ratelimit.Clock) ratelimit.Limiter {
			l, err := gcra.New(gcra.Config{Limit: limit, Window: window, Clock: clk})
			if err != nil {
				t.Fatalf("gcra.New: %v", err)
			}
			return l
		}},
	}
}

// cluster is N service instances behind a round-robin load balancer. Each runs
// its own limiter over its own state, which is precisely what a horizontally
// scaled deployment is.
//
// Every instance shares one clock. That is realistic — replicas do see roughly
// the same wall time — and it isolates the variable under test to state
// locality rather than clock skew, which is Phase 6's problem.
type cluster struct {
	instances []ratelimit.Limiter
	next      int
}

func newCluster(t *testing.T, a algorithm, n int, clk ratelimit.Clock) *cluster {
	t.Helper()
	c := &cluster{instances: make([]ratelimit.Limiter, n)}
	for i := range c.instances {
		c.instances[i] = a.new(t, clk)
	}
	return c
}

// allow routes one request to the next instance, as a load balancer would.
func (c *cluster) allow(t *testing.T, key string) bool {
	t.Helper()
	l := c.instances[c.next%len(c.instances)]
	c.next++

	d, err := l.Allow(context.Background(), key)
	if err != nil {
		t.Fatalf("Allow: %v", err)
	}
	return d.Allowed
}

// admit issues n requests round-robin and reports how many were admitted.
func (c *cluster) admit(t *testing.T, key string, n int) int {
	t.Helper()
	count := 0
	for range n {
		if c.allow(t, key) {
			count++
		}
	}
	return count
}
