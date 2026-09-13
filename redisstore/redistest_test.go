package redisstore

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testClient connects to Redis, skipping the test if none is reachable so that
// `go test ./...` stays green for anyone who has not started one.
func testClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	c := redis.NewClient(&redis.Options{Addr: addr})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Skipf("redis unavailable at %s (%v)\n"+
			"start one with: docker run -d --name ratelimit-redis -p 6379:6379 redis:7-alpine",
			addr, err)
	}

	t.Cleanup(func() { c.Close() })
	return c
}

// uniquePrefix namespaces one test's keys, so parallel subtests and repeated
// runs against a long-lived Redis cannot contaminate each other.
func uniquePrefix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return fmt.Sprintf("rltest:%s:%x", t.Name(), b)
}
