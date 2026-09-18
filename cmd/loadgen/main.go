// Command loadgen fires concurrent requests round-robin across several
// demo-server instances, as a load balancer would, and reports how many were
// admitted against the limit each instance was configured with.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		targets = flag.String("targets",
			"http://localhost:8081,http://localhost:8082,http://localhost:8083",
			"comma-separated instance URLs")
		total   = flag.Int("n", 1000, "total requests")
		workers = flag.Int("c", 16, "concurrent workers")
		key     = flag.String("key", "shared-tenant", "rate limit key")
		limit   = flag.Int("limit", 100, "the limit each instance was configured with")
	)
	flag.Parse()

	urls := strings.Split(*targets, ",")

	var admitted, rejected, failed, seq atomic.Int64

	// One client, shared: connection reuse keeps the generator from becoming
	// the bottleneck instead of the limiter.
	client := &http.Client{Timeout: 5 * time.Second}

	var wg sync.WaitGroup
	start := time.Now()

	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := seq.Add(1) - 1
				if i >= int64(*total) {
					return
				}

				url := fmt.Sprintf("%s/?key=%s", urls[int(i)%len(urls)], *key)
				resp, err := client.Get(url)
				if err != nil {
					failed.Add(1)
					continue
				}
				// Drain before closing, or the connection cannot be reused.
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK:
					admitted.Add(1)
				case http.StatusTooManyRequests:
					rejected.Add(1)
				default:
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	got := admitted.Load()
	fmt.Printf("\n%d instances, each configured for %d requests\n", len(urls), *limit)
	fmt.Printf("sent      %d in %v\n", *total, time.Since(start).Round(time.Millisecond))
	fmt.Printf("admitted  %d\n", got)
	fmt.Printf("rejected  %d\n", rejected.Load())
	if f := failed.Load(); f > 0 {
		fmt.Printf("failed    %d\n", f)
	}
	fmt.Printf("\neffective limit: %d (%.1fx the configured %d)\n",
		got, float64(got)/float64(*limit), *limit)
}
