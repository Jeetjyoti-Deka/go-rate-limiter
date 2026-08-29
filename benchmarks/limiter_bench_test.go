package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/perkey"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/sharded"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
)

const (
	// benchLimit is deliberately enormous. The deny path does less work than
	// the admit path, so a benchmark whose bucket runs dry ends up measuring
	// rejections instead of the synchronisation under test.
	benchLimit  = 1 << 30
	benchWindow = time.Second
)

type impl struct {
	name string
	new  func(tb testing.TB) ratelimit.Limiter
}

// impls returns the strategies under comparison.
//
// Every constructor leaves Clock nil, so all three use ratelimit.SystemClock.
// FakeClock guards its field with an RWMutex and would add a second contention
// point, polluting the measurement of the first.
func impls() []impl {
	return []impl{
		{"GlobalMutex", func(tb testing.TB) ratelimit.Limiter {
			l, err := tokenbucket.New(tokenbucket.Config{
				Limit: benchLimit, Window: benchWindow,
			})
			if err != nil {
				tb.Fatalf("tokenbucket.New: %v", err)
			}
			return l
		}}, {"Sharded", func(tb testing.TB) ratelimit.Limiter {
			l, err := sharded.New(sharded.Config{
				Limit: benchLimit, Window: benchWindow,
				// Pinned so the -cpu sweep varies parallelism only. The
				// default is 4*GOMAXPROCS, which would move with -cpu and
				// confound partition count with goroutine count.
				Shards: 64,
			})
			if err != nil {
				tb.Fatalf("sharded.New: %v", err)
			}
			return l
		}}, {"PerKey", func(tb testing.TB) ratelimit.Limiter {
			l, err := perkey.New(perkey.Config{
				Limit: benchLimit, Window: benchWindow,
			})
			if err != nil {
				tb.Fatalf("perkey.New: %v", err)
			}
			return l
		}},
	}
}

func keyPool(n int) []string {
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("tenant-%06d", i)
	}
	return keys
}

// BenchmarkHotKey is the case sharding cannot help with: every goroutine
// contends for one key, so all three strategies serialise on a single lock.
func BenchmarkHotKey(b *testing.B) {
	for _, im := range impls() {
		b.Run(im.name, func(b *testing.B) {
			l := im.new(b)
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := l.Allow(ctx, "hot"); err != nil {
						// Error, not Fatal: FailNow is illegal off the
						// goroutine running the benchmark.
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkDistinctKeys is the case sharding exists for. Cardinality is swept
// past the default shard count so any crossover with per-key locking shows up.
func BenchmarkDistinctKeys(b *testing.B) {
	for _, keyCount := range []int{16, 1024, 65536} {
		keys := keyPool(keyCount)
		mask := keyCount - 1 // every keyCount here is a power of two

		for _, im := range impls() {
			b.Run(fmt.Sprintf("%s/keys=%d", im.name, keyCount), func(b *testing.B) {
				l := im.new(b)
				ctx := context.Background()

				// Each parallel goroutine starts at a different offset;
				// otherwise they march the key sequence in lockstep and
				// collide on the same key far more than a real workload would.
				var seq atomic.Int64

				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := int(seq.Add(1)) * 7919
					for pb.Next() {
						if _, err := l.Allow(ctx, keys[i&mask]); err != nil {
							b.Error(err)
							return
						}
						i++
					}
				})
			})
		}
	}
}

// BenchmarkMemoryPerKey measures what one key costs each strategy, which is the
// axis BenchmarkDistinctKeys cannot see: that benchmark reports 0 allocs/op
// because it runs in steady state, after every key already exists.
//
// Two figures are reported and they answer different questions. B/op is
// allocation churn per key, including memory freed again immediately. B/key is
// what remains resident after a GC, which is the number that decides whether a
// flood of distinct keys exhausts the heap.
//
// The limiter is constructed before the first snapshot, so a strategy's fixed
// overhead (sharded allocates 4*GOMAXPROCS shard maps up front) is excluded —
// this is the marginal cost of one key, not the setup cost.
//
// Run with an explicit iteration count; at small b.N the measurement is noise:
//
//	go test -run '^$' -bench BenchmarkMemoryPerKey -benchtime=200000x ./benchmarks/
func BenchmarkMemoryPerKey(b *testing.B) {
	for _, im := range impls() {
		b.Run(im.name, func(b *testing.B) {
			// Build the key pool before measuring. The caller owns these
			// strings either way; we want the limiter's overhead, not theirs.
			// Go map keys share the string's backing array, so only the
			// 16-byte header is duplicated per entry.
			keys := keyPool(b.N)
			ctx := context.Background()

			l := im.new(b)

			// Twice: the first collection can leave work for the second.
			runtime.GC()
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if _, err := l.Allow(ctx, keys[i]); err != nil {
					b.Fatalf("Allow: %v", err)
				}
			}

			b.StopTimer()

			runtime.GC()
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			// Without this the limiter is unreachable by the second GC and
			// everything it holds could be collected before we read.
			runtime.KeepAlive(l)
			runtime.KeepAlive(keys)

			retained := float64(after.HeapAlloc-before.HeapAlloc) / float64(b.N)
			b.ReportMetric(retained, "B/key")
		})
	}
}
