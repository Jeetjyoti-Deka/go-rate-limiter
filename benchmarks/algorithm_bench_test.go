package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/fixedwindow"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowcounter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowlog"
)

// algorithm is one limiting algorithm in its natural implementation.
//
// All but GCRA use a global mutex; GCRA is lock-free because its state fits in
// one word, which is a property of the algorithm rather than a thumb on the
// scale.
type algorithm struct {
	name string
	new  func(tb testing.TB, limit int) ratelimit.Limiter
}

func algorithms() []algorithm {
	return []algorithm{
		{"FixedWindow", func(tb testing.TB, limit int) ratelimit.Limiter {
			l, err := fixedwindow.New(fixedwindow.Config{Limit: limit, Window: time.Hour})
			if err != nil {
				tb.Fatalf("fixedwindow.New: %v", err)
			}
			return l
		}},
		{"TokenBucket", func(tb testing.TB, limit int) ratelimit.Limiter {
			l, err := tokenbucket.New(tokenbucket.Config{Limit: limit, Window: time.Hour})
			if err != nil {
				tb.Fatalf("tokenbucket.New: %v", err)
			}
			return l
		}},
		{"WindowLog", func(tb testing.TB, limit int) ratelimit.Limiter {
			l, err := windowlog.New(windowlog.Config{Limit: limit, Window: time.Hour})
			if err != nil {
				tb.Fatalf("windowlog.New: %v", err)
			}
			return l
		}},
		{"WindowCounter", func(tb testing.TB, limit int) ratelimit.Limiter {
			l, err := windowcounter.New(windowcounter.Config{Limit: limit, Window: time.Hour})
			if err != nil {
				tb.Fatalf("windowcounter.New: %v", err)
			}
			return l
		}},
		{"GCRA", func(tb testing.TB, limit int) ratelimit.Limiter {
			l, err := gcra.New(gcra.Config{Limit: limit, Window: time.Hour})
			if err != nil {
				tb.Fatalf("gcra.New: %v", err)
			}
			return l
		}},
	}
}

// BenchmarkAlgorithmDeny measures the overload path: one hot key, drained, so
// every timed request is rejected.
//
// There is deliberately no sustained-admit counterpart. A sliding window log
// physically cannot admit more than its limit within a window, and raising the
// limit high enough to survive millions of iterations would allocate a ring of
// that many timestamps. Overload is also the more interesting case: it is when
// a limiter is under the most pressure, and where GCRA's write-free denial
// should show.
func BenchmarkAlgorithmDeny(b *testing.B) {
	const limit = 64

	for _, a := range algorithms() {
		b.Run(a.name, func(b *testing.B) {
			l := a.new(b, limit)
			ctx := context.Background()

			for range limit {
				if _, err := l.Allow(ctx, "hot"); err != nil {
					b.Fatalf("drain: %v", err)
				}
			}
			if d, _ := l.Allow(ctx, "hot"); d.Allowed {
				b.Fatal("limiter not exhausted; benchmark precondition failed")
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := l.Allow(ctx, "hot"); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkAlgorithmMemoryPerKey reports retained bytes per key, and as ns/op
// the cold first-touch admission cost.
//
// Limit is swept because one algorithm's footprint depends on it: a sliding
// window log stores every admission timestamp, so its per-key cost grows with
// the limit while every other algorithm's stays flat.
func BenchmarkAlgorithmMemoryPerKey(b *testing.B) {
	for _, limit := range []int{10, 100, 1000} {
		for _, a := range algorithms() {
			b.Run(fmt.Sprintf("%s/limit=%d", a.name, limit), func(b *testing.B) {
				keys := keyPool(b.N)
				ctx := context.Background()

				l := a.new(b, limit)

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

				runtime.KeepAlive(l)
				runtime.KeepAlive(keys)

				b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(b.N), "B/key")
			})
		}
	}
}
