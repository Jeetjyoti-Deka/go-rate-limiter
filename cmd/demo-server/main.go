// Command demo-server runs one instance of a rate-limited HTTP service.
//
// Start several on different ports and point cmd/loadgen at all of them to
// watch the configured limit multiply by the instance count. See
// docs/05-the-multi-instance-break.md.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	ratelimit "github.com/Jeetjyoti-Deka/go-rate-limiter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/fixedwindow"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/gcra"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/middleware"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/tokenbucket"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowcounter"
	"github.com/Jeetjyoti-Deka/go-rate-limiter/windowlog"
)

func main() {
	var (
		addr   = flag.String("addr", ":8081", "listen address")
		algo   = flag.String("algo", "tokenbucket", "fixedwindow, tokenbucket, windowlog, windowcounter, or gcra")
		limit  = flag.Int("limit", 100, "requests per window")
		window = flag.Duration("window", time.Minute, "window duration")
	)
	flag.Parse()

	lim, err := build(*algo, *limit, *window)
	if err != nil {
		log.Fatal(err)
	}

	// The Decision-to-HTTP translation that used to live here inline now comes
	// from the middleware package. See docs/08-middleware.md.
	mw, err := middleware.New(middleware.Config{
		Limiter: lim,
		// The demo drives the key from a query parameter so one loadgen can
		// target a shared tenant across instances. A real deployment would use
		// ByIP alone, or an API key ahead of it.
		KeyFunc: middleware.FirstNonEmpty(
			func(r *http.Request) string { return r.URL.Query().Get("key") },
			middleware.ByIP,
		),
	})
	if err != nil {
		log.Fatal(err)
	}

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("demo-server on %s: %s, %d per %v", *addr, *algo, *limit, *window)
	log.Fatal(http.ListenAndServe(*addr, withInstance(*addr, mw(ok))))
}

func build(algo string, limit int, window time.Duration) (ratelimit.Limiter, error) {
	switch algo {
	case "fixedwindow":
		return fixedwindow.New(fixedwindow.Config{Limit: limit, Window: window})
	case "tokenbucket":
		return tokenbucket.New(tokenbucket.Config{Limit: limit, Window: window})
	case "windowlog":
		return windowlog.New(windowlog.Config{Limit: limit, Window: window})
	case "windowcounter":
		return windowcounter.New(windowcounter.Config{Limit: limit, Window: window})
	case "gcra":
		return gcra.New(gcra.Config{Limit: limit, Window: window})
	default:
		return nil, fmt.Errorf("unknown algorithm %q", algo)
	}
}

// withInstance stamps every response — admitted or rejected — with the instance
// that served it. The point of the demo is that this varies while the limits
// fail to add up.
//
// It wraps the middleware rather than sitting inside it, so the header is
// present on the 429s too.
func withInstance(addr string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Instance", addr)
		next.ServeHTTP(w, r)
	})
}
