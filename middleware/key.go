package middleware

import (
	"net"
	"net/http"
)

// ByIP keys on the client's IP address, taken from RemoteAddr.
//
// RemoteAddr is the TCP peer and cannot be forged by that peer, because the
// connection exists. X-Forwarded-For can be forged by anyone, which is why
// there is deliberately no ByForwardedFor in this package: the safe version
// requires a proxy that overwrites the header and a network where nothing
// bypasses that proxy, and a library cannot verify either. See
// docs/08-middleware.md.
func ByIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// No port, or a malformed address. Use it verbatim rather than
		// collapsing every such request into one shared bucket.
		return r.RemoteAddr
	}
	return host
}

// ByHeader keys on a request header — an API key, a tenant id.
//
// Requests without the header get the empty key and therefore share a single
// bucket, which is usually the right treatment for anonymous traffic. It is a
// policy rather than a default to absorb by accident; compose with
// FirstNonEmpty to fall back to per-IP instead.
func ByHeader(name string) func(*http.Request) string {
	return func(r *http.Request) string {
		return r.Header.Get(name)
	}
}

// FirstNonEmpty returns the first non-empty key its arguments produce, so
// authenticated clients can be limited per credential and everyone else per IP.
func FirstNonEmpty(fns ...func(*http.Request) string) func(*http.Request) string {
	return func(r *http.Request) string {
		for _, fn := range fns {
			if k := fn(r); k != "" {
				return k
			}
		}
		return ""
	}
}
