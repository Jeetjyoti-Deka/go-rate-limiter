// Package benchmarks holds the cross-implementation benchmark harness. It
// contains no library code: every limiter is driven through the
// ratelimit.Limiter interface so the concurrency strategies are compared on
// equal terms.
//
// See docs/03-concurrency.md for the reasoning and RESULTS.md for the numbers.
package benchmarks
