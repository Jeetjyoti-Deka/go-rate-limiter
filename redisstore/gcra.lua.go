package redisstore

// gcraScript is the whole limiter, executed inside Redis.
//
// Redis runs commands on a single thread and a script runs to completion before
// any other command is served, so the GET, the decision and the SET cannot be
// interleaved. The script is the critical section and Redis is the lock — which
// is the only lock available across processes.
//
// It must stay short for the same reason it works: a script that blocks for a
// millisecond blocks every client for that millisecond. This is arithmetic on
// one key with no loops.
//
//	KEYS[1]  the rate limit key
//	ARGV[1]  emission, microseconds per unit
//	ARGV[2]  tolerance, microseconds (burst * emission)
//	ARGV[3]  cost, microseconds (n * emission)
//	ARGV[4]  key TTL in milliseconds
//	ARGV[5]  now in microseconds, or 0 to read the server clock
//
// Returns {allowed, remaining, retryAfterMicros, resetAfterMicros}.
var gcraSource = `
  local emission  = tonumber(ARGV[1])
  local tolerance = tonumber(ARGV[2])
  local cost      = tonumber(ARGV[3])
  local ttl       = tonumber(ARGV[4])
  local now       = tonumber(ARGV[5])

  if now == 0 then
    local t = redis.call('TIME')
    now = tonumber(t[1]) * 1000000 + tonumber(t[2])
  end

  local tat = tonumber(redis.call('GET', KEYS[1]))
  if not tat or tat < now then
    tat = now
  end

  local newTat  = tat + cost
  local allowAt = newTat - tolerance

  if allowAt > now then
    local retry = 0
    if cost <= tolerance then
      retry = allowAt - now
    end
    return { 0, math.floor((now + tolerance - tat) / emission), retry, tat - now }
  end

  redis.call('SET', KEYS[1], newTat, 'PX', ttl)
  return { 1, math.floor((now + tolerance - newTat) / emission), 0, newTat - now }
  `
