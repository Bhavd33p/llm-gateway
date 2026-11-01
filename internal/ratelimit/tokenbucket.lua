-- Token bucket, run atomically inside Redis so concurrent gateway replicas cannot race on the same bucket.
-- KEYS[1]  bucket hash with fields tokens (float) and ts (milliseconds)
-- ARGV[1]  refill rate, tokens per second
-- ARGV[2]  capacity (burst)
-- ARGV[3]  tokens this request costs
-- Returns {allowed 0|1, tokens left as a string, retry-after in milliseconds}
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

-- Redis server time, not the caller's clock: every replica then agrees on "now" despite clock skew.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

-- Refill lazily for the time since the last call instead of running a timer per bucket.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * rate / 1000)

local allowed = 0
local retry_after_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after_ms = math.ceil((cost - tokens) / rate * 1000)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- After capacity/rate seconds an idle bucket is full again, so the key carries no information and can expire.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate * 1000) + 1000)
return {allowed, tostring(tokens), retry_after_ms}
