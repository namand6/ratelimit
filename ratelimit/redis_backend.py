"""The same algorithms for many app servers sharing one Redis.

Each check is ONE Lua script, so read-modify-write is atomic inside Redis (no races between servers,
no client-side locks), and time comes from Redis `TIME` so app servers with drifting clocks agree.
Keys expire on their own, so idle clients cost no memory.
"""

from __future__ import annotations

from .core import Decision

_NOW = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
"""

SCRIPTS = {
    "fixed_window": _NOW + """
local limit, window, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local idx = math.floor(now / window)
local key = KEYS[1] .. ':' .. idx
local count = tonumber(redis.call('GET', key) or '0')
local reset = (idx + 1) * window - now
if count + cost > limit then return {0, limit - count, tostring(reset), tostring(reset)} end
count = redis.call('INCRBY', key, cost)
redis.call('PEXPIRE', key, math.ceil(window * 1000) + 1000)
return {1, limit - count, '0', tostring(reset)}
""",
    "sliding_log": _NOW + """
local limit, window, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local n = redis.call('ZCARD', KEYS[1])
if n + cost > limit then
  local need = n + cost - limit
  local oldest = redis.call('ZRANGE', KEYS[1], need - 1, need - 1, 'WITHSCORES')
  local retry = window
  if oldest[2] then retry = tonumber(oldest[2]) + window - now end
  return {0, limit - n, tostring(retry), tostring(window)}
end
local seq = redis.call('INCR', KEYS[1] .. ':seq')
for i = 1, cost do redis.call('ZADD', KEYS[1], now, seq .. ':' .. i) end
redis.call('PEXPIRE', KEYS[1], math.ceil(window * 1000))
redis.call('PEXPIRE', KEYS[1] .. ':seq', math.ceil(window * 1000))
return {1, limit - n - cost, '0', tostring(window)}
""",
    "sliding_counter": _NOW + """
local limit, window, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local idx = math.floor(now / window)
local cur_key, prev_key = KEYS[1] .. ':' .. idx, KEYS[1] .. ':' .. (idx - 1)
local cur = tonumber(redis.call('GET', cur_key) or '0')
local prev = tonumber(redis.call('GET', prev_key) or '0')
local elapsed = (now % window) / window
local weighted = prev * (1 - elapsed) + cur
if weighted + cost > limit then
  local retry
  if cost > limit then retry = 1e18
  elseif prev > 0 and cur + cost <= limit then retry = math.max(0, (1 - (limit - cost - cur) / prev) - elapsed) * window
  else
    local need = 0
    if cur > 0 then need = math.max(0, 1 - (limit - cost) / cur) end
    retry = ((1 - elapsed) + need) * window
  end
  return {0, math.max(0, math.floor(limit - weighted)), tostring(retry), tostring((2 - elapsed) * window)}
end
redis.call('INCRBY', cur_key, cost)
redis.call('PEXPIRE', cur_key, math.ceil(window * 2000) + 1000)
return {1, math.max(0, math.floor(limit - weighted - cost)), '0', tostring((2 - elapsed) * window)}
""",
    "token_bucket": _NOW + """
local capacity, window, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local rate = capacity / window
local s = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens, last = tonumber(s[1]) or capacity, tonumber(s[2]) or now
tokens = math.min(capacity, tokens + math.max(0, now - last) * rate)
local allowed = 0
local retry = 0
if tokens >= cost then tokens = tokens - cost; allowed = 1 else retry = (cost - tokens) / rate end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'last', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(window * 1000) + 1000)
return {allowed, math.floor(tokens), tostring(retry), tostring((capacity - tokens) / rate)}
""",
    "gcra": _NOW + """
local limit, window, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local interval = window / limit
local tolerance = interval * limit
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
tat = math.max(tat, now)
local new_tat = tat + interval * cost
local allow_at = new_tat - tolerance
if allow_at > now + 1e-9 then
  return {0, math.max(0, math.floor((now - (tat - tolerance)) / interval)), tostring(allow_at - now), tostring(tat - now)}
end
redis.call('SET', KEYS[1], tostring(new_tat), 'PX', math.ceil((new_tat - now) * 1000) + 1000)
return {1, math.max(0, math.floor((now - (new_tat - tolerance)) / interval + 1e-9)), '0', tostring(new_tat - now)}
""",
}


class RedisLimiter:
    def __init__(self, client, algorithm: str, limit: int, window: float, prefix: str = "rl"):
        if algorithm not in SCRIPTS:
            raise ValueError(f"algorithm must be one of {sorted(SCRIPTS)}")
        self.name, self.limit, self.window, self.prefix = f"redis:{algorithm}", limit, float(window), prefix
        self.algorithm = algorithm
        self.script = client.register_script(SCRIPTS[algorithm])  # EVALSHA with auto-reload

    def allow(self, key: str, cost: int = 1) -> Decision:
        ok, remaining, retry, reset = self.script(keys=[f"{self.prefix}:{self.algorithm}:{key}"],
                                                  args=[self.limit, self.window, cost])
        return Decision(bool(ok), self.limit, int(remaining), float(retry), float(reset))
