# ratelimit

[![tests](https://github.com/namand6/ratelimit/actions/workflows/tests.yml/badge.svg)](https://github.com/namand6/ratelimit/actions/workflows/tests.yml)

Five rate-limiting algorithms built from scratch, the same five as atomic Redis Lua scripts for
many app servers, WSGI middleware with standard `429` / `Retry-After` / `RateLimit-*` headers, and a
benchmark that shows the trade-offs with numbers. No required dependencies.

## Results

`python -m ratelimit.bench`, limit 100 requests per 10 s:

| Algorithm | Max admitted in any 10 s span | Admitted at 3x load for 120 s (ideal 1200; buckets +100 burst) | In-memory decisions/s |
|---|---|---|---|
| fixed_window | **200** | 1200 (100%) | 153,528 |
| sliding_log | 100 | 1200 (100%) | 127,954 |
| sliding_counter | 100 | 1189 (99%) | 112,855 |
| token_bucket | 100 | 1299 (108%) | 135,936 |
| gcra | 100 | 1299 (108%) | 138,094 |

- **Fixed window lets through 2x the limit** right around a window edge (100 at 9.99 s + 100 at 10.01 s).
  Cheap, but bursty.
- **Sliding log is exact** and costs one timestamp per request; fine for low limits, expensive at 10k/min.
- **Sliding window counter** gets within 1% with only two counters per key. Usually the right default.
- **Token bucket / GCRA** admit an initial burst (the +100) then a steady rate; GCRA stores one number per key.

With a Redis server on `localhost:6379`, the benchmark adds Redis throughput and a distributed test:
8 processes x 100 requests on one key must admit exactly 100.

## Use it

```python
from ratelimit.core import SlidingWindowCounter, TokenBucket
from ratelimit.middleware import RateLimitMiddleware

limiter = SlidingWindowCounter(limit=100, window=60)        # 100 requests per 60 seconds per key
d = limiter.allow("user-42")                                # Decision(allowed, limit, remaining, retry_after, reset_after)

api = TokenBucket(limit=10, window=1, capacity=50)          # 10/s steady, bursts up to 50
app = RateLimitMiddleware(wsgi_app, limiter)                # 429 + Retry-After + RateLimit-* headers
```

Across servers, with Redis:

```python
import redis
from ratelimit.redis_backend import RedisLimiter
limiter = RedisLimiter(redis.Redis(), "gcra", limit=100, window=60)
```

## Design

- **One interface:** `allow(key, cost=1) -> Decision`. `retry_after` is exact: waiting that long is
  always enough (tested for every algorithm).
- **Thread-safe** in memory; **atomic** in Redis: each check is one Lua script, so there's no
  read-then-write race between servers and no client-side locking.
- **Redis `TIME`**, not app-server clocks, so servers with drifting clocks agree.
- **Keys expire** on their own; idle clients cost nothing.
- **Middleware keys** by `X-API-Key` when present, else client IP, and **fails open** if the limiter
  is down (configurable), since a limiter outage shouldn't become an API outage.
- **Weighted cost:** expensive endpoints can charge more than 1 per request.

## Run it

```sh
python3.12 -m venv .venv && .venv/bin/pip install pytest redis
.venv/bin/python -m pytest -q          # 33 tests (+5 Redis tests that run when Redis is up; CI runs all 38 against a real Redis)
.venv/bin/python -m ratelimit.bench
```
