import threading

import pytest

from ratelimit import bench
from ratelimit.core import ALGORITHMS, GCRA, FixedWindow, SlidingLog, SlidingWindowCounter, TokenBucket
from ratelimit.middleware import RateLimitMiddleware

ALL = list(ALGORITHMS.values())


class Clock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


def drain(lim, key="k", n=1000):
    return sum(lim.allow(key).allowed for _ in range(n))


@pytest.mark.parametrize("cls", ALL)
def test_admits_exactly_limit_then_rejects(cls):
    lim = cls(5, 1.0, clock=Clock(0.5))
    assert drain(lim) == 5
    d = lim.allow("k")
    assert not d.allowed and d.retry_after > 0 and d.remaining == 0


@pytest.mark.parametrize("cls", ALL)
def test_keys_are_independent(cls):
    lim = cls(2, 1.0, clock=Clock(0.5))
    assert drain(lim, "a") == 2 and drain(lim, "b") == 2


@pytest.mark.parametrize("cls", ALL)
def test_retry_after_is_honest(cls):
    """Waiting exactly retry_after must be enough to get the next request in."""
    clock = Clock(3.3)
    lim = cls(4, 2.0, clock=clock)
    drain(lim)
    d = lim.allow("k")
    clock.t += d.retry_after + 1e-6
    assert lim.allow("k").allowed, cls.name


@pytest.mark.parametrize("cls", ALL)
def test_recovers_after_idle(cls):
    clock = Clock(0.0)
    lim = cls(3, 1.0, clock=clock)
    drain(lim)
    clock.t = 10.0
    assert drain(lim) == 3


def test_fixed_window_allows_double_at_edge_others_dont():
    assert bench.edge_burst(FixedWindow) == 2 * bench.LIMIT
    for cls in (SlidingLog, SlidingWindowCounter, TokenBucket, GCRA):
        assert bench.edge_burst(cls) <= bench.LIMIT, cls.name


def test_steady_overload_close_to_ideal():
    for cls in ALL:
        got, ideal = bench.steady_overload(cls, seconds=60)
        burst = bench.LIMIT if cls in (TokenBucket, GCRA) else 0
        assert ideal * 0.97 <= got <= ideal + burst, cls.name


def test_token_bucket_refills_at_rate_and_rejects_oversized_cost():
    clock = Clock(0.0)
    tb = TokenBucket(10, 1.0, clock=clock)
    assert drain(tb) == 10
    clock.t = 0.5
    assert drain(tb) == 5
    d = tb.allow("k", cost=11)
    assert not d.allowed and d.retry_after == float("inf")


def test_gcra_spaces_requests_after_burst():
    clock = Clock(0.0)
    g = GCRA(10, 1.0, clock=clock, burst=2)
    assert drain(g) == 2
    clock.t = 0.1
    assert drain(g) == 1        # one emission interval later, exactly one more
    clock.t = 0.15
    assert drain(g) == 0


def test_sliding_log_is_exact():
    clock = Clock(0.0)
    s = SlidingLog(3, 1.0, clock=clock)
    for t in (0.0, 0.4, 0.8):
        clock.t = t
        assert s.allow("k").allowed
    clock.t = 0.99
    assert not s.allow("k").allowed
    clock.t = 1.0001                 # first request expired
    assert s.allow("k").allowed and not s.allow("k").allowed


def test_cost_validation():
    with pytest.raises(ValueError):
        FixedWindow(0, 1)
    with pytest.raises(ValueError):
        FixedWindow(1, 1).allow("k", cost=0)


@pytest.mark.parametrize("cls", ALL)
def test_thread_safety_never_over_admits(cls):
    lim = cls(500, 1000.0)
    got = []

    def hammer():
        got.append(sum(lim.allow("hot").allowed for _ in range(300)))
    ts = [threading.Thread(target=hammer) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sum(got) == 500


def _app(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"ok"]


def call(mw, ip="1.2.3.4", api_key=None):
    out = {}

    def start(status, headers, exc_info=None):
        out["status"], out["headers"] = status, dict(headers)
    environ = {"REMOTE_ADDR": ip}
    if api_key:
        environ["HTTP_X_API_KEY"] = api_key
    body = b"".join(mw(environ, start))
    return out["status"], out["headers"], body


def test_middleware_headers_and_429():
    mw = RateLimitMiddleware(_app, FixedWindow(2, 60, clock=Clock(1.0)))
    s, h, _ = call(mw)
    assert s == "200 OK" and h["RateLimit-Limit"] == "2" and h["RateLimit-Remaining"] == "1"
    call(mw)
    s, h, body = call(mw)
    assert s.startswith("429") and int(h["Retry-After"]) >= 1 and b"rate_limited" in body
    assert call(mw, ip="5.6.7.8")[0] == "200 OK"            # different client
    assert call(mw, api_key="abc")[0] == "200 OK"           # API key beats IP


def test_middleware_fails_open_or_closed():
    class Down:
        def allow(self, *a):
            raise ConnectionError("redis down")
    open_mw = RateLimitMiddleware(_app, Down())
    assert call(open_mw)[0] == "200 OK" and open_mw.errors == 1
    assert call(RateLimitMiddleware(_app, Down(), fail_open=False))[0].startswith("503")


redis_only = pytest.mark.skipif(not bench.redis_up(), reason="needs redis-server on localhost:6379")


@redis_only
@pytest.mark.parametrize("algo", ["fixed_window", "sliding_log", "sliding_counter", "token_bucket", "gcra"])
def test_redis_limits_and_distributed_correctness(algo):
    import time

    import redis

    from ratelimit.redis_backend import RedisLimiter
    lim = RedisLimiter(redis.Redis(), algo, 5, 60, prefix=f"t{time.time_ns()}")
    assert drain(lim, n=20) == 5
    assert not lim.allow("k").allowed
    assert bench.distributed(algo, procs=4, each=60) == bench.LIMIT
