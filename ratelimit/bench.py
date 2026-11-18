"""Benchmarks that answer the interview questions with numbers.

1. Window-edge burst: how many requests get through in ANY window-length span?
2. Accuracy under steady 3x overload: admitted vs the ideal.
3. Throughput: decisions/second in memory and against Redis.
4. Distributed correctness: many processes hammering one key through Redis never over-admit.

  python -m ratelimit.bench            (Redis parts run if a server is on localhost:6379)
"""

from __future__ import annotations

import multiprocessing as mp
import sys
import threading
import time

from .core import ALGORITHMS

LIMIT, WINDOW = 100, 10.0


class FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


def edge_burst(cls) -> int:
    """Fill the limit just before a window boundary and again just after; worst case in one window span."""
    clock = FakeClock(0.0)
    lim = cls(LIMIT, WINDOW, clock=clock)
    times = []
    for t0 in (WINDOW - 0.01, WINDOW + 0.01):
        clock.t = t0
        for _ in range(LIMIT * 2):
            if lim.allow("k").allowed:
                times.append(t0)
    return max(sum(1 for t in times if s <= t < s + WINDOW) for s in times)


def steady_overload(cls, seconds: float = 120.0, factor: int = 3) -> tuple[int, float]:
    """Requests arrive evenly at `factor` x the allowed rate. Returns (admitted, ideal)."""
    clock = FakeClock(0.0)
    lim = cls(LIMIT, WINDOW, clock=clock)
    rate = LIMIT / WINDOW * factor
    n, allowed = int(seconds * rate), 0
    for i in range(n):
        clock.t = i / rate
        allowed += lim.allow("k").allowed
    return allowed, LIMIT * seconds / WINDOW


def throughput(lim, seconds: float = 2.0, threads: int = 4, keys: int = 1000) -> float:
    stop, counts = time.perf_counter() + seconds, [0] * threads

    def work(i):
        n = 0
        while time.perf_counter() < stop:
            lim.allow(f"user{(n * 7919 + i) % keys}")
            n += 1
        counts[i] = n
    ts = [threading.Thread(target=work, args=(i,)) for i in range(threads)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return sum(counts) / seconds


def _hammer(algorithm, key, n, q):
    import redis
    from .redis_backend import RedisLimiter
    lim = RedisLimiter(redis.Redis(), algorithm, LIMIT, 60)
    q.put(sum(lim.allow(key).allowed for _ in range(n)))


def distributed(algorithm: str, procs: int = 8, each: int = 100) -> int:
    key = f"bench-{algorithm}-{time.time_ns()}"
    q = mp.Queue()
    ps = [mp.Process(target=_hammer, args=(algorithm, key, each, q)) for _ in range(procs)]
    [p.start() for p in ps]
    total = sum(q.get() for _ in ps)
    [p.join() for p in ps]
    return total


def redis_up() -> bool:
    try:
        import redis
        return bool(redis.Redis(socket_connect_timeout=0.5).ping())
    except Exception:  # noqa: BLE001
        return False


def main() -> int:
    print(f"limit {LIMIT} requests / {WINDOW:.0f}s\n")
    print("| Algorithm | Max admitted in any 10s span | Admitted at 3x load for 120s (ideal 1200; buckets +100 burst) | In-memory decisions/s |")
    print("|---|---|---|---|")
    for name, cls in ALGORITHMS.items():
        burst = edge_burst(cls)
        got, ideal = steady_overload(cls)
        tps = throughput(cls(LIMIT, WINDOW))
        print(f"| {name} | {burst} | {got} ({got / ideal:.0%}) | {tps:,.0f} |")
    if not redis_up():
        print("\n(Redis not running on localhost:6379; skipped the Redis benchmarks)")
        return 0
    import redis
    from .redis_backend import SCRIPTS, RedisLimiter
    print("\n| Redis algorithm | Decisions/s (4 threads) | 8 processes x 100 requests on one key: admitted (limit 100) |")
    print("|---|---|---|")
    for name in SCRIPTS:
        tps = throughput(RedisLimiter(redis.Redis(), name, LIMIT, WINDOW, prefix=f"bench{time.time_ns()}"))
        print(f"| {name} | {tps:,.0f} | {distributed(name)} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
