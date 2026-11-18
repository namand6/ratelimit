"""Five rate-limiting algorithms, in memory, thread-safe, with an injectable clock.

All share one interface:  limiter.allow(key, cost=1) -> Decision

| Algorithm               | Memory / key | Exact? | Bursts                                   |
|-------------------------|--------------|--------|------------------------------------------|
| FixedWindow             | 1 counter    | no     | up to 2x the limit across a window edge  |
| SlidingLog              | 1 ts / req   | yes    | none beyond the limit                    |
| SlidingWindowCounter    | 2 counters   | ~      | smoothed; small error, bounded below     |
| TokenBucket             | 2 numbers    | yes    | up to `capacity`, then steady refill     |
| GCRA                    | 1 timestamp  | yes    | up to `burst`, evenly spaced afterwards  |
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    retry_after: float   # seconds until this request would be allowed (0 if allowed)
    reset_after: float   # seconds until the limiter is fully reset for this key


class Limiter:
    name = "base"

    def __init__(self, limit: int, window: float, clock=time.monotonic):
        if limit <= 0 or window <= 0:
            raise ValueError("limit and window must be positive")
        self.limit, self.window, self.clock = limit, float(window), clock
        self._lock = threading.Lock()

    def allow(self, key: str, cost: int = 1) -> Decision:
        if cost <= 0:
            raise ValueError("cost must be positive")
        with self._lock:
            return self._allow(key, cost, self.clock())

    def _allow(self, key: str, cost: int, now: float) -> Decision:  # pragma: no cover
        raise NotImplementedError


class FixedWindow(Limiter):
    name = "fixed_window"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.state: dict[str, tuple[int, int]] = {}   # key -> (window index, count)

    def _allow(self, key, cost, now):
        idx = int(now // self.window)
        w, count = self.state.get(key, (idx, 0))
        if w != idx:
            w, count = idx, 0
        reset = (idx + 1) * self.window - now
        if count + cost > self.limit:
            self.state[key] = (w, count)
            return Decision(False, self.limit, self.limit - count, reset, reset)
        self.state[key] = (w, count + cost)
        return Decision(True, self.limit, self.limit - count - cost, 0.0, reset)


class SlidingLog(Limiter):
    name = "sliding_log"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.logs: dict[str, deque] = {}

    def _allow(self, key, cost, now):
        log = self.logs.setdefault(key, deque())
        while log and log[0] <= now - self.window:
            log.popleft()
        if len(log) + cost > self.limit:
            # wait until enough old entries expire
            need = len(log) + cost - self.limit
            retry = log[need - 1] + self.window - now if need <= len(log) else self.window
            return Decision(False, self.limit, self.limit - len(log), max(retry, 0.0), log[-1] + self.window - now)
        log.extend([now] * cost)
        return Decision(True, self.limit, self.limit - len(log), 0.0, log[-1] + self.window - now)


class SlidingWindowCounter(Limiter):
    """Cloudflare-style approximation: count = current + previous * (overlap of the sliding window)."""
    name = "sliding_counter"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.state: dict[str, tuple[int, int, int]] = {}  # key -> (window idx, prev count, cur count)

    def _allow(self, key, cost, now):
        idx = int(now // self.window)
        w, prev, cur = self.state.get(key, (idx, 0, 0))
        if idx == w + 1:
            prev, cur = cur, 0
        elif idx > w + 1:
            prev, cur = 0, 0
        elapsed = (now % self.window) / self.window
        weighted = prev * (1 - elapsed) + cur
        if weighted + cost > self.limit:
            self.state[key] = (idx, prev, cur)
            retry = self._retry(prev, cur, cost, elapsed)
            return Decision(False, self.limit, max(0, int(self.limit - weighted)), retry, (2 - elapsed) * self.window)
        self.state[key] = (idx, prev, cur + cost)
        return Decision(True, self.limit, max(0, int(self.limit - weighted - cost)), 0.0, (2 - elapsed) * self.window)

    def _retry(self, prev: int, cur: int, cost: int, elapsed: float) -> float:
        """Smallest wait after which prev*(1-e) + cur + cost <= limit holds."""
        if cost > self.limit:
            return math.inf
        if prev and cur + cost <= self.limit:          # fits later in this window, as prev's weight decays
            return max(0.0, (1 - (self.limit - cost - cur) / prev) - elapsed) * self.window
        need = max(0.0, 1 - (self.limit - cost) / cur) if cur else 0.0   # next window: cur becomes prev
        return ((1 - elapsed) + need) * self.window


class TokenBucket(Limiter):
    """`limit` tokens refill evenly over `window` seconds; capacity defaults to `limit`."""
    name = "token_bucket"

    def __init__(self, limit, window, clock=time.monotonic, capacity: int | None = None):
        super().__init__(limit, window, clock)
        self.capacity = capacity or limit
        self.rate = limit / self.window
        self.state: dict[str, tuple[float, float]] = {}  # key -> (tokens, last)

    def _allow(self, key, cost, now):
        tokens, last = self.state.get(key, (float(self.capacity), now))
        tokens = min(self.capacity, tokens + (now - last) * self.rate)
        full_in = (self.capacity - tokens) / self.rate
        if cost > self.capacity:
            return Decision(False, self.capacity, int(tokens), math.inf, full_in)
        if tokens < cost:
            self.state[key] = (tokens, now)
            return Decision(False, self.capacity, int(tokens), (cost - tokens) / self.rate, full_in)
        tokens -= cost
        self.state[key] = (tokens, now)
        return Decision(True, self.capacity, int(tokens), 0.0, (self.capacity - tokens) / self.rate)


class GCRA(Limiter):
    """Generic cell rate algorithm: stores only the 'theoretical arrival time' per key."""
    name = "gcra"

    def __init__(self, limit, window, clock=time.monotonic, burst: int | None = None):
        super().__init__(limit, window, clock)
        self.burst = burst or limit
        self.interval = self.window / limit             # emission interval T
        self.tolerance = self.interval * self.burst      # tau: how far ahead TAT may run
        self.tat: dict[str, float] = {}

    def _allow(self, key, cost, now):
        tat = max(self.tat.get(key, now), now)
        new_tat = tat + self.interval * cost
        allow_at = new_tat - self.tolerance
        if allow_at > now + 1e-9:
            remaining = max(0, int((now - (tat - self.tolerance)) / self.interval))
            return Decision(False, self.burst, remaining, allow_at - now, tat - now)
        self.tat[key] = new_tat
        remaining = max(0, int((now - (new_tat - self.tolerance)) / self.interval + 1e-9))
        return Decision(True, self.burst, remaining, 0.0, new_tat - now)


ALGORITHMS = {cls.name: cls for cls in (FixedWindow, SlidingLog, SlidingWindowCounter, TokenBucket, GCRA)}
