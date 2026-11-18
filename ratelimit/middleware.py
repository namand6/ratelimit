"""WSGI middleware: put any limiter in front of any WSGI app (Flask, Django, plain WSGI).

Rejections get HTTP 429 with `Retry-After`; every response carries the IETF draft
`RateLimit-Limit / RateLimit-Remaining / RateLimit-Reset` headers so clients can back off politely.
If the limiter itself fails (say Redis is down), it fails open by default and counts the error,
because an outage in the limiter shouldn't take the API down with it.
"""

from __future__ import annotations

import math


def client_key(environ) -> str:
    """API key if present, else the client IP. (Only trust X-Forwarded-For behind your own proxy.)"""
    api_key = environ.get("HTTP_X_API_KEY")
    return f"key:{api_key}" if api_key else f"ip:{environ.get('REMOTE_ADDR', 'unknown')}"


class RateLimitMiddleware:
    def __init__(self, app, limiter, key_func=client_key, fail_open: bool = True, cost_func=None):
        self.app, self.limiter, self.key_func, self.fail_open = app, limiter, key_func, fail_open
        self.cost_func = cost_func or (lambda environ: 1)
        self.errors = 0

    def __call__(self, environ, start_response):
        try:
            d = self.limiter.allow(self.key_func(environ), self.cost_func(environ))
        except Exception:  # noqa: BLE001 - limiter outage
            self.errors += 1
            if self.fail_open:
                return self.app(environ, start_response)
            start_response("503 Service Unavailable", [("Content-Type", "text/plain")])
            return [b"rate limiter unavailable\n"]
        headers = [("RateLimit-Limit", str(d.limit)), ("RateLimit-Remaining", str(max(0, d.remaining))),
                   ("RateLimit-Reset", str(math.ceil(d.reset_after)))]
        if not d.allowed:
            retry = "3600" if math.isinf(d.retry_after) else str(max(1, math.ceil(d.retry_after)))
            start_response("429 Too Many Requests", [("Content-Type", "application/json"), ("Retry-After", retry), *headers])
            return [b'{"error": "rate_limited"}\n']

        def start(status, response_headers, exc_info=None):
            return start_response(status, list(response_headers) + headers, exc_info)
        return self.app(environ, start)
