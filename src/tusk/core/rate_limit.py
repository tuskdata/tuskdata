"""Simple in-memory per-IP rate limiter shared across the app.

Kept deliberately minimal: no persistence, resets on restart. Good enough to
stop accidental floods and trivial DoS. For distributed deployments, replace
with Redis-backed limiter.
"""

from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock

_buckets: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
_lock = Lock()


def check(bucket: str, key: str, max_attempts: int, window_seconds: int) -> bool:
    """Return True if the caller is within the rate limit.

    Args:
        bucket: Logical action name (e.g. "upload", "export", "scan").
        key: Identity for the caller, usually IP address.
        max_attempts: Allowed attempts per window.
        window_seconds: Sliding window length.
    """
    now = time.monotonic()
    with _lock:
        attempts = _buckets[bucket][key]
        fresh = [t for t in attempts if now - t < window_seconds]
        _buckets[bucket][key] = fresh
        return len(fresh) < max_attempts


def record(bucket: str, key: str) -> None:
    """Record an attempt against the bucket/key pair."""
    with _lock:
        _buckets[bucket][key].append(time.monotonic())


def check_and_record(bucket: str, key: str, max_attempts: int, window_seconds: int) -> bool:
    """Check + record in one call. Returns True if allowed, False if denied."""
    if not check(bucket, key, max_attempts, window_seconds):
        return False
    record(bucket, key)
    return True
