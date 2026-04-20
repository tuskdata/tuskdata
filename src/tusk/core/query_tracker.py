"""Track in-flight queries by request_id so clients can cancel them server-side.

The client sends a request_id with each /api/query call. The engine records the
backend PID (PostgreSQL) or a cancel handle (DuckDB) against that id. A separate
/api/query/cancel endpoint looks up the id and signals the backend to stop.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TrackedQuery:
    request_id: str
    connection_id: str
    engine: str  # "postgres" | "duckdb" | "sqlite"
    pid: int | None = None
    cancel_handle: Any = None  # engine-specific (e.g., duckdb Connection)
    extra: dict = field(default_factory=dict)


_lock = threading.Lock()
_queries: dict[str, TrackedQuery] = {}


def register(q: TrackedQuery) -> None:
    with _lock:
        _queries[q.request_id] = q


def update(request_id: str, **fields) -> None:
    with _lock:
        q = _queries.get(request_id)
        if q is None:
            return
        for k, v in fields.items():
            if hasattr(q, k):
                setattr(q, k, v)
            else:
                q.extra[k] = v


def unregister(request_id: str) -> None:
    with _lock:
        _queries.pop(request_id, None)


def get(request_id: str) -> TrackedQuery | None:
    with _lock:
        return _queries.get(request_id)


def list_active() -> list[TrackedQuery]:
    with _lock:
        return list(_queries.values())
