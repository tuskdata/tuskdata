"""Rolling history of PostgreSQL server stats for the admin sparklines.

Captures a minimal snapshot (connections, active_queries, cache_hit_ratio,
db_size_bytes) every N minutes into a per-connection SQLite file. Old rows
beyond `MAX_POINTS` are pruned so the file stays bounded.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from tusk.core.logging import get_logger
from tusk.core import meta

log = get_logger("stats_history")

STATS_DB = meta.TUSK_DB  # was ~/.tusk/stats_history.db before 0.4.38
MAX_POINTS = 288  # 24h @ 5min granularity


def _init_db() -> None:
    STATS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = meta.connect(STATS_DB)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stats_points (
                connection_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                connections INTEGER,
                max_connections INTEGER,
                active_queries INTEGER,
                cache_hit_ratio REAL,
                db_size_bytes INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_conn_ts "
            "ON stats_points(connection_id, ts DESC)"
        )
        conn.commit()
    finally:
        conn.close()


def record_stats(connection_id: str, stats: object) -> None:
    """Persist a single data point for `connection_id`. Safe to call often."""
    try:
        _init_db()
        ts = datetime.now(timezone.utc).isoformat()
        conn = meta.connect(STATS_DB)
        try:
            conn.execute(
                """
                INSERT INTO stats_points
                (connection_id, ts, connections, max_connections,
                 active_queries, cache_hit_ratio, db_size_bytes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    connection_id,
                    ts,
                    getattr(stats, "connections", None),
                    getattr(stats, "max_connections", None),
                    getattr(stats, "active_queries", None),
                    getattr(stats, "cache_hit_ratio", None),
                    getattr(stats, "db_size_bytes", None),
                ),
            )
            # Prune older rows beyond MAX_POINTS
            conn.execute(
                """
                DELETE FROM stats_points
                WHERE connection_id = ?
                  AND ts NOT IN (
                    SELECT ts FROM stats_points
                    WHERE connection_id = ?
                    ORDER BY ts DESC
                    LIMIT ?
                  )
                """,
                (connection_id, connection_id, MAX_POINTS),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning("stats_record_failed", error=str(e))


def get_history(connection_id: str, limit: int = 288) -> list[dict]:
    """Return recent points oldest → newest for rendering a sparkline."""
    try:
        _init_db()
        conn = meta.connect(STATS_DB)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT * FROM stats_points
                WHERE connection_id = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (connection_id, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]
        finally:
            conn.close()
    except Exception as e:
        log.warning("stats_history_failed", error=str(e))
        return []
