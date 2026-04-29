"""Conversation memory for the AI Copilot.

Persists the last N exchanges per (user, connection) so the model has
context across follow-up questions. The user kept getting hallucinated
tables in v0.4.6.x because every prompt was answered cold — no memory
of what we talked about, no idea which connection's schema the user
meant.

Storage: SQLite at ``~/.tusk/ai_memory.db``. Two tables:

- ``conversations`` — one row per exchange (user + assistant turns
  stored as separate rows so we can chunk them in/out cleanly).
- ``conversation_meta`` — last-touched timestamp per session, used
  to prune old sessions.

A "session key" is whatever the caller passes — typically
``f"{user_id}:{connection_id}"`` so swapping connections gives a
fresh thread. We don't enforce this from the schema; callers decide
the granularity that makes sense.

The API is intentionally tiny:

    add_turn(session_key, role, content)
    get_recent_turns(session_key, limit=10) → [{role, content, ts}]
    clear_session(session_key)
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from tusk.core.logging import get_logger

log = get_logger("ai_memory")

DB_PATH = Path.home() / ".tusk" / "ai_memory.db"

# Hard cap on rows per session so a runaway loop doesn't fill the disk.
_MAX_ROWS_PER_SESSION = 200
# Drop sessions that haven't been touched in 30 days.
_SESSION_TTL_SECONDS = 30 * 24 * 3600


def _connect() -> sqlite3.Connection:
    """Open the AI memory DB with WAL + busy_timeout so concurrent
    writers (e.g. /api/ai/sql and /api/ai/explain firing at the same
    time from the same browser) don't trip on `database is locked`."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    # WAL is set once per database and persists; setting it on every
    # connect is cheap because SQLite skips the no-op.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.Error:
        pass
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_key TEXT NOT NULL,
            role TEXT NOT NULL,           -- 'user' | 'assistant' | 'system'
            content TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_conv_session_id
        ON conversations(session_key, id DESC)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS conversation_meta (
            session_key TEXT PRIMARY KEY,
            last_touched REAL NOT NULL
        )
    """)
    conn.commit()


def add_turn(session_key: str, role: str, content: str) -> None:
    """Append a turn. Trims the per-session row count to the cap."""
    if not session_key or not content:
        return
    if role not in ("user", "assistant", "system"):
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    now = time.time()
    try:
        with _connect() as conn:
            _init_db(conn)
            conn.execute(
                "INSERT INTO conversations (session_key, role, content, created_at) "
                "VALUES (?, ?, ?, ?)",
                (session_key, role, content, now_iso),
            )
            conn.execute(
                "INSERT INTO conversation_meta (session_key, last_touched) "
                "VALUES (?, ?) "
                "ON CONFLICT(session_key) DO UPDATE SET last_touched = excluded.last_touched",
                (session_key, now),
            )
            # Trim oldest rows above the cap. Cheaper than a TRIGGER and we
            # only do it on write.
            conn.execute(
                "DELETE FROM conversations WHERE id IN "
                "(SELECT id FROM conversations WHERE session_key = ? "
                " ORDER BY id DESC LIMIT -1 OFFSET ?)",
                (session_key, _MAX_ROWS_PER_SESSION),
            )
            conn.commit()
    except sqlite3.Error as e:
        log.warning("ai_memory.add_turn failed", error=str(e))


def get_recent_turns(session_key: str, limit: int = 10) -> list[dict]:
    """Return up to `limit` most-recent turns, oldest first.

    Returned dicts have `role`, `content`, `created_at`. Always safe to
    iterate — empty list if the session hasn't been used or the DB
    isn't there.
    """
    if not session_key:
        return []
    try:
        with _connect() as conn:
            _init_db(conn)
            rows = conn.execute(
                "SELECT role, content, created_at FROM conversations "
                "WHERE session_key = ? ORDER BY id DESC LIMIT ?",
                (session_key, max(1, min(limit, 50))),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]
    except sqlite3.Error as e:
        log.warning("ai_memory.get_recent_turns failed", error=str(e))
        return []


def clear_session(session_key: str) -> int:
    """Drop every turn for a session. Returns rows deleted."""
    if not session_key:
        return 0
    try:
        with _connect() as conn:
            _init_db(conn)
            cursor = conn.execute(
                "DELETE FROM conversations WHERE session_key = ?",
                (session_key,),
            )
            conn.execute(
                "DELETE FROM conversation_meta WHERE session_key = ?",
                (session_key,),
            )
            conn.commit()
            return cursor.rowcount or 0
    except sqlite3.Error as e:
        log.warning("ai_memory.clear_session failed", error=str(e))
        return 0


def prune_stale_sessions() -> int:
    """Drop sessions untouched for `_SESSION_TTL_SECONDS`. Returns the
    number of sessions removed. Call from the scheduler periodically."""
    cutoff = time.time() - _SESSION_TTL_SECONDS
    removed = 0
    try:
        with _connect() as conn:
            _init_db(conn)
            stale = [r["session_key"] for r in conn.execute(
                "SELECT session_key FROM conversation_meta WHERE last_touched < ?",
                (cutoff,),
            )]
            for key in stale:
                conn.execute("DELETE FROM conversations WHERE session_key = ?", (key,))
                conn.execute("DELETE FROM conversation_meta WHERE session_key = ?", (key,))
                removed += 1
            conn.commit()
    except sqlite3.Error as e:
        log.warning("ai_memory.prune_stale_sessions failed", error=str(e))
    return removed


def format_for_prompt(turns: Iterable[dict], max_chars: int = 1200) -> str:
    """Render `turns` (output of `get_recent_turns`) as a flat string
    suitable for an LLM prompt prefix. Truncates the OLDEST turns first
    if the total exceeds `max_chars`."""
    lines: list[str] = []
    for t in turns:
        role = t.get("role", "user")
        content = (t.get("content") or "").strip()
        if not content:
            continue
        # Cap per-turn at 400 chars to avoid one huge SQL block hogging
        # the budget.
        if len(content) > 400:
            content = content[:380] + "…(truncated)"
        lines.append(f"{role.upper()}: {content}")
    text = "\n".join(lines)
    if len(text) > max_chars:
        # Drop oldest lines until we fit.
        while lines and len(text) > max_chars:
            lines.pop(0)
            text = "\n".join(lines)
    return text
