"""Personal API tokens.

A token is the non-browser way into Tusk: MCP clients (Claude Code,
Cursor), scripts, CI. It stands in for the session cookie — the request
is treated exactly as if that user had logged in — so it inherits the
user's permissions and ownership rules, nothing more.

Tokens are only meaningful in multi-user mode (in single-user mode there
is nobody to be). They are stored hashed (SHA-256); the plaintext is shown
exactly once, at creation. Format: ``tusk_`` + 43 url-safe chars, so a
leaked token is easy to grep for.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone

import msgspec

from tusk.core import auth as _auth
from tusk.core import meta

TOKEN_PREFIX = "tusk_"
# How often `last_used_at` is refreshed. Every MCP call would otherwise be
# a write to users.db.
_LAST_USED_REFRESH = timedelta(minutes=5)


class ApiToken(msgspec.Struct):
    """A token row — never carries the secret."""

    id: str
    user_id: str
    name: str
    prefix: str  # first characters of the plaintext, for display only
    created_at: str
    last_used_at: str | None = None
    expires_at: str | None = None
    revoked_at: str | None = None

    @property
    def is_active(self) -> bool:
        if self.revoked_at:
            return False
        if self.expires_at and _parse(self.expires_at) < datetime.now(timezone.utc):
            return False
        return True


def _parse(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _hash(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def _connect() -> sqlite3.Connection:
    _auth.init_auth_db()
    conn = meta.connect(_auth.AUTH_DB)
    conn.row_factory = sqlite3.Row
    return conn


def _row(r: sqlite3.Row) -> ApiToken:
    return ApiToken(
        id=r["id"],
        user_id=r["user_id"],
        name=r["name"],
        prefix=r["prefix"],
        created_at=r["created_at"],
        last_used_at=r["last_used_at"],
        expires_at=r["expires_at"],
        revoked_at=r["revoked_at"],
    )


def create_token(user_id: str, name: str, expires_days: int | None = None) -> tuple[ApiToken, str]:
    """Mint a token for `user_id`. Returns (row, plaintext).

    The plaintext is returned exactly once and never stored.
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("token name is required")
    if expires_days is not None and expires_days <= 0:
        raise ValueError("expires_days must be positive")

    plaintext = TOKEN_PREFIX + secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(days=expires_days)).isoformat() if expires_days else None
    token = ApiToken(
        id=secrets.token_hex(6),
        user_id=user_id,
        name=name,
        prefix=plaintext[: len(TOKEN_PREFIX) + 6],
        created_at=now.isoformat(),
        expires_at=expires,
    )
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO api_tokens (id, user_id, name, token_hash, prefix, created_at, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (token.id, user_id, name, _hash(plaintext), token.prefix, token.created_at, expires),
        )
        conn.commit()
    finally:
        conn.close()
    return token, plaintext


def verify_token(plaintext: str) -> ApiToken | None:
    """Return the active token matching `plaintext`, or None.

    Constant-time as far as it matters: the lookup is by hash, so a wrong
    token simply finds no row.
    """
    if not plaintext or not plaintext.startswith(TOKEN_PREFIX):
        return None
    conn = _connect()
    try:
        r = conn.execute("SELECT * FROM api_tokens WHERE token_hash = ?", (_hash(plaintext),)).fetchone()
        if not r:
            return None
        token = _row(r)
        if not token.is_active:
            return None
        now = datetime.now(timezone.utc)
        if not token.last_used_at or now - _parse(token.last_used_at) > _LAST_USED_REFRESH:
            conn.execute("UPDATE api_tokens SET last_used_at = ? WHERE id = ?", (now.isoformat(), token.id))
            conn.commit()
            token.last_used_at = now.isoformat()
        return token
    finally:
        conn.close()


def list_tokens(user_id: str, include_revoked: bool = False) -> list[ApiToken]:
    conn = _connect()
    try:
        sql = "SELECT * FROM api_tokens WHERE user_id = ?"
        if not include_revoked:
            sql += " AND revoked_at IS NULL"
        rows = conn.execute(sql + " ORDER BY created_at DESC", (user_id,)).fetchall()
        return [_row(r) for r in rows]
    finally:
        conn.close()


def get_token(token_id: str) -> ApiToken | None:
    conn = _connect()
    try:
        r = conn.execute("SELECT * FROM api_tokens WHERE id = ?", (token_id,)).fetchone()
        return _row(r) if r else None
    finally:
        conn.close()


def revoke_token(token_id: str, user_id: str | None = None) -> bool:
    """Revoke a token. With `user_id`, only that user's token is touched
    (so a user can't revoke somebody else's by guessing an id)."""
    conn = _connect()
    try:
        params: list = [datetime.now(timezone.utc).isoformat(), token_id]
        sql = "UPDATE api_tokens SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL"
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
