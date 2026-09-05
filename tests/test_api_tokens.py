"""Personal API tokens: minting, verification, revocation, and the request
paths that accept them (session middleware, CSRF exemption, MCP audit).
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from tusk.core import api_tokens
from tusk.core.auth import create_user, init_auth_db, log_audit, resolve_user  # noqa: F401


@pytest.fixture
def auth_db(tmp_path):
    db = tmp_path / "users.db"
    with patch("tusk.core.auth.AUTH_DB", db):
        init_auth_db()
        user = create_user("alice", "Secret123", email="a@example.com")
        yield db, user


# ── core ─────────────────────────────────────────────────────


def test_create_returns_plaintext_once_and_stores_hash(auth_db):
    db, user = auth_db
    token, plaintext = api_tokens.create_token(user.id, "laptop")

    assert plaintext.startswith("tusk_") and len(plaintext) > 30
    assert token.prefix == plaintext[:11]
    row = sqlite3.connect(db).execute("SELECT token_hash FROM api_tokens WHERE id = ?", (token.id,)).fetchone()
    assert row and row[0] != plaintext and len(row[0]) == 64


def test_verify_roundtrip_and_rejects_garbage(auth_db):
    _, user = auth_db
    token, plaintext = api_tokens.create_token(user.id, "ci")
    found = api_tokens.verify_token(plaintext)
    assert found and found.id == token.id and found.last_used_at
    assert api_tokens.verify_token("tusk_nope") is None
    assert api_tokens.verify_token("") is None
    assert api_tokens.verify_token("Bearer " + plaintext) is None


def test_revoke_stops_verification_and_respects_owner(auth_db):
    _, user = auth_db
    token, plaintext = api_tokens.create_token(user.id, "temp")
    assert api_tokens.revoke_token(token.id, user_id="someone-else") is False
    assert api_tokens.verify_token(plaintext) is not None
    assert api_tokens.revoke_token(token.id, user_id=user.id) is True
    assert api_tokens.verify_token(plaintext) is None
    assert api_tokens.list_tokens(user.id) == []
    assert len(api_tokens.list_tokens(user.id, include_revoked=True)) == 1


def test_expired_token_is_rejected(auth_db):
    db, user = auth_db
    token, plaintext = api_tokens.create_token(user.id, "short", expires_days=1)
    past = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    conn = sqlite3.connect(db)
    conn.execute("UPDATE api_tokens SET expires_at = ? WHERE id = ?", (past, token.id))
    conn.commit()
    conn.close()
    assert api_tokens.verify_token(plaintext) is None


def test_create_validates_input(auth_db):
    _, user = auth_db
    with pytest.raises(ValueError):
        api_tokens.create_token(user.id, "   ")
    with pytest.raises(ValueError):
        api_tokens.create_token(user.id, "x", expires_days=0)


def test_resolve_user_prefers_bearer_over_cookie(auth_db, monkeypatch):
    _, user = auth_db
    from tusk.core import config as cfg

    monkeypatch.setattr(cfg, "_config", cfg.TuskConfig(auth_mode="multi"))
    _, plaintext = api_tokens.create_token(user.id, "mcp")

    assert resolve_user({}, {"authorization": f"Bearer {plaintext}"}).id == user.id
    assert resolve_user({}, {"Authorization": f"bearer {plaintext}"}).id == user.id
    assert resolve_user({}, {"authorization": "Bearer tusk_bad"}) is None
    assert resolve_user({"tusk_session": "nope"}, {}) is None


# ── HTTP: middleware, CSRF, MCP ───────────────────────────────


@pytest.fixture(scope="module")
def multi_user_client():
    """App in multi-user mode with one user and one token."""
    home = tempfile.mkdtemp(prefix="tusk_tokens_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home

    # get_config() serves a module-level cache; swap it for a multi-user
    # config for the life of this module and put the old one back after.
    from tusk.core import auth as auth_mod
    from tusk.core import config as cfg

    old_config = cfg._config
    cfg._config = cfg.TuskConfig(auth_mode="multi")

    db = Path(home) / ".tusk" / "users.db"
    with patch.object(auth_mod, "AUTH_DB", db):
        auth_mod.init_auth_db()
        user = auth_mod.create_user("bob", "Secret123")
        _, plaintext = api_tokens.create_token(user.id, "test-client")

        from litestar.testing import TestClient
        from tusk.studio.app import app

        saved_startup = list(app.on_startup or [])
        saved_shutdown = list(app.on_shutdown or [])
        app.on_startup[:] = [f for f in saved_startup if not getattr(f, "__module__", "").startswith("tusk.")]
        app.on_shutdown[:] = [f for f in saved_shutdown if not getattr(f, "__module__", "").startswith("tusk.")]
        try:
            with TestClient(app=app) as client:
                yield client, plaintext, user, db
        finally:
            app.on_startup[:] = saved_startup
            app.on_shutdown[:] = saved_shutdown
            cfg._config = old_config


def _is_multi(client) -> bool:
    return client.get("/api/auth/status").json().get("auth_enabled") is True


def test_bearer_passes_session_middleware(multi_user_client):
    client, plaintext, user, _ = multi_user_client
    if not _is_multi(client):
        pytest.skip("app did not come up in multi-user mode (config override not honoured)")
    assert client.get("/api/connections").status_code == 401
    r = client.get("/api/connections", headers={"Authorization": f"Bearer {plaintext}"})
    assert r.status_code == 200
    status = client.get("/api/auth/status", headers={"Authorization": f"Bearer {plaintext}"}).json()
    assert status["user"]["username"] == "bob"


def test_bearer_without_cookie_skips_csrf(multi_user_client):
    client, plaintext, _, _ = multi_user_client
    if not _is_multi(client):
        pytest.skip("app did not come up in multi-user mode")
    # A state-changing call with only a Bearer header: no CSRF cookie, no 403.
    r = client.post(
        "/api/profile/tokens",
        json={"name": "from-script"},
        headers={"Authorization": f"Bearer {plaintext}"},
    )
    assert r.status_code in (200, 201), r.text
    assert r.json()["token"].startswith("tusk_")


def test_mcp_call_with_token_is_audited(multi_user_client):
    client, plaintext, user, db = multi_user_client
    if not _is_multi(client):
        pytest.skip("app did not come up in multi-user mode")
    meta = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {},
    }
    r = client.post(
        "/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": "list_connections", "arguments": {}, "_meta": meta}},
        headers={
            "Authorization": f"Bearer {plaintext}",
            "MCP-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "list_connections",
        },
    )
    assert r.status_code == 200, r.text
    rows = sqlite3.connect(db).execute(
        "SELECT user_id, action FROM audit_log WHERE action = 'mcp.list_connections'"
    ).fetchall()
    assert rows and rows[-1][0] == user.id


# ── log lines carry the user ──────────────────────────────────


def test_log_processor_adds_request_user():
    from tusk.core.logging import _correlation_processor, _request_user

    token = _request_user.set("alice")
    try:
        out = _correlation_processor(None, "info", {"event": "mcp_run_query"})
    finally:
        _request_user.reset(token)
    assert out["user"] == "alice"
    # Outside a request nothing is added.
    assert "user" not in _correlation_processor(None, "info", {"event": "x"})
