"""Lightweight unit tests for engine helpers and core utilities.

These don't need a real PostgreSQL — they cover pure-Python behavior:
DSN redaction, error-position parsing, query tracker bookkeeping,
backup format detection, rate-limit semantics, and SSH-tunnel input
validation.
"""

import pytest

from tusk.core.result import QueryResult, ColumnInfo


def test_query_result_from_error_carries_position():
    r = QueryResult.from_error("syntax error", position=42)
    assert r.error == "syntax error"
    assert r.error_position == 42
    d = r.to_dict()
    assert d["error_position"] == 42


def test_query_result_omits_position_when_absent():
    r = QueryResult.from_error("oops")
    d = r.to_dict()
    assert "error_position" not in d


def test_query_result_pagination_fields():
    r = QueryResult(
        columns=[ColumnInfo(name="x", type="int")],
        rows=[(1,)], row_count=1, execution_time_ms=0.5,
        total_count=1000, page=2, page_size=100,
    )
    d = r.to_dict()
    assert d["total_count"] == 1000
    assert d["page"] == 2


def test_redact_dsn_strips_credentials_url_form():
    from tusk.engines.postgres import _redact_dsn
    redacted = _redact_dsn("postgresql://user:secret@host:5432/db")
    assert "secret" not in redacted
    assert "user" not in redacted
    assert "host" in redacted


def test_redact_dsn_strips_credentials_keyword_form():
    from tusk.engines.postgres import _redact_dsn
    redacted = _redact_dsn("host=db.example.com user=foo password=topsecret")
    assert "topsecret" not in redacted
    assert "***" in redacted


def test_error_position_returns_none_for_plain_exception():
    from tusk.engines.postgres import _error_position
    assert _error_position(ValueError("nope")) is None


def test_error_position_handles_diag_attr():
    from tusk.engines.postgres import _error_position

    class Diag:
        statement_position = 17

    class Exc(Exception):
        diag = Diag()

    assert _error_position(Exc("bad sql")) == 17


def test_query_tracker_register_and_clear():
    from tusk.core import query_tracker

    rid = "test-rid-1"
    q = query_tracker.TrackedQuery(
        request_id=rid, connection_id="c1", engine="postgres",
    )
    query_tracker.register(q)
    assert any(t.request_id == rid for t in query_tracker.list_active())
    query_tracker.update(rid, pid=12345)
    info = query_tracker.get(rid)
    assert info is not None
    assert info.pid == 12345
    query_tracker.unregister(rid)
    assert not any(t.request_id == rid for t in query_tracker.list_active())


def test_backup_format_detection_from_filename():
    from tusk.admin.backup import _format_from_filename, _is_backup_file
    assert _format_from_filename("mydb_2026-04-26.sql.gz") == "plain"
    assert _format_from_filename("mydb.dump") == "custom"
    assert _format_from_filename("mydb_archive.tar.gz") == "directory"
    assert _format_from_filename("unknown.zip") == "plain"  # safe default
    assert _is_backup_file("ok.sql.gz")
    assert _is_backup_file("ok.dump")
    assert _is_backup_file("ok.tar.gz")
    assert not _is_backup_file("ok.txt")


def test_rate_limit_per_ip_window():
    from tusk.core import rate_limit

    bucket = "test-bucket"
    ip1 = "1.2.3.4"
    # First 3 attempts should be allowed
    for _ in range(3):
        assert rate_limit.check_and_record(bucket, ip1, max_attempts=3, window_seconds=60)
    # 4th attempt blocked
    assert not rate_limit.check_and_record(bucket, ip1, max_attempts=3, window_seconds=60)
    # Different IP gets a fresh budget
    assert rate_limit.check_and_record(bucket, "9.9.9.9", max_attempts=3, window_seconds=60)


def test_ssh_tunnel_close_all_when_empty_is_safe():
    """Closing tunnels with none open must not raise."""
    import asyncio
    from tusk.core import ssh_tunnel
    asyncio.run(ssh_tunnel.close_all_tunnels())


def test_settings_validator_rejects_bad_names():
    """`set_setting` must refuse anything that isn't a safe identifier."""
    from tusk.admin.processes import set_setting
    import asyncio
    from tusk.core.connection import ConnectionConfig

    cfg = ConnectionConfig(
        id="x", name="x", type="postgres",
        host="localhost", port=5432, user="x",
        database="x", password="x",
    )
    ok, msg = asyncio.run(set_setting(cfg, "; DROP TABLE users", "0"))
    assert ok is False
    assert "Invalid" in msg


def test_format_to_extension_round_trip():
    from tusk.admin.backup import _format_to_extension, _format_from_filename
    for fmt in ("plain", "custom", "directory"):
        ext = _format_to_extension(fmt)
        # Round-trip via a synthetic filename
        assert _format_from_filename(f"foo.{ext}") == fmt
