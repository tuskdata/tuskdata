"""Tests for connection management — DSN escaping, CRUD."""

import pytest

from tusk.core.connection import (
    ConnectionConfig,
    add_connection,
    get_connection,
    list_connections,
    delete_connection,
    update_connection,
    _connections,
)


@pytest.fixture(autouse=True)
def _clear_connections():
    """Clear in-memory connection registry between tests."""
    _connections.clear()
    yield
    _connections.clear()


class TestDSNEscaping:
    """Verify that special characters in DSN fields are properly escaped."""

    def test_basic_dsn(self):
        config = ConnectionConfig(
            name="test",
            type="postgres",
            host="localhost",
            port=5432,
            database="mydb",
            user="admin",
            password="simple",
        )
        dsn = config.dsn
        assert "postgresql://admin:simple@localhost:5432/mydb" == dsn

    def test_special_chars_in_password(self):
        config = ConnectionConfig(
            name="test",
            type="postgres",
            host="localhost",
            port=5432,
            database="mydb",
            user="admin",
            password="p@ss:w/rd#123",
        )
        dsn = config.dsn
        # Password should be URL-encoded
        assert "p%40ss%3Aw%2Frd%23123" in dsn
        assert "@localhost:5432/mydb" in dsn

    def test_special_chars_in_username(self):
        config = ConnectionConfig(
            name="test",
            type="postgres",
            host="localhost",
            port=5432,
            database="mydb",
            user="user@domain",
            password="pass",
        )
        dsn = config.dsn
        assert "user%40domain:pass@" in dsn

    def test_dsn_raises_for_non_postgres(self):
        config = ConnectionConfig(name="test", type="duckdb", path="/tmp/test.db")
        with pytest.raises(ValueError, match="DSN only for PostgreSQL"):
            _ = config.dsn


class TestConnectionCRUD:
    """Test in-memory connection registry operations."""

    def test_add_and_get(self):
        config = ConnectionConfig(name="test", type="postgres", host="localhost")
        conn_id = add_connection(config)
        assert conn_id == config.id

        result = get_connection(conn_id)
        assert result is not None
        assert result.name == "test"

    def test_list_connections(self):
        add_connection(ConnectionConfig(name="conn1", type="postgres", host="h1"))
        add_connection(ConnectionConfig(name="conn2", type="postgres", host="h2"))
        conns = list_connections()
        assert len(conns) == 2

    def test_delete_connection(self):
        config = ConnectionConfig(name="test", type="postgres", host="localhost")
        add_connection(config)
        assert delete_connection(config.id) is True
        assert get_connection(config.id) is None

    def test_delete_nonexistent(self):
        assert delete_connection("nonexistent") is False

    def test_update_connection(self):
        config = ConnectionConfig(name="old", type="postgres", host="localhost")
        add_connection(config)
        updated = update_connection(config.id, name="new")
        assert updated is not None
        assert updated.name == "new"
        assert updated.host == "localhost"  # Unchanged fields preserved

    def test_update_nonexistent(self):
        result = update_connection("nonexistent", name="new")
        assert result is None

    def test_to_dict_excludes_password(self):
        config = ConnectionConfig(
            name="test", type="postgres", host="localhost",
            user="admin", password="secret",
        )
        d = config.to_dict(include_password=False)
        assert "password" not in d

    def test_to_dict_includes_password(self):
        config = ConnectionConfig(
            name="test", type="postgres", host="localhost",
            user="admin", password="secret",
        )
        d = config.to_dict(include_password=True)
        assert d["password"] == "secret"
