"""Connection management for Tusk"""

import os
import stat
from typing import Literal
from pathlib import Path
from urllib.parse import quote
import uuid
import tomllib
import tomli_w
import msgspec

from tusk.core.crypto import encrypt, decrypt, is_encrypted

ConnectionType = Literal["postgres", "sqlite", "duckdb"]

TUSK_DIR = Path.home() / ".tusk"
CONN_FILE = TUSK_DIR / "connections.toml"


class ConnectionConfig(msgspec.Struct):
    """Database connection configuration"""

    name: str
    type: ConnectionType
    id: str = msgspec.field(default_factory=lambda: uuid.uuid4().hex[:12])

    # PostgreSQL fields
    host: str | None = None
    port: int = 5432
    database: str | None = None
    user: str | None = None
    password: str | None = None

    # SQLite fields
    path: str | None = None

    # Optional SSH tunnel (PostgreSQL only). When ssh_host is set, the
    # postgres engine opens a forwarded port via asyncssh and points
    # psycopg at the local end. ssh_password and ssh_private_key are
    # encrypted at rest just like `password`.
    ssh_host: str | None = None
    ssh_port: int = 22
    ssh_user: str | None = None
    ssh_password: str | None = None
    ssh_private_key: str | None = None  # PEM contents, not a path
    ssh_known_hosts: str | None = None  # optional pin; default = accept-new

    @property
    def dsn(self) -> str:
        """PostgreSQL connection string (direct, no tunnel)."""
        if self.type != "postgres":
            raise ValueError("DSN only for PostgreSQL")
        user = quote(self.user or "", safe="")
        password = quote(self.password or "", safe="")
        host = self.host or "localhost"
        database = self.database or "postgres"
        return f"postgresql://{user}:{password}@{host}:{self.port}/{database}"

    @property
    def uses_ssh_tunnel(self) -> bool:
        return bool(self.ssh_host and self.ssh_user)

    def local_dsn(self, local_port: int) -> str:
        """DSN pointing at a forwarded local port (for tunneled connections)."""
        if self.type != "postgres":
            raise ValueError("DSN only for PostgreSQL")
        user = quote(self.user or "", safe="")
        password = quote(self.password or "", safe="")
        database = self.database or "postgres"
        return f"postgresql://{user}:{password}@127.0.0.1:{local_port}/{database}"

    def to_dict(self, include_secrets: bool = False, include_password: bool | None = None) -> dict:
        """Convert to dictionary for serialization.

        `include_password` is the legacy alias kept for backwards compat
        — pass `include_secrets=True` for new code.
        """
        if include_password is not None:
            include_secrets = include_secrets or include_password

        data = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
        }
        if self.type == "postgres":
            if self.host is not None:
                data["host"] = self.host
            data["port"] = self.port
            if self.database is not None:
                data["database"] = self.database
            if self.user is not None:
                data["user"] = self.user
            if include_secrets and self.password is not None:
                data["password"] = self.password

            if self.ssh_host:
                data["ssh_host"] = self.ssh_host
                data["ssh_port"] = self.ssh_port
                if self.ssh_user is not None:
                    data["ssh_user"] = self.ssh_user
                if self.ssh_known_hosts is not None:
                    data["ssh_known_hosts"] = self.ssh_known_hosts
                if include_secrets:
                    if self.ssh_password is not None:
                        data["ssh_password"] = self.ssh_password
                    if self.ssh_private_key is not None:
                        data["ssh_private_key"] = self.ssh_private_key
        elif self.type in ("sqlite", "duckdb"):
            if self.path is not None:
                data["path"] = self.path
        return data


# In-memory registry
_connections: dict[str, ConnectionConfig] = {}


def add_connection(config: ConnectionConfig, persist: bool = True) -> str:
    """Add a connection to the registry and save to disk"""
    _connections[config.id] = config
    if persist:
        save_connections_to_file()
    return config.id


def get_connection(conn_id: str) -> ConnectionConfig | None:
    """Get a connection by ID"""
    return _connections.get(conn_id)


def list_connections() -> list[ConnectionConfig]:
    """List all connections"""
    return list(_connections.values())


def _drop_schema_cache(conn_id: str | None = None) -> None:
    """Best-effort: clear the postgres engine's in-process schema cache.

    Imported lazily to avoid a circular import (engines.postgres imports
    ConnectionConfig from this module).
    """
    try:
        from tusk.engines.postgres import invalidate_schema_cache
        invalidate_schema_cache(conn_id)
    except Exception:
        pass


def delete_connection(conn_id: str) -> bool:
    """Delete a connection and save to disk"""
    if conn_id in _connections:
        del _connections[conn_id]
        save_connections_to_file()
        _drop_schema_cache(conn_id)
        return True
    return False


def update_connection(conn_id: str, **kwargs) -> ConnectionConfig | None:
    """Update a connection's fields"""
    if conn_id not in _connections:
        return None

    old_config = _connections[conn_id]

    # Build new config with updated fields
    new_config = ConnectionConfig(
        id=conn_id,
        name=kwargs.get("name", old_config.name),
        type=kwargs.get("type", old_config.type),
        host=kwargs.get("host", old_config.host),
        port=kwargs.get("port", old_config.port),
        database=kwargs.get("database", old_config.database),
        user=kwargs.get("user", old_config.user),
        password=kwargs.get("password", old_config.password),
        path=kwargs.get("path", old_config.path),
        ssh_host=kwargs.get("ssh_host", old_config.ssh_host),
        ssh_port=kwargs.get("ssh_port", old_config.ssh_port),
        ssh_user=kwargs.get("ssh_user", old_config.ssh_user),
        ssh_password=kwargs.get("ssh_password", old_config.ssh_password),
        ssh_private_key=kwargs.get("ssh_private_key", old_config.ssh_private_key),
        ssh_known_hosts=kwargs.get("ssh_known_hosts", old_config.ssh_known_hosts),
    )

    _connections[conn_id] = new_config
    save_connections_to_file()
    _drop_schema_cache(conn_id)
    return new_config


_SECRET_FIELDS = ("password", "ssh_password", "ssh_private_key")


def save_connections_to_file() -> None:
    """Save all connections to TOML file.

    Every secret field (db password, ssh password, ssh private key) is
    encrypted with Fernet before writing. File mode is 0600.
    """
    TUSK_DIR.mkdir(parents=True, exist_ok=True)

    connections_data = []
    for conn in _connections.values():
        d = conn.to_dict(include_secrets=True)
        for field in _SECRET_FIELDS:
            value = d.get(field)
            if value:
                d[field] = encrypt(value) if not is_encrypted(value) else value
        connections_data.append(d)

    data = {"connections": connections_data}

    with open(CONN_FILE, "wb") as f:
        tomli_w.dump(data, f)

    try:
        os.chmod(CONN_FILE, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def load_connections_from_file() -> None:
    """Load connections from TOML file into registry.

    Decrypts secret fields that are prefixed as encrypted. Plain-text
    legacy values are accepted and re-saved encrypted on next write.
    """
    if not CONN_FILE.exists():
        return

    with open(CONN_FILE, "rb") as f:
        data = tomllib.load(f)

    needs_migration = False
    for conn_data in data.get("connections", []):
        decoded: dict = {}
        for field in _SECRET_FIELDS:
            raw = conn_data.get(field)
            if raw and not is_encrypted(raw):
                needs_migration = True
            decoded[field] = decrypt(raw) if raw else None

        config = ConnectionConfig(
            id=conn_data.get("id", uuid.uuid4().hex[:12]),
            name=conn_data["name"],
            type=conn_data["type"],
            host=conn_data.get("host"),
            port=conn_data.get("port", 5432),
            database=conn_data.get("database"),
            user=conn_data.get("user"),
            password=decoded["password"],
            path=conn_data.get("path"),
            ssh_host=conn_data.get("ssh_host"),
            ssh_port=conn_data.get("ssh_port", 22),
            ssh_user=conn_data.get("ssh_user"),
            ssh_password=decoded["ssh_password"],
            ssh_private_key=decoded["ssh_private_key"],
            ssh_known_hosts=conn_data.get("ssh_known_hosts"),
        )
        add_connection(config, persist=False)

    if needs_migration:
        save_connections_to_file()
