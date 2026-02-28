"""Connection management for Tusk"""

from typing import Literal
from pathlib import Path
from urllib.parse import quote
import uuid
import tomllib
import tomli_w
import msgspec

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

    @property
    def dsn(self) -> str:
        """PostgreSQL connection string"""
        if self.type != "postgres":
            raise ValueError("DSN only for PostgreSQL")
        user = quote(self.user or "", safe="")
        password = quote(self.password or "", safe="")
        host = self.host or "localhost"
        database = self.database or "postgres"
        return f"postgresql://{user}:{password}@{host}:{self.port}/{database}"

    def to_dict(self, include_password: bool = False) -> dict:
        """Convert to dictionary for serialization"""
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
            if include_password and self.password is not None:
                data["password"] = self.password
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


def delete_connection(conn_id: str) -> bool:
    """Delete a connection and save to disk"""
    if conn_id in _connections:
        del _connections[conn_id]
        save_connections_to_file()
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
    )

    _connections[conn_id] = new_config
    save_connections_to_file()
    return new_config


def save_connections_to_file() -> None:
    """Save all connections to TOML file"""
    TUSK_DIR.mkdir(parents=True, exist_ok=True)

    data = {
        "connections": [
            conn.to_dict(include_password=True)
            for conn in _connections.values()
        ]
    }

    with open(CONN_FILE, "wb") as f:
        tomli_w.dump(data, f)


def load_connections_from_file() -> None:
    """Load connections from TOML file into registry"""
    if not CONN_FILE.exists():
        return

    with open(CONN_FILE, "rb") as f:
        data = tomllib.load(f)

    for conn_data in data.get("connections", []):
        config = ConnectionConfig(
            id=conn_data.get("id", uuid.uuid4().hex[:12]),
            name=conn_data["name"],
            type=conn_data["type"],
            host=conn_data.get("host"),
            port=conn_data.get("port", 5432),
            database=conn_data.get("database"),
            user=conn_data.get("user"),
            password=conn_data.get("password"),
            path=conn_data.get("path"),
        )
        add_connection(config, persist=False)
