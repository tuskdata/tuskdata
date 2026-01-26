"""Connection management for Tusk"""

from typing import Literal
from pathlib import Path
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
    id: str = msgspec.field(default_factory=lambda: str(uuid.uuid4())[:8])

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
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def to_dict(self, include_password: bool = False) -> dict:
        """Convert to dictionary for serialization"""
        data = {
            "id": self.id,
            "name": self.name,
            "type": self.type,
        }
        if self.type == "postgres":
            data.update({
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user,
            })
            if include_password:
                data["password"] = self.password
        elif self.type in ("sqlite", "duckdb"):
            data["path"] = self.path
        return data


# In-memory registry
_connections: dict[str, ConnectionConfig] = {}


def add_connection(config: ConnectionConfig) -> str:
    """Add a connection to the registry"""
    _connections[config.id] = config
    return config.id


def get_connection(conn_id: str) -> ConnectionConfig | None:
    """Get a connection by ID"""
    return _connections.get(conn_id)


def list_connections() -> list[ConnectionConfig]:
    """List all connections"""
    return list(_connections.values())


def delete_connection(conn_id: str) -> bool:
    """Delete a connection"""
    if conn_id in _connections:
        del _connections[conn_id]
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
            id=conn_data.get("id", str(uuid.uuid4())[:8]),
            name=conn_data["name"],
            type=conn_data["type"],
            host=conn_data.get("host"),
            port=conn_data.get("port", 5432),
            database=conn_data.get("database"),
            user=conn_data.get("user"),
            password=conn_data.get("password"),
            path=conn_data.get("path"),
        )
        _connections[config.id] = config
