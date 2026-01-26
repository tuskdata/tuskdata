"""Global configuration for Tusk"""

import tomllib
import tomli_w
from pathlib import Path
from typing import Any
import msgspec

TUSK_DIR = Path.home() / ".tusk"
CONFIG_FILE = TUSK_DIR / "config.toml"


class TuskConfig(msgspec.Struct):
    """Global Tusk configuration"""

    # PostgreSQL client binaries path (for pg_dump, psql, etc.)
    pg_bin_path: str | None = None

    # Server settings
    host: str = "127.0.0.1"
    port: int = 8000

    # UI settings
    theme: str = "dark"
    editor_font_size: int = 14

    # Auth settings
    auth_mode: str = "single"  # "single" or "multi"
    session_lifetime: int = 86400  # 24 hours in seconds
    allow_registration: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for TOML serialization"""
        return {
            "postgresql": {
                "bin_path": self.pg_bin_path,
            },
            "server": {
                "host": self.host,
                "port": self.port,
            },
            "ui": {
                "theme": self.theme,
                "editor_font_size": self.editor_font_size,
            },
            "auth": {
                "mode": self.auth_mode,
                "session_lifetime": self.session_lifetime,
                "allow_registration": self.allow_registration,
            },
        }


# Global config instance
_config: TuskConfig | None = None


def get_config() -> TuskConfig:
    """Get the global configuration (loads from file if needed)"""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def load_config() -> TuskConfig:
    """Load configuration from file"""
    global _config

    if not CONFIG_FILE.exists():
        _config = TuskConfig()
        return _config

    try:
        with open(CONFIG_FILE, "rb") as f:
            data = tomllib.load(f)

        _config = TuskConfig(
            pg_bin_path=data.get("postgresql", {}).get("bin_path"),
            host=data.get("server", {}).get("host", "127.0.0.1"),
            port=data.get("server", {}).get("port", 8000),
            theme=data.get("ui", {}).get("theme", "dark"),
            editor_font_size=data.get("ui", {}).get("editor_font_size", 14),
            auth_mode=data.get("auth", {}).get("mode", "single"),
            session_lifetime=data.get("auth", {}).get("session_lifetime", 86400),
            allow_registration=data.get("auth", {}).get("allow_registration", False),
        )
        return _config

    except Exception:
        _config = TuskConfig()
        return _config


def save_config(config: TuskConfig | None = None) -> None:
    """Save configuration to file"""
    global _config

    if config is not None:
        _config = config

    if _config is None:
        _config = TuskConfig()

    TUSK_DIR.mkdir(parents=True, exist_ok=True)

    # Filter out None values for cleaner TOML
    data = {}

    if _config.pg_bin_path:
        data["postgresql"] = {"bin_path": _config.pg_bin_path}

    data["server"] = {
        "host": _config.host,
        "port": _config.port,
    }

    data["ui"] = {
        "theme": _config.theme,
        "editor_font_size": _config.editor_font_size,
    }

    data["auth"] = {
        "mode": _config.auth_mode,
        "session_lifetime": _config.session_lifetime,
        "allow_registration": _config.allow_registration,
    }

    with open(CONFIG_FILE, "wb") as f:
        tomli_w.dump(data, f)


def update_config(**kwargs) -> TuskConfig:
    """Update specific config values and save"""
    global _config

    if _config is None:
        _config = load_config()

    # Update fields
    for key, value in kwargs.items():
        if hasattr(_config, key):
            # Create new config with updated value (msgspec.Struct is immutable)
            current = {
                "pg_bin_path": _config.pg_bin_path,
                "host": _config.host,
                "port": _config.port,
                "theme": _config.theme,
                "editor_font_size": _config.editor_font_size,
                "auth_mode": _config.auth_mode,
                "session_lifetime": _config.session_lifetime,
                "allow_registration": _config.allow_registration,
            }
            current[key] = value
            _config = TuskConfig(**current)

    save_config(_config)
    return _config


def set_pg_bin_path(path: str | None) -> None:
    """Set the PostgreSQL binaries path"""
    update_config(pg_bin_path=path)
