"""TOML configuration for plugins.

Each plugin gets its own config file: ~/.tusk/plugins/{id}.toml

Example:
    from tusk.plugins.config import get_plugin_config, save_plugin_config

    # Load config
    config = get_plugin_config("tusk-security")
    adguard_url = config.get("adguard", {}).get("url", "http://localhost:3000")

    # Save config
    save_plugin_config("tusk-security", {
        "adguard": {"url": "http://192.168.1.1:3000", "username": "admin"}
    })

    # Get nested value with dot notation
    url = get_plugin_config_value("tusk-security", "adguard.url", default="")
"""

from pathlib import Path
from typing import Any

import tomllib
import tomli_w

from tusk.plugins.storage import get_plugins_dir


def get_plugin_config_path(plugin_id: str) -> Path:
    """Get path to plugin's config file

    Args:
        plugin_id: Plugin identifier (e.g., 'tusk-security')

    Returns:
        Path to ~/.tusk/plugins/{sanitized_id}.toml
    """
    safe_id = plugin_id.replace("-", "_").replace(".", "_")
    return get_plugins_dir() / f"{safe_id}.toml"


def get_plugin_config(plugin_id: str) -> dict[str, Any]:
    """Load plugin configuration.

    Args:
        plugin_id: Plugin identifier

    Returns:
        Config dict (empty dict if config doesn't exist)
    """
    config_path = get_plugin_config_path(plugin_id)

    if not config_path.exists():
        return {}

    with open(config_path, "rb") as f:
        return tomllib.load(f)


def save_plugin_config(plugin_id: str, config: dict[str, Any]) -> None:
    """Save plugin configuration.

    Args:
        plugin_id: Plugin identifier
        config: Config dict to save
    """
    config_path = get_plugin_config_path(plugin_id)

    # Ensure plugins directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)

    with open(config_path, "wb") as f:
        tomli_w.dump(config, f)


def get_plugin_config_value(plugin_id: str, key: str, default: Any = None) -> Any:
    """Get a specific config value with dot notation support.

    Args:
        plugin_id: Plugin identifier
        key: Config key with optional dot notation (e.g., "adguard.url")
        default: Default value if key not found

    Returns:
        Config value or default

    Example:
        get_plugin_config_value("tusk-security", "adguard.url")
        get_plugin_config_value("tusk-security", "scan.timeout", default=300)
    """
    config = get_plugin_config(plugin_id)

    keys = key.split(".")
    value = config

    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default

    return value


def set_plugin_config_value(plugin_id: str, key: str, value: Any) -> None:
    """Set a specific config value with dot notation support.

    Args:
        plugin_id: Plugin identifier
        key: Config key with optional dot notation (e.g., "adguard.url")
        value: Value to set

    Example:
        set_plugin_config_value("tusk-security", "adguard.url", "http://localhost:3000")
    """
    config = get_plugin_config(plugin_id)

    keys = key.split(".")
    current = config

    # Navigate to parent of final key, creating dicts as needed
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]

    # Set the final value
    current[keys[-1]] = value

    save_plugin_config(plugin_id, config)
