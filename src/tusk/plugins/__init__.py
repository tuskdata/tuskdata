"""Tusk Plugin System

Plugins extend Tusk functionality via entry_points.

Example plugin pyproject.toml:
    [project.entry-points."tusk.plugins"]
    security = "tusk_security:SecurityPlugin"
"""

from tusk.plugins.base import TuskPlugin
from tusk.plugins.registry import (
    discover_plugins,
    get_plugin,
    get_all_plugins,
    get_plugin_tabs,
    get_plugin_datasets,
)
from tusk.plugins.storage import (
    get_plugin_db,
    get_plugin_db_path,
    init_plugin_db,
    query_plugin_db,
)
from tusk.plugins.config import (
    get_plugin_config,
    save_plugin_config,
    get_plugin_config_value,
)
from tusk.plugins.templates import (
    setup_plugin_templates,
    cleanup_plugin_templates,
)

__all__ = [
    # Base class
    "TuskPlugin",
    # Registry
    "discover_plugins",
    "get_plugin",
    "get_all_plugins",
    "get_plugin_tabs",
    "get_plugin_datasets",
    # Storage
    "get_plugin_db",
    "get_plugin_db_path",
    "init_plugin_db",
    "query_plugin_db",
    # Config
    "get_plugin_config",
    "save_plugin_config",
    "get_plugin_config_value",
    # Templates
    "setup_plugin_templates",
    "cleanup_plugin_templates",
]
