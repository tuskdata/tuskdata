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
# Re-export the job system so plugins can submit long-running scans
# (e.g. AdGuard query log fetch, network/port scans, dependency audits)
# without blocking the route handler. Import path stays stable across
# Tusk versions even if the underlying registry moves.
from tusk.core.jobs import (
    submit_job_sync,
    submit_job_async,
    get_registry as get_jobs_registry,
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
    # Background jobs
    "submit_job_sync",
    "submit_job_async",
    "get_jobs_registry",
]
