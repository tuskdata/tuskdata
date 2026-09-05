"""Plugin discovery and registration"""

import sys
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from tusk.core.logging import get_logger

if TYPE_CHECKING:
    from tusk.plugins.base import TuskPlugin

log = get_logger("plugins")

# Global registry
_plugins: dict[str, "TuskPlugin"] = {}
_discovered: bool = False


def _load_bi_plugin() -> "TuskPlugin":
    from tusk.bi import BIPlugin

    return BIPlugin()


_BUILTIN_PLUGINS = [_load_bi_plugin]


def discover_plugins() -> dict[str, "TuskPlugin"]:
    """Discover and load plugins from entry_points.

    Plugins register via pyproject.toml:
    [project.entry-points."tusk.plugins"]
    security = "tusk_security:SecurityPlugin"

    Returns:
        Dict mapping plugin name to plugin instance
    """
    global _plugins, _discovered

    if _discovered:
        return _plugins

    # Built-in plugins shipped inside the core package (tusk-bi moved in
    # with 0.4.36). They keep the plugin shape — tab, templates, statics,
    # storage, CLI — so nothing else in the app had to change; they just
    # don't need an entry point or a separate wheel.
    for loader in _BUILTIN_PLUGINS:
        try:
            plugin = loader()
            _plugins[plugin.name] = plugin
            log.info("Built-in plugin loaded", plugin=plugin.name, version=plugin.version)
        except Exception as e:  # noqa: BLE001
            log.error("Failed to load built-in plugin", error=str(e))

    # Python 3.10+ style
    if sys.version_info >= (3, 10):
        eps = entry_points(group="tusk.plugins")
    else:
        all_eps = entry_points()
        eps = all_eps.get("tusk.plugins", [])

    for ep in eps:
        try:
            plugin_class = ep.load()
            plugin = plugin_class()

            # An external wheel of a plugin that is now built in (e.g. a
            # stale tusk-bi install): the core copy wins.
            if plugin.name in _plugins:
                log.warning("Plugin already provided by core, skipping entry point",
                            plugin=plugin.name, entry_point=ep.name)
                continue

            # Check compatibility
            if not plugin.is_compatible():
                log.warning(
                    "Plugin incompatible",
                    plugin=plugin.name,
                    min_version=plugin.min_tusk_version,
                )
                continue

            _plugins[plugin.name] = plugin
            log.info("Plugin loaded", plugin=plugin.name, version=plugin.version)

        except Exception as e:
            log.error("Failed to load plugin", entry_point=ep.name, error=str(e))

    _discovered = True
    return _plugins


def get_plugin(name: str) -> "TuskPlugin | None":
    """Get a loaded plugin by name"""
    if not _discovered:
        discover_plugins()
    return _plugins.get(name)


def get_all_plugins() -> list["TuskPlugin"]:
    """Get all loaded plugins"""
    if not _discovered:
        discover_plugins()
    return list(_plugins.values())


def get_plugin_tabs() -> list[dict]:
    """Get tab info for all plugins (for sidebar rendering)

    Returns:
        List of tab dicts with id, label, icon, url
    """
    if not _discovered:
        discover_plugins()

    tabs = []
    for plugin in _plugins.values():
        tabs.append({
            "id": plugin.tab_id,
            "label": plugin.tab_label,
            "icon": plugin.tab_icon,
            "url": plugin.tab_url,
        })
    return sorted(tabs, key=lambda t: t["label"])


def get_plugin_datasets() -> list[dict]:
    """Get all datasets exposed by plugins (for Data module)

    Returns:
        List of dataset descriptors with db_path for DuckDB sqlite_scan
    """
    if not _discovered:
        discover_plugins()

    from tusk.plugins.storage import get_plugin_db_path

    datasets = []
    for plugin in _plugins.values():
        if not plugin.requires_storage:
            continue

        db_path = get_plugin_db_path(plugin.name)
        if not db_path.exists():
            continue

        for ds in plugin.get_datasets():
            datasets.append({
                "name": f"plugin_{plugin.tab_id}_{ds['name']}",
                "description": ds.get("description", ""),
                "source": "plugin",
                "plugin": plugin.name,
                "db_path": str(db_path),
                "table": ds["table"],
            })

    return datasets


def get_plugin_cli_commands() -> dict[str, tuple["TuskPlugin", callable]]:
    """Get all CLI commands from plugins

    Returns:
        Dict mapping command name to (plugin, handler) tuple
    """
    if not _discovered:
        discover_plugins()

    commands = {}
    for plugin in _plugins.values():
        for cmd_name, handler in plugin.get_cli_commands().items():
            commands[cmd_name] = (plugin, handler)
    return commands


def get_plugin_route_handlers() -> list:
    """Get all route handlers from plugins

    Returns:
        List of Litestar Controller classes
    """
    if not _discovered:
        discover_plugins()

    handlers = []
    for plugin in _plugins.values():
        handlers.extend(plugin.get_route_handlers())
    return handlers


def reset_registry() -> None:
    """Reset the plugin registry (for testing)"""
    global _plugins, _discovered
    _plugins = {}
    _discovered = False
