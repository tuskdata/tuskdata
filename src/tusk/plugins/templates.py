"""Template and static file loading for plugins.

MiniJinja only accepts one template directory.
We copy plugin templates to templates/plugins/{id}/
We copy plugin statics to static/plugins/{id}/

Example plugin template usage:
    # In plugin route
    return Template("plugins/security/dashboard.html", context=...)

Example plugin static usage:
    # In template
    <script src="/static/plugins/bi/chart.js"></script>
"""

import shutil
from pathlib import Path

from tusk.plugins.registry import get_all_plugins
from tusk.core.logging import get_logger

log = get_logger("plugins.templates")


def setup_plugin_templates(base_templates_dir: Path) -> None:
    """Copy plugin templates to main templates directory.

    Called on startup to make plugin templates available.

    Args:
        base_templates_dir: Main templates directory (tusk/studio/templates)
    """
    plugins_template_dir = base_templates_dir / "plugins"
    plugins_template_dir.mkdir(exist_ok=True)

    for plugin in get_all_plugins():
        templates_path = plugin.get_templates_path()
        if not templates_path or not templates_path.exists():
            continue

        dest = plugins_template_dir / plugin.tab_id

        # Remove old templates
        if dest.exists():
            shutil.rmtree(dest)

        # Copy new templates
        shutil.copytree(templates_path, dest)
        log.info("Plugin templates copied", plugin=plugin.name, dest=str(dest))


def setup_plugin_statics(base_static_dir: Path) -> None:
    """Copy plugin static files to main static directory.

    Called on startup to make plugin statics servable at
    /static/plugins/{tab_id}/filename.js

    Args:
        base_static_dir: Main static directory (tusk/studio/static)
    """
    plugins_static_dir = base_static_dir / "plugins"
    plugins_static_dir.mkdir(exist_ok=True)

    for plugin in get_all_plugins():
        static_path = plugin.get_static_path()
        if not static_path or not static_path.exists():
            continue

        dest = plugins_static_dir / plugin.tab_id

        # Remove old statics
        if dest.exists():
            shutil.rmtree(dest)

        # Copy new statics
        shutil.copytree(static_path, dest)
        log.info("Plugin statics copied", plugin=plugin.name, dest=str(dest))


def cleanup_plugin_templates(base_templates_dir: Path) -> None:
    """Remove plugin templates on shutdown.

    Args:
        base_templates_dir: Main templates directory
    """
    plugins_template_dir = base_templates_dir / "plugins"
    if plugins_template_dir.exists():
        shutil.rmtree(plugins_template_dir)
        log.debug("Plugin templates cleaned up")


def cleanup_plugin_statics(base_static_dir: Path) -> None:
    """Remove plugin static files on shutdown.

    Args:
        base_static_dir: Main static directory
    """
    plugins_static_dir = base_static_dir / "plugins"
    if plugins_static_dir.exists():
        shutil.rmtree(plugins_static_dir)
        log.debug("Plugin statics cleaned up")
