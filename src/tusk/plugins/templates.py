"""Template and static file loading for plugins.

MiniJinja only accepts one template directory, so plugin templates are
copied to ``templates/plugins/{id}/`` inside the venv at startup. This
is fine because templates are only consumed once at process start.

Plugin **static** assets are different — they're served live by
Litestar's StaticFiles handler and need to be writable at runtime so
deploys (Docker, Coolify) don't require a rebuild whenever a plugin
ships a new JS/CSS file. We relocate them to a runtime-writable
directory under the user's home (or ``$TUSK_PLUGIN_STATIC_DIR`` if set)
and the app registers a second ``StaticFilesConfig`` pointing there.

Example plugin template usage:
    # In plugin route
    return Template("plugins/security/dashboard.html", context=...)

Example plugin static usage:
    # In template
    <script src="/static/plugins/bi/chart.js"></script>
"""

import os
import re
import shutil
from pathlib import Path

from tusk.plugins.registry import get_all_plugins
from tusk.core.logging import get_logger

log = get_logger("plugins.templates")


# Runtime-writable directory for plugin static assets. Defaults to
# ``~/.tusk/plugin_static`` so that nothing inside the venv (read-only
# in many container deploys) needs to be touched. Override with
# ``TUSK_PLUGIN_STATIC_DIR`` for non-default volume layouts.
PLUGIN_STATIC_DIR = Path(
    os.environ.get("TUSK_PLUGIN_STATIC_DIR")
    or (Path.home() / ".tusk" / "plugin_static")
)


# Defense-in-depth: tab_id is already validated upstream in the plugin
# registry (audit fix #13), but we re-check here before constructing a
# filesystem destination. Reject anything that could escape the parent
# directory or smuggle separators.
_TAB_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,30}$")


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
    """Copy plugin static files to a runtime-writable directory.

    Called on startup to make plugin statics servable at
    ``/static/plugins/{tab_id}/filename.js``. The destination is
    :data:`PLUGIN_STATIC_DIR` (i.e. ``~/.tusk/plugin_static``), NOT the
    venv. The ``base_static_dir`` argument is kept for backwards
    compatibility but ignored.

    Args:
        base_static_dir: Legacy parameter, retained for back-compat.
    """
    del base_static_dir  # legacy: now writes to PLUGIN_STATIC_DIR

    PLUGIN_STATIC_DIR.mkdir(parents=True, exist_ok=True)

    for plugin in get_all_plugins():
        tab_id = plugin.tab_id
        if not _TAB_ID_RE.match(tab_id or ""):
            log.warning("Plugin skipped — invalid tab_id", plugin=plugin.name, tab_id=tab_id)
            continue

        static_path = plugin.get_static_path()
        if not static_path or not static_path.exists():
            continue

        dest = PLUGIN_STATIC_DIR / tab_id

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

    Cleans :data:`PLUGIN_STATIC_DIR`. The ``base_static_dir`` argument
    is retained for backwards compatibility but ignored.

    Args:
        base_static_dir: Legacy parameter, retained for back-compat.
    """
    del base_static_dir  # legacy: now cleans PLUGIN_STATIC_DIR

    if PLUGIN_STATIC_DIR.exists():
        shutil.rmtree(PLUGIN_STATIC_DIR, ignore_errors=True)
        log.debug("Plugin statics cleaned up")
