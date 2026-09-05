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
    """Copy plugin static files to a runtime-writable directory and
    expose them under the venv's static dir via a symlink.

    Called on startup so plugin assets are servable at
    ``/static/plugins/{tab_id}/filename.js``.

    The flow:

    1. Copy each plugin's static files into ``PLUGIN_STATIC_DIR/{tab_id}/``
       (defaults to ``~/.tusk/plugin_static``). This is the runtime-
       writable canonical location — it survives venv rebuilds, and a
       Docker deploy can ship new assets without rebuilding the image.
    2. Symlink ``base_static_dir/plugins`` → ``PLUGIN_STATIC_DIR``. That
       way the existing ``/static`` mount serves both core assets AND
       plugin assets without needing a second StaticFilesConfig (which
       Litestar's prefix matcher couldn't disambiguate).

    On read-only venvs (some container deploys), the symlink step is
    skipped silently — the explicit `/static/plugins/...` route falls
    back to reading PLUGIN_STATIC_DIR directly.

    Args:
        base_static_dir: The main static directory (used for the symlink).
    """
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

    # No symlink: Starlette's StaticFiles security check rejects symlink
    # targets outside the configured directory. Plugin assets are served
    # by the explicit `/static/plugins/{plugin_id}/{filename:path}`
    # handler in PageController, which reads PLUGIN_STATIC_DIR directly.
    del base_static_dir


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
    """Deliberately a no-op (kept for import compatibility).

    This used to ``rmtree`` :data:`PLUGIN_STATIC_DIR` on shutdown. That
    directory lives under the user's HOME and is shared by every Tusk
    process using it, so any instance shutting down — an overlapping
    restart, a test server, or Granian recycling a worker every hour in
    production — wiped the assets of the instance that was still
    serving: every plugin .js/.css answered 404 (empty BI charts) until
    the next startup. Startup already does a fresh copy per plugin
    (:func:`setup_plugin_statics`), so there is nothing to clean here.

    Args:
        base_static_dir: Legacy parameter, ignored.
    """
    del base_static_dir
    log.debug("Plugin statics left in place on shutdown (shared directory)")
