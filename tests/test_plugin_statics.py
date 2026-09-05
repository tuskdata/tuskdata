"""Plugin static assets must survive another process shutting down.

`PLUGIN_STATIC_DIR` (`~/.tusk/plugin_static`) is shared by every Tusk
process on the same HOME. Until 0.4.30 `on_shutdown` removed it, so an
overlapping restart or Granian's hourly worker recycle left the live
instance answering 404 for every plugin .js/.css (empty BI charts).
"""

from pathlib import Path

from tusk.plugins import templates as plugin_templates


def test_cleanup_plugin_statics_keeps_shared_dir(tmp_path: Path, monkeypatch):
    shared = tmp_path / "plugin_static"
    (shared / "bi").mkdir(parents=True)
    (shared / "bi" / "bi.js").write_text("// served to the live instance")
    monkeypatch.setattr(plugin_templates, "PLUGIN_STATIC_DIR", shared)

    plugin_templates.cleanup_plugin_statics(tmp_path / "unused")

    assert (shared / "bi" / "bi.js").exists()


def test_app_shutdown_does_not_wipe_plugin_statics():
    """Guard against re-adding the call: on_shutdown must not reference
    the cleanup helper at all."""
    import inspect

    from tusk.studio import app

    assert "cleanup_plugin_statics(STATIC_DIR)" not in inspect.getsource(app.on_shutdown)
