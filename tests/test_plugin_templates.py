"""Plugin templates are copied to a writable directory, never into the venv."""

from __future__ import annotations

import importlib


def test_plugin_templates_land_outside_site_packages(tmp_path, monkeypatch):
    monkeypatch.setenv("TUSK_PLUGIN_TEMPLATE_DIR", str(tmp_path / "pt"))
    from tusk.plugins import templates as mod

    importlib.reload(mod)
    from tusk.plugins.registry import discover_plugins

    discover_plugins()
    fake_venv_templates = tmp_path / "site-packages-templates"
    fake_venv_templates.mkdir()
    root = mod.setup_plugin_templates(fake_venv_templates)  # legacy arg is ignored
    assert root == tmp_path / "pt"
    assert (root / "plugins" / "bi" / "overview.html").is_file()
    # Nothing was written next to the package.
    assert not (fake_venv_templates / "plugins").exists()
    # cleanup is a no-op: the copy survives for the next start
    mod.cleanup_plugin_templates()
    assert (root / "plugins" / "bi" / "overview.html").is_file()
