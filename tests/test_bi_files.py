"""`tusk bi export` / `tusk bi import`: YAML round trip, idempotent by name."""

from __future__ import annotations

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _patch_storage(tmp_path):
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir()
    with patch("tusk.plugins.storage.get_plugins_dir", lambda: plugins_dir), \
         patch("tusk.plugins.storage.get_plugin_db_path", lambda pid: plugins_dir / f"{pid.replace('-', '_')}.db"), \
         patch("tusk.core.config.TUSK_DIR", tmp_path):
        from tusk.bi.db import init_db

        init_db()
        yield


def test_export_import_roundtrip(tmp_path, capsys):
    from tusk.bi import cli
    from tusk.bi.db import create_dashboard, create_data_source, create_saved_query, create_widget, get_dashboards, get_widgets

    src = create_data_source(name="local", source_type="sqlite", connection_ref=str(tmp_path / "x.db"))
    q = create_saved_query(name="count", sql="SELECT 1 AS n", source_id=src)
    d = create_dashboard(name="Ops board", description="demo")
    create_widget(d, widget_type="stat", query_id=q, title="Rows")

    cli._cmd_export(["all", "--out", str(tmp_path / "out")])
    files = sorted((tmp_path / "out").glob("*.yaml"))
    assert [f.name for f in files] == ["ops-board.yaml"]
    text = files[0].read_text()
    assert "Ops board" in text and "SELECT 1 AS n" in text

    # Importing twice does not duplicate: same name → replaced.
    cli._cmd_import([str(files[0])])
    cli._cmd_import([str(files[0])])
    boards = [b for b in get_dashboards() if b["name"] == "Ops board"]
    assert len(boards) == 1
    assert len(get_widgets(boards[0]["id"])) == 1
    out = capsys.readouterr().out
    assert "replaced" in out
