"""Tests for tusk-bi plugin.

Covers: models, DB CRUD, query engine, chart config builder,
dashboard operations, snapshot rotation, cron parsing, prebuilt dashboards.
"""

import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import tusk.bi  # noqa: F401 — template tests resolve paths from the package


# ─────────────────────────────────────────────────────────────
# Fixtures: patch storage to use temp dir
# ─────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _patch_storage(tmp_path):
    """Redirect all plugin storage to a temp directory."""
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir()

    def _get_plugins_dir():
        return plugins_dir

    def _get_plugin_db_path(plugin_id):
        safe_id = plugin_id.replace("-", "_").replace(".", "_")
        return plugins_dir / f"{safe_id}.db"

    with patch("tusk.plugins.storage.get_plugins_dir", _get_plugins_dir), \
         patch("tusk.plugins.storage.get_plugin_db_path", _get_plugin_db_path), \
         patch("tusk.core.config.TUSK_DIR", tmp_path):
        yield


@pytest.fixture
def db_init():
    """Initialize the BI database."""
    from tusk.bi.db import init_db
    init_db()


# ─────────────────────────────────────────────────────────────
# Model Tests
# ─────────────────────────────────────────────────────────────

class TestModels:
    def test_data_source_defaults(self):
        from tusk.bi.models import DataSource
        ds = DataSource()
        assert ds.id == 0
        assert ds.source_type == "sqlite"
        assert ds.plugin_id is None
        assert ds.tables == []

    def test_saved_query_defaults(self):
        from tusk.bi.models import SavedQuery
        q = SavedQuery()
        assert q.id == 0
        assert q.chart_config == "{}"
        assert q.last_executed_at is None

    def test_widget_defaults(self):
        from tusk.bi.models import Widget
        w = Widget()
        assert w.col_start == 1
        assert w.col_span == 6
        assert w.row_start == 1
        assert w.row_span == 4
        assert w.widget_type == "chart"

    def test_dashboard_defaults(self):
        from tusk.bi.models import Dashboard
        d = Dashboard()
        assert d.is_default is False
        assert d.is_prebuilt is False
        assert d.filters == "[]"
        # v0.3.0 fields: opt-in by default
        assert d.is_public is False
        assert d.refresh_interval_seconds == 0

    def test_chart_config_defaults(self):
        from tusk.bi.models import ChartConfig
        cc = ChartConfig()
        assert cc.chart_type == "bar"
        assert cc.stacked is False
        assert cc.show_legend is True

    def test_query_parameter_defaults(self):
        from tusk.bi.models import QueryParameter
        qp = QueryParameter()
        assert qp.param_type == "text"
        assert qp.options == []

    def test_snapshot_summary_defaults(self):
        from tusk.bi.models import SnapshotSummary
        ss = SnapshotSummary()
        assert ss.value is None
        assert ss.row_count == 0

    def test_dashboard_filter_defaults(self):
        from tusk.bi.models import DashboardFilter
        df = DashboardFilter()
        assert df.filter_type == "select"

    def test_model_serialization(self):
        import msgspec
        from tusk.bi.models import DataSource
        ds = DataSource(id=1, name="test", source_type="sqlite")
        data = msgspec.json.encode(ds)
        decoded = msgspec.json.decode(data, type=DataSource)
        assert decoded.id == 1
        assert decoded.name == "test"


# ─────────────────────────────────────────────────────────────
# DB CRUD Tests
# ─────────────────────────────────────────────────────────────

class TestDBDataSources:
    def test_create_and_get_source(self, db_init):
        from tusk.bi.db import create_data_source, get_data_source, get_data_sources
        sid = create_data_source("Test DB", "sqlite", "/tmp/test.db")
        assert sid > 0

        source = get_data_source(sid)
        assert source is not None
        assert source["name"] == "Test DB"
        assert source["source_type"] == "sqlite"

        sources = get_data_sources()
        assert len(sources) == 1

    def test_delete_source(self, db_init):
        from tusk.bi.db import create_data_source, delete_data_source, get_data_sources
        sid = create_data_source("To Delete", "sqlite", "/tmp/x.db")
        delete_data_source(sid)
        assert len(get_data_sources()) == 0

    def test_get_nonexistent_source(self, db_init):
        from tusk.bi.db import get_data_source
        assert get_data_source(999) is None


class TestDBQueries:
    def test_create_and_get_query(self, db_init):
        from tusk.bi.db import create_data_source, create_saved_query, get_saved_query

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(
            name="Test Query",
            source_id=sid,
            sql="SELECT 1",
            description="A test query",
            chart_type="bar",
            tags="test,demo",
        )
        assert qid > 0

        query = get_saved_query(qid)
        assert query is not None
        assert query["name"] == "Test Query"
        assert query["sql"] == "SELECT 1"
        assert query["chart_type"] == "bar"
        assert query["tags"] == "test,demo"
        assert query["source_name"] == "DB"

    def test_update_query(self, db_init):
        from tusk.bi.db import create_data_source, create_saved_query, update_saved_query, get_saved_query

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Old Name", source_id=sid, sql="SELECT 1")

        update_saved_query(qid, name="New Name", sql="SELECT 2")
        query = get_saved_query(qid)
        assert query["name"] == "New Name"
        assert query["sql"] == "SELECT 2"

    def test_delete_query(self, db_init):
        from tusk.bi.db import create_data_source, create_saved_query, delete_saved_query, get_saved_queries

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        delete_saved_query(qid)
        assert len(get_saved_queries()) == 0

    def test_list_queries_with_filter(self, db_init):
        from tusk.bi.db import create_data_source, create_saved_query, get_saved_queries

        s1 = create_data_source("DB1", "sqlite", "/tmp/1.db")
        s2 = create_data_source("DB2", "sqlite", "/tmp/2.db")

        create_saved_query(name="Q1", source_id=s1, sql="SELECT 1", tags="alpha")
        create_saved_query(name="Q2", source_id=s2, sql="SELECT 2", tags="beta")

        all_q = get_saved_queries()
        assert len(all_q) == 2

        filtered = get_saved_queries(source_id=s1)
        assert len(filtered) == 1
        assert filtered[0]["name"] == "Q1"

        tagged = get_saved_queries(tag="alpha")
        assert len(tagged) == 1

    def test_mark_query_executed(self, db_init):
        from tusk.bi.db import create_data_source, create_saved_query, mark_query_executed, get_saved_query

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        mark_query_executed(qid)
        query = get_saved_query(qid)
        assert query["last_executed_at"] is not None


class TestDBDashboards:
    def test_create_and_get_dashboard(self, db_init):
        from tusk.bi.db import create_dashboard, get_dashboard, get_dashboards

        did = create_dashboard("My Dashboard", "A test dashboard")
        assert did > 0

        dash = get_dashboard(did)
        assert dash["name"] == "My Dashboard"
        assert dash["description"] == "A test dashboard"
        assert dash["is_default"] == 0

        assert len(get_dashboards()) == 1

    def test_default_dashboard(self, db_init):
        from tusk.bi.db import create_dashboard, get_default_dashboard

        create_dashboard("D1", is_default=True)
        assert get_default_dashboard() is not None
        assert get_default_dashboard()["name"] == "D1"

        # Creating another default should unset the first
        create_dashboard("D2", is_default=True)
        default = get_default_dashboard()
        assert default["name"] == "D2"

    def test_update_dashboard(self, db_init):
        from tusk.bi.db import create_dashboard, update_dashboard, get_dashboard

        did = create_dashboard("Old")
        update_dashboard(did, name="New", description="Updated")
        dash = get_dashboard(did)
        assert dash["name"] == "New"
        assert dash["description"] == "Updated"

    def test_update_dashboard_public_and_refresh(self, db_init):
        """v0.3.0 fields: is_public toggle + clamped refresh interval."""
        from tusk.bi.db import create_dashboard, update_dashboard, get_dashboard

        did = create_dashboard("D")

        # Defaults
        dash = get_dashboard(did)
        assert dash["is_public"] == 0
        assert dash["refresh_interval_seconds"] == 0

        # Toggle public
        update_dashboard(did, is_public=True)
        dash = get_dashboard(did)
        assert dash["is_public"] == 1

        # Set a valid interval
        update_dashboard(did, refresh_interval_seconds=60)
        assert get_dashboard(did)["refresh_interval_seconds"] == 60

        # Below band clamps up to 5s
        update_dashboard(did, refresh_interval_seconds=2)
        assert get_dashboard(did)["refresh_interval_seconds"] == 5

        # Above band clamps down to 3600s
        update_dashboard(did, refresh_interval_seconds=99999)
        assert get_dashboard(did)["refresh_interval_seconds"] == 3600

        # 0 means off and stays 0 (special-cased, no clamp)
        update_dashboard(did, refresh_interval_seconds=0)
        assert get_dashboard(did)["refresh_interval_seconds"] == 0

    def test_delete_dashboard_cascades_widgets(self, db_init):
        from tusk.bi.db import (
            create_dashboard, create_data_source, create_saved_query,
            create_widget, delete_dashboard, get_dashboards, get_widgets,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        did = create_dashboard("D")
        create_widget(dashboard_id=did, query_id=qid, title="W1")
        create_widget(dashboard_id=did, query_id=qid, title="W2")

        delete_dashboard(did)
        assert len(get_dashboards()) == 0
        assert len(get_widgets(did)) == 0

    def test_clone_dashboard(self, db_init):
        from tusk.bi.db import (
            create_dashboard, create_data_source, create_saved_query,
            create_widget, clone_dashboard, get_dashboard, get_widgets,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        did = create_dashboard("Original")
        create_widget(dashboard_id=did, query_id=qid, title="W1")
        create_widget(dashboard_id=did, query_id=qid, title="W2")

        new_id = clone_dashboard(did)
        assert new_id is not None

        new_dash = get_dashboard(new_id)
        assert "Copy" in new_dash["name"]

        new_widgets = get_widgets(new_id)
        assert len(new_widgets) == 2


class TestDBWidgets:
    def test_create_and_get_widgets(self, db_init):
        from tusk.bi.db import (
            create_dashboard, create_data_source, create_saved_query,
            create_widget, get_widgets, get_widget,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        did = create_dashboard("D")

        wid = create_widget(
            dashboard_id=did, query_id=qid,
            widget_type="chart", title="My Chart",
            col_start=1, col_span=6, row_start=1, row_span=4,
        )
        assert wid > 0

        widgets = get_widgets(did)
        assert len(widgets) == 1
        assert widgets[0]["title"] == "My Chart"

        widget = get_widget(wid)
        assert widget is not None
        assert widget["query_sql"] == "SELECT 1"

    def test_update_widget_position(self, db_init):
        from tusk.bi.db import (
            create_dashboard, create_widget, update_widget, get_widget,
        )

        did = create_dashboard("D")
        wid = create_widget(dashboard_id=did, title="W", col_start=1, col_span=6)

        update_widget(wid, col_start=4, col_span=8, row_start=3, row_span=6)
        widget = get_widget(wid)
        assert widget["col_start"] == 4
        assert widget["col_span"] == 8
        assert widget["row_start"] == 3
        assert widget["row_span"] == 6

    def test_delete_widget(self, db_init):
        from tusk.bi.db import create_dashboard, create_widget, delete_widget, get_widgets

        did = create_dashboard("D")
        wid = create_widget(dashboard_id=did, title="W")
        delete_widget(wid)
        assert len(get_widgets(did)) == 0


# ─────────────────────────────────────────────────────────────
# Snapshot Tests
# ─────────────────────────────────────────────────────────────

class TestSnapshots:
    def test_save_and_get_snapshots(self, db_init):
        from tusk.bi.db import (
            create_data_source, create_saved_query,
            save_snapshot, get_snapshots,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")

        save_snapshot(qid, row_count=5, data='{"columns":[],"rows":[]}', value=42.0)
        save_snapshot(qid, row_count=10, data='{}', value=84.0)

        snaps = get_snapshots(qid)
        assert len(snaps) == 2
        # Both values should be present (order depends on insertion time granularity)
        values = {s["value"] for s in snaps}
        assert 42.0 in values
        assert 84.0 in values

    def test_rotate_snapshots(self, db_init):
        from tusk.bi.db import (
            create_data_source, create_saved_query,
            save_snapshot, rotate_snapshots, get_snapshots,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")

        # Create 5 snapshots
        for i in range(5):
            save_snapshot(qid, row_count=i, data='{}', value=float(i))

        # Keep only 3
        deleted = rotate_snapshots(qid, max_keep=3)
        assert deleted == 2

        remaining = get_snapshots(qid)
        assert len(remaining) == 3

    def test_rotate_under_limit(self, db_init):
        from tusk.bi.db import (
            create_data_source, create_saved_query,
            save_snapshot, rotate_snapshots,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")

        save_snapshot(qid, row_count=1, data='{}')
        deleted = rotate_snapshots(qid, max_keep=10)
        assert deleted == 0


# ─────────────────────────────────────────────────────────────
# Schedule Tests
# ─────────────────────────────────────────────────────────────

class TestSchedules:
    def test_create_and_list_schedules(self, db_init):
        from tusk.bi.db import (
            create_data_source, create_saved_query,
            create_schedule, get_schedules,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")

        create_schedule(qid, "*/15 * * * *", max_snapshots=50)
        schedules = get_schedules()
        assert len(schedules) == 1
        assert schedules[0]["cron_expr"] == "*/15 * * * *"
        assert schedules[0]["max_snapshots"] == 50

    def test_delete_schedule(self, db_init):
        from tusk.bi.db import (
            create_data_source, create_saved_query,
            create_schedule, delete_schedule, get_schedules,
        )

        sid = create_data_source("DB", "sqlite", "/tmp/db.db")
        qid = create_saved_query(name="Q", source_id=sid, sql="SELECT 1")
        sc_id = create_schedule(qid, "0 * * * *")
        delete_schedule(sc_id)
        assert len(get_schedules()) == 0


# ─────────────────────────────────────────────────────────────
# Query Engine Tests
# ─────────────────────────────────────────────────────────────

class TestQueryEngine:
    def test_execute_sqlite(self, tmp_path):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        db_path = str(tmp_path / "test.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO t VALUES (1, 'Alice')")
        conn.execute("INSERT INTO t VALUES (2, 'Bob')")
        conn.commit()
        conn.close()

        result = engine.execute("sqlite", db_path, "SELECT * FROM t")
        assert result["columns"] == ["id", "name"]
        assert len(result["rows"]) == 2
        assert result["rows"][0] == [1, "Alice"]

    def test_execute_sqlite_with_limit(self, tmp_path):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        db_path = str(tmp_path / "big.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER)")
        for i in range(100):
            conn.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        conn.close()

        result = engine.execute("sqlite", db_path, "SELECT * FROM t", limit=10)
        assert len(result["rows"]) == 10

    def test_execute_duckdb(self):
        pytest.importorskip("duckdb")
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        result = engine.execute("duckdb", ":memory:", "SELECT 42 AS answer")
        assert result["columns"] == ["answer"]
        assert result["rows"] == [[42]]

    def test_execute_unsupported_type(self):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        with pytest.raises(ValueError, match="Unsupported"):
            engine.execute("redis", "", "GET key")

    def test_apply_params(self):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        # _apply_params returns (sql_with_placeholders, ordered_params).
        # Switched to parameterized binding for SQL injection safety —
        # the test previously checked for inline-escaped values, which
        # is no longer the engine's behavior.
        sql, params = engine._apply_params(
            "SELECT * FROM t WHERE name = :name AND age > :age AND ref IS :ref",
            {"name": "O'Brien", "age": 25, "ref": None},
        )
        assert "?" in sql
        # Order follows the order the placeholders appear in the SQL.
        assert params == ["O'Brien", 25, None]

    def test_get_table_list_sqlite(self, tmp_path):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        db_path = str(tmp_path / "tables.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE alpha (id INTEGER)")
        conn.execute("CREATE TABLE beta (id INTEGER)")
        conn.commit()
        conn.close()

        tables = engine.get_table_list("sqlite", db_path)
        assert "alpha" in tables
        assert "beta" in tables

    def test_get_table_schema_sqlite(self, tmp_path):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        db_path = str(tmp_path / "schema.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
        conn.commit()
        conn.close()

        schema = engine.get_table_schema("sqlite", db_path, "t")
        assert len(schema) == 3
        assert schema[0]["name"] == "id"
        assert schema[1]["name"] == "name"
        assert schema[2]["type"] == "REAL"

    def test_get_table_preview_sqlite(self, tmp_path):
        from tusk.bi.engine import BIQueryEngine
        engine = BIQueryEngine()

        db_path = str(tmp_path / "preview.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE t (id INTEGER)")
        for i in range(30):
            conn.execute("INSERT INTO t VALUES (?)", (i,))
        conn.commit()
        conn.close()

        result = engine.get_table_preview("sqlite", db_path, "t", limit=5)
        assert len(result["rows"]) == 5


# ─────────────────────────────────────────────────────────────
# Chart Config Builder Tests
# ─────────────────────────────────────────────────────────────

class TestChartBuilder:
    COLUMNS = ["category", "value"]
    ROWS = [["A", 10], ["B", 20], ["C", 30]]

    def test_bar_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("bar", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "bar"
        assert cfg["data"]["labels"] == ["A", "B", "C"]
        assert cfg["data"]["datasets"][0]["data"] == [10, 20, 30]

    def test_line_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("line", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "line"

    def test_area_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("area", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "line"
        assert cfg["data"]["datasets"][0].get("fill") is True

    def test_pie_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("pie", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "pie"
        assert len(cfg["data"]["datasets"][0]["backgroundColor"]) == 3

    def test_doughnut_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("doughnut", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "doughnut"

    def test_scatter_chart(self):
        from tusk.bi.charts import build_chart_config
        rows = [[1, 10], [2, 20], [3, 30]]
        cfg = build_chart_config("scatter", ["x", "y"], rows)
        assert cfg["type"] == "scatter"
        points = cfg["data"]["datasets"][0]["data"]
        assert points[0] == {"x": 1.0, "y": 10.0}

    def test_radar_chart(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("radar", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "radar"

    def test_horizontal_bar(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("horizontal_bar", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "bar"
        assert cfg["options"]["indexAxis"] == "y"

    def test_stacked_bar(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("stacked_bar", self.COLUMNS, self.ROWS)
        assert cfg["type"] == "bar"
        assert cfg["options"]["scales"]["x"]["stacked"] is True
        assert cfg["options"]["scales"]["y"]["stacked"] is True

    def test_grouped_config(self):
        from tusk.bi.charts import build_chart_config
        columns = ["date", "value", "group"]
        rows = [
            ["Jan", 10, "A"],
            ["Jan", 20, "B"],
            ["Feb", 15, "A"],
            ["Feb", 25, "B"],
        ]
        cfg = build_chart_config(
            "bar", columns, rows,
            config={"x_column": "date", "y_column": "value", "group_by": "group"},
        )
        assert len(cfg["data"]["datasets"]) == 2
        assert cfg["data"]["labels"] == ["Jan", "Feb"]

    def test_custom_x_y_columns(self):
        from tusk.bi.charts import build_chart_config
        columns = ["name", "amount", "count"]
        rows = [["A", 10, 5], ["B", 20, 3]]
        cfg = build_chart_config(
            "bar", columns, rows,
            config={"x_column": "name", "y_column": "count"},
        )
        assert cfg["data"]["datasets"][0]["data"] == [5, 3]

    def test_to_number_edge_cases(self):
        from tusk.bi.charts import _to_number
        assert _to_number(None) == 0
        assert _to_number("abc") == 0
        assert _to_number("42.5") == 42.5
        assert _to_number(10) == 10.0

    def test_empty_data(self):
        from tusk.bi.charts import build_chart_config
        cfg = build_chart_config("bar", ["x", "y"], [])
        assert cfg["data"]["labels"] == []
        assert cfg["data"]["datasets"][0]["data"] == []


# ─────────────────────────────────────────────────────────────
# Cron Parser Tests
# ─────────────────────────────────────────────────────────────

class TestCronParser:
    def test_every_minute(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("* * * * *")
        assert cron["minute"] == list(range(60))
        assert cron["hour"] == list(range(24))

    def test_specific_minute(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("30 * * * *")
        assert cron["minute"] == [30]

    def test_every_15_minutes(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("*/15 * * * *")
        assert cron["minute"] == [0, 15, 30, 45]

    def test_range(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("0 9-17 * * *")
        assert cron["hour"] == list(range(9, 18))

    def test_comma_separated(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("0 0 1,15 * *")
        assert cron["day"] == [1, 15]

    def test_weekday_specific(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("0 0 * * 1,3,5")
        assert cron["weekday"] == [1, 3, 5]

    def test_step_with_base(self):
        from tusk.bi.scheduler import parse_cron
        cron = parse_cron("5/10 * * * *")
        assert 5 in cron["minute"]
        assert 15 in cron["minute"]
        assert 25 in cron["minute"]

    def test_invalid_expression(self):
        from tusk.bi.scheduler import parse_cron
        with pytest.raises(ValueError, match="5 fields"):
            parse_cron("* * *")

    def test_calculate_next_run(self):
        from tusk.bi.scheduler import calculate_next_run

        # Every hour at :00
        base = datetime(2026, 2, 7, 10, 30)
        next_run = calculate_next_run("0 * * * *", after=base)
        assert next_run.minute == 0
        assert next_run.hour == 11

    def test_calculate_next_run_specific_hour(self):
        from tusk.bi.scheduler import calculate_next_run

        base = datetime(2026, 2, 7, 8, 0)
        next_run = calculate_next_run("30 9 * * *", after=base)
        assert next_run.hour == 9
        assert next_run.minute == 30
        assert next_run.day == 7

    def test_calculate_next_run_past_time(self):
        from tusk.bi.scheduler import calculate_next_run

        base = datetime(2026, 2, 7, 22, 0)
        next_run = calculate_next_run("0 8 * * *", after=base)
        assert next_run.day == 8
        assert next_run.hour == 8


# ─────────────────────────────────────────────────────────────
# Prebuilt Dashboard Tests
# ─────────────────────────────────────────────────────────────

class TestPrebuilt:
    def test_ensure_prebuilt_no_plugins(self, db_init):
        """With no plugins only the demo gallery exists (always created
        since 0.3.1); no Security/Cluster dashboards."""
        mock_get_plugin = MagicMock(return_value=None)
        with patch("tusk.bi.prebuilt.get_plugin", mock_get_plugin, create=True), \
             patch("tusk.plugins.registry.get_plugin", mock_get_plugin, create=True):
            from tusk.bi.prebuilt import ensure_prebuilt_dashboards
            ensure_prebuilt_dashboards()

        from tusk.bi.db import get_dashboards
        assert [d["name"] for d in get_dashboards()] == ["Chart Gallery (Demo)"]

    def test_get_source_id_for_plugin(self, db_init):
        from tusk.bi.db import create_data_source
        from tusk.bi.prebuilt import _get_source_id_for_plugin

        create_data_source("Security", "sqlite", "/tmp/sec.db", plugin_id="tusk-security")
        sid = _get_source_id_for_plugin("tusk-security")
        assert sid is not None

        assert _get_source_id_for_plugin("nonexistent") is None

    def test_create_security_dashboard(self, db_init):
        from tusk.bi.db import create_data_source, get_dashboards, get_widgets
        from tusk.bi.prebuilt import _create_security_dashboard

        create_data_source("Security", "sqlite", "/tmp/sec.db", plugin_id="tusk-security")
        _create_security_dashboard()

        dashboards = get_dashboards()
        assert len(dashboards) == 1
        assert dashboards[0]["name"] == "Security Overview"
        assert dashboards[0]["is_prebuilt"] == 1

        widgets = get_widgets(dashboards[0]["id"])
        assert len(widgets) == 8

    def test_create_cluster_dashboard(self, db_init):
        from tusk.bi.db import create_data_source, get_dashboards, get_widgets
        from tusk.bi.prebuilt import _create_cluster_dashboard

        create_data_source("Cluster", "sqlite", "/tmp/cluster.db", plugin_id="tusk-cluster")
        _create_cluster_dashboard()

        dashboards = get_dashboards()
        assert len(dashboards) == 1
        assert dashboards[0]["name"] == "Cluster Monitor"

        widgets = get_widgets(dashboards[0]["id"])
        assert len(widgets) == 5

    def test_idempotent_prebuilt(self, db_init):
        """Calling ensure_prebuilt twice should not create duplicates."""
        from tusk.bi.db import create_data_source, get_dashboards
        create_data_source("Security", "sqlite", "/tmp/sec.db", plugin_id="tusk-security")

        mock_plugin = MagicMock()
        mock_plugin.name = "tusk-security"

        def mock_get_plugin(name):
            if name == "tusk-security":
                return mock_plugin
            return None

        with patch("tusk.bi.prebuilt.get_plugin", mock_get_plugin, create=True), \
             patch("tusk.plugins.registry.get_plugin", mock_get_plugin, create=True):
            from tusk.bi.prebuilt import ensure_prebuilt_dashboards
            ensure_prebuilt_dashboards()
            ensure_prebuilt_dashboards()

        dashboards = get_dashboards()
        # Should have exactly 1 Security dashboard
        sec_dashes = [d for d in dashboards if d["name"] == "Security Overview"]
        assert len(sec_dashes) == 1


# ─────────────────────────────────────────────────────────────
# v0.3.0 — New widget partials
# ─────────────────────────────────────────────────────────────


class TestV030Widgets:
    """Confirm the new widget types resolve to their partials in the
    render-endpoint mapping, and the partials themselves render valid
    HTML for representative inputs.
    """

    def test_widget_type_partials_registered(self):
        # The mapping lives inline in api.py's render_widget — re-import
        # it via a tiny stub so this test does not bind to a private name.
        # We just confirm the partial files exist on disk.
        from pathlib import Path
        import tusk.bi
        root = Path(tusk.bi.__file__).parent / "templates" / "bi" / "partials"
        assert (root / "top_n.html").exists()
        assert (root / "funnel.html").exists()
        assert (root / "stat.html").exists()
        assert (root / "map.html").exists()

    def test_top_n_template_renders(self, tmp_path):
        # Render the top_n partial directly with MiniJinja to catch
        # syntax errors and confirm the bar geometry math works.
        import minijinja
        root = Path(tusk.bi.__file__).parent / "templates"
        env = minijinja.Environment(loader=minijinja.load_from_path(str(root)))
        out = env.render_template(
            "bi/partials/top_n.html",
            widget={"title": "Top apps"},
            data={"columns": ["app", "uses"], "rows": [["a", 100], ["b", 60], ["c", 30]]},
            widget_id=42,
            config={},
            error=None,
        )
        assert "bi-value-list" in out
        assert "a" in out and "100" in out
        # Largest bar is 100% wide.
        assert "100.0%" in out
        # Smallest bar is 30% wide.
        assert "30.0%" in out

    def test_funnel_template_renders(self):
        import minijinja
        root = Path(tusk.bi.__file__).parent / "templates"
        env = minijinja.Environment(loader=minijinja.load_from_path(str(root)))
        out = env.render_template(
            "bi/partials/funnel.html",
            widget={"title": "Funnel"},
            data={"columns": ["stage", "users"], "rows": [
                ["Visitors", 1000], ["Signed up", 500], ["Active", 100],
            ]},
            widget_id=7,
            config={},
            error=None,
        )
        assert "bi-funnel" in out
        assert "Visitors" in out and "1000" in out
        assert "100.0%" in out
        assert "50.0%" in out
        assert "10.0%" in out

    def test_stat_template_with_sparkline(self):
        import minijinja
        root = Path(tusk.bi.__file__).parent / "templates"
        env = minijinja.Environment(loader=minijinja.load_from_path(str(root)))
        out = env.render_template(
            "bi/partials/stat.html",
            widget={"title": "MRR"},
            data={"columns": ["v"], "rows": [["$284K"]]},
            widget_id=9,
            config={},
            error=None,
            sparkline_values=[1, 2, 3, 4],
            delta="+12.4%",
            delta_type="up",
        )
        assert "dash-card-v" in out
        assert "$284K" in out
        assert "+12.4%" in out
        assert "dash-spark" in out
        assert "sparkline-9" in out

    def test_stat_template_empty_renders_dash(self):
        import minijinja
        root = Path(tusk.bi.__file__).parent / "templates"
        env = minijinja.Environment(loader=minijinja.load_from_path(str(root)))
        out = env.render_template(
            "bi/partials/stat.html",
            widget={"title": "X"},
            data={"columns": [], "rows": []},
            widget_id=1,
            config={},
            error=None,
        )
        assert "—" in out

    def test_dashboard_viewer_template_has_dash_grid(self):
        """Smoke check: the rewritten viewer template uses the v0.3.0
        CSS-grid markup (.dash-grid + .span-N) and the new header
        chrome (.dash-head + .dash-title)."""
        from pathlib import Path
        import tusk.bi
        viewer = Path(tusk.bi.__file__).parent / "templates" / "bi" / "dashboard.html"
        html = viewer.read_text()
        assert "dash-grid" in html
        assert "dash-title" in html
        assert "dash-head" in html
        assert "span-" in html  # any span class
        # Old GridStack viewer markup is gone from this template.
        assert "grid-stack-item" not in html
