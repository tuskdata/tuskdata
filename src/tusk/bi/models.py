"""Data models for BI plugin"""

import msgspec
from typing import Any


class DataSource(msgspec.Struct):
    """A queryable data source (SQLite, DuckDB, PostgreSQL)"""
    id: int = 0
    name: str = ""
    source_type: str = "sqlite"  # sqlite, duckdb, postgres
    connection_ref: str = ""  # db path or connection name
    plugin_id: str | None = None
    tables: list[str] = []
    created_at: str = ""


class SavedQuery(msgspec.Struct):
    """A saved SQL query with optional chart config"""
    id: int = 0
    name: str = ""
    description: str = ""
    source_id: int = 0
    sql: str = ""
    chart_type: str | None = None  # bar, line, pie, etc.
    chart_config: str = "{}"  # JSON string
    tags: str = ""  # comma-separated
    created_at: str = ""
    updated_at: str = ""
    last_executed_at: str | None = None


class ChartConfig(msgspec.Struct):
    """Configuration for a chart visualization"""
    chart_type: str = "bar"
    x_column: str = ""
    y_column: str = ""
    group_by: str | None = None
    colors: list[str] = []
    stacked: bool = False
    show_legend: bool = True
    title: str = ""


class Widget(msgspec.Struct):
    """A dashboard widget"""
    id: int = 0
    dashboard_id: int = 0
    query_id: int | None = None
    widget_type: str = "chart"  # chart, table, stat, map, text
    title: str = ""
    config: str = "{}"  # JSON string
    col_start: int = 1
    col_span: int = 6
    row_start: int = 1
    row_span: int = 4
    created_at: str = ""


class Dashboard(msgspec.Struct):
    """A dashboard containing widgets"""
    id: int = 0
    name: str = ""
    description: str = ""
    is_default: bool = False
    is_prebuilt: bool = False
    filters: str = "[]"  # JSON string
    created_at: str = ""
    updated_at: str = ""
    is_public: bool = False
    refresh_interval_seconds: int = 0  # 0 = no live refresh


class QueryParameter(msgspec.Struct):
    """A parameter for a parameterized query"""
    name: str = ""
    param_type: str = "text"  # text, number, date, select
    default_value: str = ""
    options: list[str] = []


class SnapshotSummary(msgspec.Struct):
    """Summary of a query snapshot"""
    id: int = 0
    query_id: int = 0
    row_count: int = 0
    value: float | None = None  # aggregated value for sparklines
    created_at: str = ""


class DashboardFilter(msgspec.Struct):
    """A global dashboard filter"""
    id: str = ""
    label: str = ""
    filter_type: str = "select"  # select, date_range, text
    column: str = ""
    default_value: str = ""
