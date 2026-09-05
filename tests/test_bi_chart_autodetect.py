"""Auto-axis detection for the chart builder (charts.py suggest_axes).

Pure-function tests — no DB, no server. The point of suggest_axes is to
pick a sensible dimension (X) and measure (Y) from query results the way
Tusk's Explore profiler does, instead of blindly grabbing columns[0]
and columns[1].
"""

from datetime import date

from tusk.bi.charts import infer_column_types, suggest_axes, build_chart_config


# ──────────────────────── infer_column_types ────────────────────────


def test_infer_numeric():
    cols = ["sales"]
    rows = [[100], [200], [300]]
    types = infer_column_types(cols, rows)
    assert types[0]["kind"] == "numeric"


def test_infer_categorical():
    cols = ["region"]
    rows = [["North"], ["South"], ["East"]]
    types = infer_column_types(cols, rows)
    assert types[0]["kind"] == "categorical"


def test_infer_temporal_from_date_objects():
    cols = ["day"]
    rows = [[date(2026, 5, 1)], [date(2026, 5, 2)]]
    types = infer_column_types(cols, rows)
    assert types[0]["kind"] == "temporal"


def test_infer_temporal_from_iso_strings():
    cols = ["created_at"]
    rows = [["2026-05-01"], ["2026-05-02"], ["2026-05-03"]]
    types = infer_column_types(cols, rows)
    assert types[0]["kind"] == "temporal"


def test_numeric_strings_count_as_numeric():
    cols = ["amount"]
    rows = [["100"], ["200.5"], ["300"]]
    types = infer_column_types(cols, rows)
    assert types[0]["kind"] == "numeric"


def test_distinct_count():
    cols = ["region"]
    rows = [["North"], ["North"], ["South"]]
    types = infer_column_types(cols, rows)
    assert types[0]["distinct"] == 2


# ──────────────────────── suggest_axes ────────────────────────


def test_skips_id_for_y_axis():
    # id, region, sales — must pick region (X) + sales (Y), NOT id.
    cols = ["id", "region", "sales"]
    rows = [[1, "North", 420], [2, "South", 310], [3, "East", 280]]
    s = suggest_axes(cols, rows)
    assert s["x_column"] == "region"
    assert s["y_column"] == "sales"
    assert s["chart_type"] == "bar"


def test_temporal_x_gives_line():
    cols = ["day", "signups"]
    rows = [[date(2026, 5, 1), 120], [date(2026, 5, 2), 145]]
    s = suggest_axes(cols, rows)
    assert s["x_column"] == "day"
    assert s["y_column"] == "signups"
    assert s["chart_type"] == "line"


def test_single_row_single_number_is_stat():
    cols = ["total_revenue"]
    rows = [[482300]]
    s = suggest_axes(cols, rows)
    assert s["chart_type"] == "stat"
    assert s["y_column"] == "total_revenue"


def test_many_categories_gives_horizontal_bar():
    cols = ["product", "units"]
    rows = [[f"Widget {i}", 1000 - i * 50] for i in range(12)]
    s = suggest_axes(cols, rows)
    assert s["x_column"] == "product"
    assert s["chart_type"] == "horizontal_bar"


def test_two_numerics_no_dimension_is_scatter():
    cols = ["height", "weight"]
    rows = [[170, 65], [180, 80], [160, 55]]
    s = suggest_axes(cols, rows)
    assert s["chart_type"] == "scatter"


def test_detects_group_by_second_categorical():
    cols = ["quarter", "region", "sales"]
    rows = [
        ["Q1", "North", 100], ["Q1", "South", 80],
        ["Q2", "North", 130], ["Q2", "South", 95],
    ]
    s = suggest_axes(cols, rows)
    assert s["y_column"] == "sales"
    assert s["group_by"] in ("region", "quarter")


def test_empty_returns_empty():
    assert suggest_axes([], []) == {}


# ──────────────────────── build_chart_config integration ────────────────────────


def test_build_chart_config_auto_fills_axes():
    # No config given — must auto-detect region/sales over id.
    cols = ["id", "region", "sales"]
    rows = [[1, "North", 420], [2, "South", 310]]
    chart = build_chart_config("bar", cols, rows, config={})
    # Labels should be the regions, not the ids.
    assert chart["data"]["labels"] == ["North", "South"]
    assert chart["data"]["datasets"][0]["data"] == [420, 310]


def test_build_chart_config_respects_explicit_axes():
    # Explicit config wins over auto-detection.
    cols = ["id", "region", "sales"]
    rows = [[1, "North", 420], [2, "South", 310]]
    chart = build_chart_config("bar", cols, rows,
                               config={"x_column": "id", "y_column": "sales"})
    assert chart["data"]["labels"] == ["1", "2"]
