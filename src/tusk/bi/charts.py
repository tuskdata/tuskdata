"""Chart configuration builder for Chart.js integration"""

from datetime import date, datetime
from typing import Any


# ──────────────────────── Column-type inference ────────────────────────
# Borrowed in spirit from Tusk's Explore profiler (routes/data.py
# `_compute_profile`): classify each column as numeric / temporal /
# categorical so we can auto-pick sensible axes instead of blindly
# grabbing columns[0] and columns[1]. Pure-Python (no Polars dep) so
# the plugin stays light — we only need coarse buckets, not full stats.

# Column-name hints — used as tie-breakers / to demote id-like numerics.
_ID_NAME_HINTS = ("id", "_id", "uuid", "guid", "pk", "fk", "_key", "code")
_TIME_NAME_HINTS = ("date", "time", "created", "updated", "_at", "year",
                    "month", "day", "timestamp", "fecha", "hora")


def _looks_numeric(v: Any) -> bool:
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        s = v.strip().replace(",", "")
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False
    return False


def _looks_temporal(v: Any) -> bool:
    if isinstance(v, (datetime, date)):
        return True
    if isinstance(v, str):
        s = v.strip()
        # Cheap ISO-ish check: YYYY-MM-DD ... — avoid a full parse on
        # every cell. Good enough to bucket a column as time-series.
        if len(s) >= 8 and s[:4].isdigit() and s[4] in "-/":
            return True
    return False


def infer_column_types(columns: list[str], rows: list[list], sample: int = 50) -> list[dict]:
    """Classify each column from a sample of the data.

    Returns one dict per column: {name, kind, distinct, idx} where kind
    is one of 'numeric' | 'temporal' | 'categorical'. `distinct` is an
    approximate distinct-count over the sample (used for cardinality
    decisions — a low-cardinality string is a good X axis / dimension,
    a high-cardinality one is a label nobody wants on an axis).
    """
    out: list[dict] = []
    sample_rows = rows[:sample]
    for i, name in enumerate(columns):
        vals = [r[i] for r in sample_rows if i < len(r) and r[i] is not None]
        nonnull = len(vals)
        lname = name.lower()

        if nonnull == 0:
            kind = "categorical"
        else:
            num = sum(1 for v in vals if _looks_numeric(v))
            tmp = sum(1 for v in vals if _looks_temporal(v))
            # A column is temporal/numeric only if the strong majority of
            # sampled cells match — guards against a stray numeric-looking
            # string in an otherwise text column.
            name_says_time = any(h in lname for h in _TIME_NAME_HINTS)
            if (tmp / nonnull >= 0.8) or (name_says_time and tmp / nonnull >= 0.5):
                kind = "temporal"
            elif num / nonnull >= 0.8:
                kind = "numeric"
            else:
                kind = "categorical"

        try:
            distinct = len({str(v) for v in vals})
        except Exception:
            distinct = nonnull

        out.append({"name": name, "kind": kind, "distinct": distinct, "idx": i})
    return out


def suggest_axes(columns: list[str], rows: list[list]) -> dict:
    """Auto-pick {x_column, y_column, group_by, chart_type} from the data.

    Heuristics, in order:
      - Y (measure): first numeric column that isn't id-like. IDs make
        terrible measures (you don't sum primary keys).
      - X (dimension): prefer a temporal column (→ line); else the
        lowest-cardinality categorical; else the first non-Y column.
      - chart_type:
          temporal X + numeric Y           → line
          categorical X (≤ 8 cats) + Y     → bar
          categorical X (9–25 cats) + Y    → horizontal_bar (room for labels)
          two numerics, no good dimension  → scatter
          single numeric, single row       → stat (big number)
      - group_by: a SECOND low-cardinality categorical, if present.

    Returns a config dict ready to merge into build_chart_config().
    Empty dict when there's nothing sensible to infer.
    """
    if not columns:
        return {}

    types = infer_column_types(columns, rows)
    numerics = [t for t in types if t["kind"] == "numeric"]
    temporals = [t for t in types if t["kind"] == "temporal"]
    categoricals = [t for t in types if t["kind"] == "categorical"]

    def _id_like(t: dict) -> bool:
        ln = t["name"].lower()
        return any(ln == h or ln.endswith(h) for h in _ID_NAME_HINTS)

    # Y = measure: first non-id numeric, falling back to any numeric.
    measure = next((t for t in numerics if not _id_like(t)), None) or (numerics[0] if numerics else None)

    # Single value, single row → big-number stat card.
    if measure and len(rows) <= 1 and len(numerics) >= 1:
        return {"chart_type": "stat", "y_column": measure["name"]}

    # X = dimension: temporal first, else lowest-cardinality categorical.
    dimension = None
    chart_type = "bar"
    if temporals:
        dimension = temporals[0]
        chart_type = "line"
    elif categoricals:
        dimension = min(categoricals, key=lambda t: t["distinct"])
        n_cats = dimension["distinct"]
        chart_type = "bar" if n_cats <= 8 else "horizontal_bar"

    # No dimension but two numerics → scatter.
    if dimension is None and len(numerics) >= 2:
        return {
            "chart_type": "scatter",
            "x_column": numerics[0]["name"],
            "y_column": numerics[1]["name"],
        }

    if dimension is None or measure is None:
        # Nothing better than the naive default; let build_chart_config
        # fall back to columns[0]/columns[1].
        return {}

    cfg: dict = {
        "chart_type": chart_type,
        "x_column": dimension["name"],
        "y_column": measure["name"],
    }

    # group_by: a second low-cardinality categorical distinct from X.
    other_cats = [t for t in categoricals
                  if t["name"] != dimension["name"] and t["distinct"] <= 8]
    if other_cats:
        cfg["group_by"] = min(other_cats, key=lambda t: t["distinct"])["name"]

    return cfg

# Dark-theme color palette matching Tusk's scheme
CHART_COLORS = [
    "rgba(59, 130, 246, 0.8)",   # blue
    "rgba(16, 185, 129, 0.8)",   # green
    "rgba(245, 158, 11, 0.8)",   # amber
    "rgba(239, 68, 68, 0.8)",    # red
    "rgba(139, 92, 246, 0.8)",   # violet
    "rgba(6, 182, 212, 0.8)",    # cyan
    "rgba(249, 115, 22, 0.8)",   # orange
    "rgba(236, 72, 153, 0.8)",   # pink
    "rgba(34, 197, 94, 0.8)",    # emerald
    "rgba(168, 85, 247, 0.8)",   # purple
]

CHART_BORDER_COLORS = [c.replace("0.8)", "1)") for c in CHART_COLORS]


def build_chart_config(
    chart_type: str,
    columns: list[str],
    rows: list[list],
    config: dict | None = None,
) -> dict:
    """Build a Chart.js-ready configuration from query results.

    Args:
        chart_type: bar, line, area, pie, doughnut, scatter, radar, stacked_bar, horizontal_bar
        columns: Column names from query result
        rows: Row data from query result
        config: Optional config overrides (x_column, y_column, group_by, etc.)

    Returns:
        Chart.js configuration dict with type, data, and options
    """
    config = config or {}

    # Auto-detect axes when the user hasn't pinned them. Borrowed from
    # Explore's column profiling: pick a real dimension for X and a real
    # measure for Y instead of blindly grabbing columns[0]/columns[1]
    # (which would put an `id` on the X axis). Only fills the gaps —
    # anything the user explicitly set in `config` wins.
    needs_x = "x_column" not in config
    needs_y = "y_column" not in config
    if (needs_x or needs_y) and columns and rows:
        suggested = suggest_axes(columns, rows)
        if needs_x and "x_column" in suggested:
            config["x_column"] = suggested["x_column"]
        if needs_y and "y_column" in suggested:
            config["y_column"] = suggested["y_column"]
        if "group_by" not in config and "group_by" in suggested:
            config["group_by"] = suggested["group_by"]

    x_column = config.get("x_column", columns[0] if columns else "")
    y_column = config.get("y_column", columns[1] if len(columns) > 1 else columns[0] if columns else "")
    group_by = config.get("group_by")
    stacked = config.get("stacked", False)
    show_legend = config.get("show_legend", True)
    title = config.get("title", "")

    x_idx = columns.index(x_column) if x_column in columns else 0
    y_idx = columns.index(y_column) if y_column in columns else min(1, len(columns) - 1)

    # Map chart types
    actual_type = chart_type
    if chart_type == "area":
        actual_type = "line"
    elif chart_type == "stacked_bar":
        actual_type = "bar"
        stacked = True
    elif chart_type == "horizontal_bar":
        actual_type = "bar"

    if chart_type in ("pie", "doughnut"):
        return _build_pie_config(actual_type, rows, x_idx, y_idx, columns, title, show_legend)

    if chart_type == "scatter":
        return _build_scatter_config(rows, x_idx, y_idx, columns, title, show_legend)

    if chart_type == "radar":
        return _build_radar_config(rows, x_idx, y_idx, columns, config, title, show_legend)

    # Bar/Line/Area with optional group_by
    if group_by and group_by in columns:
        return _build_grouped_config(
            actual_type, rows, columns, x_idx, y_idx, group_by,
            stacked, chart_type == "area", chart_type == "horizontal_bar",
            title, show_legend,
        )

    return _build_simple_config(
        actual_type, rows, x_idx, y_idx, columns,
        stacked, chart_type == "area", chart_type == "horizontal_bar",
        title, show_legend,
    )


def _build_simple_config(
    chart_type: str, rows: list, x_idx: int, y_idx: int, columns: list,
    stacked: bool, fill: bool, horizontal: bool, title: str, show_legend: bool,
) -> dict:
    labels = [str(row[x_idx]) for row in rows]
    values = [_to_number(row[y_idx]) for row in rows]

    dataset = {
        "label": columns[y_idx] if y_idx < len(columns) else "Value",
        "data": values,
        "backgroundColor": CHART_COLORS[0],
        "borderColor": CHART_BORDER_COLORS[0],
        "borderWidth": 1,
    }
    if fill:
        dataset["fill"] = True

    return {
        "type": chart_type,
        "data": {"labels": labels, "datasets": [dataset]},
        "options": _build_options(title, show_legend, stacked, horizontal),
    }


def _build_grouped_config(
    chart_type: str, rows: list, columns: list, x_idx: int, y_idx: int,
    group_by: str, stacked: bool, fill: bool, horizontal: bool,
    title: str, show_legend: bool,
) -> dict:
    group_idx = columns.index(group_by)

    # Group data
    groups: dict[str, dict[str, Any]] = {}
    all_labels: list[str] = []

    for row in rows:
        label = str(row[x_idx])
        group = str(row[group_idx])
        value = _to_number(row[y_idx])

        if label not in all_labels:
            all_labels.append(label)

        if group not in groups:
            groups[group] = {}
        groups[group][label] = value

    datasets = []
    for i, (group_name, label_values) in enumerate(groups.items()):
        color_idx = i % len(CHART_COLORS)
        dataset = {
            "label": group_name,
            "data": [label_values.get(label, 0) for label in all_labels],
            "backgroundColor": CHART_COLORS[color_idx],
            "borderColor": CHART_BORDER_COLORS[color_idx],
            "borderWidth": 1,
        }
        if fill:
            dataset["fill"] = True
        datasets.append(dataset)

    return {
        "type": chart_type,
        "data": {"labels": all_labels, "datasets": datasets},
        "options": _build_options(title, show_legend, stacked, horizontal),
    }


def _build_pie_config(
    chart_type: str, rows: list, x_idx: int, y_idx: int,
    columns: list, title: str, show_legend: bool,
) -> dict:
    labels = [str(row[x_idx]) for row in rows]
    values = [_to_number(row[y_idx]) for row in rows]
    colors = [CHART_COLORS[i % len(CHART_COLORS)] for i in range(len(values))]
    borders = [CHART_BORDER_COLORS[i % len(CHART_BORDER_COLORS)] for i in range(len(values))]

    return {
        "type": chart_type,
        "data": {
            "labels": labels,
            "datasets": [{
                "data": values,
                "backgroundColor": colors,
                "borderColor": borders,
                "borderWidth": 1,
            }],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "plugins": {
                "legend": {"display": show_legend, "position": "right",
                           "labels": {"color": "#e5e7eb"}},
                "title": {"display": bool(title), "text": title, "color": "#e5e7eb"},
            },
        },
    }


def _build_scatter_config(
    rows: list, x_idx: int, y_idx: int, columns: list,
    title: str, show_legend: bool,
) -> dict:
    points = [{"x": _to_number(row[x_idx]), "y": _to_number(row[y_idx])} for row in rows]

    return {
        "type": "scatter",
        "data": {
            "datasets": [{
                "label": f"{columns[x_idx]} vs {columns[y_idx]}",
                "data": points,
                "backgroundColor": CHART_COLORS[0],
                "borderColor": CHART_BORDER_COLORS[0],
                "borderWidth": 1,
            }],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "scales": {
                "x": {"title": {"display": True, "text": columns[x_idx], "color": "#e5e7eb"},
                       "ticks": {"color": "#9ca3af"}, "grid": {"color": "rgba(255,255,255,0.1)"}},
                "y": {"title": {"display": True, "text": columns[y_idx], "color": "#e5e7eb"},
                       "ticks": {"color": "#9ca3af"}, "grid": {"color": "rgba(255,255,255,0.1)"}},
            },
            "plugins": {
                "legend": {"display": show_legend, "labels": {"color": "#e5e7eb"}},
                "title": {"display": bool(title), "text": title, "color": "#e5e7eb"},
            },
        },
    }


def _build_radar_config(
    rows: list, x_idx: int, y_idx: int, columns: list,
    config: dict, title: str, show_legend: bool,
) -> dict:
    labels = [str(row[x_idx]) for row in rows]
    values = [_to_number(row[y_idx]) for row in rows]

    return {
        "type": "radar",
        "data": {
            "labels": labels,
            "datasets": [{
                "label": columns[y_idx] if y_idx < len(columns) else "Value",
                "data": values,
                "backgroundColor": CHART_COLORS[0].replace("0.8)", "0.3)"),
                "borderColor": CHART_BORDER_COLORS[0],
                "borderWidth": 2,
            }],
        },
        "options": {
            "responsive": True,
            "maintainAspectRatio": False,
            "scales": {
                "r": {"ticks": {"color": "#9ca3af"},
                       "grid": {"color": "rgba(255,255,255,0.1)"},
                       "pointLabels": {"color": "#e5e7eb"}},
            },
            "plugins": {
                "legend": {"display": show_legend, "labels": {"color": "#e5e7eb"}},
                "title": {"display": bool(title), "text": title, "color": "#e5e7eb"},
            },
        },
    }


def _build_options(
    title: str, show_legend: bool, stacked: bool, horizontal: bool
) -> dict:
    options: dict = {
        "responsive": True,
        "maintainAspectRatio": False,
        "indexAxis": "y" if horizontal else "x",
        "scales": {
            "x": {"ticks": {"color": "#9ca3af"}, "grid": {"color": "rgba(255,255,255,0.1)"},
                   "stacked": stacked},
            "y": {"ticks": {"color": "#9ca3af"}, "grid": {"color": "rgba(255,255,255,0.1)"},
                   "stacked": stacked},
        },
        "plugins": {
            "legend": {"display": show_legend, "labels": {"color": "#e5e7eb"}},
            "title": {"display": bool(title), "text": title, "color": "#e5e7eb"},
        },
    }
    return options


def _to_number(value: Any) -> float:
    """Convert a value to a number for charting."""
    if value is None:
        return 0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0
