"""Ibis engine: unified DataFrame pipeline execution across backends.

v0.3.0 ships this as an opt-in alongside the Polars engine. Pick the backend
with `engine="ibis+duckdb"` (default when ibis is the driver) or `"ibis+polars"`.
Pipeline definitions (sources + transforms) are reused verbatim from
`polars_engine` — the Ibis runner just compiles them to Ibis expressions and
executes on the chosen backend.

New capabilities that Polars-engine didn't expose:

- CaseWhenTransform: conditional column generation
- UnpivotTransform: wide → long reshape (MELT)
- DateArithmeticTransform: date_add / date_diff / truncation / extraction
- profile(): per-column nulls, distinct, min, max, mean for quick sanity checks

Falls back cleanly if `ibis-framework` is not installed — import-time flag
`HAS_IBIS` lets callers route around it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal
import msgspec

try:
    import ibis
    from ibis import _ as ibis_col
    HAS_IBIS = True
except ImportError:
    ibis = None
    ibis_col = None
    HAS_IBIS = False

from tusk.core.logging import get_logger
from tusk.engines.polars_engine import (
    DataSource, Pipeline,
    FilterTransform, SelectTransform, RenameTransform, SortTransform,
    GroupByTransform, AddColumnTransform, DropNullsTransform,
    LimitTransform, JoinTransform, ConcatTransform, DistinctTransform,
    WindowTransform,
)

log = get_logger("ibis_engine")


# ============================================================================
# New transforms (Ibis-native — Polars engine ignores these)
# ============================================================================


class CaseWhenBranch(msgspec.Struct):
    """A single when/then clause."""
    column: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "is_null", "is_not_null"]
    value: Any = None
    result: Any = None


class CaseWhenTransform(msgspec.Struct, tag="case_when"):
    """Conditional column: CASE WHEN ... THEN ... ELSE default END."""
    alias: str
    branches: list[CaseWhenBranch]
    default: Any = None


class UnpivotTransform(msgspec.Struct, tag="unpivot"):
    """Reshape wide → long. `id_cols` stay, `value_cols` collapse into
    variable/value columns."""
    id_cols: list[str]
    value_cols: list[str]
    variable_name: str = "variable"
    value_name: str = "value"


class DateArithmeticTransform(msgspec.Struct, tag="date_arithmetic"):
    """Date operations: add/subtract a unit, extract a part, or truncate."""
    operation: Literal["add", "sub", "diff", "extract", "truncate"]
    column: str
    alias: str
    unit: Literal["year", "month", "day", "hour", "minute", "second", "week"] = "day"
    amount: int = 0
    other_column: str | None = None  # For "diff"


IbisExtraTransform = CaseWhenTransform | UnpivotTransform | DateArithmeticTransform


# ============================================================================
# Ibis runner
# ============================================================================


_FILTER_OPS = {
    "eq": lambda c, v: c == v,
    "ne": lambda c, v: c != v,
    "gt": lambda c, v: c > v,
    "gte": lambda c, v: c >= v,
    "lt": lambda c, v: c < v,
    "lte": lambda c, v: c <= v,
    "contains": lambda c, v: c.contains(v),
    "starts_with": lambda c, v: c.startswith(v),
    "ends_with": lambda c, v: c.endswith(v),
    "is_null": lambda c, _: c.isnull(),
    "is_not_null": lambda c, _: c.notnull(),
    "is_empty": lambda c, _: (c == "") | c.isnull(),
    "is_not_empty": lambda c, _: (c != "") & c.notnull(),
}


def _require_ibis() -> None:
    if not HAS_IBIS:
        raise RuntimeError(
            "ibis-framework is not installed. Install with: "
            "uv pip install 'ibis-framework[duckdb,polars]' --python .venv/bin/python"
        )


def get_backend(name: str = "duckdb"):
    """Return a fresh Ibis backend instance.

    Supported: "duckdb" (default, in-memory), "polars", "postgres" (needs DSN).
    """
    _require_ibis()
    if name == "duckdb":
        return ibis.duckdb.connect(":memory:")
    if name == "polars":
        return ibis.polars.connect()
    raise ValueError(f"Unsupported Ibis backend: {name!r}")


def load_source(backend, source: DataSource):
    """Load a DataSource into the given Ibis backend as a table expression."""
    _require_ibis()

    if source.source_type == "csv":
        path = str(Path(source.path).expanduser())
        return backend.read_csv(path, table_name=_safe_id(source.id))
    if source.source_type == "parquet":
        path = str(Path(source.path).expanduser())
        return backend.read_parquet(path, table_name=_safe_id(source.id))
    if source.source_type == "json":
        path = str(Path(source.path).expanduser())
        return backend.read_json(path, table_name=_safe_id(source.id))
    if source.source_type in ("sql", "database"):
        return _load_sql_source(backend, source)
    if source.source_type == "osm":
        raise NotImplementedError("OSM loading via Ibis not yet supported; use Polars engine")
    raise ValueError(f"Unknown source_type: {source.source_type}")


def _load_sql_source(backend, source: DataSource):
    """Fetch via psycopg and register as an in-memory table on the Ibis backend."""
    from tusk.core.connection import get_connection
    import psycopg

    if not source.connection_id or not source.query:
        raise ValueError("SQL source requires connection_id and query")
    config = get_connection(source.connection_id)
    if config is None or config.type != "postgres":
        raise ValueError("SQL source requires a PostgreSQL connection")

    with psycopg.connect(config.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(source.query)
            columns = [d.name for d in cur.description or []]
            rows = cur.fetchall()

    import pyarrow as pa
    cols = {c: [r[i] for r in rows] for i, c in enumerate(columns)}
    table = pa.table(cols)
    return backend.register(table, table_name=_safe_id(source.id))


def _safe_id(source_id: str) -> str:
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in source_id)
    return f"t_{safe}" if safe and safe[0].isdigit() else safe or "t_anon"


def apply_transform(expr, transform, backend=None, loaded_tables: dict | None = None):
    """Apply a single transform to an Ibis expression and return the new expr."""
    _require_ibis()

    if isinstance(transform, FilterTransform):
        col = expr[transform.column]
        op = _FILTER_OPS.get(transform.operator)
        if op is None:
            raise ValueError(f"Unknown filter operator: {transform.operator}")
        return expr.filter(op(col, transform.value))

    if isinstance(transform, SelectTransform):
        return expr.select(*transform.columns)

    if isinstance(transform, RenameTransform):
        return expr.rename(**{new: old for old, new in transform.mapping.items()})

    if isinstance(transform, SortTransform):
        descending = transform.descending or [False] * len(transform.columns)
        order_keys = []
        for col, desc in zip(transform.columns, descending):
            order_keys.append(expr[col].desc() if desc else expr[col].asc())
        return expr.order_by(order_keys)

    if isinstance(transform, GroupByTransform):
        aggs = {}
        for a in transform.aggregations:
            col_name = a.get("column")
            fn = a.get("agg")
            alias = a.get("alias") or f"{col_name}_{fn}"
            col = expr[col_name] if col_name else None
            if fn == "count":
                aggs[alias] = expr.count() if col is None else col.count()
            elif fn in ("sum", "mean", "min", "max", "nunique", "std", "var"):
                aggs[alias] = getattr(col, fn)()
            elif fn == "median":
                aggs[alias] = col.quantile(0.5)
            else:
                raise ValueError(f"Unknown aggregation: {fn}")
        return expr.group_by(transform.by).aggregate(**aggs)

    if isinstance(transform, DropNullsTransform):
        subset = transform.subset or list(expr.columns)
        cond = None
        for col_name in subset:
            c = expr[col_name].notnull()
            cond = c if cond is None else cond & c
        return expr.filter(cond) if cond is not None else expr

    if isinstance(transform, LimitTransform):
        return expr.limit(transform.n)

    if isinstance(transform, DistinctTransform):
        subset = transform.subset
        return expr.distinct(on=subset) if subset else expr.distinct()

    if isinstance(transform, JoinTransform):
        if not loaded_tables:
            raise ValueError("Join needs loaded_tables to look up right source")
        right = loaded_tables.get(transform.right_source_id)
        if right is None:
            raise ValueError(f"Right source {transform.right_source_id!r} not loaded")
        if transform.on:
            return expr.join(right, transform.on, how=_ibis_join_how(transform.how))
        predicates = [expr[l] == right[r] for l, r in zip(
            transform.left_on or [], transform.right_on or []
        )]
        return expr.join(right, predicates, how=_ibis_join_how(transform.how))

    if isinstance(transform, ConcatTransform):
        if not loaded_tables:
            raise ValueError("Concat needs loaded_tables")
        tables = [expr] + [loaded_tables[sid] for sid in transform.source_ids if sid in loaded_tables]
        if len(tables) == 1:
            return expr
        return ibis.union(*tables, distinct=False)

    if isinstance(transform, AddColumnTransform):
        # Accept only simple "col op literal" style expressions for safety.
        return expr.mutate(**{transform.name: _eval_simple_expr(expr, transform.expression)})

    if isinstance(transform, CaseWhenTransform):
        branches = []
        for b in transform.branches:
            col = expr[b.column]
            op = _FILTER_OPS.get(b.operator)
            if op is None:
                raise ValueError(f"Unknown case_when operator: {b.operator}")
            branches.append((op(col, b.value), b.result))
        if not branches:
            return expr.mutate(**{transform.alias: ibis.literal(transform.default)})
        built = ibis.cases(*branches, else_=transform.default)
        return expr.mutate(**{transform.alias: built})

    if isinstance(transform, UnpivotTransform):
        # Ibis 10+ has `pivot_longer`; older releases use `melt`. Try new first.
        if hasattr(expr, "pivot_longer"):
            return expr.pivot_longer(
                transform.value_cols,
                names_to=transform.variable_name,
                values_to=transform.value_name,
            )
        return expr.melt(
            id_vars=transform.id_cols,
            value_vars=transform.value_cols,
            var_name=transform.variable_name,
            value_name=transform.value_name,
        )

    if isinstance(transform, DateArithmeticTransform):
        col = expr[transform.column]
        if transform.operation == "extract":
            part = getattr(col, transform.unit)()
            return expr.mutate(**{transform.alias: part})
        if transform.operation == "truncate":
            return expr.mutate(**{transform.alias: col.truncate(transform.unit)})
        if transform.operation == "add":
            delta = ibis.interval(**{f"{transform.unit}s": transform.amount})
            return expr.mutate(**{transform.alias: col + delta})
        if transform.operation == "sub":
            delta = ibis.interval(**{f"{transform.unit}s": transform.amount})
            return expr.mutate(**{transform.alias: col - delta})
        if transform.operation == "diff":
            if not transform.other_column:
                raise ValueError("diff requires other_column")
            diff_expr = col - expr[transform.other_column]
            return expr.mutate(**{transform.alias: diff_expr})
        raise ValueError(f"Unknown date_arithmetic operation: {transform.operation}")

    if isinstance(transform, WindowTransform):
        partition_keys = transform.partition_by or []
        order_keys = [expr[c] for c in transform.order_by]
        if transform.descending:
            order_keys = [k.desc() for k in order_keys]
        window = ibis.window(group_by=partition_keys, order_by=order_keys)
        fn = transform.function
        if fn == "row_number":
            wexpr = ibis.row_number().over(window)
        elif fn == "rank":
            wexpr = ibis.rank().over(window)
        elif fn == "dense_rank":
            wexpr = ibis.dense_rank().over(window)
        elif fn in ("lag", "lead") and transform.column:
            col = expr[transform.column]
            wexpr = (col.lag(transform.offset) if fn == "lag" else col.lead(transform.offset)).over(window)
        elif fn in ("cum_sum", "cum_max", "cum_min") and transform.column:
            method = {"cum_sum": "sum", "cum_max": "max", "cum_min": "min"}[fn]
            wexpr = getattr(expr[transform.column], method)().over(window)
        else:
            raise ValueError(f"Unknown window function: {fn}")
        return expr.mutate(**{transform.alias: wexpr})

    raise TypeError(f"Unsupported transform type: {type(transform).__name__}")


def _ibis_join_how(how: str) -> str:
    mapping = {"outer": "outer", "cross": "cross", "inner": "inner", "left": "left", "right": "right"}
    return mapping.get(how, "inner")


def _eval_simple_expr(expr, source: str):
    """Only support `col OP literal` / `col` / `literal`. Anything else is rejected."""
    import ast
    tree = ast.parse(source, mode="eval").body

    def visit(node):
        if isinstance(node, ast.Name):
            if node.id not in expr.columns:
                raise ValueError(f"Unknown column in expression: {node.id}")
            return expr[node.id]
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.BinOp):
            left = visit(node.left)
            right = visit(node.right)
            op_map = {
                ast.Add: lambda a, b: a + b, ast.Sub: lambda a, b: a - b,
                ast.Mult: lambda a, b: a * b, ast.Div: lambda a, b: a / b,
                ast.Mod: lambda a, b: a % b,
            }
            fn = op_map.get(type(node.op))
            if fn is None:
                raise ValueError("Unsupported operator")
            return fn(left, right)
        raise ValueError(f"Unsupported node: {type(node).__name__}")

    return visit(tree)


def execute_pipeline(pipeline: Pipeline, *, backend: str = "duckdb", limit: int | None = None):
    """Run a pipeline through Ibis and return the final result as a Polars
    DataFrame (for UI parity with the Polars engine)."""
    _require_ibis()

    be = get_backend(backend)

    loaded: dict[str, Any] = {}
    for src in pipeline.sources:
        loaded[src.id] = load_source(be, src)

    output = loaded.get(pipeline.output_source_id)
    if output is None:
        raise ValueError(f"output_source_id {pipeline.output_source_id!r} not in loaded sources")

    for t in pipeline.transforms:
        output = apply_transform(output, t, backend=be, loaded_tables=loaded)

    if limit is not None:
        output = output.limit(limit)

    return output.to_polars()


def profile(pipeline: Pipeline, *, backend: str = "duckdb", sample_limit: int = 10_000) -> dict:
    """Per-column stats: null count, distinct count, min, max, mean (numeric).

    Runs against the pipeline output; uses a `sample_limit` to keep it cheap
    on big tables. Returns a dict shaped for the UI.
    """
    _require_ibis()

    be = get_backend(backend)
    loaded = {s.id: load_source(be, s) for s in pipeline.sources}
    output = loaded[pipeline.output_source_id]
    for t in pipeline.transforms:
        output = apply_transform(output, t, backend=be, loaded_tables=loaded)
    output = output.limit(sample_limit)

    schema = output.schema()
    columns_stats = []
    for col_name in output.columns:
        col = output[col_name]
        stats: dict = {"name": col_name, "type": str(schema[col_name])}
        try:
            agg = output.aggregate(
                null_count=col.isnull().sum(),
                distinct=col.nunique(),
                rows=output.count(),
            ).to_polars()
            row = agg.row(0, named=True)
            stats["null_count"] = int(row.get("null_count") or 0)
            stats["distinct"] = int(row.get("distinct") or 0)
            stats["rows"] = int(row.get("rows") or 0)
        except Exception as e:
            stats["error"] = str(e)

        try:
            dtype = str(schema[col_name])
            if any(x in dtype.lower() for x in ("int", "float", "decimal", "numeric")):
                num_agg = output.aggregate(
                    min=col.min(), max=col.max(), mean=col.mean()
                ).to_polars()
                row = num_agg.row(0, named=True)
                stats["min"] = row.get("min")
                stats["max"] = row.get("max")
                stats["mean"] = float(row["mean"]) if row.get("mean") is not None else None
        except Exception:
            pass

        columns_stats.append(stats)

    return {
        "columns": columns_stats,
        "sample_limit": sample_limit,
        "backend": backend,
    }
