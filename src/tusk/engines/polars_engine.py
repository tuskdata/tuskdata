"""Polars engine for data transformations and ETL pipelines"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Literal
import ast
import msgspec
import polars as pl
import duckdb

from tusk.core.logging import get_logger

log = get_logger("polars_engine")


# ============================================================================
# Pipeline Models
# ============================================================================

class DataSource(msgspec.Struct):
    """A data source for the pipeline"""
    id: str
    name: str
    source_type: Literal["csv", "parquet", "json", "sql", "database", "osm"]
    path: str | None = None  # For file sources
    connection_id: str | None = None  # For SQL sources
    query: str | None = None  # For SQL sources
    osm_layer: str | None = None  # For OSM: buildings, landuse, natural, pois, etc.


class FilterTransform(msgspec.Struct, tag="filter"):
    """Filter rows based on condition"""
    column: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "contains", "starts_with", "ends_with", "is_null", "is_not_null", "is_empty", "is_not_empty"]
    value: Any = None


class SelectTransform(msgspec.Struct, tag="select"):
    """Select specific columns"""
    columns: list[str]


class RenameTransform(msgspec.Struct, tag="rename"):
    """Rename columns"""
    mapping: dict[str, str]  # old_name -> new_name


class SortTransform(msgspec.Struct, tag="sort"):
    """Sort by columns"""
    columns: list[str]
    descending: list[bool] | None = None


class GroupByTransform(msgspec.Struct, tag="group_by"):
    """Group by columns with aggregations"""
    by: list[str]
    aggregations: list[dict]  # [{"column": "x", "agg": "sum", "alias": "x_sum"}, ...]


class AddColumnTransform(msgspec.Struct, tag="add_column"):
    """Add a computed column"""
    name: str
    expression: str  # Polars expression as string


class DropNullsTransform(msgspec.Struct, tag="drop_nulls"):
    """Drop rows with null values"""
    subset: list[str] | None = None


class LimitTransform(msgspec.Struct, tag="limit"):
    """Limit number of rows"""
    n: int


class JoinTransform(msgspec.Struct, tag="join"):
    """Join with another dataset"""
    right_source_id: str
    on: list[str] | None = None
    left_on: list[str] | None = None
    right_on: list[str] | None = None
    how: Literal["inner", "left", "right", "outer", "cross"] = "inner"


class ConcatTransform(msgspec.Struct, tag="concat"):
    """Concatenate (UNION) with another dataset"""
    source_ids: list[str]
    how: Literal["vertical", "diagonal", "align"] = "vertical"


class DistinctTransform(msgspec.Struct, tag="distinct"):
    """Remove duplicate rows"""
    subset: list[str] | None = None
    keep: Literal["first", "last", "any", "none"] = "first"


class WindowTransform(msgspec.Struct, tag="window"):
    """Window function (row_number, rank, lag, lead, etc.) over partitions"""
    function: Literal["row_number", "rank", "dense_rank", "lag", "lead", "cum_sum", "cum_max", "cum_min"]
    order_by: list[str]
    partition_by: list[str] | None = None
    alias: str = "window_col"
    descending: bool = False
    column: str | None = None  # Required for lag/lead/cum_*
    offset: int = 1  # For lag/lead


# Union of all transform types
Transform = (
    FilterTransform | SelectTransform | RenameTransform | SortTransform |
    GroupByTransform | AddColumnTransform | DropNullsTransform |
    LimitTransform | JoinTransform | ConcatTransform | DistinctTransform |
    WindowTransform
)


class Pipeline(msgspec.Struct):
    """A data transformation pipeline"""
    id: str
    name: str
    sources: list[DataSource]
    transforms: list[Transform]
    output_source_id: str  # Which source the transforms apply to
    created_at: str | None = None
    updated_at: str | None = None


# ============================================================================
# Code Generation
# ============================================================================

def generate_code(pipeline: Pipeline) -> str:
    """Generate Polars Python code from a pipeline"""
    lines = ["import polars as pl", ""]

    # Load sources
    for source in pipeline.sources:
        var_name = _safe_var_name(source.id)
        if source.source_type == "csv":
            lines.append(f'{var_name} = pl.read_csv("{source.path}")')
        elif source.source_type == "parquet":
            lines.append(f'{var_name} = pl.read_parquet("{source.path}")')
        elif source.source_type == "json":
            lines.append(f'{var_name} = pl.read_json("{source.path}")')
        elif source.source_type == "osm":
            layer = source.osm_layer or "nodes"
            lines.append(f'# Load OSM data using DuckDB spatial')
            lines.append(f'import duckdb')
            lines.append(f'_conn = duckdb.connect()')
            lines.append(f'_conn.execute("INSTALL spatial; LOAD spatial;")')
            lines.append(f'{var_name} = _conn.execute("""')
            lines.append(f'    SELECT id, kind, tags, lat, lon, ST_AsGeoJSON(ST_Point(lon, lat)) as geometry')
            lines.append(f'    FROM st_readosm(\'{source.path}\')')
            lines.append(f'    WHERE kind = \'{layer.rstrip("s") if layer != "all" else "node"}\' AND lat IS NOT NULL')
            lines.append(f'""").pl()  # Remove LIMIT clause for full data')
        elif source.source_type == "sql":
            lines.append(f'# {var_name} = load from SQL connection {source.connection_id}')

    lines.append("")

    # Apply transforms
    output_var = _safe_var_name(pipeline.output_source_id)
    lines.append(f"result = {output_var}")

    for transform in pipeline.transforms:
        code = _transform_to_code(transform, pipeline.sources)
        if code:
            lines.append(f"result = result{code}")

    lines.append("")
    lines.append("print(result)")

    return "\n".join(lines)


def _safe_var_name(name: str) -> str:
    """Convert name to safe Python variable name"""
    return "df_" + "".join(c if c.isalnum() else "_" for c in name)


def _transform_to_code(transform: Transform, sources: list[DataSource]) -> str:
    """Convert a transform to Polars method chain code"""
    if isinstance(transform, FilterTransform):
        col = f'pl.col("{transform.column}")'
        op = transform.operator
        val = repr(transform.value) if transform.value is not None else None

        if op == "eq":
            return f'.filter({col} == {val})'
        elif op == "ne":
            return f'.filter({col} != {val})'
        elif op == "gt":
            return f'.filter({col} > {val})'
        elif op == "gte":
            return f'.filter({col} >= {val})'
        elif op == "lt":
            return f'.filter({col} < {val})'
        elif op == "lte":
            return f'.filter({col} <= {val})'
        elif op == "contains":
            return f'.filter({col}.str.contains({val}))'
        elif op == "starts_with":
            return f'.filter({col}.str.starts_with({val}))'
        elif op == "ends_with":
            return f'.filter({col}.str.ends_with({val}))'
        elif op == "is_null":
            return f'.filter({col}.is_null())'
        elif op == "is_not_null":
            return f'.filter({col}.is_not_null())'
        elif op == "is_empty":
            return f'.filter({col}.list.len() == 0)'
        elif op == "is_not_empty":
            return f'.filter({col}.list.len() > 0)'

    elif isinstance(transform, SelectTransform):
        cols = ", ".join(f'"{c}"' for c in transform.columns)
        return f'.select([{cols}])'

    elif isinstance(transform, RenameTransform):
        mapping = ", ".join(f'"{k}": "{v}"' for k, v in transform.mapping.items())
        return f'.rename({{{mapping}}})'

    elif isinstance(transform, SortTransform):
        cols = ", ".join(f'"{c}"' for c in transform.columns)
        if transform.descending:
            desc = ", ".join(str(d).lower() for d in transform.descending)
            return f'.sort([{cols}], descending=[{desc}])'
        return f'.sort([{cols}])'

    elif isinstance(transform, GroupByTransform):
        by_cols = ", ".join(f'"{c}"' for c in transform.by)
        aggs = []
        for agg in transform.aggregations:
            col = agg["column"]
            func = agg["agg"]
            alias = agg.get("alias", f"{col}_{func}")
            aggs.append(f'pl.col("{col}").{func}().alias("{alias}")')
        aggs_str = ", ".join(aggs)
        return f'.group_by([{by_cols}]).agg([{aggs_str}])'

    elif isinstance(transform, AddColumnTransform):
        return f'.with_columns({transform.expression}.alias("{transform.name}"))'

    elif isinstance(transform, DropNullsTransform):
        if transform.subset:
            cols = ", ".join(f'"{c}"' for c in transform.subset)
            return f'.drop_nulls(subset=[{cols}])'
        return '.drop_nulls()'

    elif isinstance(transform, LimitTransform):
        return f'.limit({transform.n})'

    elif isinstance(transform, JoinTransform):
        right_var = _safe_var_name(transform.right_source_id)
        if transform.on:
            on_cols = ", ".join(f'"{c}"' for c in transform.on)
            return f'.join({right_var}, on=[{on_cols}], how="{transform.how}")'
        else:
            left_on = ", ".join(f'"{c}"' for c in (transform.left_on or []))
            right_on = ", ".join(f'"{c}"' for c in (transform.right_on or []))
            return f'.join({right_var}, left_on=[{left_on}], right_on=[{right_on}], how="{transform.how}")'

    elif isinstance(transform, ConcatTransform):
        vars_list = ", ".join(_safe_var_name(sid) for sid in transform.source_ids)
        return f'\nresult = pl.concat([result, {vars_list}], how="{transform.how}")\nresult = result'

    elif isinstance(transform, DistinctTransform):
        if transform.subset:
            cols = ", ".join(f'"{c}"' for c in transform.subset)
            return f'.unique(subset=[{cols}], keep="{transform.keep}")'
        return f'.unique(keep="{transform.keep}")'

    elif isinstance(transform, WindowTransform):
        fn = transform.function
        order_cols = ", ".join(f'"{c}"' for c in transform.order_by)
        desc = "True" if transform.descending else "False"
        partition = ""
        if transform.partition_by:
            part_cols = ", ".join(f'"{c}"' for c in transform.partition_by)
            partition = f".over([{part_cols}])"

        if fn == "row_number":
            expr = f"pl.int_range(1, pl.len() + 1){partition}"
        elif fn == "rank":
            expr = f'pl.col("{transform.order_by[0]}").rank(method="min", descending={desc}){partition}'
        elif fn == "dense_rank":
            expr = f'pl.col("{transform.order_by[0]}").rank(method="dense", descending={desc}){partition}'
        elif fn in ("lag", "lead"):
            shift = transform.offset if fn == "lag" else -transform.offset
            expr = f'pl.col("{transform.column}").shift({shift}){partition}'
        elif fn in ("cum_sum", "cum_max", "cum_min"):
            expr = f'pl.col("{transform.column}").{fn}(){partition}'
        else:
            return ""

        return f'.sort([{order_cols}], descending={desc}).with_columns({expr}.alias("{transform.alias}"))'

    return ""


# ============================================================================
# Execution
# ============================================================================

def load_source(source: DataSource) -> pl.DataFrame:
    """Load a data source into a Polars DataFrame"""
    if source.source_type == "csv":
        path = Path(source.path).expanduser()
        return pl.read_csv(path)
    elif source.source_type == "parquet":
        path = Path(source.path).expanduser()
        return pl.read_parquet(path)
    elif source.source_type == "json":
        path = Path(source.path).expanduser()
        return pl.read_json(path)
    elif source.source_type == "osm":
        return load_osm(source.path, source.osm_layer)
    elif source.source_type in ("sql", "database"):
        return load_sql_source(source.connection_id, source.query)
    else:
        raise ValueError(f"Unknown source type: {source.source_type}")


def load_osm(path: str, layer: str | None = None, limit: int | None = None) -> pl.DataFrame:
    """Load OSM data from .osm.pbf file using DuckDB spatial ST_ReadOSM

    Args:
        path: Path to the .osm.pbf file
        layer: Optional layer filter (nodes, ways, relations, all)
        limit: Optional row limit. If None, loads all data.
    """
    p = Path(path).expanduser()

    log.info("Loading OSM file", path=str(p), layer=layer, limit=limit)

    # Check file exists
    if not p.exists():
        log.error("OSM file not found", path=str(p))
        return pl.DataFrame({"error": [f"File not found: {p}"]})

    # Create a temporary DuckDB connection with spatial extension
    conn = duckdb.connect()

    try:
        log.debug("Installing DuckDB spatial extension")
        conn.execute("INSTALL spatial; LOAD spatial;")
    except Exception as e:
        log.error("Failed to load DuckDB spatial extension", error=str(e))
        conn.close()
        return pl.DataFrame({"error": [f"DuckDB spatial extension error: {e}"]})

    # Layer/kind filter for OSM data
    # ST_ReadOSM returns: kind (node/way/relation), id, tags, refs, lat, lon, ref_roles
    layer = layer or "all"
    limit_clause = f"LIMIT {int(limit)}" if limit else ""

    # Escape path for DuckDB string literal (prevent SQL injection)
    safe_p = str(p).replace("'", "''")

    try:
        # Use ST_ReadOSM for .osm.pbf files - it's much faster and works directly
        if layer == "nodes" or layer == "points":
            # Nodes with coordinates (POIs, etc)
            query = f"""
                SELECT
                    id,
                    kind,
                    tags,
                    lat,
                    lon,
                    ST_AsGeoJSON(ST_Point(lon, lat)) as geometry
                FROM st_readosm('{safe_p}')
                WHERE kind = 'node' AND lat IS NOT NULL
                {limit_clause}
            """
        elif layer == "ways" or layer == "lines":
            # Ways (roads, paths, etc) - raw data without geometry construction
            query = f"""
                SELECT
                    id,
                    kind,
                    tags,
                    refs,
                    len(refs) as ref_count
                FROM st_readosm('{safe_p}')
                WHERE kind = 'way'
                {limit_clause}
            """
        elif layer == "relations":
            # Relations (boundaries, routes, etc)
            query = f"""
                SELECT
                    id,
                    kind,
                    tags,
                    refs,
                    ref_roles,
                    len(refs) as ref_count
                FROM st_readosm('{safe_p}')
                WHERE kind = 'relation'
                {limit_clause}
            """
        else:
            # All data
            query = f"""
                SELECT
                    id,
                    kind,
                    tags,
                    CASE WHEN lat IS NOT NULL THEN ST_AsGeoJSON(ST_Point(lon, lat)) END as geometry,
                    CASE WHEN refs IS NOT NULL THEN len(refs) END as ref_count
                FROM st_readosm('{safe_p}')
                {limit_clause}
            """

        log.debug("Executing DuckDB query", query=query[:200])
        result = conn.execute(query).pl()

        log.info("OSM loaded successfully", rows=result.height, columns=len(result.columns))
        return result

    except Exception as e:
        log.error("Failed to load OSM file", path=str(p), layer=layer, error=str(e))
        return pl.DataFrame({
            "error": [str(e)],
            "hint": ["Make sure you have DuckDB spatial extension. Try layers: nodes, ways, relations, or all"]
        })
    finally:
        conn.close()


def load_sql_source(connection_id: str | None, query: str | None) -> pl.DataFrame:
    """Load data from a PostgreSQL connection into a Polars DataFrame.

    Uses psycopg synchronous connection to fetch results and convert to Polars.
    """
    if not connection_id:
        raise ValueError("SQL source requires a connection_id")
    if not query:
        raise ValueError("SQL source requires a query")

    from tusk.core.connection import get_connection

    conn_config = get_connection(connection_id)
    if conn_config is None:
        raise ValueError(f"Connection not found: {connection_id}")

    if conn_config.type != "postgres":
        raise ValueError(f"SQL source only supports PostgreSQL connections, got: {conn_config.type}")

    try:
        import psycopg

        dsn = conn_config.dsn
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description is None:
                    raise ValueError("Query did not return any results")

                columns = [desc.name for desc in cur.description]
                rows = cur.fetchall()

                if not rows:
                    # Return empty DataFrame with proper schema
                    return pl.DataFrame({col: [] for col in columns})

                # Build dict of columns
                data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                return pl.DataFrame(data)

    except ImportError:
        raise ValueError("psycopg is required for SQL sources. Install with: pip install psycopg[binary]")


# ============================================================================
# Safe Polars Expression Parser (replaces eval())
# ============================================================================

_ALLOWED_PL_FUNCS = {"col", "lit", "when", "concat_str"}
_ALLOWED_CHAIN_METHODS = {
    "str": {"to_uppercase", "to_lowercase", "contains", "strip_chars", "replace",
            "starts_with", "ends_with", "slice", "lengths", "len_chars", "to_integer"},
    "cast": True,
    "abs": True, "round": True, "fill_null": True, "fill_nan": True,
    "alias": True, "is_null": True, "is_not_null": True,
    "sort": True, "reverse": True, "shift": True, "over": True,
}


def _safe_eval_polars_expr(expression: str) -> pl.Expr:
    """Safely parse a Polars expression string without eval()"""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ValueError(f"Invalid expression syntax: {e}")

    return _eval_node(tree.body)


def _eval_node(node):
    """Recursively evaluate AST node into Polars expression"""
    # Number/string literals
    if isinstance(node, ast.Constant):
        return pl.lit(node.value)

    # Binary operations: expr + expr, expr * expr, etc.
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left)
        right = _eval_node(node.right)
        ops = {
            ast.Add: lambda l, r: l + r,
            ast.Sub: lambda l, r: l - r,
            ast.Mult: lambda l, r: l * r,
            ast.Div: lambda l, r: l / r,
            ast.FloorDiv: lambda l, r: l // r,
            ast.Mod: lambda l, r: l % r,
        }
        op_func = ops.get(type(node.op))
        if op_func is None:
            raise ValueError(f"Unsupported operator: {type(node.op).__name__}")
        return op_func(left, right)

    # Comparison operations
    if isinstance(node, ast.Compare):
        left = _eval_node(node.left)
        if len(node.ops) != 1:
            raise ValueError("Only single comparisons supported")
        right = _eval_node(node.comparators[0])
        cmp_ops = {
            ast.Gt: lambda l, r: l > r,
            ast.Lt: lambda l, r: l < r,
            ast.GtE: lambda l, r: l >= r,
            ast.LtE: lambda l, r: l <= r,
            ast.Eq: lambda l, r: l == r,
            ast.NotEq: lambda l, r: l != r,
        }
        op_func = cmp_ops.get(type(node.ops[0]))
        if op_func is None:
            raise ValueError(f"Unsupported comparison: {type(node.ops[0]).__name__}")
        return op_func(left, right)

    # Unary operations (negation)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_eval_node(node.operand)

    # Function calls: pl.col("x"), pl.lit(42), .method()
    if isinstance(node, ast.Call):
        # pl.col("name") or pl.lit(value)
        if (isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "pl"):
            func_name = node.func.attr
            if func_name not in _ALLOWED_PL_FUNCS:
                raise ValueError(f"Disallowed pl function: {func_name}")

            if func_name == "col":
                args = [_eval_literal(a) for a in node.args]
                return pl.col(*args)
            elif func_name == "lit":
                args = [_eval_literal(a) for a in node.args]
                return pl.lit(*args)
            elif func_name == "concat_str":
                return pl.concat_str(*[_eval_node(a) for a in node.args])
            elif func_name == "when":
                args = [_eval_node(a) for a in node.args]
                return pl.when(*args)

        # Method calls on expressions: expr.str.to_uppercase(), expr.round(2)
        if isinstance(node.func, ast.Attribute):
            obj = _eval_node(node.func.value)
            method_name = node.func.attr

            # Disallow bare .str call (must be .str.method())
            if method_name == "str":
                raise ValueError("Use full method like .str.to_uppercase()")

            # Check if it's a str sub-method (obj is already ExprStringNameSpace)
            allowed_str = _ALLOWED_CHAIN_METHODS.get("str", set())
            if isinstance(allowed_str, set) and method_name in allowed_str:
                args = [_eval_literal(a) for a in node.args]
                return getattr(obj, method_name)(*args)

            # Check allowed chain methods
            if method_name in _ALLOWED_CHAIN_METHODS and method_name != "str":
                # Cast needs special handling for pl.Int64 etc
                if method_name == "cast":
                    dtype_arg = node.args[0] if node.args else None
                    if (dtype_arg
                            and isinstance(dtype_arg, ast.Attribute)
                            and isinstance(dtype_arg.value, ast.Name)
                            and dtype_arg.value.id == "pl"):
                        dtype_name = dtype_arg.attr
                        dtype = getattr(pl, dtype_name, None)
                        if dtype is None:
                            raise ValueError(f"Unknown dtype: pl.{dtype_name}")
                        return obj.cast(dtype)
                    raise ValueError("cast() requires a pl.DataType argument like pl.Int64")

                args = [_eval_literal(a) for a in node.args]
                return getattr(obj, method_name)(*args)

            raise ValueError(f"Disallowed method: {method_name}")

        raise ValueError(f"Unsupported call: {ast.dump(node.func)}")

    # Attribute access for .str accessor
    if isinstance(node, ast.Attribute):
        obj = _eval_node(node.value)
        attr = node.attr
        if attr == "str":
            return obj.str
        raise ValueError(f"Disallowed attribute access: {attr}")

    # Name lookups (only allow "pl")
    if isinstance(node, ast.Name):
        if node.id == "pl":
            return pl  # Will be used in pl.col, pl.lit calls
        raise ValueError(f"Disallowed name: {node.id}")

    raise ValueError(f"Unsupported expression: {ast.dump(node)}")


def _eval_literal(node):
    """Extract a literal value from an AST node"""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name) and node.id == "pl":
        return pl
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "pl":
        # pl.Int64, pl.Utf8, etc.
        return getattr(pl, node.attr)
    raise ValueError(f"Expected literal, got: {ast.dump(node)}")


def apply_transform(df: pl.DataFrame, transform: Transform, sources_map: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Apply a transform to a DataFrame"""
    if isinstance(transform, FilterTransform):
        col = pl.col(transform.column)
        op = transform.operator
        val = transform.value

        if op == "eq":
            return df.filter(col == val)
        elif op == "ne":
            return df.filter(col != val)
        elif op == "gt":
            return df.filter(col > val)
        elif op == "gte":
            return df.filter(col >= val)
        elif op == "lt":
            return df.filter(col < val)
        elif op == "lte":
            return df.filter(col <= val)
        elif op == "contains":
            return df.filter(col.str.contains(val))
        elif op == "starts_with":
            return df.filter(col.str.starts_with(val))
        elif op == "ends_with":
            return df.filter(col.str.ends_with(val))
        elif op == "is_null":
            return df.filter(col.is_null())
        elif op == "is_not_null":
            return df.filter(col.is_not_null())
        elif op == "is_empty":
            return df.filter(col.list.len() == 0)
        elif op == "is_not_empty":
            return df.filter(col.list.len() > 0)

    elif isinstance(transform, SelectTransform):
        return df.select(transform.columns)

    elif isinstance(transform, RenameTransform):
        return df.rename(transform.mapping)

    elif isinstance(transform, SortTransform):
        descending = transform.descending or [False] * len(transform.columns)
        return df.sort(transform.columns, descending=descending)

    elif isinstance(transform, GroupByTransform):
        aggs = []
        for agg in transform.aggregations:
            col = pl.col(agg["column"])
            func = agg["agg"]
            alias = agg.get("alias", f"{agg['column']}_{func}")

            if func == "sum":
                aggs.append(col.sum().alias(alias))
            elif func == "mean":
                aggs.append(col.mean().alias(alias))
            elif func == "min":
                aggs.append(col.min().alias(alias))
            elif func == "max":
                aggs.append(col.max().alias(alias))
            elif func == "count":
                aggs.append(col.count().alias(alias))
            elif func == "first":
                aggs.append(col.first().alias(alias))
            elif func == "last":
                aggs.append(col.last().alias(alias))

        return df.group_by(transform.by).agg(aggs)

    elif isinstance(transform, AddColumnTransform):
        expr = _safe_eval_polars_expr(transform.expression)
        return df.with_columns(expr.alias(transform.name))

    elif isinstance(transform, DropNullsTransform):
        if transform.subset:
            return df.drop_nulls(subset=transform.subset)
        return df.drop_nulls()

    elif isinstance(transform, LimitTransform):
        return df.limit(transform.n)

    elif isinstance(transform, JoinTransform):
        right_df = sources_map.get(transform.right_source_id)
        if right_df is None:
            raise ValueError(f"Source not found: {transform.right_source_id}")

        if transform.on:
            return df.join(right_df, on=transform.on, how=transform.how)
        else:
            return df.join(
                right_df,
                left_on=transform.left_on,
                right_on=transform.right_on,
                how=transform.how
            )

    elif isinstance(transform, ConcatTransform):
        frames = [df]
        for source_id in transform.source_ids:
            other = sources_map.get(source_id)
            if other is None:
                raise ValueError(f"Source not found for concat: {source_id}")
            frames.append(other)
        return pl.concat(frames, how=transform.how)

    elif isinstance(transform, DistinctTransform):
        return df.unique(subset=transform.subset, keep=transform.keep)

    elif isinstance(transform, WindowTransform):
        order_expr = [pl.col(c).sort(descending=transform.descending) for c in transform.order_by]
        partition = transform.partition_by or []

        fn = transform.function
        if fn == "row_number":
            expr = pl.int_range(1, pl.len() + 1)
        elif fn == "rank":
            # Rank based on order_by column
            col = transform.order_by[0]
            expr = pl.col(col).rank(method="min", descending=transform.descending)
        elif fn == "dense_rank":
            col = transform.order_by[0]
            expr = pl.col(col).rank(method="dense", descending=transform.descending)
        elif fn == "lag":
            if not transform.column:
                raise ValueError("lag requires a column")
            expr = pl.col(transform.column).shift(transform.offset)
        elif fn == "lead":
            if not transform.column:
                raise ValueError("lead requires a column")
            expr = pl.col(transform.column).shift(-transform.offset)
        elif fn == "cum_sum":
            if not transform.column:
                raise ValueError("cum_sum requires a column")
            expr = pl.col(transform.column).cum_sum()
        elif fn == "cum_max":
            if not transform.column:
                raise ValueError("cum_max requires a column")
            expr = pl.col(transform.column).cum_max()
        elif fn == "cum_min":
            if not transform.column:
                raise ValueError("cum_min requires a column")
            expr = pl.col(transform.column).cum_min()
        else:
            raise ValueError(f"Unknown window function: {fn}")

        if partition:
            expr = expr.over(partition)

        return df.sort(transform.order_by, descending=transform.descending).with_columns(
            expr.alias(transform.alias)
        )

    return df


def execute_pipeline(pipeline: Pipeline, limit: int | None = 100) -> dict:
    """Execute a pipeline and return results"""
    try:
        result = _run_pipeline(pipeline, limit)

        # Convert to dict for JSON serialization
        columns = [{"name": name, "type": str(dtype)} for name, dtype in zip(result.columns, result.dtypes)]
        rows = result.rows()

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "total_count": result.height if not limit else None,
            "engine_used": "polars",
        }

    except Exception as e:
        return {"error": str(e)}


def get_schema(path: str, osm_layer: str | None = None) -> dict:
    """Get schema of a file"""
    p = Path(path).expanduser()
    log.info("Getting schema", path=str(p), osm_layer=osm_layer)

    try:
        if p.suffix.lower() == ".csv":
            df = pl.read_csv(p, n_rows=0)
        elif p.suffix.lower() == ".parquet":
            df = pl.read_parquet(p, n_rows=0)
        elif p.suffix.lower() == ".json":
            df = pl.read_json(p)
            df = df.limit(0)
        elif p.suffix.lower() == ".pbf" or path.endswith(".osm.pbf"):
            # For OSM, load a small sample to get schema
            df = load_osm(path, osm_layer or "nodes", limit=10)
            # Check if we got an error
            if "error" in df.columns:
                return {"error": df["error"][0]}
        else:
            log.warning("Unsupported file type", path=str(p), suffix=p.suffix)
            return {"error": f"Unsupported file type: {p.suffix}"}

        log.debug("Schema retrieved", columns=len(df.columns))
        return {
            "columns": [{"name": name, "type": str(dtype)} for name, dtype in zip(df.columns, df.dtypes)]
        }

    except Exception as e:
        log.error("Failed to get schema", path=str(p), error=str(e))
        return {"error": str(e)}


def preview_file(path: str, limit: int = 100, osm_layer: str | None = None) -> dict:
    """Preview a file's contents"""
    import time
    start = time.perf_counter()

    p = Path(path).expanduser()
    log.info("Previewing file", path=str(p), limit=limit, osm_layer=osm_layer)

    try:
        t1 = time.perf_counter()
        if p.suffix.lower() == ".csv":
            # Use scan + collect for faster partial reads
            df = pl.scan_csv(p).head(limit).collect()
        elif p.suffix.lower() == ".parquet":
            df = pl.scan_parquet(p).limit(limit).collect()
        elif p.suffix.lower() == ".json":
            df = pl.read_json(p).limit(limit)
        elif p.suffix.lower() == ".pbf" or path.endswith(".osm.pbf"):
            # Pass limit directly to load_osm for efficiency
            df = load_osm(path, osm_layer or "nodes", limit=limit)
            # Check if we got an error
            if "error" in df.columns:
                return {"error": df["error"][0], "hint": df["hint"][0] if "hint" in df.columns else None}
        else:
            log.warning("Unsupported file type", path=str(p), suffix=p.suffix)
            return {"error": f"Unsupported file type: {p.suffix}"}
        t2 = time.perf_counter()

        columns = [{"name": name, "type": str(dtype)} for name, dtype in zip(df.columns, df.dtypes)]
        rows = df.rows()
        t3 = time.perf_counter()

        log.info("Preview successful",
                 rows=len(rows),
                 columns=len(columns),
                 read_ms=round((t2-t1)*1000, 2),
                 convert_ms=round((t3-t2)*1000, 2),
                 total_ms=round((t3-start)*1000, 2))
        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows)
        }

    except Exception as e:
        log.error("Failed to preview file", path=str(p), error=str(e))
        return {"error": str(e)}


def get_osm_layers(path: str) -> dict:
    """Get available layers in an OSM file"""
    # ST_ReadOSM returns kind: node, way, relation
    layers = [
        {"id": "all", "name": "All Data", "description": "All OSM elements (nodes, ways, relations)"},
        {"id": "nodes", "name": "Nodes/Points", "description": "POIs, addresses, standalone points with coordinates"},
        {"id": "ways", "name": "Ways/Lines", "description": "Roads, paths, rivers, building outlines"},
        {"id": "relations", "name": "Relations", "description": "Complex structures: routes, boundaries, multipolygons"},
    ]

    return {"layers": layers, "path": path}


# ============================================================================
# Export Functions
# ============================================================================

def _run_pipeline(pipeline: Pipeline, limit: int | None = None) -> pl.DataFrame:
    """Load sources, apply transforms, return result DataFrame.

    Shared helper used by execute_pipeline, export, and import functions.
    Raises ValueError on errors.
    """
    sources_map = {}
    for source in pipeline.sources:
        sources_map[source.id] = load_source(source)

    result = sources_map.get(pipeline.output_source_id)
    if result is None:
        raise ValueError(f"Output source not found: {pipeline.output_source_id}")

    for transform in pipeline.transforms:
        result = apply_transform(result, transform, sources_map)
        if isinstance(transform, (JoinTransform, ConcatTransform)):
            sources_map[pipeline.output_source_id] = result

    if limit:
        result = result.limit(limit)
    return result


def export_to_csv(pipeline: Pipeline, output_path: str, limit: int | None = None) -> dict:
    """Export pipeline results to CSV file"""
    try:
        result = _run_pipeline(pipeline, limit)

        if limit:
            result = result.limit(limit)

        # Export to CSV
        p = Path(output_path).expanduser()
        result.write_csv(p)

        log.info("Exported to CSV", path=str(p), rows=result.height)
        return {"success": True, "path": str(p), "rows": result.height}

    except Exception as e:
        log.error("Failed to export CSV", error=str(e))
        return {"error": str(e)}


def export_to_parquet(pipeline: Pipeline, output_path: str, limit: int | None = None) -> dict:
    """Export pipeline results to Parquet file"""
    try:
        result = _run_pipeline(pipeline, limit)

        # Export to Parquet
        p = Path(output_path).expanduser()
        result.write_parquet(p)

        log.info("Exported to Parquet", path=str(p), rows=result.height)
        return {"success": True, "path": str(p), "rows": result.height}

    except Exception as e:
        log.error("Failed to export Parquet", error=str(e))
        return {"error": str(e)}


# ============================================================================
# Import to Database Functions
# ============================================================================

def import_to_duckdb(pipeline: Pipeline, table_name: str, db_path: str | None = None, limit: int | None = None) -> dict:
    """Import pipeline results to DuckDB table"""
    try:
        result = _run_pipeline(pipeline, limit)

        # Connect to DuckDB (in-memory or file)
        conn = duckdb.connect(db_path or ":memory:")

        try:
            # Register the polars DataFrame and create table
            conn.register("temp_df", result)
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM temp_df')

            row_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

            log.info("Imported to DuckDB", table=table_name, db=db_path or ":memory:", rows=row_count)
            return {
                "success": True,
                "table": table_name,
                "database": db_path or ":memory:",
                "rows": row_count,
                "columns": result.columns
            }
        finally:
            conn.close()

    except Exception as e:
        log.error("Failed to import to DuckDB", error=str(e))
        return {"error": str(e)}


async def import_to_postgres(
    pipeline: Pipeline,
    table_name: str,
    connection_id: str,
    limit: int | None = None,
    progress_callback: callable = None
) -> dict:
    """Import pipeline results to PostgreSQL table using COPY for fast bulk loading

    Args:
        pipeline: The data pipeline to execute
        table_name: Target table name in PostgreSQL
        connection_id: ID of the PostgreSQL connection
        limit: Optional row limit
        progress_callback: Optional async callback(current, total, message) for progress updates
    """
    import json
    import time

    try:
        import psycopg
        from psycopg import sql

        async def report_progress(current: int, total: int, message: str):
            if progress_callback:
                try:
                    await progress_callback(current, total, message)
                except Exception:
                    pass  # Don't fail on progress errors
            log.debug("Import progress", current=current, total=total, message=message)

        await report_progress(0, 100, "Loading data...")

        result = _run_pipeline(pipeline, limit)
        total_rows = result.height
        await report_progress(10, 100, f"Loaded {total_rows:,} rows, connecting to PostgreSQL...")

        # Get connection from registry
        from tusk.core.connection import get_connection
        conn_info = get_connection(connection_id)
        if not conn_info:
            return {"error": f"Connection not found: {connection_id}"}

        if conn_info.type != "postgres":
            return {"error": f"Connection {connection_id} is not a PostgreSQL connection"}

        # Build connection string
        conn_str = f"postgresql://{conn_info.user}:{conn_info.password}@{conn_info.host}:{conn_info.port}/{conn_info.database}"

        # Polars type to PostgreSQL type mapping
        type_map = {
            "Int64": "BIGINT",
            "Int32": "INTEGER",
            "Int16": "SMALLINT",
            "Int8": "SMALLINT",
            "UInt64": "BIGINT",
            "UInt32": "INTEGER",
            "UInt16": "INTEGER",
            "UInt8": "SMALLINT",
            "Float64": "DOUBLE PRECISION",
            "Float32": "REAL",
            "Boolean": "BOOLEAN",
            "String": "TEXT",
            "Utf8": "TEXT",
            "Date": "DATE",
            "Datetime": "TIMESTAMP",
            "Time": "TIME",
            "List": "JSONB",
            "Struct": "JSONB",
        }

        # Identify columns that need JSON conversion
        json_columns = set()
        for i, dtype in enumerate(result.dtypes):
            dtype_str = str(dtype)
            if dtype_str.startswith("List") or dtype_str.startswith("Struct"):
                json_columns.add(i)

        # Normalize identifiers to lowercase for PostgreSQL compatibility
        table_name = table_name.lower()
        pg_columns = [c.lower() for c in result.columns]

        async with await psycopg.AsyncConnection.connect(conn_str) as conn:
            async with conn.cursor() as cur:
                # Build CREATE TABLE statement
                columns_def = []
                for name, dtype in zip(pg_columns, result.dtypes):
                    dtype_str = str(dtype)
                    if dtype_str.startswith("List") or dtype_str.startswith("Struct"):
                        pg_type = "JSONB"
                    else:
                        pg_type = type_map.get(dtype_str, "TEXT")
                    columns_def.append(f'"{name}" {pg_type}')

                create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(columns_def)})'
                await cur.execute(create_sql)
                await report_progress(15, 100, "Created table, starting bulk import...")

                # Use COPY for fast bulk loading - process in batches
                batch_size = 100_000
                col_names = ", ".join(f'"{c}"' for c in pg_columns)
                # Use BINARY format for best performance, fall back to TEXT
                copy_sql = f'COPY "{table_name}" ({col_names}) FROM STDIN'

                start_time = time.perf_counter()
                rows_written = 0

                # Process in batches to show progress and avoid memory issues
                for batch_start in range(0, total_rows, batch_size):
                    batch_end = min(batch_start + batch_size, total_rows)
                    batch_df = result.slice(batch_start, batch_size)

                    # Use psycopg's COPY with write_row() for proper escaping
                    async with cur.copy(copy_sql) as copy:
                        for row in batch_df.rows():
                            converted_row = []
                            for i, val in enumerate(row):
                                if val is None:
                                    converted_row.append(None)
                                elif i in json_columns or isinstance(val, (list, dict)):
                                    converted_row.append(json.dumps(val))
                                else:
                                    converted_row.append(val)
                            await copy.write_row(converted_row)

                    rows_written = batch_end
                    elapsed = time.perf_counter() - start_time
                    rate = rows_written / elapsed if elapsed > 0 else 0

                    # Progress: 15-95% is for data loading
                    progress_pct = 15 + int(80 * rows_written / total_rows)
                    await report_progress(
                        progress_pct,
                        100,
                        f"Imported {rows_written:,}/{total_rows:,} rows ({rate:,.0f} rows/sec)"
                    )

                await conn.commit()
                await report_progress(100, 100, "Import complete!")

        elapsed = time.perf_counter() - start_time
        log.info("Imported to PostgreSQL",
                 table=table_name,
                 connection=connection_id,
                 rows=total_rows,
                 elapsed_sec=round(elapsed, 2),
                 rows_per_sec=round(total_rows/elapsed if elapsed > 0 else 0))
        return {
            "success": True,
            "table": table_name,
            "connection": connection_id,
            "rows": total_rows,
            "columns": list(result.columns),
            "elapsed_sec": round(elapsed, 2)
        }

    except Exception as e:
        log.error("Failed to import to PostgreSQL", error=str(e))
        return {"error": str(e)}
