"""Polars engine for data transformations and ETL pipelines"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Literal
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
    source_type: Literal["csv", "parquet", "json", "sql", "osm"]
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


# Union of all transform types
Transform = (
    FilterTransform | SelectTransform | RenameTransform | SortTransform |
    GroupByTransform | AddColumnTransform | DropNullsTransform |
    LimitTransform | JoinTransform
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
            lines.append(f'    SELECT id, kind, tags, lat, lon, ST_AsText(ST_Point(lon, lat)) as geometry')
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
    elif source.source_type == "sql":
        raise NotImplementedError("SQL source not yet implemented")
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
        return pl.DataFrame({"error": [f"DuckDB spatial extension error: {e}"]})

    # Layer/kind filter for OSM data
    # ST_ReadOSM returns: kind (node/way/relation), id, tags, refs, lat, lon, ref_roles
    layer = layer or "all"
    limit_clause = f"LIMIT {limit}" if limit else ""

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
                    ST_AsText(ST_Point(lon, lat)) as geometry
                FROM st_readosm('{p}')
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
                FROM st_readosm('{p}')
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
                FROM st_readosm('{p}')
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
                    CASE WHEN lat IS NOT NULL THEN ST_AsText(ST_Point(lon, lat)) END as geometry,
                    CASE WHEN refs IS NOT NULL THEN len(refs) END as ref_count
                FROM st_readosm('{p}')
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
        # Parse expression - simplified version
        expr = eval(transform.expression, {"pl": pl})
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

    return df


def execute_pipeline(pipeline: Pipeline, limit: int | None = 100) -> dict:
    """Execute a pipeline and return results"""
    try:
        # Load all sources
        sources_map = {}
        for source in pipeline.sources:
            sources_map[source.id] = load_source(source)

        # Get the output source
        result = sources_map.get(pipeline.output_source_id)
        if result is None:
            return {"error": f"Output source not found: {pipeline.output_source_id}"}

        # Apply transforms
        for transform in pipeline.transforms:
            result = apply_transform(result, transform, sources_map)

        # Limit for preview
        if limit:
            result = result.limit(limit)

        # Convert to dict for JSON serialization
        columns = [{"name": name, "type": str(dtype)} for name, dtype in zip(result.columns, result.dtypes)]
        rows = result.rows()

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "total_count": result.height if not limit else None
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

def export_to_csv(pipeline: Pipeline, output_path: str, limit: int | None = None) -> dict:
    """Export pipeline results to CSV file"""
    try:
        # Load and transform data
        sources_map = {}
        for source in pipeline.sources:
            sources_map[source.id] = load_source(source)

        result = sources_map.get(pipeline.output_source_id)
        if result is None:
            return {"error": f"Output source not found: {pipeline.output_source_id}"}

        for transform in pipeline.transforms:
            result = apply_transform(result, transform, sources_map)

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
        # Load and transform data
        sources_map = {}
        for source in pipeline.sources:
            sources_map[source.id] = load_source(source)

        result = sources_map.get(pipeline.output_source_id)
        if result is None:
            return {"error": f"Output source not found: {pipeline.output_source_id}"}

        for transform in pipeline.transforms:
            result = apply_transform(result, transform, sources_map)

        if limit:
            result = result.limit(limit)

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
        # Load and transform data
        sources_map = {}
        for source in pipeline.sources:
            sources_map[source.id] = load_source(source)

        result = sources_map.get(pipeline.output_source_id)
        if result is None:
            return {"error": f"Output source not found: {pipeline.output_source_id}"}

        for transform in pipeline.transforms:
            result = apply_transform(result, transform, sources_map)

        if limit:
            result = result.limit(limit)

        # Connect to DuckDB (in-memory or file)
        conn = duckdb.connect(db_path or ":memory:")

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

        # Load and transform data
        sources_map = {}
        for source in pipeline.sources:
            sources_map[source.id] = load_source(source)

        result = sources_map.get(pipeline.output_source_id)
        if result is None:
            return {"error": f"Output source not found: {pipeline.output_source_id}"}

        for transform in pipeline.transforms:
            result = apply_transform(result, transform, sources_map)

        if limit:
            result = result.limit(limit)

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

        async with await psycopg.AsyncConnection.connect(conn_str) as conn:
            async with conn.cursor() as cur:
                # Build CREATE TABLE statement
                columns_def = []
                for name, dtype in zip(result.columns, result.dtypes):
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
                col_names = ", ".join(f'"{c}"' for c in result.columns)
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
