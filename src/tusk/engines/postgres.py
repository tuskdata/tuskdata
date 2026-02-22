"""PostgreSQL engine using psycopg3 async"""

import re
import time
import psycopg
from psycopg.rows import tuple_row

from tusk.core.connection import ConnectionConfig
from tusk.core.result import QueryResult, ColumnInfo
from tusk.core.logging import get_logger

log = get_logger("postgres")


async def execute_query(config: ConnectionConfig, sql: str, *, params: tuple | None = None) -> QueryResult:
    """Execute SQL query and return results.

    Args:
        config: Connection configuration
        sql: SQL query (use %s placeholders for params)
        params: Optional query parameters for safe interpolation
    """
    start = time.perf_counter()

    try:
        async with await psycopg.AsyncConnection.connect(
            config.dsn, row_factory=tuple_row
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)

                # Get column info
                columns = []
                if cur.description:
                    columns = [
                        ColumnInfo(name=desc.name, type=str(desc.type_code))
                        for desc in cur.description
                    ]

                # Fetch rows if SELECT-like query
                rows = []
                if cur.description:
                    rows = await cur.fetchall()

                    # Detect hex WKB geometry values and convert to WKT
                    # PostGIS returns geometry as hex WKB strings by default
                    if rows:
                        geo_cols = _detect_hex_wkb_columns(columns, rows)
                        if geo_cols:
                            rows, columns = await _convert_geo_columns(
                                conn, sql, params, geo_cols, columns, rows
                            )

                elapsed = (time.perf_counter() - start) * 1000

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                    execution_time_ms=round(elapsed, 2),
                )

    except Exception as e:
        return QueryResult.from_error(str(e))


_HEX_WKB_RE = re.compile(r'^(01|00)[0-9a-fA-F]{8,}$')


def _detect_hex_wkb_columns(
    columns: list[ColumnInfo], rows: list[tuple]
) -> list[int]:
    """Detect columns containing hex WKB geometry strings."""
    geo_cols = []
    for i, col in enumerate(columns):
        # Check first few rows for hex WKB pattern
        for row in rows[:3]:
            if i < len(row) and isinstance(row[i], str) and len(row[i]) >= 10:
                if _HEX_WKB_RE.match(row[i]):
                    geo_cols.append(i)
                    break
    return geo_cols


async def _convert_geo_columns(
    conn, sql: str, params: tuple | None,
    geo_cols: list[int], columns: list[ColumnInfo], rows: list[tuple]
) -> tuple[list[tuple], list[ColumnInfo]]:
    """Re-execute query converting geometry columns to GeoJSON strings.

    Uses ST_AsGeoJSON(geom) without server-side transform - the client handles
    CRS reprojection via proj4js if needed.

    Skips conversion for large result sets (>50k rows) to avoid memory issues.
    """
    # Skip geometry conversion for large result sets
    if len(rows) > 50_000:
        log.debug("Skipping geometry conversion for large result set", rows=len(rows))
        return rows, columns

    try:
        async with conn.cursor() as cur:
            wrapped = []
            for i, col in enumerate(columns):
                col_name = col.name.replace('"', '""')
                if i in geo_cols:
                    wrapped.append(f'ST_AsGeoJSON("{col_name}") AS "{col_name}"')
                else:
                    wrapped.append(f'"{col_name}"')
            wrapped_sql = f"SELECT {', '.join(wrapped)} FROM ({sql}) AS _tusk_geo"
            await cur.execute(wrapped_sql, params)
            new_rows = await cur.fetchall()
            new_columns = list(columns)
            for i in geo_cols:
                new_columns[i] = ColumnInfo(name=columns[i].name, type="geometry")
            return new_rows, new_columns
    except Exception:
        return rows, columns


async def execute_query_paginated(
    config: ConnectionConfig,
    sql: str,
    *,
    page: int = 1,
    page_size: int = 100,
    params: tuple | None = None,
) -> QueryResult:
    """Execute SQL query with server-side pagination.

    Returns:
        QueryResult with total_count, page, and page_size fields populated.
    """
    start = time.perf_counter()

    try:
        async with await psycopg.AsyncConnection.connect(
            config.dsn, row_factory=tuple_row
        ) as conn:
            # Get total count first
            count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _tusk_count"
            async with conn.cursor() as cur:
                await cur.execute(count_sql, params)
                total_count = (await cur.fetchone())[0]

            # Paginated query with LIMIT/OFFSET
            offset = (page - 1) * page_size
            paginated_sql = f"SELECT * FROM ({sql}) AS _tusk_page LIMIT {page_size} OFFSET {offset}"

            async with conn.cursor() as cur:
                await cur.execute(paginated_sql, params)

                columns = []
                if cur.description:
                    columns = [
                        ColumnInfo(name=desc.name, type=str(desc.type_code))
                        for desc in cur.description
                    ]

                rows = []
                if cur.description:
                    rows = await cur.fetchall()

                    # Convert geometry columns
                    if rows:
                        geo_cols = _detect_hex_wkb_columns(columns, rows)
                        if geo_cols:
                            rows, columns = await _convert_geo_columns(
                                conn, paginated_sql, None, geo_cols, columns, rows
                            )

                elapsed = (time.perf_counter() - start) * 1000

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                    execution_time_ms=round(elapsed, 2),
                    total_count=total_count,
                    page=page,
                    page_size=page_size,
                )

    except Exception as e:
        return QueryResult.from_error(str(e))


async def fetch_geometries(
    config: ConnectionConfig,
    sql: str,
    *,
    params: tuple | None = None,
    simplify_tolerance: float | None = None,
    max_features: int = 100_000,
) -> dict:
    """Fetch only geometry data from a query for map rendering.

    This endpoint is optimized for map display:
    - Only fetches geometry column(s) + a simple ID
    - Optionally simplifies geometries server-side
    - Limits features to avoid browser memory issues

    Returns:
        dict with 'features' (GeoJSON), 'total_count', 'truncated' flag
    """
    start = time.perf_counter()

    try:
        async with await psycopg.AsyncConnection.connect(
            config.dsn, row_factory=tuple_row
        ) as conn:
            # First, get count
            count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _tusk_count"
            async with conn.cursor() as cur:
                await cur.execute(count_sql, params)
                total_count = (await cur.fetchone())[0]

            truncated = total_count > max_features

            # Detect geometry columns from a sample
            sample_sql = f"SELECT * FROM ({sql}) AS _tusk_sample LIMIT 5"
            async with conn.cursor() as cur:
                await cur.execute(sample_sql, params)
                sample_cols = [
                    ColumnInfo(name=desc.name, type=str(desc.type_code))
                    for desc in cur.description
                ]
                sample_rows = await cur.fetchall()

            geo_cols = _detect_hex_wkb_columns(sample_cols, sample_rows)
            if not geo_cols:
                return {
                    "features": [],
                    "total_count": total_count,
                    "truncated": False,
                    "error": "No geometry columns detected",
                }

            # Build geometry-only SELECT
            geo_col_idx = geo_cols[0]
            geo_col_name = sample_cols[geo_col_idx].name.replace('"', '""')

            if simplify_tolerance and simplify_tolerance > 0:
                geo_expr = f'ST_AsGeoJSON(ST_Simplify("{geo_col_name}", {simplify_tolerance}))'
            else:
                geo_expr = f'ST_AsGeoJSON("{geo_col_name}")'

            # Try to find a primary key or unique column for properties
            pk_col = None
            for i, col in enumerate(sample_cols):
                col_name_lower = col.name.lower()
                if col_name_lower in ('id', 'gid', 'fid', 'ogc_fid', 'pk'):
                    pk_col = col.name.replace('"', '""')
                    break

            if pk_col:
                geo_select = f'{geo_expr} AS geom, "{pk_col}" AS id'
            else:
                geo_select = f'{geo_expr} AS geom, row_number() OVER () AS id'

            geo_sql = f"SELECT {geo_select} FROM ({sql}) AS _tusk_geo LIMIT {max_features}"

            async with conn.cursor() as cur:
                await cur.execute(geo_sql, params)
                rows = await cur.fetchall()

            # Build GeoJSON features
            import json
            features = []
            for row in rows:
                geom_str, feature_id = row
                if geom_str:
                    try:
                        geom = json.loads(geom_str)
                        features.append({
                            "type": "Feature",
                            "geometry": geom,
                            "properties": {"id": feature_id},
                        })
                    except json.JSONDecodeError:
                        pass

            elapsed = (time.perf_counter() - start) * 1000

            return {
                "type": "FeatureCollection",
                "features": features,
                "total_count": total_count,
                "returned_count": len(features),
                "truncated": truncated,
                "execution_time_ms": round(elapsed, 2),
            }

    except Exception as e:
        log.error("Failed to fetch geometries", error=str(e))
        return {"error": str(e), "features": [], "total_count": 0, "truncated": False}


async def get_schema(config: ConnectionConfig) -> dict:
    """Get database schema (tables and columns with PK/FK info)"""
    # Main columns query
    sql = """
        SELECT
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type
        FROM information_schema.tables t
        JOIN information_schema.columns c
            ON t.table_name = c.table_name
            AND t.table_schema = c.table_schema
        WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
    """

    result = await execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    # Get primary keys
    pk_sql = """
        SELECT
            tc.table_schema,
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
    """
    pk_result = await execute_query(config, pk_sql)
    primary_keys = set()
    if not pk_result.error:
        for row in pk_result.rows:
            primary_keys.add((row[0], row[1], row[2]))  # schema, table, column

    # Get foreign keys
    fk_sql = """
        SELECT
            tc.table_schema,
            tc.table_name,
            kcu.column_name,
            ccu.table_schema AS ref_schema,
            ccu.table_name AS ref_table,
            ccu.column_name AS ref_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
    """
    fk_result = await execute_query(config, fk_sql)
    foreign_keys = {}  # (schema, table, column) -> (ref_schema, ref_table, ref_column)
    if not fk_result.error:
        for row in fk_result.rows:
            foreign_keys[(row[0], row[1], row[2])] = (row[3], row[4], row[5])

    # Build schema tree
    schema: dict = {}
    for row in result.rows:
        schema_name, table_name, col_name, col_type = row
        if schema_name not in schema:
            schema[schema_name] = {}
        if table_name not in schema[schema_name]:
            schema[schema_name][table_name] = []

        col_info = {"name": col_name, "type": col_type}

        # Check if primary key
        if (schema_name, table_name, col_name) in primary_keys:
            col_info["is_primary_key"] = True

        # Check if foreign key
        fk_key = (schema_name, table_name, col_name)
        if fk_key in foreign_keys:
            ref = foreign_keys[fk_key]
            col_info["is_foreign_key"] = True
            col_info["references"] = f"{ref[0]}.{ref[1]}.{ref[2]}"

        schema[schema_name][table_name].append(col_info)

    return schema


async def test_connection(config: ConnectionConfig) -> tuple[bool, str]:
    """Test if connection works"""
    result = await execute_query(config, "SELECT 1")
    if result.error:
        return False, result.error
    return True, "Connection successful"


async def check_connection(config: ConnectionConfig) -> bool:
    """Quick check if connection is online"""
    result = await execute_query(config, "SELECT 1")
    return not result.error


async def get_row_counts(config: ConnectionConfig) -> dict:
    """Get estimated row counts for all tables (fast, uses pg_stat)"""
    sql = """
        SELECT
            schemaname || '.' || relname as table_name,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        ORDER BY schemaname, relname
    """
    result = await execute_query(config, sql)
    counts = {}
    if not result.error:
        for row in result.rows:
            counts[row[0]] = row[1]
    return counts


async def list_databases(config: ConnectionConfig) -> list[dict]:
    """List all databases on the PostgreSQL server"""
    sql = """
        SELECT
            d.datname as name,
            pg_catalog.pg_get_userbyid(d.datdba) as owner,
            pg_catalog.pg_encoding_to_char(d.encoding) as encoding,
            pg_catalog.pg_database_size(d.datname) as size_bytes,
            d.datname = current_database() as is_current
        FROM pg_catalog.pg_database d
        WHERE d.datistemplate = false
          AND has_database_privilege(d.datname, 'CONNECT')
        ORDER BY d.datname
    """

    result = await execute_query(config, sql)

    if result.error:
        return []

    databases = []
    for row in result.rows:
        name, owner, encoding, size_bytes, is_current = row
        # Format size
        if size_bytes:
            if size_bytes >= 1024 * 1024 * 1024:
                size_human = f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
            elif size_bytes >= 1024 * 1024:
                size_human = f"{size_bytes / (1024 * 1024):.1f} MB"
            elif size_bytes >= 1024:
                size_human = f"{size_bytes / 1024:.1f} KB"
            else:
                size_human = f"{size_bytes} B"
        else:
            size_human = "N/A"

        databases.append({
            "name": name,
            "owner": owner,
            "encoding": encoding,
            "size_bytes": size_bytes,
            "size_human": size_human,
            "is_current": is_current,
        })

    return databases
