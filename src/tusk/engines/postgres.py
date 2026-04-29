"""PostgreSQL engine using psycopg3 async with connection pooling"""

import os
import re
import time
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import tuple_row

from tusk.core.connection import ConnectionConfig
from tusk.core.result import QueryResult, ColumnInfo
from tusk.core.logging import get_logger
from tusk.core import query_tracker
from tusk.core.ssh_tunnel import get_tunneled_dsn

log = get_logger("postgres")


def _error_position(exc: Exception) -> int | None:
    """Extract a 1-based char index from a psycopg error if available.

    psycopg surfaces the byte position via `e.diag.statement_position` for
    PG errors (`SqlState=42*`). Returns None for non-PG errors or when the
    server didn't report a position.
    """
    try:
        diag = getattr(exc, "diag", None)
        if diag is None:
            return None
        pos = getattr(diag, "statement_position", None)
        if pos is None:
            return None
        return int(pos)
    except Exception:
        return None

# ============================================================================
# Connection Pool Manager
# ============================================================================

try:
    import asyncio
    from psycopg_pool import AsyncConnectionPool

    _pools: dict[str, AsyncConnectionPool] = {}
    _pool_lock = asyncio.Lock()

    def _redact_dsn(dsn: str) -> str:
        """Strip user/password from a DSN before logging.

        psycopg accepts both `postgresql://user:pw@host/db` and the
        keyword form (`host=... user=... password=...`). Redact both.
        """
        try:
            from urllib.parse import urlsplit, urlunsplit
            parts = urlsplit(dsn)
            if parts.scheme:
                netloc = parts.hostname or ""
                if parts.port:
                    netloc = f"{netloc}:{parts.port}"
                return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
        except Exception:
            pass
        # Keyword form — drop any `password=...` token.
        import re as _re
        return _re.sub(r"password=\S+", "password=***", dsn)

    async def _get_pool(dsn: str) -> AsyncConnectionPool:
        """Get or create a connection pool for the given DSN.

        The lock makes concurrent first-hit safe: without it, two coroutines
        observing `dsn not in _pools` could both build a pool, leaking one.
        """
        if dsn in _pools:
            return _pools[dsn]
        async with _pool_lock:
            existing = _pools.get(dsn)
            if existing is not None:
                return existing
            pool = AsyncConnectionPool(
                dsn,
                min_size=1,
                max_size=10,
                max_idle=300,  # Close idle connections after 5 min
                open=False,
            )
            await pool.open()
            _pools[dsn] = pool
            log.info("Connection pool created", dsn=_redact_dsn(dsn))
            return pool

    async def close_pools() -> None:
        """Close all connection pools (call on shutdown)."""
        for dsn, pool in _pools.items():
            await pool.close()
        _pools.clear()
        log.info("All connection pools closed")

    async def close_pool_for_dsn(dsn: str) -> bool:
        """Close + remove a single pool keyed by its DSN.
        Returns True if a pool was closed, False if there was none.
        Used by the reconnect endpoint and by the auto-retry path so a
        stale pool from a network blip can be replaced without
        restarting the whole server.
        """
        pool = _pools.pop(dsn, None)
        if pool is None:
            return False
        try:
            await pool.close()
        except Exception as e:
            log.debug("close_pool_for_dsn ignored", error=str(e))
        log.info("Connection pool closed", dsn=_redact_dsn(dsn))
        return True

    _HAS_POOL = True
except ImportError:
    _HAS_POOL = False

    async def close_pools() -> None:
        pass

    async def close_pool_for_dsn(dsn: str) -> bool:  # type: ignore[override]
        return False


# Query timeout in seconds (0 = no timeout). Set via TUSK_QUERY_TIMEOUT env var.
QUERY_TIMEOUT_SEC = int(os.environ.get("TUSK_QUERY_TIMEOUT", "300"))  # 5 min default


@asynccontextmanager
async def _connect(dsn: str):
    """Yield a connection from pool (if available) or a direct connection."""
    if _HAS_POOL:
        pool = await _get_pool(dsn)
        async with pool.connection() as conn:
            conn.row_factory = tuple_row
            yield conn
    else:
        async with await psycopg.AsyncConnection.connect(
            dsn, row_factory=tuple_row
        ) as conn:
            yield conn


# Errors where the connection (TCP / SSH) is gone and a stale pool is
# still holding dead handles. Catching these and recycling the pool +
# tunnel is the difference between "Tusk needs a restart" and "Tusk
# self-heals after a network blip".
_TRANSIENT_ERROR_HINTS = (
    "server closed the connection",
    "connection is closed",
    "connection is bad",
    "consumed connection",
    "connection refused",
    "broken pipe",
    "EOF detected",
    "no connection to the server",
    "ssl syscall error",
    "could not receive data from server",
    "could not send data to server",
)


def _is_transient_connection_error(err: Exception) -> bool:
    msg = str(err).lower()
    if any(hint in msg for hint in _TRANSIENT_ERROR_HINTS):
        return True
    # psycopg flags it explicitly when the pool returns a dead conn.
    cls = type(err).__name__
    return cls in {"OperationalError", "InterfaceError", "ConnectionTimeout"}


async def _reset_connection(config: ConnectionConfig) -> None:
    """Drop the cached pool + SSH tunnel for this connection so the
    next attempt builds fresh sockets. Used by the auto-retry path
    AND the explicit /reconnect endpoint."""
    try:
        from tusk.core.ssh_tunnel import close_tunnel
        await close_tunnel(config.id)
    except Exception as e:
        log.debug("ssh_tunnel.close_tunnel ignored", error=str(e))
    if _HAS_POOL:
        # The DSN we stored the pool under depends on the tunnel state,
        # so re-derive it; if the tunnel just got nuked, get_tunneled_dsn
        # may build a fresh forward — but we still need to drop ANY pool
        # that was keyed off the old port. Sweep them all for this conn.
        try:
            current_dsn = await get_tunneled_dsn(config)
            await close_pool_for_dsn(current_dsn)
        except Exception:
            pass
        # Also walk every pool and drop ones that reference this conn's
        # host/port pair (covers stale forwards on different local ports).
        host = (config.host or "localhost").lower()
        for dsn in list(_pools.keys()):
            if host in dsn.lower() or f"@127.0.0.1:" in dsn:
                await close_pool_for_dsn(dsn)


async def execute_query(
    config: ConnectionConfig,
    sql: str,
    *,
    params: tuple | None = None,
    request_id: str | None = None,
) -> QueryResult:
    """Execute SQL query and return results.

    Auto-recovers from transient connection errors (network blip,
    bastion reset, idle TCP dropped) by closing the stale pool +
    tunnel and retrying once. The retry is silent on success; on
    failure the original error reaches the caller so the UI shows
    the real cause.

    Args:
        config: Connection configuration
        sql: SQL query (use %s placeholders for params)
        params: Optional query parameters for safe interpolation
        request_id: Optional id for server-side cancellation via pg_cancel_backend
    """
    start = time.perf_counter()

    async def _run_once() -> QueryResult:
        async with _connect(await get_tunneled_dsn(config)) as conn:
            if request_id:
                backend_pid = getattr(conn.info, "backend_pid", None) if hasattr(conn, "info") else None
                query_tracker.update(request_id, pid=backend_pid)
            async with conn.cursor() as cur:
                if QUERY_TIMEOUT_SEC > 0:
                    await cur.execute(f"SET statement_timeout = '{QUERY_TIMEOUT_SEC * 1000}'")
                await cur.execute(sql, params)

                columns = []
                if cur.description:
                    columns = [
                        ColumnInfo(name=desc.name, type=str(desc.type_code))
                        for desc in cur.description
                    ]

                rows = []
                if cur.description:
                    rows = await cur.fetchall()

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

    try:
        return await _run_once()
    except Exception as e:
        if _is_transient_connection_error(e):
            log.info(
                "Transient connection error — recycling pool + tunnel",
                connection=config.id,
                error=str(e),
            )
            await _reset_connection(config)
            try:
                return await _run_once()
            except Exception as e2:
                return QueryResult.from_error(str(e2), position=_error_position(e2))
        return QueryResult.from_error(str(e), position=_error_position(e))


_HEX_WKB_RE = re.compile(r'^(01|00)[0-9a-fA-F]{8,}$')

# Column names that indicate geometry
_GEO_COL_NAMES = {
    "geom", "geometry", "the_geom", "shape", "geo", "wkb_geometry",
    "point", "polygon", "linestring", "multipoint", "multipolygon",
    "multilinestring", "geometrycollection",
}


def _detect_hex_wkb_columns(
    columns: list[ColumnInfo], rows: list[tuple]
) -> list[int]:
    """Detect columns containing hex WKB geometry strings.

    Checks both column names and actual data values.
    """
    geo_cols = []
    for i, col in enumerate(columns):
        col_name = col.name.lower()

        # Check by column name
        if col_name in _GEO_COL_NAMES or col_name.endswith("_geom") or col_name.endswith("_geometry"):
            geo_cols.append(i)
            continue

        # Check first few rows for hex WKB pattern
        for row in rows[:3]:
            if i < len(row) and row[i] is not None:
                val = row[i]
                # Handle memoryview/bytes from psycopg3
                if isinstance(val, (bytes, memoryview)):
                    geo_cols.append(i)
                    break
                if isinstance(val, str) and len(val) >= 10:
                    if _HEX_WKB_RE.match(val):
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
    request_id: str | None = None,
) -> QueryResult:
    """Execute SQL query with server-side pagination.

    Returns:
        QueryResult with total_count, page, and page_size fields populated.
    """
    start = time.perf_counter()

    try:
        async with _connect(await get_tunneled_dsn(config)) as conn:
            if request_id:
                backend_pid = getattr(conn.info, "backend_pid", None) if hasattr(conn, "info") else None
                query_tracker.update(request_id, pid=backend_pid)
            # Set timeout for the session
            if QUERY_TIMEOUT_SEC > 0:
                async with conn.cursor() as cur:
                    await cur.execute(f"SET statement_timeout = '{QUERY_TIMEOUT_SEC * 1000}'")

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
        return QueryResult.from_error(str(e), position=_error_position(e))


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
        async with _connect(await get_tunneled_dsn(config)) as conn:
            if QUERY_TIMEOUT_SEC > 0:
                async with conn.cursor() as cur:
                    await cur.execute(f"SET statement_timeout = '{QUERY_TIMEOUT_SEC * 1000}'")

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

            # Pick interesting properties for the popup. We always include
            # an id and any "label-ish" columns the row has (name, title,
            # description, label, address, etc.) so clicking a feature in
            # the map shows something more useful than just a UUID.
            geo_col_real_name = sample_cols[geo_col_idx].name

            pk_col = None
            label_cols: list[str] = []
            _ID_NAMES = {"id", "gid", "fid", "ogc_fid", "pk"}
            _LABEL_NAMES = {
                "name", "title", "label", "description", "address",
                "name_en", "name_es", "name_local", "name_alt",
                "city", "country", "region", "type", "category", "kind",
                "status", "code",
            }
            for col in sample_cols:
                lname = col.name.lower()
                if col.name == geo_col_real_name:
                    continue
                if pk_col is None and lname in _ID_NAMES:
                    pk_col = col.name
                elif lname in _LABEL_NAMES and len(label_cols) < 8:
                    label_cols.append(col.name)

            select_parts = [f'{geo_expr} AS geom']
            if pk_col:
                select_parts.append(f'"{pk_col.replace(chr(34), chr(34)*2)}" AS "_tusk_id"')
            else:
                select_parts.append('row_number() OVER () AS "_tusk_id"')
            for c in label_cols:
                escaped = c.replace('"', '""')
                # Alias keeps the original column name verbatim — the JS popup
                # uses these as labels.
                select_parts.append(f'"{escaped}" AS "{escaped}"')

            geo_select = ", ".join(select_parts)
            geo_sql = f"SELECT {geo_select} FROM ({sql}) AS _tusk_geo LIMIT {max_features}"

            async with conn.cursor() as cur:
                await cur.execute(geo_sql, params)
                col_names = [d.name for d in (cur.description or [])]
                rows = await cur.fetchall()

            # Build GeoJSON features. Properties carry id + every label_col
            # the row has, with NULL/empty values dropped so the popup stays
            # tight.
            import json
            features = []
            for row in rows:
                row_map = dict(zip(col_names, row))
                geom_str = row_map.pop("geom", None)
                if not geom_str:
                    continue
                try:
                    geom = json.loads(geom_str)
                except json.JSONDecodeError:
                    continue

                props: dict = {}
                feature_id = row_map.pop("_tusk_id", None)
                if feature_id is not None:
                    props["id"] = feature_id
                for k, v in row_map.items():
                    if v is None:
                        continue
                    if isinstance(v, str) and not v.strip():
                        continue
                    props[k] = v

                features.append({"type": "Feature", "geometry": geom, "properties": props})

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


# In-process schema cache. The /api/connections/{id}/schema endpoint runs
# three information_schema queries; clients hit it on every connection switch
# and tab refresh. A short TTL keeps the UI snappy without staling DDL changes
# for long. invalidate_schema_cache() is wired into connection mutations and
# DDL execution to keep the cache fresh after writes.
_schema_cache: dict[str, tuple[float, dict]] = {}
_SCHEMA_TTL = 30.0


def invalidate_schema_cache(connection_id: str | None = None) -> None:
    """Drop a cached schema. Pass None to clear everything."""
    if connection_id is None:
        _schema_cache.clear()
    else:
        _schema_cache.pop(connection_id, None)


async def get_schema(config: ConnectionConfig) -> dict:
    """Get database schema (tables and columns with PK/FK info).

    Cached in-process for `_SCHEMA_TTL` seconds keyed by connection id.
    Errors are not cached — they bubble through and the next call retries.
    """
    cache_key = config.id
    now = time.monotonic()
    cached = _schema_cache.get(cache_key)
    if cached and (now - cached[0]) < _SCHEMA_TTL:
        return cached[1]

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

    _schema_cache[cache_key] = (now, schema)
    return schema


async def cancel_query(config: ConnectionConfig, pid: int) -> bool:
    """Send pg_cancel_backend(pid) on a fresh connection. Returns True on success."""
    try:
        async with await psycopg.AsyncConnection.connect(await get_tunneled_dsn(config)) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
                result = await cur.fetchone()
                return bool(result and result[0])
    except Exception as e:
        log.warning("pg_cancel_backend failed", pid=pid, error=str(e))
        return False


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
