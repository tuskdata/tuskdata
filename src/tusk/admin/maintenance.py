"""Table maintenance operations for PostgreSQL"""

import msgspec
from tusk.core.connection import ConnectionConfig
from tusk.engines import postgres


class LockInfo(msgspec.Struct):
    """Information about a database lock"""
    pid: int
    relation: str
    mode: str
    granted: bool
    waiting_pid: int | None = None
    waiting_query: str | None = None
    blocking_pid: int | None = None
    blocking_query: str | None = None
    duration: str | None = None


class TableBloat(msgspec.Struct):
    """Table bloat information"""
    schema: str
    table: str
    size_bytes: int
    size_human: str
    dead_tuples: int
    live_tuples: int
    bloat_ratio: float
    last_vacuum: str | None
    last_analyze: str | None
    last_autovacuum: str | None


async def get_locks(config: ConnectionConfig) -> list[dict] | dict:
    """Get current locks in the database"""
    sql = """
    SELECT
        blocked_locks.pid AS blocked_pid,
        blocked_activity.usename AS blocked_user,
        blocked_activity.query AS blocked_query,
        blocking_locks.pid AS blocking_pid,
        blocking_activity.usename AS blocking_user,
        blocking_activity.query AS blocking_query,
        blocked_locks.locktype,
        blocked_locks.mode,
        COALESCE(blocked_locks.relation::regclass::text, blocked_locks.locktype) AS locked_item,
        age(now(), blocked_activity.query_start)::text AS blocked_duration
    FROM pg_catalog.pg_locks blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity
        ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks blocking_locks
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity
        ON blocking_activity.pid = blocking_locks.pid
    WHERE NOT blocked_locks.granted
    ORDER BY blocked_activity.query_start
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    locks = []
    for row in result.rows:
        locks.append({
            "blocked_pid": row[0],
            "blocked_user": row[1],
            "blocked_query": (row[2] or "")[:200],
            "blocking_pid": row[3],
            "blocking_user": row[4],
            "blocking_query": (row[5] or "")[:200],
            "lock_type": row[6],
            "mode": row[7],
            "locked_item": row[8],
            "duration": row[9],
        })

    return locks


async def get_all_locks(config: ConnectionConfig) -> list[dict] | dict:
    """Get all current locks (not just blocking ones)"""
    sql = """
    SELECT
        l.pid,
        a.usename,
        l.locktype,
        l.mode,
        l.granted,
        COALESCE(l.relation::regclass::text, l.locktype) AS locked_item,
        a.state,
        LEFT(a.query, 100) AS query,
        age(now(), a.query_start)::text AS duration
    FROM pg_locks l
    JOIN pg_stat_activity a ON l.pid = a.pid
    WHERE a.pid != pg_backend_pid()
    ORDER BY NOT l.granted, a.query_start
    LIMIT 100
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    locks = []
    for row in result.rows:
        locks.append({
            "pid": row[0],
            "user": row[1],
            "lock_type": row[2],
            "mode": row[3],
            "granted": row[4],
            "locked_item": row[5],
            "state": row[6],
            "query": row[7],
            "duration": row[8],
        })

    return locks


async def get_table_bloat(config: ConnectionConfig) -> list[dict] | dict:
    """Get table bloat and maintenance info"""
    sql = """
    SELECT
        schemaname,
        relname,
        pg_total_relation_size(schemaname || '.' || relname) as total_size,
        pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as size_human,
        n_dead_tup,
        n_live_tup,
        CASE WHEN n_live_tup > 0
            THEN round(100.0 * n_dead_tup / n_live_tup, 2)
            ELSE 0
        END as bloat_ratio,
        last_vacuum::text,
        last_analyze::text,
        last_autovacuum::text
    FROM pg_stat_user_tables
    ORDER BY n_dead_tup DESC, pg_total_relation_size(schemaname || '.' || relname) DESC
    LIMIT 50
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    tables = []
    for row in result.rows:
        tables.append({
            "schema": row[0],
            "table": row[1],
            "size_bytes": row[2] or 0,
            "size_human": row[3] or "0 B",
            "dead_tuples": row[4] or 0,
            "live_tuples": row[5] or 0,
            "bloat_ratio": float(row[6] or 0),
            "last_vacuum": row[7],
            "last_analyze": row[8],
            "last_autovacuum": row[9],
        })

    return tables


async def vacuum_table(config: ConnectionConfig, schema: str, table: str, full: bool = False) -> dict:
    """Run VACUUM on a table"""
    # VACUUM cannot run in a transaction, so we use a special approach
    table_name = f'"{schema}"."{table}"'
    cmd = "VACUUM FULL" if full else "VACUUM"

    # We need to use a raw connection for VACUUM
    result = await postgres.execute_query(config, f"{cmd} {table_name}")

    if result.error:
        return {"success": False, "error": result.error}

    return {"success": True, "message": f"VACUUM {'FULL ' if full else ''}completed on {schema}.{table}"}


async def analyze_table(config: ConnectionConfig, schema: str, table: str) -> dict:
    """Run ANALYZE on a table"""
    table_name = f'"{schema}"."{table}"'

    result = await postgres.execute_query(config, f"ANALYZE {table_name}")

    if result.error:
        return {"success": False, "error": result.error}

    return {"success": True, "message": f"ANALYZE completed on {schema}.{table}"}


async def reindex_table(config: ConnectionConfig, schema: str, table: str) -> dict:
    """Run REINDEX on a table"""
    table_name = f'"{schema}"."{table}"'

    result = await postgres.execute_query(config, f"REINDEX TABLE {table_name}")

    if result.error:
        return {"success": False, "error": result.error}

    return {"success": True, "message": f"REINDEX completed on {schema}.{table}"}
