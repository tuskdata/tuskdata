"""Advanced monitoring for PostgreSQL - pg_stat_statements, index usage, replication"""

import msgspec
from tusk.core.connection import ConnectionConfig
from tusk.engines import postgres


class SlowQuery(msgspec.Struct):
    """Slow query from pg_stat_statements"""
    query: str
    calls: int
    total_time_ms: float
    mean_time_ms: float
    rows: int
    shared_blks_hit: int
    shared_blks_read: int
    hit_ratio: float


class IndexUsage(msgspec.Struct):
    """Index usage statistics"""
    schema: str
    table: str
    index: str
    size_bytes: int
    size_human: str
    idx_scan: int
    idx_tup_read: int
    idx_tup_fetch: int
    is_unused: bool
    is_duplicate: bool | None = None


class ReplicationSlot(msgspec.Struct):
    """Replication slot info"""
    slot_name: str
    slot_type: str
    active: bool
    wal_status: str | None = None
    restart_lsn: str | None = None
    confirmed_lsn: str | None = None


class ReplicaInfo(msgspec.Struct):
    """Streaming replica info"""
    pid: int
    client_addr: str
    state: str
    sent_lsn: str
    write_lsn: str
    flush_lsn: str
    replay_lsn: str
    sync_state: str
    reply_time: str | None = None
    lag_bytes: int = 0
    lag_human: str = "0 B"


async def check_pg_stat_statements(config: ConnectionConfig) -> dict:
    """Check if pg_stat_statements is available"""
    sql = """
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) as installed,
    EXISTS (
        SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements'
    ) as available
    """
    result = await postgres.execute_query(config, sql)
    if result.error:
        return {"error": result.error}

    row = result.rows[0] if result.rows else (False, False)
    return {
        "installed": row[0],
        "available": row[1]
    }


async def get_slow_queries(
    config: ConnectionConfig,
    limit: int = 20,
    order_by: str = "total_time"
) -> list[dict] | dict:
    """Get slow queries from pg_stat_statements

    order_by options: total_time, mean_time, calls
    """
    # First check if extension is installed
    check = await check_pg_stat_statements(config)
    if "error" in check:
        return check
    if not check["installed"]:
        return {"error": "pg_stat_statements extension not installed", "available": check["available"]}

    order_col = {
        "total_time": "total_exec_time",
        "mean_time": "mean_exec_time",
        "calls": "calls"
    }.get(order_by, "total_exec_time")

    # Try newer column names first (PG13+), fall back to older names
    sql = f"""
    SELECT
        query,
        calls,
        COALESCE(total_exec_time, total_time) as total_time_ms,
        COALESCE(mean_exec_time, mean_time) as mean_time_ms,
        rows,
        shared_blks_hit,
        shared_blks_read,
        CASE WHEN shared_blks_hit + shared_blks_read > 0
            THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
            ELSE 100
        END as hit_ratio
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat_statements%'
    ORDER BY {order_col} DESC
    LIMIT {limit}
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        # Try with older column names for PG < 13
        sql_old = f"""
        SELECT
            query,
            calls,
            total_time as total_time_ms,
            mean_time as mean_time_ms,
            rows,
            shared_blks_hit,
            shared_blks_read,
            CASE WHEN shared_blks_hit + shared_blks_read > 0
                THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
                ELSE 100
            END as hit_ratio
        FROM pg_stat_statements
        WHERE query NOT LIKE '%pg_stat_statements%'
        ORDER BY {order_col.replace('_exec_', '_')} DESC
        LIMIT {limit}
        """
        result = await postgres.execute_query(config, sql_old)
        if result.error:
            return {"error": result.error}

    queries = []
    for row in result.rows:
        queries.append({
            "query": row[0][:500] if row[0] else "",  # Truncate long queries
            "calls": row[1] or 0,
            "total_time_ms": round(float(row[2] or 0), 2),
            "mean_time_ms": round(float(row[3] or 0), 2),
            "rows": row[4] or 0,
            "shared_blks_hit": row[5] or 0,
            "shared_blks_read": row[6] or 0,
            "hit_ratio": float(row[7] or 100),
        })

    return queries


async def reset_pg_stat_statements(config: ConnectionConfig) -> dict:
    """Reset pg_stat_statements statistics"""
    result = await postgres.execute_query(config, "SELECT pg_stat_statements_reset()")
    if result.error:
        return {"success": False, "error": result.error}
    return {"success": True, "message": "Statistics reset"}


async def get_index_usage(config: ConnectionConfig, include_system: bool = False) -> list[dict] | dict:
    """Get index usage statistics to identify unused indexes"""

    schema_filter = "" if include_system else "AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')"

    sql = f"""
    SELECT
        schemaname,
        relname as tablename,
        indexrelname as indexname,
        pg_relation_size(indexrelid) as size_bytes,
        pg_size_pretty(pg_relation_size(indexrelid)) as size_human,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_scan = 0 as is_unused
    FROM pg_stat_user_indexes
    WHERE 1=1 {schema_filter}
    ORDER BY idx_scan ASC, pg_relation_size(indexrelid) DESC
    LIMIT 100
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    indexes = []
    for row in result.rows:
        indexes.append({
            "schema": row[0],
            "table": row[1],
            "index": row[2],
            "size_bytes": row[3] or 0,
            "size_human": row[4] or "0 B",
            "idx_scan": row[5] or 0,
            "idx_tup_read": row[6] or 0,
            "idx_tup_fetch": row[7] or 0,
            "is_unused": row[8],
        })

    return indexes


async def get_duplicate_indexes(config: ConnectionConfig) -> list[dict] | dict:
    """Find potentially duplicate indexes (same columns)"""
    sql = """
    WITH index_cols AS (
        SELECT
            n.nspname as schema,
            t.relname as table_name,
            i.relname as index_name,
            pg_get_indexdef(i.oid) as index_def,
            array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY n.nspname, t.relname, i.relname, i.oid
    )
    SELECT
        a.schema,
        a.table_name,
        a.index_name as index1,
        b.index_name as index2,
        a.columns::text as columns,
        pg_size_pretty(pg_relation_size(a.index_name::regclass)) as index1_size,
        pg_size_pretty(pg_relation_size(b.index_name::regclass)) as index2_size
    FROM index_cols a
    JOIN index_cols b ON a.schema = b.schema
        AND a.table_name = b.table_name
        AND a.columns = b.columns
        AND a.index_name < b.index_name
    ORDER BY a.schema, a.table_name
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    duplicates = []
    for row in result.rows:
        duplicates.append({
            "schema": row[0],
            "table": row[1],
            "index1": row[2],
            "index2": row[3],
            "columns": row[4],
            "index1_size": row[5],
            "index2_size": row[6],
        })

    return duplicates


async def get_replication_status(config: ConnectionConfig) -> dict:
    """Get replication status - slots and replicas"""

    # Get replication slots
    slots_sql = """
    SELECT
        slot_name,
        slot_type,
        active,
        wal_status,
        restart_lsn::text,
        confirmed_flush_lsn::text
    FROM pg_replication_slots
    ORDER BY slot_name
    """

    slots_result = await postgres.execute_query(config, slots_sql)
    slots = []
    if not slots_result.error:
        for row in slots_result.rows:
            slots.append({
                "slot_name": row[0],
                "slot_type": row[1],
                "active": row[2],
                "wal_status": row[3],
                "restart_lsn": row[4],
                "confirmed_lsn": row[5],
            })

    # Get streaming replicas (only works on primary)
    replicas_sql = """
    SELECT
        pid,
        client_addr::text,
        state,
        sent_lsn::text,
        write_lsn::text,
        flush_lsn::text,
        replay_lsn::text,
        sync_state,
        reply_time::text,
        pg_wal_lsn_diff(sent_lsn, replay_lsn) as lag_bytes
    FROM pg_stat_replication
    ORDER BY client_addr
    """

    replicas_result = await postgres.execute_query(config, replicas_sql)
    replicas = []
    if not replicas_result.error:
        for row in replicas_result.rows:
            lag_bytes = int(row[9] or 0)
            replicas.append({
                "pid": row[0],
                "client_addr": row[1] or "local",
                "state": row[2],
                "sent_lsn": row[3],
                "write_lsn": row[4],
                "flush_lsn": row[5],
                "replay_lsn": row[6],
                "sync_state": row[7],
                "reply_time": row[8],
                "lag_bytes": lag_bytes,
                "lag_human": _format_bytes(lag_bytes),
            })

    # Check if this is a replica
    is_replica_sql = "SELECT pg_is_in_recovery()"
    is_replica_result = await postgres.execute_query(config, is_replica_sql)
    is_replica = is_replica_result.rows[0][0] if is_replica_result.rows else False

    # If replica, get receiver status
    receiver = None
    if is_replica:
        receiver_sql = """
        SELECT
            status,
            received_lsn::text,
            latest_end_lsn::text,
            sender_host,
            sender_port,
            last_msg_receipt_time::text
        FROM pg_stat_wal_receiver
        """
        receiver_result = await postgres.execute_query(config, receiver_sql)
        if receiver_result.rows:
            row = receiver_result.rows[0]
            receiver = {
                "status": row[0],
                "received_lsn": row[1],
                "latest_end_lsn": row[2],
                "sender_host": row[3],
                "sender_port": row[4],
                "last_msg_receipt_time": row[5],
            }

    return {
        "is_replica": is_replica,
        "slots": slots,
        "replicas": replicas,
        "receiver": receiver,
    }


async def get_wal_stats(config: ConnectionConfig) -> dict:
    """Get WAL statistics"""
    sql = """
    SELECT
        pg_current_wal_lsn()::text as current_lsn,
        pg_walfile_name(pg_current_wal_lsn()) as current_wal_file,
        (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
        (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
        (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
        (SELECT setting FROM pg_settings WHERE name = 'max_wal_senders') as max_wal_senders,
        (SELECT count(*) FROM pg_stat_replication) as active_senders
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        # Might fail on replica, try simpler query
        sql_simple = """
        SELECT
            pg_last_wal_receive_lsn()::text as current_lsn,
            NULL as current_wal_file,
            (SELECT setting FROM pg_settings WHERE name = 'wal_level') as wal_level,
            (SELECT setting FROM pg_settings WHERE name = 'archive_mode') as archive_mode,
            (SELECT setting FROM pg_settings WHERE name = 'archive_command') as archive_command,
            (SELECT setting FROM pg_settings WHERE name = 'max_wal_senders') as max_wal_senders,
            0 as active_senders
        """
        result = await postgres.execute_query(config, sql_simple)
        if result.error:
            return {"error": result.error}

    row = result.rows[0] if result.rows else [None] * 7
    return {
        "current_lsn": row[0],
        "current_wal_file": row[1],
        "wal_level": row[2],
        "archive_mode": row[3],
        "archive_command": row[4],
        "max_wal_senders": int(row[5] or 0),
        "active_senders": int(row[6] or 0),
    }


def _format_bytes(size: int) -> str:
    """Format bytes to human readable"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"
