"""Server statistics for PostgreSQL"""

import msgspec
from tusk.core.connection import ConnectionConfig
from tusk.engines import postgres


class ServerStats(msgspec.Struct):
    """PostgreSQL server statistics"""

    connections: int
    max_connections: int
    active_queries: int
    cache_hit_ratio: float
    db_size_bytes: int
    uptime: str
    version: str

    @property
    def db_size_human(self) -> str:
        """Human-readable database size"""
        size = self.db_size_bytes
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} PB"

    @property
    def connection_pct(self) -> float:
        """Connection usage percentage"""
        if self.max_connections == 0:
            return 0
        return (self.connections / self.max_connections) * 100


async def get_server_stats(config: ConnectionConfig) -> ServerStats | dict:
    """Get PostgreSQL server statistics"""
    sql = """
    SELECT
        (SELECT count(*) FROM pg_stat_activity) as connections,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_queries,
        (SELECT COALESCE(
            ROUND(sum(heap_blks_hit)::numeric / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100, 2),
            0
        ) FROM pg_statio_user_tables) as cache_hit_ratio,
        (SELECT pg_database_size(current_database())) as db_size,
        (SELECT COALESCE(
            date_trunc('second', current_timestamp - pg_postmaster_start_time())::text,
            'unknown'
        )) as uptime,
        (SELECT version()) as version
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    if not result.rows:
        return {"error": "No stats returned"}

    row = result.rows[0]
    return ServerStats(
        connections=row[0] or 0,
        max_connections=row[1] or 100,
        active_queries=row[2] or 0,
        cache_hit_ratio=float(row[3] or 0),
        db_size_bytes=row[4] or 0,
        uptime=str(row[5] or "unknown"),
        version=row[6] or "unknown",
    )
