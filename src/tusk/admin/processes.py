"""Active process/query management for PostgreSQL"""

import msgspec
from tusk.core.connection import ConnectionConfig
from tusk.engines import postgres


class ActiveQuery(msgspec.Struct):
    """An active query/process in PostgreSQL"""

    pid: int
    user: str
    database: str
    state: str
    query: str
    duration_seconds: int

    @property
    def duration_human(self) -> str:
        """Human-readable duration"""
        secs = self.duration_seconds
        if secs < 60:
            return f"{secs}s"
        elif secs < 3600:
            return f"{secs // 60}m {secs % 60}s"
        else:
            hours = secs // 3600
            mins = (secs % 3600) // 60
            return f"{hours}h {mins}m"

    @property
    def query_preview(self) -> str:
        """Truncated query for display"""
        q = self.query.replace("\n", " ").strip()
        return q[:80] + "..." if len(q) > 80 else q


async def get_active_queries(config: ConnectionConfig) -> list[ActiveQuery] | dict:
    """Get list of active queries from pg_stat_activity"""
    sql = """
    SELECT
        pid,
        COALESCE(usename, 'unknown') as user,
        COALESCE(datname, 'unknown') as database,
        COALESCE(state, 'unknown') as state,
        COALESCE(query, '') as query,
        COALESCE(EXTRACT(EPOCH FROM (now() - query_start))::int, 0) as duration_seconds
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
      AND query IS NOT NULL
      AND query != ''
    ORDER BY query_start DESC NULLS LAST
    """

    result = await postgres.execute_query(config, sql)

    if result.error:
        return {"error": result.error}

    queries = []
    for row in result.rows:
        queries.append(
            ActiveQuery(
                pid=row[0],
                user=row[1],
                database=row[2],
                state=row[3],
                query=row[4],
                duration_seconds=row[5] or 0,
            )
        )

    return queries


async def kill_query(config: ConnectionConfig, pid: int) -> tuple[bool, str]:
    """Terminate a query by PID"""
    sql = f"SELECT pg_terminate_backend({pid})"
    result = await postgres.execute_query(config, sql)

    if result.error:
        return False, result.error

    if result.rows and result.rows[0][0]:
        return True, f"Process {pid} terminated"
    else:
        return False, f"Could not terminate process {pid}"
