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
    result = await postgres.execute_query(
        config, "SELECT pg_terminate_backend(%s)", params=(pid,)
    )

    if result.error:
        return False, result.error

    if result.rows and result.rows[0][0]:
        return True, f"Process {pid} terminated"
    else:
        return False, f"Could not terminate process {pid}"


async def kill_queries_by_user(config: ConnectionConfig, username: str) -> tuple[int, list[str]]:
    """Terminate all active queries belonging to a user (excludes self).

    Returns (killed_count, errors).
    """
    sql = """
    SELECT pid FROM pg_stat_activity
    WHERE usename = %s
      AND pid != pg_backend_pid()
      AND state = 'active'
    """
    result = await postgres.execute_query(config, sql, params=(username,))
    if result.error:
        return 0, [result.error]

    killed = 0
    errors: list[str] = []
    for row in result.rows:
        ok, msg = await kill_query(config, row[0])
        if ok:
            killed += 1
        else:
            errors.append(f"pid {row[0]}: {msg}")
    return killed, errors


async def kill_queries_by_database(config: ConnectionConfig, database: str) -> tuple[int, list[str]]:
    """Terminate all active queries on a specific database (excludes self)."""
    sql = """
    SELECT pid FROM pg_stat_activity
    WHERE datname = %s
      AND pid != pg_backend_pid()
      AND state = 'active'
    """
    result = await postgres.execute_query(config, sql, params=(database,))
    if result.error:
        return 0, [result.error]

    killed = 0
    errors: list[str] = []
    for row in result.rows:
        ok, msg = await kill_query(config, row[0])
        if ok:
            killed += 1
        else:
            errors.append(f"pid {row[0]}: {msg}")
    return killed, errors


async def explain_query(config: ConnectionConfig, sql: str, analyze: bool = False) -> dict:
    """Return the query plan for a SQL statement.

    When `analyze=True`, runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) — this
    actually executes the query, so be careful with destructive statements.
    """
    mode = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)" if analyze else "EXPLAIN (FORMAT JSON)"
    # EXPLAIN does not accept bind parameters for the inner statement via
    # psycopg — the inner SQL must be literal. We rely on the fact that
    # this endpoint is admin-gated.
    wrapped = f"{mode} {sql}"
    result = await postgres.execute_query(config, wrapped)
    if result.error:
        return {"error": result.error}
    plan = None
    if result.rows and result.rows[0]:
        plan = result.rows[0][0]
    return {"plan": plan}


async def set_setting(config: ConnectionConfig, name: str, value: str) -> tuple[bool, str]:
    """Apply a SET for the current session. Only allows known-safe identifier
    characters in `name`; the value is bound with a parameter."""
    import re
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", name):
        return False, f"Invalid setting name: {name}"
    result = await postgres.execute_query(
        config, f"SET {name} = %s", params=(value,)
    )
    if result.error:
        return False, result.error
    return True, f"SET {name} = {value}"
