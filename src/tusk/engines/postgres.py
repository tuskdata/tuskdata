"""PostgreSQL engine using psycopg3 async"""

import time
import psycopg
from psycopg.rows import tuple_row

from tusk.core.connection import ConnectionConfig
from tusk.core.result import QueryResult, ColumnInfo


async def execute_query(config: ConnectionConfig, sql: str) -> QueryResult:
    """Execute SQL query and return results"""
    start = time.perf_counter()

    try:
        async with await psycopg.AsyncConnection.connect(
            config.dsn, row_factory=tuple_row
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

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

                elapsed = (time.perf_counter() - start) * 1000

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                    execution_time_ms=round(elapsed, 2),
                )

    except Exception as e:
        return QueryResult.from_error(str(e))


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
