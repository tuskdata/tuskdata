"""PostgreSQL catalog snapshot shared by the AI Copilot and Schema Watch.

One query against ``pg_catalog`` returns every user table with its
columns (type, nullability), primary key and foreign keys; a second one
adds the indexes. The result is a plain dict so it can be diffed and
stored as JSON.

Shape::

    {
      "orders": {
        "cols": [{"name": "id", "type": "integer", "nn": True}, ...],
        "pks": ["id"],
        "fks": [{"col": "customer_id", "to_table": "customers", "to_col": "id"}],
        "indexes": [{"name": "orders_pkey", "def": "CREATE UNIQUE INDEX ..."}],
      },
      "sales.invoices": {...}      # schema-qualified unless it's public
    }
"""

from __future__ import annotations

from tusk.core.connection import ConnectionConfig

_COLUMNS_SQL = """
    SELECT
        ns.nspname  AS schema,
        cl.relname  AS table_name,
        att.attname AS column_name,
        pg_catalog.format_type(att.atttypid, att.atttypmod) AS data_type,
        CASE WHEN pk.contype = 'p' THEN 1 ELSE 0 END AS is_pk,
        fk.confrelid::regclass::text AS fk_to_table,
        fk_col.attname AS fk_to_column,
        att.attnotnull AS notnull
    FROM pg_attribute att
    JOIN pg_class cl ON cl.oid = att.attrelid
    JOIN pg_namespace ns ON ns.oid = cl.relnamespace
    LEFT JOIN pg_constraint pk
        ON pk.conrelid = cl.oid
        AND pk.contype = 'p'
        AND att.attnum = ANY(pk.conkey)
    LEFT JOIN pg_constraint fk
        ON fk.conrelid = cl.oid
        AND fk.contype = 'f'
        AND att.attnum = ANY(fk.conkey)
    LEFT JOIN pg_attribute fk_col
        ON fk_col.attrelid = fk.confrelid
        AND fk_col.attnum = ANY(fk.confkey)
    WHERE cl.relkind = 'r'
      AND att.attnum > 0
      AND NOT att.attisdropped
      AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY ns.nspname, cl.relname, att.attnum
"""

_INDEXES_SQL = """
    SELECT schemaname, tablename, indexname, indexdef
    FROM pg_indexes
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schemaname, tablename, indexname
"""


def qualified(schema: str | None, table: str) -> str:
    """`public.orders` → `orders`; anything else keeps its schema."""
    return f"{schema}.{table}" if schema and schema != "public" else table


async def fetch_catalog(conn: ConnectionConfig, *, with_indexes: bool = True) -> dict[str, dict]:
    """Tables → columns / PK / FK (/ indexes) for a PostgreSQL connection.

    Raises RuntimeError with the database error when the catalog query
    fails, so callers can distinguish "empty database" from "couldn't ask".
    """
    from tusk.engines.postgres import execute_query

    result = await execute_query(conn, _COLUMNS_SQL)
    if result.error:
        raise RuntimeError(result.error)

    tables: dict[str, dict] = {}
    for row in result.rows:
        schema, tname, col, dtype, is_pk, fk_to, fk_col, notnull = row
        t = tables.setdefault(qualified(schema, tname), {"cols": [], "pks": [], "fks": [], "indexes": []})
        t["cols"].append({"name": col, "type": dtype, "nn": bool(notnull)})
        if is_pk:
            t["pks"].append(col)
        if fk_to and fk_col:
            # `fk_to` is the qualified relname; drop "public." for parity.
            t["fks"].append({"col": col, "to_table": fk_to.replace("public.", ""), "to_col": fk_col})

    if with_indexes and tables:
        idx = await execute_query(conn, _INDEXES_SQL)
        if not idx.error:
            for schema, tname, iname, idef in idx.rows:
                t = tables.get(qualified(schema, tname))
                if t is not None:
                    t["indexes"].append({"name": iname, "def": idef})

    return tables
