"""Database operations for BI plugin"""

import json
from datetime import datetime

from tusk.plugins.storage import (
    get_plugin_db,
    get_plugin_db_path,
    init_plugin_db,
    query_plugin_db,
    execute_plugin_db,
)

import structlog
log = structlog.get_logger()

PLUGIN_ID = "tusk-bi"

SCHEMA = """
CREATE TABLE IF NOT EXISTS data_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'sqlite',
    connection_ref TEXT NOT NULL,
    plugin_id TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS saved_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    source_id INTEGER NOT NULL,
    sql TEXT NOT NULL,
    chart_type TEXT,
    chart_config TEXT DEFAULT '{}',
    tags TEXT DEFAULT '',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    last_executed_at TEXT,
    FOREIGN KEY (source_id) REFERENCES data_sources(id)
);

CREATE TABLE IF NOT EXISTS dashboards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    is_default INTEGER DEFAULT 0,
    is_prebuilt INTEGER DEFAULT 0,
    filters TEXT DEFAULT '[]',
    is_public INTEGER DEFAULT 0,
    refresh_interval_seconds INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dashboard_tabs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id INTEGER NOT NULL,
    name TEXT NOT NULL DEFAULT 'Default',
    tab_order INTEGER DEFAULT 0,
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS widgets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id INTEGER NOT NULL,
    query_id INTEGER,
    widget_type TEXT NOT NULL DEFAULT 'chart',
    title TEXT DEFAULT '',
    config TEXT DEFAULT '{}',
    col_start INTEGER DEFAULT 1,
    col_span INTEGER DEFAULT 6,
    row_start INTEGER DEFAULT 1,
    row_span INTEGER DEFAULT 4,
    tab_id INTEGER DEFAULT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE,
    FOREIGN KEY (query_id) REFERENCES saved_queries(id) ON DELETE SET NULL,
    FOREIGN KEY (tab_id) REFERENCES dashboard_tabs(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id INTEGER NOT NULL,
    row_count INTEGER DEFAULT 0,
    data TEXT,
    value REAL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (query_id) REFERENCES saved_queries(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id INTEGER NOT NULL UNIQUE,
    cron_expr TEXT NOT NULL,
    max_snapshots INTEGER DEFAULT 100,
    enabled INTEGER DEFAULT 1,
    last_run_at TEXT,
    next_run_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (query_id) REFERENCES saved_queries(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dashboard_variables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    var_type TEXT NOT NULL DEFAULT 'text',
    default_value TEXT DEFAULT '',
    options TEXT DEFAULT '',
    label TEXT DEFAULT '',
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE,
    UNIQUE(dashboard_id, name)
);

CREATE TABLE IF NOT EXISTS public_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id INTEGER NOT NULL,
    token TEXT NOT NULL UNIQUE,
    expires_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS embed_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL UNIQUE,
    dashboard_id INTEGER NOT NULL,
    rls_clauses TEXT DEFAULT '{}',
    expires_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    app_id TEXT DEFAULT '',
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE
);
"""


def init_db() -> None:
    """Initialize the BI plugin database"""
    init_plugin_db(PLUGIN_ID, SCHEMA)

    # Migration: add tab_id column to widgets if missing (existing DBs)
    try:
        with get_plugin_db(PLUGIN_ID) as db:
            cursor = db.execute("PRAGMA table_info(widgets)")
            cols = [row[1] for row in cursor.fetchall()]
            if "tab_id" not in cols:
                db.execute("ALTER TABLE widgets ADD COLUMN tab_id INTEGER DEFAULT NULL REFERENCES dashboard_tabs(id) ON DELETE SET NULL")
                log.info("Migrated widgets table: added tab_id column")
    except Exception as e:
        log.debug("Migration check for tab_id", error=str(e))

    # Migration v0.3.0: dashboards.is_public + refresh_interval_seconds.
    # Both default to 0 so existing dashboards stay private with no live
    # refresh — opt-in only.
    try:
        with get_plugin_db(PLUGIN_ID) as db:
            cursor = db.execute("PRAGMA table_info(dashboards)")
            cols = [row[1] for row in cursor.fetchall()]
            if "is_public" not in cols:
                db.execute("ALTER TABLE dashboards ADD COLUMN is_public INTEGER DEFAULT 0")
                log.info("Migrated dashboards table: added is_public column")
            if "refresh_interval_seconds" not in cols:
                db.execute("ALTER TABLE dashboards ADD COLUMN refresh_interval_seconds INTEGER DEFAULT 0")
                log.info("Migrated dashboards table: added refresh_interval_seconds column")
    except Exception as e:
        log.debug("Migration check for dashboard public/refresh", error=str(e))

    log.info("BI database initialized")


def discover_plugin_sources() -> int:
    """Discover data sources from installed plugins.

    Scans the plugin registry for plugins that expose datasets
    and registers them as BI data sources.

    Returns:
        Number of sources discovered
    """
    try:
        from tusk.plugins.registry import get_all_plugins
        from tusk.plugins.storage import get_plugin_db_path as _get_db_path
    except ImportError:
        log.warning("Plugin registry not available")
        return 0

    plugins = get_all_plugins()
    discovered = 0

    for plugin in plugins:
        datasets = plugin.get_datasets()
        if not datasets:
            continue

        db_path = str(_get_db_path(plugin.name))
        source_name = f"{plugin.tab_label} ({plugin.name})"

        # Check if source already exists
        existing = query_plugin_db(
            PLUGIN_ID,
            "SELECT id FROM data_sources WHERE plugin_id = ?",
            (plugin.name,),
        )
        if existing:
            continue

        execute_plugin_db(
            PLUGIN_ID,
            """INSERT INTO data_sources (name, source_type, connection_ref, plugin_id)
               VALUES (?, 'sqlite', ?, ?)""",
            (source_name, db_path, plugin.name),
        )
        discovered += 1
        log.info("Discovered plugin data source", plugin=plugin.name, tables=len(datasets))

    # Also register the main TuskData DuckDB engine as a source
    existing_duckdb = query_plugin_db(
        PLUGIN_ID,
        "SELECT id FROM data_sources WHERE source_type = 'duckdb' AND plugin_id IS NULL",
    )
    if not existing_duckdb:
        execute_plugin_db(
            PLUGIN_ID,
            """INSERT INTO data_sources (name, source_type, connection_ref)
               VALUES ('DuckDB (Analytics)', 'duckdb', ':memory:')""",
        )
        discovered += 1

    # Auto-discover PostgreSQL connections from TuskData core
    try:
        from tusk.core.connection import list_connections
        for conn in list_connections():
            if conn.type != "postgres":
                continue
            # Check if already registered
            existing = query_plugin_db(
                PLUGIN_ID,
                "SELECT id FROM data_sources WHERE source_type = 'postgres' AND connection_ref = ?",
                (conn.name,),
            )
            if existing:
                continue
            display_name = f"PostgreSQL: {conn.database or conn.name} ({conn.name})"
            execute_plugin_db(
                PLUGIN_ID,
                """INSERT INTO data_sources (name, source_type, connection_ref)
                   VALUES (?, 'postgres', ?)""",
                (display_name, conn.name),
            )
            discovered += 1
            log.info("Discovered PostgreSQL source", connection=conn.name)
    except Exception as e:
        log.debug("Could not discover PostgreSQL connections", error=str(e))

    return discovered


# ─────────────────────────────────────────────────────────────
# Data Sources CRUD
# ─────────────────────────────────────────────────────────────

def get_data_sources() -> list[dict]:
    return query_plugin_db(PLUGIN_ID, "SELECT * FROM data_sources ORDER BY name")


def get_data_source(source_id: int) -> dict | None:
    rows = query_plugin_db(PLUGIN_ID, "SELECT * FROM data_sources WHERE id = ?", (source_id,))
    return rows[0] if rows else None


def create_data_source(name: str, source_type: str, connection_ref: str, plugin_id: str | None = None) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT INTO data_sources (name, source_type, connection_ref, plugin_id) VALUES (?, ?, ?, ?)",
        (name, source_type, connection_ref, plugin_id),
    )


def delete_data_source(source_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM data_sources WHERE id = ?", (source_id,))


# ─────────────────────────────────────────────────────────────
# Saved Queries CRUD
# ─────────────────────────────────────────────────────────────

def get_saved_queries(source_id: int | None = None, tag: str | None = None) -> list[dict]:
    sql = """SELECT q.*, s.name as source_name
             FROM saved_queries q
             JOIN data_sources s ON q.source_id = s.id
             WHERE 1=1"""
    params: list = []

    if source_id:
        sql += " AND q.source_id = ?"
        params.append(source_id)
    if tag:
        sql += " AND q.tags LIKE ?"
        params.append(f"%{tag}%")

    sql += " ORDER BY q.updated_at DESC"
    return query_plugin_db(PLUGIN_ID, sql, tuple(params))


def get_saved_query(query_id: int) -> dict | None:
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT q.*, s.name as source_name, s.source_type, s.connection_ref
           FROM saved_queries q
           JOIN data_sources s ON q.source_id = s.id
           WHERE q.id = ?""",
        (query_id,),
    )
    return rows[0] if rows else None


def create_saved_query(
    name: str,
    source_id: int,
    sql: str,
    description: str = "",
    chart_type: str | None = None,
    chart_config: str = "{}",
    tags: str = "",
) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        """INSERT INTO saved_queries (name, description, source_id, sql, chart_type, chart_config, tags)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (name, description, source_id, sql, chart_type, chart_config, tags),
    )


def update_saved_query(
    query_id: int,
    name: str | None = None,
    sql: str | None = None,
    description: str | None = None,
    chart_type: str | None = None,
    chart_config: str | None = None,
    tags: str | None = None,
) -> None:
    fields = []
    params: list = []

    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if sql is not None:
        fields.append("sql = ?")
        params.append(sql)
    if description is not None:
        fields.append("description = ?")
        params.append(description)
    if chart_type is not None:
        fields.append("chart_type = ?")
        params.append(chart_type)
    if chart_config is not None:
        fields.append("chart_config = ?")
        params.append(chart_config)
    if tags is not None:
        fields.append("tags = ?")
        params.append(tags)

    if not fields:
        return

    fields.append("updated_at = ?")
    params.append(datetime.now().isoformat())
    params.append(query_id)

    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(f"UPDATE saved_queries SET {', '.join(fields)} WHERE id = ?", tuple(params))


def delete_saved_query(query_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM saved_queries WHERE id = ?", (query_id,))


def mark_query_executed(query_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(
            "UPDATE saved_queries SET last_executed_at = ? WHERE id = ?",
            (datetime.now().isoformat(), query_id),
        )


# ─────────────────────────────────────────────────────────────
# Dashboards CRUD
# ─────────────────────────────────────────────────────────────

def get_dashboards() -> list[dict]:
    return query_plugin_db(PLUGIN_ID, "SELECT * FROM dashboards ORDER BY name")


def get_dashboard(dashboard_id: int) -> dict | None:
    rows = query_plugin_db(PLUGIN_ID, "SELECT * FROM dashboards WHERE id = ?", (dashboard_id,))
    return rows[0] if rows else None


def get_default_dashboard() -> dict | None:
    rows = query_plugin_db(PLUGIN_ID, "SELECT * FROM dashboards WHERE is_default = 1 LIMIT 1")
    return rows[0] if rows else None


def create_dashboard(name: str, description: str = "", is_default: bool = False, is_prebuilt: bool = False) -> int:
    if is_default:
        # Unset other defaults
        with get_plugin_db(PLUGIN_ID) as db:
            db.execute("UPDATE dashboards SET is_default = 0")

    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT INTO dashboards (name, description, is_default, is_prebuilt) VALUES (?, ?, ?, ?)",
        (name, description, int(is_default), int(is_prebuilt)),
    )


def update_dashboard(
    dashboard_id: int,
    name: str | None = None,
    description: str | None = None,
    is_default: bool | None = None,
    filters: str | None = None,
    is_public: bool | None = None,
    refresh_interval_seconds: int | None = None,
) -> None:
    fields = []
    params: list = []

    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if description is not None:
        fields.append("description = ?")
        params.append(description)
    if is_default is not None:
        if is_default:
            with get_plugin_db(PLUGIN_ID) as db:
                db.execute("UPDATE dashboards SET is_default = 0")
        fields.append("is_default = ?")
        params.append(int(is_default))
    if filters is not None:
        fields.append("filters = ?")
        params.append(filters)
    if is_public is not None:
        fields.append("is_public = ?")
        params.append(int(is_public))
    if refresh_interval_seconds is not None:
        # Clamp to a sane band: 0 (off) or 5s-3600s.
        v = int(refresh_interval_seconds)
        if v != 0:
            v = max(5, min(3600, v))
        fields.append("refresh_interval_seconds = ?")
        params.append(v)

    if not fields:
        return

    fields.append("updated_at = ?")
    params.append(datetime.now().isoformat())
    params.append(dashboard_id)

    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(f"UPDATE dashboards SET {', '.join(fields)} WHERE id = ?", tuple(params))


def delete_dashboard(dashboard_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM widgets WHERE dashboard_id = ?", (dashboard_id,))
        db.execute("DELETE FROM dashboards WHERE id = ?", (dashboard_id,))


def clone_dashboard(dashboard_id: int) -> int | None:
    dash = get_dashboard(dashboard_id)
    if not dash:
        return None

    new_id = create_dashboard(
        name=f"{dash['name']} (Copy)",
        description=dash.get("description", ""),
    )

    # Clone tabs and build mapping
    tab_map: dict[int, int] = {}
    tabs = get_dashboard_tabs(dashboard_id)
    for t in tabs:
        new_tab_id = create_dashboard_tab(
            dashboard_id=new_id,
            name=t.get("name", "Tab"),
            tab_order=t.get("tab_order", 0),
        )
        tab_map[t["id"]] = new_tab_id

    widgets = get_widgets(dashboard_id)
    for w in widgets:
        old_tab_id = w.get("tab_id")
        new_tab_id = tab_map.get(old_tab_id) if old_tab_id else None
        create_widget(
            dashboard_id=new_id,
            query_id=w.get("query_id"),
            widget_type=w.get("widget_type", "chart"),
            title=w.get("title", ""),
            config=w.get("config", "{}"),
            col_start=w.get("col_start", 1),
            col_span=w.get("col_span", 6),
            row_start=w.get("row_start", 1),
            row_span=w.get("row_span", 4),
            tab_id=new_tab_id,
        )

    return new_id


# ─────────────────────────────────────────────────────────────
# Widgets CRUD
# ─────────────────────────────────────────────────────────────

def get_widgets(dashboard_id: int) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        """SELECT w.*, q.name as query_name, q.sql as query_sql
           FROM widgets w
           LEFT JOIN saved_queries q ON w.query_id = q.id
           WHERE w.dashboard_id = ?
           ORDER BY w.row_start, w.col_start""",
        (dashboard_id,),
    )


def get_widget(widget_id: int) -> dict | None:
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT w.*, q.name as query_name, q.sql as query_sql,
                  q.source_id, q.chart_type, q.chart_config
           FROM widgets w
           LEFT JOIN saved_queries q ON w.query_id = q.id
           WHERE w.id = ?""",
        (widget_id,),
    )
    return rows[0] if rows else None


def create_widget(
    dashboard_id: int,
    widget_type: str = "chart",
    query_id: int | None = None,
    title: str = "",
    config: str = "{}",
    col_start: int = 1,
    col_span: int = 6,
    row_start: int = 1,
    row_span: int = 4,
    tab_id: int | None = None,
) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        """INSERT INTO widgets (dashboard_id, query_id, widget_type, title, config,
                                col_start, col_span, row_start, row_span, tab_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (dashboard_id, query_id, widget_type, title, config,
         col_start, col_span, row_start, row_span, tab_id),
    )


def update_widget(
    widget_id: int,
    title: str | None = None,
    config: str | None = None,
    col_start: int | None = None,
    col_span: int | None = None,
    row_start: int | None = None,
    row_span: int | None = None,
    query_id: int | None = None,
    tab_id: int | None = None,
) -> None:
    fields = []
    params: list = []

    if title is not None:
        fields.append("title = ?")
        params.append(title)
    if config is not None:
        fields.append("config = ?")
        params.append(config)
    if col_start is not None:
        fields.append("col_start = ?")
        params.append(col_start)
    if col_span is not None:
        fields.append("col_span = ?")
        params.append(col_span)
    if row_start is not None:
        fields.append("row_start = ?")
        params.append(row_start)
    if row_span is not None:
        fields.append("row_span = ?")
        params.append(row_span)
    if query_id is not None:
        fields.append("query_id = ?")
        params.append(query_id)
    if tab_id is not None:
        fields.append("tab_id = ?")
        params.append(tab_id)

    if not fields:
        return

    params.append(widget_id)

    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(f"UPDATE widgets SET {', '.join(fields)} WHERE id = ?", tuple(params))


def delete_widget(widget_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM widgets WHERE id = ?", (widget_id,))


# ─────────────────────────────────────────────────────────────
# Snapshots
# ─────────────────────────────────────────────────────────────

def save_snapshot(query_id: int, row_count: int, data: str, value: float | None = None) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT INTO snapshots (query_id, row_count, data, value) VALUES (?, ?, ?, ?)",
        (query_id, row_count, data, value),
    )


def get_snapshots(query_id: int, limit: int = 50) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT id, query_id, row_count, value, created_at FROM snapshots WHERE query_id = ? ORDER BY created_at DESC LIMIT ?",
        (query_id, limit),
    )


def get_snapshot_data(snapshot_id: int) -> dict | None:
    rows = query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM snapshots WHERE id = ?",
        (snapshot_id,),
    )
    return rows[0] if rows else None


def rotate_snapshots(query_id: int, max_keep: int = 100) -> int:
    """Delete old snapshots beyond the max_keep limit. Returns count deleted."""
    with get_plugin_db(PLUGIN_ID) as db:
        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM snapshots WHERE query_id = ?",
            (query_id,),
        )
        count = cursor.fetchone()[0]
        if count <= max_keep:
            return 0

        to_delete = count - max_keep
        db.execute(
            """DELETE FROM snapshots WHERE id IN (
                SELECT id FROM snapshots WHERE query_id = ?
                ORDER BY created_at ASC LIMIT ?
            )""",
            (query_id, to_delete),
        )
        return to_delete


# ─────────────────────────────────────────────────────────────
# Schedules
# ─────────────────────────────────────────────────────────────

def get_schedules(enabled_only: bool = False) -> list[dict]:
    sql = """SELECT sc.*, q.name as query_name, q.sql as query_sql
             FROM schedules sc
             JOIN saved_queries q ON sc.query_id = q.id"""
    if enabled_only:
        sql += " WHERE sc.enabled = 1"
    sql += " ORDER BY sc.next_run_at"
    return query_plugin_db(PLUGIN_ID, sql)


def create_schedule(query_id: int, cron_expr: str, max_snapshots: int = 100) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT OR REPLACE INTO schedules (query_id, cron_expr, max_snapshots) VALUES (?, ?, ?)",
        (query_id, cron_expr, max_snapshots),
    )


def delete_schedule(schedule_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))


def update_schedule_run(schedule_id: int, next_run_at: str) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(
            "UPDATE schedules SET last_run_at = ?, next_run_at = ? WHERE id = ?",
            (datetime.now().isoformat(), next_run_at, schedule_id),
        )


# ─────────────────────────────────────────────────────────────
# Dashboard Variables
# ─────────────────────────────────────────────────────────────

def get_dashboard_variables(dashboard_id: int) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM dashboard_variables WHERE dashboard_id = ? ORDER BY id",
        (dashboard_id,),
    )


def create_dashboard_variable(
    dashboard_id: int, name: str, var_type: str = "text",
    default_value: str = "", options: str = "", label: str = "",
) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        """INSERT INTO dashboard_variables (dashboard_id, name, var_type, default_value, options, label)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (dashboard_id, name, var_type, default_value, options, label),
    )


def update_dashboard_variable(
    var_id: int, name: str | None = None, var_type: str | None = None,
    default_value: str | None = None, options: str | None = None, label: str | None = None,
) -> None:
    fields: list[str] = []
    params: list = []
    if name is not None:
        fields.append("name = ?"); params.append(name)
    if var_type is not None:
        fields.append("var_type = ?"); params.append(var_type)
    if default_value is not None:
        fields.append("default_value = ?"); params.append(default_value)
    if options is not None:
        fields.append("options = ?"); params.append(options)
    if label is not None:
        fields.append("label = ?"); params.append(label)
    if not fields:
        return
    params.append(var_id)
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(f"UPDATE dashboard_variables SET {', '.join(fields)} WHERE id = ?", tuple(params))


def delete_dashboard_variable(var_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM dashboard_variables WHERE id = ?", (var_id,))


# ─────────────────────────────────────────────────────────────
# Public Links
# ─────────────────────────────────────────────────────────────

def create_public_link(dashboard_id: int, token: str, expires_at: str | None = None) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT INTO public_links (dashboard_id, token, expires_at) VALUES (?, ?, ?)",
        (dashboard_id, token, expires_at),
    )


def get_public_link_by_token(token: str) -> dict | None:
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT pl.*, d.name as dashboard_name
           FROM public_links pl JOIN dashboards d ON pl.dashboard_id = d.id
           WHERE pl.token = ?""",
        (token,),
    )
    return rows[0] if rows else None


def get_public_links(dashboard_id: int) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM public_links WHERE dashboard_id = ? ORDER BY created_at DESC",
        (dashboard_id,),
    )


def delete_public_link(link_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM public_links WHERE id = ?", (link_id,))


# ─────────────────────────────────────────────────────────────
# Embed Tokens
# ─────────────────────────────────────────────────────────────

def create_embed_token(
    dashboard_id: int, token: str, rls_clauses: str = "{}",
    expires_at: str = "", app_id: str = "",
) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        """INSERT INTO embed_tokens (token, dashboard_id, rls_clauses, expires_at, app_id)
           VALUES (?, ?, ?, ?, ?)""",
        (token, dashboard_id, rls_clauses, expires_at, app_id),
    )


def get_embed_token(token: str) -> dict | None:
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT et.*, d.name as dashboard_name
           FROM embed_tokens et
           JOIN dashboards d ON et.dashboard_id = d.id
           WHERE et.token = ?""",
        (token,),
    )
    return rows[0] if rows else None


def get_embed_tokens(dashboard_id: int) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM embed_tokens WHERE dashboard_id = ? ORDER BY created_at DESC",
        (dashboard_id,),
    )


def delete_embed_token(token_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute("DELETE FROM embed_tokens WHERE id = ?", (token_id,))


def delete_expired_embed_tokens() -> int:
    """Delete expired embed tokens. Returns count deleted."""
    with get_plugin_db(PLUGIN_ID) as db:
        cursor = db.execute(
            "DELETE FROM embed_tokens WHERE expires_at < ?",
            (datetime.now().isoformat(),),
        )
        return cursor.rowcount


# ─────────────────────────────────────────────────────────────
# Export / Import
# ─────────────────────────────────────────────────────────────

def export_dashboard(dashboard_id: int) -> dict | None:
    dash = get_dashboard(dashboard_id)
    if not dash:
        return None
    widgets = get_widgets(dashboard_id)
    variables = get_dashboard_variables(dashboard_id)
    tabs = get_dashboard_tabs(dashboard_id)

    query_ids = set(w.get("query_id") for w in widgets if w.get("query_id"))
    queries = []
    for qid in query_ids:
        q = get_saved_query(qid)
        if q:
            queries.append(q)

    return {
        "dashboard": {k: v for k, v in dash.items() if k != "id"},
        "widgets": [
            {k: v for k, v in w.items() if k not in ("id", "dashboard_id", "query_name", "query_sql")}
            for w in widgets
        ],
        "variables": [
            {k: v for k, v in v.items() if k not in ("id", "dashboard_id")}
            for v in variables
        ],
        "queries": [
            {k: v for k, v in q.items() if k not in ("id", "source_name", "source_type", "connection_ref")}
            for q in queries
        ],
        "tabs": [
            {k: v for k, v in t.items() if k not in ("id", "dashboard_id")}
            for t in tabs
        ],
    }


# ─────────────────────────────────────────────────────────────
# Dashboard Tabs CRUD
# ─────────────────────────────────────────────────────────────

def get_dashboard_tabs(dashboard_id: int) -> list[dict]:
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM dashboard_tabs WHERE dashboard_id = ? ORDER BY tab_order, id",
        (dashboard_id,),
    )


def create_dashboard_tab(dashboard_id: int, name: str, tab_order: int = 0) -> int:
    return execute_plugin_db(
        PLUGIN_ID,
        "INSERT INTO dashboard_tabs (dashboard_id, name, tab_order) VALUES (?, ?, ?)",
        (dashboard_id, name, tab_order),
    )


def update_dashboard_tab(tab_id: int, name: str | None = None, tab_order: int | None = None) -> None:
    fields: list[str] = []
    params: list = []
    if name is not None:
        fields.append("name = ?")
        params.append(name)
    if tab_order is not None:
        fields.append("tab_order = ?")
        params.append(tab_order)
    if not fields:
        return
    params.append(tab_id)
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(f"UPDATE dashboard_tabs SET {', '.join(fields)} WHERE id = ?", tuple(params))


def delete_dashboard_tab(tab_id: int) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        # Unset tab_id on widgets assigned to this tab
        db.execute("UPDATE widgets SET tab_id = NULL WHERE tab_id = ?", (tab_id,))
        db.execute("DELETE FROM dashboard_tabs WHERE id = ?", (tab_id,))


# ─────────────────────────────────────────────────────────────
# Widget Threshold Helpers
# ─────────────────────────────────────────────────────────────

def get_widget_thresholds(query_id: int) -> list[dict]:
    """Find all widgets that use this query and return their threshold_rules from config."""
    widgets = query_plugin_db(
        PLUGIN_ID,
        "SELECT id, config FROM widgets WHERE query_id = ?",
        (query_id,),
    )
    results = []
    for w in widgets:
        config_str = w.get("config", "{}")
        try:
            config = json.loads(config_str) if isinstance(config_str, str) else config_str
        except (json.JSONDecodeError, TypeError):
            continue
        rules = config.get("threshold_rules", [])
        if rules:
            results.append({"widget_id": w["id"], "rules": rules})
    return results


# ─────────────────────────────────────────────────────────────
# Schedule Helpers
# ─────────────────────────────────────────────────────────────

def toggle_schedule(schedule_id: int, enabled: bool) -> None:
    with get_plugin_db(PLUGIN_ID) as db:
        db.execute(
            "UPDATE schedules SET enabled = ? WHERE id = ?",
            (int(enabled), schedule_id),
        )


# ─────────────────────────────────────────────────────────────
# Dashboard Provisioning
# ─────────────────────────────────────────────────────────────

def provision_dashboard(data: dict, source_id: int | None = None) -> int:
    """Create or update a dashboard from JSON definition.

    If a dashboard with the same name exists, update it.
    Otherwise, create a new one.
    """
    dash_data = data.get("dashboard", {})
    name = dash_data.get("name", "Provisioned Dashboard")

    # Check if dashboard with this name already exists
    existing = query_plugin_db(
        PLUGIN_ID,
        "SELECT id FROM dashboards WHERE name = ?",
        (name,),
    )

    if existing:
        dashboard_id = existing[0]["id"]
        update_dashboard(
            dashboard_id,
            description=dash_data.get("description"),
        )
        # Remove existing widgets for re-provisioning
        with get_plugin_db(PLUGIN_ID) as db:
            db.execute("DELETE FROM widgets WHERE dashboard_id = ?", (dashboard_id,))
            db.execute("DELETE FROM dashboard_variables WHERE dashboard_id = ?", (dashboard_id,))
            db.execute("DELETE FROM dashboard_tabs WHERE dashboard_id = ?", (dashboard_id,))
    else:
        dashboard_id = create_dashboard(
            name=name,
            description=dash_data.get("description", ""),
            is_default=dash_data.get("is_default", False),
        )

    # Import queries — build old query_id -> new query_id mapping
    query_map: dict[int, int] = {}
    for q in data.get("queries", []):
        sid = source_id or q.get("source_id", 1)
        new_qid = create_saved_query(
            name=q["name"], source_id=sid, sql=q["sql"],
            description=q.get("description", ""),
            chart_type=q.get("chart_type"),
            chart_config=q.get("chart_config", "{}"),
            tags=q.get("tags", ""),
        )
        old_qid = q.get("id")
        if old_qid is not None:
            query_map[old_qid] = new_qid

    # Import tabs
    tab_map: dict[int, int] = {}
    for t in data.get("tabs", []):
        new_tab_id = create_dashboard_tab(
            dashboard_id=dashboard_id,
            name=t.get("name", "Tab"),
            tab_order=t.get("tab_order", 0),
        )
        old_tab_id = t.get("id")
        if old_tab_id is not None:
            tab_map[old_tab_id] = new_tab_id

    for w in data.get("widgets", []):
        mapped_query = query_map.get(w.get("query_id")) or w.get("query_id")
        mapped_tab = tab_map.get(w.get("tab_id")) or w.get("tab_id")
        create_widget(
            dashboard_id=dashboard_id,
            query_id=mapped_query,
            widget_type=w.get("widget_type", "chart"),
            title=w.get("title", ""),
            config=w.get("config", "{}"),
            col_start=w.get("col_start", 1),
            col_span=w.get("col_span", 6),
            row_start=w.get("row_start", 1),
            row_span=w.get("row_span", 4),
            tab_id=mapped_tab,
        )

    for v in data.get("variables", []):
        create_dashboard_variable(
            dashboard_id=dashboard_id,
            name=v["name"], var_type=v.get("var_type", "text"),
            default_value=v.get("default_value", ""),
            options=v.get("options", ""), label=v.get("label", ""),
        )

    return dashboard_id


# ─────────────────────────────────────────────────────────────
# Overview / Stats
# ─────────────────────────────────────────────────────────────

def get_overview_stats() -> dict:
    """Aggregate stats for the BI overview page."""
    dashboards = query_plugin_db(PLUGIN_ID, "SELECT COUNT(*) as cnt FROM dashboards")
    prebuilt = query_plugin_db(PLUGIN_ID, "SELECT COUNT(*) as cnt FROM dashboards WHERE is_prebuilt = 1")
    custom = query_plugin_db(PLUGIN_ID, "SELECT COUNT(*) as cnt FROM dashboards WHERE is_prebuilt = 0")
    tokens = query_plugin_db(
        PLUGIN_ID,
        "SELECT COUNT(*) as cnt FROM embed_tokens WHERE expires_at > ?",
        (datetime.now().isoformat(),),
    )
    snaps_today = query_plugin_db(
        PLUGIN_ID,
        "SELECT COUNT(*) as cnt FROM snapshots WHERE created_at >= date('now')",
    )
    sources = query_plugin_db(PLUGIN_ID, "SELECT COUNT(*) as cnt FROM data_sources")
    return {
        "total_dashboards": dashboards[0]["cnt"] if dashboards else 0,
        "prebuilt_count": prebuilt[0]["cnt"] if prebuilt else 0,
        "custom_count": custom[0]["cnt"] if custom else 0,
        "active_embed_tokens": tokens[0]["cnt"] if tokens else 0,
        "snapshots_today": snaps_today[0]["cnt"] if snaps_today else 0,
        "total_sources": sources[0]["cnt"] if sources else 0,
    }


def get_connected_apps() -> list[dict]:
    """Get connected apps grouped by app_id from embed_tokens."""
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT
               et.app_id,
               COUNT(DISTINCT et.dashboard_id) as dashboard_count,
               COUNT(et.id) as token_count,
               GROUP_CONCAT(DISTINCT d.name) as dashboard_names,
               MAX(et.created_at) as last_created,
               MAX(CASE WHEN et.rls_clauses != '{}' AND et.rls_clauses != '' THEN 1 ELSE 0 END) as has_rls
           FROM embed_tokens et
           JOIN dashboards d ON et.dashboard_id = d.id
           WHERE et.app_id != '' AND et.expires_at > ?
           GROUP BY et.app_id
           ORDER BY token_count DESC""",
        (datetime.now().isoformat(),),
    )
    return rows


def get_query_volume_7d() -> list[dict]:
    """Get snapshot counts per day for the last 7 days."""
    rows = query_plugin_db(
        PLUGIN_ID,
        """SELECT date(created_at) as day, COUNT(*) as count
           FROM snapshots
           WHERE created_at >= date('now', '-7 days')
           GROUP BY date(created_at)
           ORDER BY day""",
    )
    return rows


def get_recent_dashboards(limit: int = 5) -> list[dict]:
    """Get recently updated dashboards."""
    return query_plugin_db(
        PLUGIN_ID,
        "SELECT * FROM dashboards ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    )


def import_dashboard(data: dict, source_id: int | None = None) -> int:
    dash_data = data["dashboard"]
    dashboard_id = create_dashboard(
        name=dash_data.get("name", "Imported Dashboard"),
        description=dash_data.get("description", ""),
    )

    # Import queries — build old query_id -> new query_id mapping
    query_map: dict[int, int] = {}
    for q in data.get("queries", []):
        sid = source_id or q.get("source_id", 1)
        new_qid = create_saved_query(
            name=q["name"], source_id=sid, sql=q["sql"],
            description=q.get("description", ""),
            chart_type=q.get("chart_type"),
            chart_config=q.get("chart_config", "{}"),
            tags=q.get("tags", ""),
        )
        old_qid = q.get("id")
        if old_qid is not None:
            query_map[old_qid] = new_qid

    # Import tabs — build old tab_id -> new tab_id mapping
    tab_map: dict[int, int] = {}
    for t in data.get("tabs", []):
        new_tab_id = create_dashboard_tab(
            dashboard_id=dashboard_id,
            name=t.get("name", "Tab"),
            tab_order=t.get("tab_order", 0),
        )
        old_tab_id = t.get("id")
        if old_tab_id is not None:
            tab_map[old_tab_id] = new_tab_id

    for w in data.get("widgets", []):
        mapped_query = query_map.get(w.get("query_id"), w.get("query_id"))
        mapped_tab = tab_map.get(w.get("tab_id")) if w.get("tab_id") else None
        create_widget(
            dashboard_id=dashboard_id,
            query_id=mapped_query,
            widget_type=w.get("widget_type", "chart"),
            title=w.get("title", ""),
            config=w.get("config", "{}"),
            col_start=w.get("col_start", 1),
            col_span=w.get("col_span", 6),
            row_start=w.get("row_start", 1),
            row_span=w.get("row_span", 4),
            tab_id=mapped_tab,
        )

    for v in data.get("variables", []):
        create_dashboard_variable(
            dashboard_id=dashboard_id,
            name=v["name"], var_type=v.get("var_type", "text"),
            default_value=v.get("default_value", ""),
            options=v.get("options", ""), label=v.get("label", ""),
        )

    return dashboard_id
