"""PostgreSQL server settings management"""

import msgspec
from ..core.connection import ConnectionConfig
from ..engines.postgres import execute_query


class PgSetting(msgspec.Struct):
    """PostgreSQL configuration setting"""
    name: str
    setting: str
    unit: str | None = None
    category: str | None = None
    short_desc: str | None = None
    context: str | None = None
    boot_val: str | None = None
    reset_val: str | None = None
    pending_restart: bool = False


async def get_settings(config: ConnectionConfig, category: str | None = None) -> list[PgSetting]:
    """Get PostgreSQL configuration settings"""

    sql = """
    SELECT
        name,
        setting,
        unit,
        category,
        short_desc,
        context,
        boot_val,
        reset_val,
        pending_restart
    FROM pg_settings
    """

    if category:
        # Validate category to prevent SQL injection
        safe_categories = [
            'File Locations', 'Resource Usage', 'Write Ahead Log',
            'Replication', 'Query Tuning', 'Reporting and Logging',
            'Autovacuum', 'Client Connection Defaults', 'Lock Management',
            'Version and Platform Compatibility', 'Error Handling',
            'Preset Options', 'Developer Options', 'Connections and Authentication'
        ]
        if category in safe_categories:
            sql += f" WHERE category LIKE '{category}%'"

    sql += " ORDER BY category, name"

    result = await execute_query(config, sql)

    settings = []
    for row in result.rows:
        settings.append(PgSetting(
            name=row[0],
            setting=str(row[1]) if row[1] is not None else '',
            unit=row[2],
            category=row[3],
            short_desc=row[4],
            context=row[5],
            boot_val=str(row[6]) if row[6] is not None else None,
            reset_val=str(row[7]) if row[7] is not None else None,
            pending_restart=row[8] if row[8] is not None else False
        ))

    return settings


async def get_setting_categories(config: ConnectionConfig) -> list[str]:
    """Get all setting categories"""

    sql = """
    SELECT DISTINCT category
    FROM pg_settings
    WHERE category IS NOT NULL
    ORDER BY category
    """

    result = await execute_query(config, sql)
    return [row[0] for row in result.rows if row[0]]


async def get_important_settings(config: ConnectionConfig) -> list[PgSetting]:
    """Get commonly monitored settings"""

    important_settings = [
        'shared_buffers', 'effective_cache_size', 'work_mem', 'maintenance_work_mem',
        'max_connections', 'max_parallel_workers', 'max_parallel_workers_per_gather',
        'random_page_cost', 'effective_io_concurrency', 'wal_buffers',
        'checkpoint_completion_target', 'max_wal_size', 'min_wal_size',
        'default_statistics_target', 'log_statement', 'log_min_duration_statement',
        'autovacuum', 'autovacuum_vacuum_scale_factor', 'autovacuum_analyze_scale_factor'
    ]

    placeholders = ', '.join(f"'{s}'" for s in important_settings)

    sql = f"""
    SELECT
        name,
        setting,
        unit,
        category,
        short_desc,
        context,
        boot_val,
        reset_val,
        pending_restart
    FROM pg_settings
    WHERE name IN ({placeholders})
    ORDER BY
        CASE
            WHEN category LIKE 'Resource%' THEN 1
            WHEN category LIKE 'Write%' THEN 2
            WHEN category LIKE 'Query%' THEN 3
            WHEN category LIKE 'Autovacuum%' THEN 4
            ELSE 5
        END,
        name
    """

    result = await execute_query(config, sql)

    settings = []
    for row in result.rows:
        settings.append(PgSetting(
            name=row[0],
            setting=str(row[1]) if row[1] is not None else '',
            unit=row[2],
            category=row[3],
            short_desc=row[4],
            context=row[5],
            boot_val=str(row[6]) if row[6] is not None else None,
            reset_val=str(row[7]) if row[7] is not None else None,
            pending_restart=row[8] if row[8] is not None else False
        ))

    return settings


def format_setting_value(value: str, unit: str | None) -> str:
    """Format a setting value with its unit for display"""
    if not value:
        return '-'

    try:
        num = int(value)
        if unit == 'kB':
            if num >= 1048576:
                return f"{num / 1048576:.1f} GB"
            elif num >= 1024:
                return f"{num / 1024:.1f} MB"
            else:
                return f"{num} kB"
        elif unit == '8kB':
            size_kb = num * 8
            if size_kb >= 1048576:
                return f"{size_kb / 1048576:.1f} GB"
            elif size_kb >= 1024:
                return f"{size_kb / 1024:.1f} MB"
            else:
                return f"{size_kb} kB"
        elif unit == 'ms':
            if num >= 60000:
                return f"{num / 60000:.1f} min"
            elif num >= 1000:
                return f"{num / 1000:.1f} s"
            else:
                return f"{num} ms"
        elif unit == 's':
            if num >= 3600:
                return f"{num / 3600:.1f} h"
            elif num >= 60:
                return f"{num / 60:.1f} min"
            else:
                return f"{num} s"
        elif unit:
            return f"{value} {unit}"
        else:
            return value
    except ValueError:
        if unit:
            return f"{value} {unit}"
        return value
