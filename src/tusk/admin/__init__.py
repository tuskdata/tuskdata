"""PostgreSQL administration module"""

from tusk.admin.stats import get_server_stats, ServerStats
from tusk.admin.processes import get_active_queries, kill_query, ActiveQuery
from tusk.admin.backup import create_backup, list_backups, get_backup_path
from tusk.admin.extensions import (
    get_extensions,
    get_installed_extensions,
    install_extension,
    uninstall_extension,
    Extension,
)
from tusk.admin.maintenance import (
    get_locks,
    get_all_locks,
    get_table_bloat,
    vacuum_table,
    analyze_table,
    reindex_table,
)

__all__ = [
    "get_server_stats",
    "ServerStats",
    "get_active_queries",
    "kill_query",
    "ActiveQuery",
    "create_backup",
    "list_backups",
    "get_backup_path",
    "get_extensions",
    "get_installed_extensions",
    "install_extension",
    "uninstall_extension",
    "Extension",
    "get_locks",
    "get_all_locks",
    "get_table_bloat",
    "vacuum_table",
    "analyze_table",
    "reindex_table",
]
