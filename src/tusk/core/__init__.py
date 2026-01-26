"""Core abstractions for Tusk"""

from tusk.core.connection import (
    ConnectionConfig,
    add_connection,
    get_connection,
    list_connections,
    delete_connection,
    update_connection,
    save_connections_to_file,
    load_connections_from_file,
)
from tusk.core.result import QueryResult, ColumnInfo
from tusk.core.config import (
    TuskConfig,
    get_config,
    load_config,
    save_config,
    update_config,
    set_pg_bin_path,
)
from tusk.core.history import (
    QueryHistory,
    QueryHistoryEntry,
    get_history,
)
from tusk.core.workspace import (
    WorkspaceState,
    DatasetState,
    save_workspace,
    load_workspace,
    delete_workspace,
    list_workspaces,
)

__all__ = [
    "ConnectionConfig",
    "add_connection",
    "get_connection",
    "list_connections",
    "delete_connection",
    "update_connection",
    "save_connections_to_file",
    "load_connections_from_file",
    "QueryResult",
    "ColumnInfo",
    "TuskConfig",
    "get_config",
    "load_config",
    "save_config",
    "update_config",
    "set_pg_bin_path",
    "QueryHistory",
    "QueryHistoryEntry",
    "get_history",
    "WorkspaceState",
    "DatasetState",
    "save_workspace",
    "load_workspace",
    "delete_workspace",
    "list_workspaces",
]
