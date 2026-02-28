"""Admin API routes for PostgreSQL administration"""

import msgspec
from litestar import Controller, get, post, delete, Request
from litestar.params import Body
from litestar.response import File, Template, Response
from litestar.exceptions import NotAuthorizedException

from tusk.studio.htmx import is_htmx, htmx_toast

from tusk.core.connection import get_connection
from tusk.core.config import get_config


def _check_admin_auth(connection: Request, _: object) -> None:
    """Guard: require admin auth when auth_mode is 'multi'.

    In single-user mode (auth_mode != 'multi'), all access is allowed.
    In multi-user mode, only authenticated admin users can access admin endpoints.
    """
    config = get_config()
    if config.auth_mode != "multi":
        return  # Single user mode — no auth required

    from tusk.core.auth import get_session, get_user_by_id
    session_id = connection.cookies.get("tusk_session")
    if not session_id:
        raise NotAuthorizedException("Authentication required")

    session = get_session(session_id)
    if not session:
        raise NotAuthorizedException("Invalid or expired session")

    user = get_user_by_id(session.user_id)
    if not user or not user.is_active:
        raise NotAuthorizedException("User not found or inactive")

    if not user.is_admin:
        raise NotAuthorizedException("Admin access required")
from tusk.admin.stats import get_server_stats, ServerStats
from tusk.admin.processes import get_active_queries, kill_query, ActiveQuery
from tusk.admin.backup import (
    create_backup, list_backups, get_backup_path, restore_backup,
    create_database, create_database_from_backup
)
from tusk.admin.extensions import get_extensions, install_extension, uninstall_extension
from tusk.admin.maintenance import (
    get_locks,
    get_all_locks,
    get_table_bloat,
    vacuum_table,
    analyze_table,
    reindex_table,
)
from tusk.admin.roles import (
    get_roles,
    create_role,
    alter_role,
    drop_role,
    grant_role,
    revoke_role,
)
from tusk.admin.settings import (
    get_settings,
    get_important_settings,
    get_setting_categories,
    format_setting_value,
)
from tusk.admin.monitoring import (
    check_pg_stat_statements,
    get_slow_queries,
    reset_pg_stat_statements,
    get_index_usage,
    get_duplicate_indexes,
    get_replication_status,
    get_wal_stats,
    get_logs,
)
from tusk.admin.pitr import (
    get_pitr_config,
    get_pitr_status,
    create_base_backup,
    list_base_backups,
    delete_base_backup,
    list_archived_wal,
    prepare_recovery,
    get_archive_command,
    get_restore_command,
)


class AdminController(Controller):
    """Admin API for PostgreSQL management"""

    path = "/api/admin"
    guards = [_check_admin_auth]

    @get("/{conn_id:str}/stats")
    async def get_stats(self, request: Request, conn_id: str) -> dict | Template:
        """Get server statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Admin features only available for PostgreSQL"}

        stats = await get_server_stats(config)

        if isinstance(stats, dict):
            return stats  # Error case

        data = {
            "connections": stats.connections,
            "max_connections": stats.max_connections,
            "connection_pct": round(stats.connection_pct, 1),
            "active_queries": stats.active_queries,
            "cache_hit_ratio": stats.cache_hit_ratio,
            "db_size_bytes": stats.db_size_bytes,
            "db_size_human": stats.db_size_human,
            "uptime": stats.uptime,
            "version": stats.version,
        }

        if is_htmx(request):
            return Template("partials/admin/stats.html", context=data)
        return data

    @get("/{conn_id:str}/processes")
    async def get_processes(self, request: Request, conn_id: str) -> dict | Template:
        """Get active queries/processes"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Admin features only available for PostgreSQL"}

        queries = await get_active_queries(config)

        if isinstance(queries, dict):
            return queries  # Error case

        processes = [
            {
                "pid": q.pid,
                "user": q.user,
                "database": q.database,
                "state": q.state,
                "query": q.query,
                "query_preview": q.query_preview,
                "duration_seconds": q.duration_seconds,
                "duration_human": q.duration_human,
            }
            for q in queries
        ]

        if is_htmx(request):
            return Template("partials/admin/processes.html", context={"processes": processes, "conn_id": conn_id})
        return {"processes": processes}

    @post("/{conn_id:str}/kill/{pid:int}")
    async def kill_process(self, request: Request, conn_id: str, pid: int) -> dict | Template | Response:
        """Kill a query by PID"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Admin features only available for PostgreSQL"}

        success, message = await kill_query(config, pid)

        if is_htmx(request):
            if not success:
                return Response(content="", headers=htmx_toast(message, "error"))
            queries = await get_active_queries(config)
            processes = [
                {
                    "pid": q.pid,
                    "user": q.user,
                    "database": q.database,
                    "state": q.state,
                    "query": q.query,
                    "query_preview": q.query_preview,
                    "duration_seconds": q.duration_seconds,
                    "duration_human": q.duration_human,
                }
                for q in queries
            ] if not isinstance(queries, dict) else []
            return Template("partials/admin/processes.html", context={"processes": processes, "conn_id": conn_id})

        return {"success": success, "message": message}

    @post("/{conn_id:str}/backup")
    async def create_db_backup(self, request: Request, conn_id: str) -> dict | Response:
        """Create a database backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Backup only available for PostgreSQL"}

        success, message, filepath = create_backup(config)

        if is_htmx(request):
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        return {
            "success": success,
            "message": message,
            "filename": filepath.name if filepath else None,
        }

    @get("/{conn_id:str}/backups")
    async def list_db_backups(self, request: Request, conn_id: str) -> dict | Template:
        """List available backups"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        backups = list_backups()
        # Filter to show only backups for this database
        db_backups = [b for b in backups if b["filename"].startswith(config.database or "")]

        if is_htmx(request):
            return Template("partials/admin/backups.html", context={"backups": db_backups, "conn_id": conn_id})

        return {"backups": db_backups}

    @get("/backups/{filename:str}")
    async def download_backup(self, filename: str) -> File:
        """Download a backup file"""
        filepath = get_backup_path(filename)
        if not filepath:
            from litestar.exceptions import NotFoundException
            raise NotFoundException(f"Backup not found: {filename}")

        return File(
            path=filepath,
            filename=filename,
            media_type="application/gzip",
        )

    @delete("/backups/{filename:str}", status_code=200)
    async def delete_backup_file(self, request: Request, filename: str) -> dict | Response:
        """Delete a backup file"""
        from tusk.admin.backup import delete_backup
        success, message = delete_backup(filename)

        if is_htmx(request):
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        if success:
            return {"deleted": True, "message": message}
        return {"deleted": False, "error": message}

    @post("/{conn_id:str}/restore")
    async def restore_db_backup(self, request: Request, conn_id: str, data: dict = Body()) -> dict | Response:
        """Restore database from backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Restore only available for PostgreSQL"}

        filename = data.get("filename")
        if not filename:
            if is_htmx(request):
                return Response(content="", headers=htmx_toast("No filename provided", "error"))
            return {"success": False, "error": "No filename provided"}

        success, message = restore_backup(config, filename)

        if is_htmx(request):
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        return {"success": success, "message": message}

    @post("/{conn_id:str}/databases")
    async def create_new_database(self, request: Request, conn_id: str, data: dict = Body()) -> dict | Response:
        """Create a new database on the PostgreSQL server"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Database creation only available for PostgreSQL"}

        db_name = data.get("name")
        if not db_name:
            if is_htmx(request):
                return Response(content="", headers=htmx_toast("Database name is required", "error"))
            return {"success": False, "error": "Database name is required"}

        owner = data.get("owner")
        success, message = create_database(config, db_name, owner)

        if is_htmx(request):
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        return {"success": success, "message": message}

    @post("/{conn_id:str}/databases/from-backup")
    async def create_database_from_backup_file(self, request: Request, conn_id: str, data: dict = Body()) -> dict | Response:
        """Create a new database from a backup file"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Database creation only available for PostgreSQL"}

        db_name = data.get("name")
        filename = data.get("filename")

        if not db_name:
            if is_htmx(request):
                return Response(content="", headers=htmx_toast("Database name is required", "error"))
            return {"success": False, "error": "Database name is required"}
        if not filename:
            if is_htmx(request):
                return Response(content="", headers=htmx_toast("Backup filename is required", "error"))
            return {"success": False, "error": "Backup filename is required"}

        owner = data.get("owner")
        success, message = create_database_from_backup(config, filename, db_name, owner)

        if is_htmx(request):
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        return {"success": success, "message": message}

    @get("/{conn_id:str}/extensions")
    async def list_extensions(self, request: Request, conn_id: str) -> dict | Template:
        """List all extensions (installed and available)"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Extensions only available for PostgreSQL"}

        try:
            extensions = await get_extensions(config)
            ext_list = [
                {
                    "name": e.name,
                    "installed_version": e.installed_version,
                    "default_version": e.default_version,
                    "description": e.description,
                    "is_installed": e.is_installed,
                }
                for e in extensions
            ]

            if is_htmx(request):
                show_all = request.query_params.get("show_all", "false") == "true"
                return Template("partials/admin/extensions.html", context={"extensions": ext_list, "show_all": show_all, "conn_id": conn_id})

            return {"extensions": ext_list}
        except Exception as e:
            if is_htmx(request):
                return Template("partials/error-message.html", context={"error": str(e), "title": "Extensions Error"})
            return {"error": str(e)}

    @post("/{conn_id:str}/extensions/{name:str}/install")
    async def install_ext(self, request: Request, conn_id: str, name: str) -> dict | Template | Response:
        """Install an extension"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Extensions only available for PostgreSQL"}

        try:
            await install_extension(config, name)

            if is_htmx(request):
                extensions = await get_extensions(config)
                ext_list = [
                    {
                        "name": e.name,
                        "installed_version": e.installed_version,
                        "default_version": e.default_version,
                        "description": e.description,
                        "is_installed": e.is_installed,
                    }
                    for e in extensions
                ]
                show_all = request.query_params.get("show_all", "false") == "true"
                return Template("partials/admin/extensions.html", context={"extensions": ext_list, "show_all": show_all, "conn_id": conn_id})

            return {"success": True, "message": f"Extension '{name}' installed successfully"}
        except Exception as e:
            if is_htmx(request):
                return Response(
                    content=f'<div class="text-red-400 text-sm p-2"><i data-lucide="alert-circle" class="w-4 h-4 inline"></i> {e}</div>',
                    headers=htmx_toast(str(e), "error"),
                )
            return {"success": False, "error": str(e)}

    @post("/{conn_id:str}/extensions/{name:str}/uninstall")
    async def uninstall_ext(self, request: Request, conn_id: str, name: str, data: dict = Body(default={})) -> dict | Template | Response:
        """Uninstall an extension"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Extensions only available for PostgreSQL"}

        try:
            cascade = data.get("cascade", False)
            await uninstall_extension(config, name, cascade=cascade)

            if is_htmx(request):
                extensions = await get_extensions(config)
                ext_list = [
                    {
                        "name": e.name,
                        "installed_version": e.installed_version,
                        "default_version": e.default_version,
                        "description": e.description,
                        "is_installed": e.is_installed,
                    }
                    for e in extensions
                ]
                show_all = request.query_params.get("show_all", "false") == "true"
                return Template("partials/admin/extensions.html", context={"extensions": ext_list, "show_all": show_all, "conn_id": conn_id})

            return {"success": True, "message": f"Extension '{name}' uninstalled successfully"}
        except Exception as e:
            if is_htmx(request):
                return Response(
                    content=f'<div class="text-red-400 text-sm p-2"><i data-lucide="alert-circle" class="w-4 h-4 inline"></i> {e}</div>',
                    headers=htmx_toast(str(e), "error"),
                )
            return {"success": False, "error": str(e)}

    @get("/{conn_id:str}/locks")
    async def list_locks(self, request: Request, conn_id: str) -> dict | Template:
        """Get blocking locks in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Locks monitor only available for PostgreSQL"}

        result = await get_locks(config)
        if isinstance(result, dict) and "error" in result:
            return result

        if is_htmx(request):
            return Template("partials/admin/locks.html", context={"locks": result, "show_all": False, "conn_id": conn_id})
        return {"locks": result}

    @get("/{conn_id:str}/locks/all")
    async def list_all_locks(self, request: Request, conn_id: str) -> dict | Template:
        """Get all locks in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Locks monitor only available for PostgreSQL"}

        result = await get_all_locks(config)
        if isinstance(result, dict) and "error" in result:
            return result

        if is_htmx(request):
            return Template("partials/admin/locks.html", context={"locks": result, "show_all": True, "conn_id": conn_id})
        return {"locks": result}

    @get("/{conn_id:str}/tables/bloat")
    async def list_table_bloat(self, request: Request, conn_id: str) -> dict | Template:
        """Get table bloat and maintenance info"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Table maintenance only available for PostgreSQL"}

        result = await get_table_bloat(config)
        if isinstance(result, dict) and "error" in result:
            return result

        if is_htmx(request):
            return Template("partials/admin/bloat.html", context={"tables": result, "conn_id": conn_id})
        return {"tables": result}

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/vacuum")
    async def run_vacuum(self, request: Request, conn_id: str, schema: str, table: str, data: dict = Body(default={})) -> dict | Template | Response:
        """Run VACUUM on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        full = data.get("full", False)
        result = await vacuum_table(config, schema, table, full=full)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Vacuum failed"), "error"))
            bloat = await get_table_bloat(config)
            tables = bloat if not (isinstance(bloat, dict) and "error" in bloat) else []
            return Template("partials/admin/bloat.html", context={"tables": tables, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/analyze")
    async def run_analyze(self, request: Request, conn_id: str, schema: str, table: str) -> dict | Template | Response:
        """Run ANALYZE on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        result = await analyze_table(config, schema, table)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Analyze failed"), "error"))
            bloat = await get_table_bloat(config)
            tables = bloat if not (isinstance(bloat, dict) and "error" in bloat) else []
            return Template("partials/admin/bloat.html", context={"tables": tables, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/reindex")
    async def run_reindex(self, request: Request, conn_id: str, schema: str, table: str) -> dict | Template | Response:
        """Run REINDEX on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        result = await reindex_table(config, schema, table)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Reindex failed"), "error"))
            bloat = await get_table_bloat(config)
            tables = bloat if not (isinstance(bloat, dict) and "error" in bloat) else []
            return Template("partials/admin/bloat.html", context={"tables": tables, "conn_id": conn_id})

        return result

    # ===== Roles Management =====

    @get("/{conn_id:str}/roles")
    async def list_roles(self, request: Request, conn_id: str) -> dict | Template:
        """Get all roles in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Roles management only available for PostgreSQL"}

        try:
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]

            if is_htmx(request):
                return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})
            return {"roles": roles_list}
        except Exception as e:
            if is_htmx(request):
                return Template("partials/error-message.html", context={"error": str(e), "title": "Roles Error"})
            return {"error": str(e)}

    @post("/{conn_id:str}/roles")
    async def create_new_role(self, request: Request, conn_id: str, data: dict = Body()) -> dict | Template | Response:
        """Create a new role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        result = await create_role(
            config,
            name=data.get("name"),
            password=data.get("password"),
            login=data.get("login", True),
            superuser=data.get("superuser", False),
            createdb=data.get("createdb", False),
            createrole=data.get("createrole", False),
            connection_limit=data.get("connection_limit", -1),
            valid_until=data.get("valid_until"),
            in_roles=data.get("in_roles")
        )

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Failed to create role"), "error"))
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]
            return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/roles/{name:str}")
    async def update_role(self, request: Request, conn_id: str, name: str, data: dict = Body()) -> dict | Template | Response:
        """Update an existing role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        result = await alter_role(
            config,
            name=name,
            password=data.get("password"),
            login=data.get("login"),
            superuser=data.get("superuser"),
            createdb=data.get("createdb"),
            createrole=data.get("createrole"),
            connection_limit=data.get("connection_limit"),
            valid_until=data.get("valid_until")
        )

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Failed to update role"), "error"))
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]
            return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/roles/{name:str}/delete")
    async def delete_role(self, request: Request, conn_id: str, name: str) -> dict | Template | Response:
        """Delete a role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        result = await drop_role(config, name)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Failed to delete role"), "error"))
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]
            return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/roles/{role:str}/grant/{to_role:str}")
    async def grant_role_membership(self, request: Request, conn_id: str, role: str, to_role: str) -> dict | Template | Response:
        """Grant role membership"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        result = await grant_role(config, role, to_role)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Failed to grant role"), "error"))
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]
            return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})

        return result

    @post("/{conn_id:str}/roles/{role:str}/revoke/{from_role:str}")
    async def revoke_role_membership(self, request: Request, conn_id: str, role: str, from_role: str) -> dict | Template | Response:
        """Revoke role membership"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        result = await revoke_role(config, role, from_role)

        if is_htmx(request):
            if isinstance(result, dict) and not result.get("success", True):
                return Response(content="", headers=htmx_toast(result.get("error", "Failed to revoke role"), "error"))
            roles = await get_roles(config)
            roles_list = [
                {
                    "name": r.name,
                    "is_superuser": r.is_superuser,
                    "can_login": r.can_login,
                    "can_create_db": r.can_create_db,
                    "can_create_role": r.can_create_role,
                    "connection_limit": r.connection_limit,
                    "valid_until": r.valid_until,
                    "member_of": r.member_of,
                }
                for r in roles
            ]
            return Template("partials/admin/roles.html", context={"roles": roles_list, "conn_id": conn_id})

        return result

    # ===== Settings Viewer =====

    @get("/{conn_id:str}/settings")
    async def list_settings(self, request: Request, conn_id: str, category: str | None = None, important_only: bool = False) -> dict | Template:
        """Get PostgreSQL configuration settings"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Settings viewer only available for PostgreSQL"}

        try:
            if important_only:
                settings = await get_important_settings(config)
            else:
                settings = await get_settings(config, category)

            settings_list = [
                {
                    "name": s.name,
                    "setting": s.setting,
                    "formatted": format_setting_value(s.setting, s.unit),
                    "unit": s.unit,
                    "category": s.category,
                    "description": s.short_desc,
                    "context": s.context,
                    "boot_val": s.boot_val,
                    "reset_val": s.reset_val,
                    "pending_restart": s.pending_restart,
                }
                for s in settings
            ]

            if is_htmx(request):
                return Template("partials/admin/settings.html", context={"settings": settings_list})
            return {"settings": settings_list}
        except Exception as e:
            if is_htmx(request):
                return Template("partials/error-message.html", context={"error": str(e), "title": "Settings Error"})
            return {"error": str(e)}

    @get("/{conn_id:str}/settings/categories")
    async def list_setting_categories(self, conn_id: str) -> dict:
        """Get all setting categories"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Settings viewer only available for PostgreSQL"}

        try:
            categories = await get_setting_categories(config)
            return {"categories": categories}
        except Exception as e:
            return {"error": str(e)}

    # ===== Advanced Monitoring =====

    @get("/{conn_id:str}/slow-queries")
    async def list_slow_queries(
        self, request: Request, conn_id: str, limit: int = 20, order_by: str = "total_time"
    ) -> dict | Template:
        """Get slow queries from pg_stat_statements"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Slow queries only available for PostgreSQL"}

        if is_htmx(request):
            status = await check_pg_stat_statements(config)
            result_queries = await get_slow_queries(config, limit=limit, order_by=order_by)
            queries = result_queries if not (isinstance(result_queries, dict) and "error" in result_queries) else []
            return Template("partials/admin/slow-queries.html", context={
                "available": status.get("installed", False),
                "queries": queries,
            })

        result = await get_slow_queries(config, limit=limit, order_by=order_by)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"queries": result}

    @get("/{conn_id:str}/slow-queries/status")
    async def slow_queries_status(self, conn_id: str) -> dict:
        """Check if pg_stat_statements is available"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        return await check_pg_stat_statements(config)

    @post("/{conn_id:str}/slow-queries/reset")
    async def reset_slow_queries(self, request: Request, conn_id: str) -> dict | Response:
        """Reset pg_stat_statements statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        result = await reset_pg_stat_statements(config)

        if is_htmx(request):
            success = result.get("success", False) if isinstance(result, dict) else False
            message = result.get("message", "Statistics reset") if isinstance(result, dict) else "Statistics reset"
            return Response(content="", headers=htmx_toast(message, "success" if success else "error"))

        return result

    @get("/{conn_id:str}/indexes")
    async def list_index_usage(
        self, request: Request, conn_id: str,
        include_system: bool = False, unused_only: bool = False,
    ) -> dict | Template:
        """Get index usage statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Index stats only available for PostgreSQL"}

        result = await get_index_usage(config, include_system=include_system)
        if isinstance(result, dict) and "error" in result:
            if is_htmx(request):
                return Template("partials/admin/indexes.html", context={"indexes": []})
            return result

        indexes = result
        if unused_only:
            indexes = [idx for idx in indexes if idx.get("is_unused") or idx.get("idx_scan", 0) == 0]

        if is_htmx(request):
            return Template("partials/admin/indexes.html", context={"indexes": indexes})

        return {"indexes": indexes}

    @get("/{conn_id:str}/indexes/duplicates")
    async def list_duplicate_indexes(self, conn_id: str) -> dict:
        """Find potentially duplicate indexes"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Index stats only available for PostgreSQL"}

        result = await get_duplicate_indexes(config)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"duplicates": result}

    @get("/{conn_id:str}/replication")
    async def get_replication(self, request: Request, conn_id: str) -> dict | Template:
        """Get replication status - slots, replicas, WAL"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Replication stats only available for PostgreSQL"}

        if is_htmx(request):
            replication_data = await get_replication_status(config)
            wal_data = await get_wal_stats(config)
            return Template("partials/admin/replication.html", context={
                "replication": replication_data,
                "wal_stats": wal_data,
            })

        return await get_replication_status(config)

    @get("/{conn_id:str}/wal")
    async def get_wal(self, conn_id: str) -> dict:
        """Get WAL statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "WAL stats only available for PostgreSQL"}

        return await get_wal_stats(config)

    @get("/{conn_id:str}/logs")
    async def get_server_logs(self, request: Request, conn_id: str, limit: int = 100, level: str | None = None) -> dict | Template:
        """Get PostgreSQL server logs"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Logs only available for PostgreSQL"}

        data = await get_logs(config, limit=limit, level=level)

        if is_htmx(request):
            return Template("partials/admin/logs.html", context={"logs": data.get("logs", [])})

        return data

    # ===== Point-In-Time Recovery (PITR) =====

    @get("/{conn_id:str}/pitr")
    async def get_pitr(self, request: Request, conn_id: str) -> dict | Template:
        """Get PITR status and configuration"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "PITR only available for PostgreSQL"}

        status = get_pitr_status(conn_id)

        if is_htmx(request):
            backups = list_base_backups(conn_id)
            return Template("partials/admin/pitr.html", context={
                "config": status,
                "base_backups": backups,
                "conn_id": conn_id,
            })

        return status

    @post("/{conn_id:str}/pitr/base-backup")
    async def create_pitr_base_backup(self, request: Request, conn_id: str, data: dict = Body(default={})) -> dict | Template | Response:
        """Create a base backup for PITR"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "PITR only available for PostgreSQL"}

        label = data.get("label")
        success, message, backup_info = create_base_backup(config, label=label)

        if is_htmx(request):
            if not success:
                return Response(content="", headers=htmx_toast(message, "error"))
            status = get_pitr_status(conn_id)
            backups = list_base_backups(conn_id)
            return Template("partials/admin/pitr.html", context={
                "config": status,
                "base_backups": backups,
                "conn_id": conn_id,
            })

        result = {"success": success, "message": message}
        if backup_info:
            result["backup"] = {
                "name": backup_info.name,
                "path": backup_info.path,
                "created_at": backup_info.created_at,
                "size_human": backup_info.size_human,
            }
        return result

    @get("/{conn_id:str}/pitr/base-backups")
    async def list_pitr_base_backups(self, conn_id: str) -> dict:
        """List all base backups"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        backups = list_base_backups(conn_id)
        return {
            "backups": [
                {
                    "name": b.name,
                    "path": b.path,
                    "created_at": b.created_at,
                    "size_human": b.size_human,
                    "label": b.label,
                }
                for b in backups
            ]
        }

    @post("/{conn_id:str}/pitr/base-backups/{name:str}/delete")
    async def delete_pitr_base_backup(self, request: Request, conn_id: str, name: str) -> dict | Template | Response:
        """Delete a base backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        success, message = delete_base_backup(conn_id, name)

        if is_htmx(request):
            if not success:
                return Response(content="", headers=htmx_toast(message, "error"))
            status = get_pitr_status(conn_id)
            backups = list_base_backups(conn_id)
            return Template("partials/admin/pitr.html", context={
                "config": status,
                "base_backups": backups,
                "conn_id": conn_id,
            })

        return {"success": success, "message": message}

    @get("/{conn_id:str}/pitr/wal")
    async def list_pitr_wal(self, conn_id: str) -> dict:
        """List archived WAL files"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        wal_files = list_archived_wal(conn_id)
        return {"wal_files": wal_files}

    @post("/{conn_id:str}/pitr/prepare-recovery")
    async def prepare_pitr_recovery(self, conn_id: str, data: dict = Body()) -> dict:
        """Prepare recovery from a base backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "PITR only available for PostgreSQL"}

        backup_name = data.get("backup_name")
        if not backup_name:
            return {"success": False, "error": "backup_name is required"}

        target_time = data.get("target_time")  # Optional ISO timestamp

        success, message, recovery_info = prepare_recovery(
            conn_id,
            backup_name,
            target_time=target_time
        )

        result = {"success": success, "message": message}
        if recovery_info:
            result["recovery"] = recovery_info
        return result

    @get("/{conn_id:str}/pitr/archive-command")
    async def get_pitr_archive_command(self, conn_id: str) -> dict:
        """Get the archive_command to configure in PostgreSQL"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        return {
            "archive_command": get_archive_command(conn_id),
            "restore_command": get_restore_command(conn_id),
            "instructions": [
                "To enable WAL archiving, add these to postgresql.conf:",
                "",
                "archive_mode = on",
                f"archive_command = '{get_archive_command(conn_id)}'",
                "",
                "Then restart PostgreSQL.",
            ]
        }
