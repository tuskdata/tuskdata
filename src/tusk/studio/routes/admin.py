"""Admin API routes for PostgreSQL administration"""

import msgspec
from litestar import Controller, get, post
from litestar.params import Body
from litestar.response import File

from tusk.core.connection import get_connection
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

    @get("/{conn_id:str}/stats")
    async def get_stats(self, conn_id: str) -> dict:
        """Get server statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Admin features only available for PostgreSQL"}

        stats = await get_server_stats(config)

        if isinstance(stats, dict):
            return stats  # Error case

        return {
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

    @get("/{conn_id:str}/processes")
    async def get_processes(self, conn_id: str) -> dict:
        """Get active queries/processes"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Admin features only available for PostgreSQL"}

        queries = await get_active_queries(config)

        if isinstance(queries, dict):
            return queries  # Error case

        return {
            "processes": [
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
        }

    @post("/{conn_id:str}/kill/{pid:int}")
    async def kill_process(self, conn_id: str, pid: int) -> dict:
        """Kill a query by PID"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Admin features only available for PostgreSQL"}

        success, message = await kill_query(config, pid)
        return {"success": success, "message": message}

    @post("/{conn_id:str}/backup")
    async def create_db_backup(self, conn_id: str) -> dict:
        """Create a database backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Backup only available for PostgreSQL"}

        success, message, filepath = create_backup(config)
        return {
            "success": success,
            "message": message,
            "filename": filepath.name if filepath else None,
        }

    @get("/{conn_id:str}/backups")
    async def list_db_backups(self, conn_id: str) -> dict:
        """List available backups"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        backups = list_backups()
        # Filter to show only backups for this database
        db_backups = [b for b in backups if b["filename"].startswith(config.database or "")]
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

    @post("/{conn_id:str}/restore")
    async def restore_db_backup(self, conn_id: str, data: dict = Body()) -> dict:
        """Restore database from backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Restore only available for PostgreSQL"}

        filename = data.get("filename")
        if not filename:
            return {"success": False, "error": "No filename provided"}

        success, message = restore_backup(config, filename)
        return {"success": success, "message": message}

    @post("/{conn_id:str}/databases")
    async def create_new_database(self, conn_id: str, data: dict = Body()) -> dict:
        """Create a new database on the PostgreSQL server"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Database creation only available for PostgreSQL"}

        db_name = data.get("name")
        if not db_name:
            return {"success": False, "error": "Database name is required"}

        owner = data.get("owner")
        success, message = create_database(config, db_name, owner)
        return {"success": success, "message": message}

    @post("/{conn_id:str}/databases/from-backup")
    async def create_database_from_backup_file(self, conn_id: str, data: dict = Body()) -> dict:
        """Create a new database from a backup file"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Database creation only available for PostgreSQL"}

        db_name = data.get("name")
        filename = data.get("filename")

        if not db_name:
            return {"success": False, "error": "Database name is required"}
        if not filename:
            return {"success": False, "error": "Backup filename is required"}

        owner = data.get("owner")
        success, message = create_database_from_backup(config, filename, db_name, owner)
        return {"success": success, "message": message}

    @get("/{conn_id:str}/extensions")
    async def list_extensions(self, conn_id: str) -> dict:
        """List all extensions (installed and available)"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Extensions only available for PostgreSQL"}

        try:
            extensions = await get_extensions(config)
            return {
                "extensions": [
                    {
                        "name": e.name,
                        "installed_version": e.installed_version,
                        "default_version": e.default_version,
                        "description": e.description,
                        "is_installed": e.is_installed,
                    }
                    for e in extensions
                ]
            }
        except Exception as e:
            return {"error": str(e)}

    @post("/{conn_id:str}/extensions/{name:str}/install")
    async def install_ext(self, conn_id: str, name: str) -> dict:
        """Install an extension"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Extensions only available for PostgreSQL"}

        try:
            await install_extension(config, name)
            return {"success": True, "message": f"Extension '{name}' installed successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @post("/{conn_id:str}/extensions/{name:str}/uninstall")
    async def uninstall_ext(self, conn_id: str, name: str, data: dict = Body(default={})) -> dict:
        """Uninstall an extension"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Extensions only available for PostgreSQL"}

        try:
            cascade = data.get("cascade", False)
            await uninstall_extension(config, name, cascade=cascade)
            return {"success": True, "message": f"Extension '{name}' uninstalled successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    @get("/{conn_id:str}/locks")
    async def list_locks(self, conn_id: str) -> dict:
        """Get blocking locks in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Locks monitor only available for PostgreSQL"}

        result = await get_locks(config)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"locks": result}

    @get("/{conn_id:str}/locks/all")
    async def list_all_locks(self, conn_id: str) -> dict:
        """Get all locks in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Locks monitor only available for PostgreSQL"}

        result = await get_all_locks(config)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"locks": result}

    @get("/{conn_id:str}/tables/bloat")
    async def list_table_bloat(self, conn_id: str) -> dict:
        """Get table bloat and maintenance info"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Table maintenance only available for PostgreSQL"}

        result = await get_table_bloat(config)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"tables": result}

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/vacuum")
    async def run_vacuum(self, conn_id: str, schema: str, table: str, data: dict = Body(default={})) -> dict:
        """Run VACUUM on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        full = data.get("full", False)
        return await vacuum_table(config, schema, table, full=full)

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/analyze")
    async def run_analyze(self, conn_id: str, schema: str, table: str) -> dict:
        """Run ANALYZE on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        return await analyze_table(config, schema, table)

    @post("/{conn_id:str}/tables/{schema:str}/{table:str}/reindex")
    async def run_reindex(self, conn_id: str, schema: str, table: str) -> dict:
        """Run REINDEX on a table"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Table maintenance only available for PostgreSQL"}

        return await reindex_table(config, schema, table)

    # ===== Roles Management =====

    @get("/{conn_id:str}/roles")
    async def list_roles(self, conn_id: str) -> dict:
        """Get all roles in the database"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Roles management only available for PostgreSQL"}

        try:
            roles = await get_roles(config)
            return {
                "roles": [
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
            }
        except Exception as e:
            return {"error": str(e)}

    @post("/{conn_id:str}/roles")
    async def create_new_role(self, conn_id: str, data: dict = Body()) -> dict:
        """Create a new role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        return await create_role(
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

    @post("/{conn_id:str}/roles/{name:str}")
    async def update_role(self, conn_id: str, name: str, data: dict = Body()) -> dict:
        """Update an existing role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        return await alter_role(
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

    @post("/{conn_id:str}/roles/{name:str}/delete")
    async def delete_role(self, conn_id: str, name: str) -> dict:
        """Delete a role"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        return await drop_role(config, name)

    @post("/{conn_id:str}/roles/{role:str}/grant/{to_role:str}")
    async def grant_role_membership(self, conn_id: str, role: str, to_role: str) -> dict:
        """Grant role membership"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        return await grant_role(config, role, to_role)

    @post("/{conn_id:str}/roles/{role:str}/revoke/{from_role:str}")
    async def revoke_role_membership(self, conn_id: str, role: str, from_role: str) -> dict:
        """Revoke role membership"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "Roles management only available for PostgreSQL"}

        return await revoke_role(config, role, from_role)

    # ===== Settings Viewer =====

    @get("/{conn_id:str}/settings")
    async def list_settings(self, conn_id: str, category: str | None = None, important_only: bool = False) -> dict:
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

            return {
                "settings": [
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
            }
        except Exception as e:
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
        self, conn_id: str, limit: int = 20, order_by: str = "total_time"
    ) -> dict:
        """Get slow queries from pg_stat_statements"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Slow queries only available for PostgreSQL"}

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
    async def reset_slow_queries(self, conn_id: str) -> dict:
        """Reset pg_stat_statements statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        return await reset_pg_stat_statements(config)

    @get("/{conn_id:str}/indexes")
    async def list_index_usage(self, conn_id: str, include_system: bool = False) -> dict:
        """Get index usage statistics"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Index stats only available for PostgreSQL"}

        result = await get_index_usage(config, include_system=include_system)
        if isinstance(result, dict) and "error" in result:
            return result

        return {"indexes": result}

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
    async def get_replication(self, conn_id: str) -> dict:
        """Get replication status - slots, replicas, WAL"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "Replication stats only available for PostgreSQL"}

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

    # ===== Point-In-Time Recovery (PITR) =====

    @get("/{conn_id:str}/pitr")
    async def get_pitr(self, conn_id: str) -> dict:
        """Get PITR status and configuration"""
        config = get_connection(conn_id)
        if not config:
            return {"error": "Connection not found"}

        if config.type != "postgres":
            return {"error": "PITR only available for PostgreSQL"}

        return get_pitr_status(conn_id)

    @post("/{conn_id:str}/pitr/base-backup")
    async def create_pitr_base_backup(self, conn_id: str, data: dict = Body(default={})) -> dict:
        """Create a base backup for PITR"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        if config.type != "postgres":
            return {"success": False, "error": "PITR only available for PostgreSQL"}

        label = data.get("label")
        success, message, backup_info = create_base_backup(config, label=label)

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
    async def delete_pitr_base_backup(self, conn_id: str, name: str) -> dict:
        """Delete a base backup"""
        config = get_connection(conn_id)
        if not config:
            return {"success": False, "error": "Connection not found"}

        success, message = delete_base_backup(conn_id, name)
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
