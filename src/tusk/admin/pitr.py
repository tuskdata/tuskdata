"""Point-In-Time Recovery (PITR) for PostgreSQL

PITR allows restoring a database to any point in time by:
1. Taking periodic base backups with pg_basebackup
2. Continuously archiving WAL files
3. Replaying WAL files up to a specific timestamp

Directory structure:
~/.tusk/pitr/
├── {connection_id}/
│   ├── config.toml          # PITR configuration
│   ├── base/                 # Base backups
│   │   ├── 2026-01-25_120000/
│   │   └── ...
│   └── wal/                  # Archived WAL files
│       ├── 000000010000000000000001
│       └── ...
"""

import os
import subprocess
import shutil
from pathlib import Path
from datetime import datetime
import tomllib
import tomli_w
import msgspec
import structlog

from tusk.core.connection import ConnectionConfig, TUSK_DIR
from tusk.admin.backup import _find_pg_binary

log = structlog.get_logger()

PITR_DIR = TUSK_DIR / "pitr"


class PITRConfig(msgspec.Struct):
    """PITR configuration for a connection"""
    connection_id: str
    enabled: bool = False
    wal_archive_enabled: bool = False
    last_base_backup: str | None = None
    base_backup_count: int = 0


class BaseBackupInfo(msgspec.Struct):
    """Information about a base backup"""
    name: str
    path: str
    created_at: str
    size_bytes: int
    size_human: str
    wal_start: str | None = None
    label: str | None = None


def _get_pitr_dir(conn_id: str) -> Path:
    """Get PITR directory for a connection"""
    return PITR_DIR / conn_id


def _get_config_path(conn_id: str) -> Path:
    """Get config file path"""
    return _get_pitr_dir(conn_id) / "config.toml"


def _get_base_dir(conn_id: str) -> Path:
    """Get base backups directory"""
    return _get_pitr_dir(conn_id) / "base"


def _get_wal_dir(conn_id: str) -> Path:
    """Get WAL archive directory"""
    return _get_pitr_dir(conn_id) / "wal"


def get_pitr_config(conn_id: str) -> PITRConfig:
    """Get PITR configuration for a connection"""
    config_path = _get_config_path(conn_id)

    if not config_path.exists():
        return PITRConfig(connection_id=conn_id)

    try:
        with open(config_path, "rb") as f:
            data = tomllib.load(f)
        return PITRConfig(
            connection_id=conn_id,
            enabled=data.get("enabled", False),
            wal_archive_enabled=data.get("wal_archive_enabled", False),
            last_base_backup=data.get("last_base_backup"),
            base_backup_count=data.get("base_backup_count", 0),
        )
    except Exception as e:
        log.error("Failed to load PITR config", conn_id=conn_id, error=str(e))
        return PITRConfig(connection_id=conn_id)


def save_pitr_config(config: PITRConfig) -> None:
    """Save PITR configuration"""
    pitr_dir = _get_pitr_dir(config.connection_id)
    pitr_dir.mkdir(parents=True, exist_ok=True)

    config_path = _get_config_path(config.connection_id)
    data = {
        "enabled": config.enabled,
        "wal_archive_enabled": config.wal_archive_enabled,
        "last_base_backup": config.last_base_backup,
        "base_backup_count": config.base_backup_count,
    }

    with open(config_path, "wb") as f:
        tomli_w.dump(data, f)


def get_pg_basebackup_path() -> str:
    """Get path to pg_basebackup binary"""
    return _find_pg_binary("pg_basebackup")


def get_archive_command(conn_id: str) -> str:
    """Generate archive_command for postgresql.conf

    This command copies WAL files to our archive directory.
    """
    wal_dir = _get_wal_dir(conn_id)
    # %p = path to WAL file, %f = filename
    return f"cp %p {wal_dir}/%f"


def get_restore_command(conn_id: str) -> str:
    """Generate restore_command for recovery

    This command retrieves WAL files from our archive.
    """
    wal_dir = _get_wal_dir(conn_id)
    # %p = path to restore to, %f = filename needed
    return f"cp {wal_dir}/%f %p"


def create_base_backup(
    config: ConnectionConfig,
    label: str | None = None
) -> tuple[bool, str, BaseBackupInfo | None]:
    """Create a base backup using pg_basebackup

    Args:
        config: PostgreSQL connection config
        label: Optional label for the backup

    Returns:
        (success, message, backup_info)
    """
    conn_id = config.id
    base_dir = _get_base_dir(conn_id)
    base_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    backup_name = f"{timestamp}"
    if label:
        backup_name = f"{timestamp}_{label}"

    backup_path = base_dir / backup_name

    pg_basebackup = get_pg_basebackup_path()

    # Build command
    cmd = [
        pg_basebackup,
        "-h", config.host or "localhost",
        "-p", str(config.port),
        "-U", config.user or "postgres",
        "-D", str(backup_path),
        "-Ft",  # tar format
        "-z",   # gzip compression
        "-Xs",  # stream WAL during backup
        "-P",   # show progress
        "-v",   # verbose
    ]

    if label:
        cmd.extend(["-l", label])

    env = os.environ.copy()
    if config.password:
        env["PGPASSWORD"] = config.password

    log.info("Starting base backup", conn_id=conn_id, path=str(backup_path))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=3600  # 1 hour timeout
        )

        if result.returncode != 0:
            # Clean up failed backup
            if backup_path.exists():
                shutil.rmtree(backup_path)
            error = result.stderr or "Unknown error"
            log.error("Base backup failed", conn_id=conn_id, error=error)
            return False, f"pg_basebackup failed: {error}", None

        # Calculate size
        total_size = sum(f.stat().st_size for f in backup_path.rglob("*") if f.is_file())
        size_human = _format_bytes(total_size)

        # Update PITR config
        pitr_config = get_pitr_config(conn_id)
        pitr_config.enabled = True
        pitr_config.last_base_backup = timestamp
        pitr_config.base_backup_count += 1
        save_pitr_config(pitr_config)

        # Ensure WAL directory exists
        wal_dir = _get_wal_dir(conn_id)
        wal_dir.mkdir(parents=True, exist_ok=True)

        backup_info = BaseBackupInfo(
            name=backup_name,
            path=str(backup_path),
            created_at=datetime.now().isoformat(),
            size_bytes=total_size,
            size_human=size_human,
            label=label,
        )

        log.info("Base backup completed", conn_id=conn_id, size=size_human)
        return True, f"Base backup created: {backup_name} ({size_human})", backup_info

    except FileNotFoundError:
        return False, f"pg_basebackup not found at '{pg_basebackup}'. Install PostgreSQL client tools.", None
    except subprocess.TimeoutExpired:
        if backup_path.exists():
            shutil.rmtree(backup_path)
        return False, "Backup timed out after 1 hour", None
    except Exception as e:
        if backup_path.exists():
            shutil.rmtree(backup_path)
        log.error("Base backup error", conn_id=conn_id, error=str(e))
        return False, f"Backup failed: {str(e)}", None


def list_base_backups(conn_id: str) -> list[BaseBackupInfo]:
    """List all base backups for a connection"""
    base_dir = _get_base_dir(conn_id)
    if not base_dir.exists():
        return []

    backups = []
    for path in sorted(base_dir.iterdir(), reverse=True):
        if path.is_dir():
            # Calculate size
            total_size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

            # Parse timestamp from name
            name = path.name
            try:
                # Format: YYYY-MM-DD_HHMMSS or YYYY-MM-DD_HHMMSS_label
                parts = name.split("_", 2)
                date_str = f"{parts[0]}_{parts[1]}"
                created = datetime.strptime(date_str, "%Y-%m-%d_%H%M%S")
                label = parts[2] if len(parts) > 2 else None
            except (ValueError, IndexError):
                created = datetime.fromtimestamp(path.stat().st_mtime)
                label = None

            backups.append(BaseBackupInfo(
                name=name,
                path=str(path),
                created_at=created.isoformat(),
                size_bytes=total_size,
                size_human=_format_bytes(total_size),
                label=label,
            ))

    return backups


def delete_base_backup(conn_id: str, backup_name: str) -> tuple[bool, str]:
    """Delete a base backup"""
    backup_path = _get_base_dir(conn_id) / backup_name

    if not backup_path.exists():
        return False, f"Backup not found: {backup_name}"

    try:
        shutil.rmtree(backup_path)

        # Update config
        pitr_config = get_pitr_config(conn_id)
        pitr_config.base_backup_count = max(0, pitr_config.base_backup_count - 1)
        save_pitr_config(pitr_config)

        log.info("Base backup deleted", conn_id=conn_id, name=backup_name)
        return True, f"Backup deleted: {backup_name}"
    except Exception as e:
        return False, f"Failed to delete backup: {str(e)}"


def list_archived_wal(conn_id: str) -> list[dict]:
    """List archived WAL files"""
    wal_dir = _get_wal_dir(conn_id)
    if not wal_dir.exists():
        return []

    wal_files = []
    for f in sorted(wal_dir.iterdir(), reverse=True):
        if f.is_file():
            stat = f.stat()
            wal_files.append({
                "name": f.name,
                "size_bytes": stat.st_size,
                "size_human": _format_bytes(stat.st_size),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })

    return wal_files[:100]  # Limit to last 100


def get_pitr_status(conn_id: str) -> dict:
    """Get overall PITR status for a connection"""
    pitr_config = get_pitr_config(conn_id)
    base_backups = list_base_backups(conn_id)
    wal_files = list_archived_wal(conn_id)

    # Calculate total sizes
    base_size = sum(b.size_bytes for b in base_backups)
    wal_size = sum(w["size_bytes"] for w in wal_files)

    # Get recovery window
    oldest_backup = base_backups[-1] if base_backups else None
    newest_wal = wal_files[0] if wal_files else None

    return {
        "enabled": pitr_config.enabled,
        "wal_archive_enabled": pitr_config.wal_archive_enabled,
        "base_backup_count": len(base_backups),
        "wal_file_count": len(wal_files),
        "base_backup_size": _format_bytes(base_size),
        "wal_size": _format_bytes(wal_size),
        "total_size": _format_bytes(base_size + wal_size),
        "oldest_backup": oldest_backup.created_at if oldest_backup else None,
        "newest_wal": newest_wal["modified"] if newest_wal else None,
        "archive_command": get_archive_command(conn_id),
        "restore_command": get_restore_command(conn_id),
    }


def generate_recovery_conf(
    conn_id: str,
    target_time: str | None = None,
    target_name: str | None = None,
    target_xid: str | None = None,
    target_inclusive: bool = True,
) -> str:
    """Generate recovery configuration for PITR

    For PostgreSQL 12+, this goes in postgresql.conf or postgresql.auto.conf
    For older versions, this is recovery.conf

    Args:
        target_time: Recover to this timestamp (ISO format)
        target_name: Recover to this restore point name
        target_xid: Recover to this transaction ID
        target_inclusive: Whether to include the target or stop just before

    Returns:
        Configuration text to add to postgresql.conf
    """
    lines = [
        "# PITR Recovery Configuration",
        "# Generated by Tusk",
        "",
        f"restore_command = '{get_restore_command(conn_id)}'",
    ]

    if target_time:
        lines.append(f"recovery_target_time = '{target_time}'")
    elif target_name:
        lines.append(f"recovery_target_name = '{target_name}'")
    elif target_xid:
        lines.append(f"recovery_target_xid = '{target_xid}'")

    if target_time or target_name or target_xid:
        inclusive = "true" if target_inclusive else "false"
        lines.append(f"recovery_target_inclusive = {inclusive}")
        lines.append("recovery_target_action = 'promote'")

    return "\n".join(lines)


def prepare_recovery(
    conn_id: str,
    backup_name: str,
    target_time: str | None = None,
) -> tuple[bool, str, dict | None]:
    """Prepare files for PITR recovery

    This creates a recovery directory with:
    - Extracted base backup
    - recovery.signal file (PG12+)
    - Recovery configuration

    Args:
        conn_id: Connection ID
        backup_name: Name of base backup to restore from
        target_time: Optional target time for recovery

    Returns:
        (success, message, recovery_info)
    """
    base_dir = _get_base_dir(conn_id)
    backup_path = base_dir / backup_name

    if not backup_path.exists():
        return False, f"Backup not found: {backup_name}", None

    # Create recovery directory
    recovery_dir = PITR_DIR / conn_id / "recovery" / datetime.now().strftime("%Y%m%d_%H%M%S")
    recovery_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Extract base backup (it's in tar.gz format)
        base_tar = backup_path / "base.tar.gz"
        if base_tar.exists():
            subprocess.run(
                ["tar", "-xzf", str(base_tar), "-C", str(recovery_dir)],
                check=True,
                capture_output=True
            )
        else:
            # Maybe it's an uncompressed backup
            for item in backup_path.iterdir():
                if item.is_file():
                    shutil.copy2(item, recovery_dir)

        # Create recovery.signal (PostgreSQL 12+)
        (recovery_dir / "recovery.signal").touch()

        # Generate recovery config
        recovery_conf = generate_recovery_conf(
            conn_id,
            target_time=target_time,
        )

        # Write to postgresql.auto.conf (appending)
        auto_conf = recovery_dir / "postgresql.auto.conf"
        with open(auto_conf, "a") as f:
            f.write("\n\n")
            f.write(recovery_conf)

        recovery_info = {
            "recovery_dir": str(recovery_dir),
            "backup_name": backup_name,
            "target_time": target_time,
            "instructions": [
                "1. Stop the PostgreSQL server",
                "2. Backup your current data directory",
                f"3. Replace data directory contents with: {recovery_dir}",
                "4. Start PostgreSQL - recovery will begin automatically",
                "5. Once recovered, the recovery.signal file will be removed",
            ],
        }

        log.info("Recovery prepared", conn_id=conn_id, recovery_dir=str(recovery_dir))
        return True, f"Recovery prepared at {recovery_dir}", recovery_info

    except Exception as e:
        # Clean up on failure
        if recovery_dir.exists():
            shutil.rmtree(recovery_dir)
        log.error("Failed to prepare recovery", conn_id=conn_id, error=str(e))
        return False, f"Failed to prepare recovery: {str(e)}", None


def _format_bytes(size: int) -> str:
    """Format bytes to human readable"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"
