"""Backup and restore functionality for PostgreSQL"""

import subprocess
import os
import tempfile
import stat
from pathlib import Path
from datetime import datetime

import structlog
from tusk.core.connection import ConnectionConfig, TUSK_DIR
from tusk.core.config import get_config

log = structlog.get_logger("backup")

BACKUP_DIR = TUSK_DIR / "backups"


def _pg_env(config: ConnectionConfig) -> tuple[dict, Path | None]:
    """Create environment for pg_dump/psql with secure password handling.

    Uses a temporary .pgpass file instead of PGPASSWORD env var.
    Returns (env_dict, pgpass_path_or_None).
    Caller must delete pgpass_path when done.
    """
    env = os.environ.copy()
    pgpass_path = None

    if config.password:
        # Create temp .pgpass file with restrictive permissions
        fd, pgpass_path = tempfile.mkstemp(prefix="tusk_pgpass_")
        pgpass_file = Path(pgpass_path)
        # Escape colons and backslashes in pgpass fields
        host = (config.host or "localhost").replace("\\", "\\\\").replace(":", "\\:")
        port = str(config.port)
        db = (config.database or "*").replace("\\", "\\\\").replace(":", "\\:")
        user = (config.user or "postgres").replace("\\", "\\\\").replace(":", "\\:")
        pw = config.password.replace("\\", "\\\\").replace(":", "\\:")
        os.write(fd, f"{host}:{port}:{db}:{user}:{pw}\n".encode())
        os.close(fd)
        os.chmod(pgpass_path, stat.S_IRUSR)  # 0o400 — owner read only
        env["PGPASSFILE"] = pgpass_path
        pgpass_path = pgpass_file

    return env, pgpass_path


def _get_pg_bin_search_paths() -> list[Path]:
    """Get list of paths to search for PostgreSQL binaries"""
    paths = []
    seen_resolved = set()

    def add_path(p: Path):
        """Add path if not already seen (resolves symlinks to avoid duplicates)"""
        try:
            resolved = p.resolve()
            if resolved not in seen_resolved and resolved.exists():
                seen_resolved.add(resolved)
                paths.append(p)
        except Exception:
            pass

    # Postgres.app (macOS) - check latest first
    add_path(Path("/Applications/Postgres.app/Contents/Versions/latest/bin"))

    # Add versioned Postgres.app paths
    pg_app_versions = Path("/Applications/Postgres.app/Contents/Versions")
    if pg_app_versions.exists():
        for version_dir in sorted(pg_app_versions.iterdir(), reverse=True):
            if version_dir.name != "latest" and version_dir.is_dir():
                add_path(version_dir / "bin")

    # Homebrew (macOS)
    for p in [
        Path("/opt/homebrew/opt/postgresql/bin"),
        Path("/opt/homebrew/bin"),
        Path("/usr/local/opt/postgresql/bin"),
        Path("/usr/local/bin"),
    ]:
        add_path(p)

    # Linux common paths
    for p in [
        Path("/usr/lib/postgresql/16/bin"),
        Path("/usr/lib/postgresql/15/bin"),
        Path("/usr/lib/postgresql/14/bin"),
        Path("/usr/pgsql-16/bin"),
        Path("/usr/pgsql-15/bin"),
        Path("/usr/bin"),
    ]:
        add_path(p)

    return paths


def _find_pg_binary(name: str) -> str:
    """Find a PostgreSQL binary (pg_dump, psql, etc.)

    Priority:
    1. User-configured pg_bin_path in ~/.tusk/config.toml
    2. System PATH
    3. Common installation locations (Postgres.app, Homebrew, etc.)
    """
    import shutil

    # First check user-configured path
    config = get_config()
    if config.pg_bin_path:
        configured_path = Path(config.pg_bin_path) / name
        if configured_path.exists() and configured_path.is_file():
            return str(configured_path)

    # Then check if it's in PATH
    if shutil.which(name):
        return name

    # Search in common locations
    for search_path in _get_pg_bin_search_paths():
        binary_path = search_path / name
        if binary_path.exists() and binary_path.is_file():
            return str(binary_path)

    # Fallback to just the name (will fail with clear error if not found)
    return name


def get_pg_dump_path() -> str:
    """Get the path to pg_dump binary"""
    return _find_pg_binary("pg_dump")


def get_psql_path() -> str:
    """Get the path to psql binary"""
    return _find_pg_binary("psql")


def create_backup(config: ConnectionConfig) -> tuple[bool, str, Path | None]:
    """Create a pg_dump backup of the database

    Returns: (success, message, filepath)
    """
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"{config.database}_{timestamp}.sql.gz"
    filepath = BACKUP_DIR / filename

    pg_dump = get_pg_dump_path()

    cmd = [
        pg_dump,
        "-h", config.host or "localhost",
        "-p", str(config.port),
        "-U", config.user or "postgres",
        "-d", config.database or "postgres",
        "--format=plain",
    ]

    env, pgpass_path = _pg_env(config)

    try:
        # pg_dump | gzip > file
        with open(filepath, "wb") as f:
            dump_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            gzip_proc = subprocess.Popen(
                ["gzip"],
                stdin=dump_proc.stdout,
                stdout=f,
                stderr=subprocess.PIPE
            )

            dump_proc.stdout.close()
            gzip_proc.communicate()
            dump_proc.wait()

            if dump_proc.returncode != 0:
                stderr = dump_proc.stderr.read().decode() if dump_proc.stderr else "Unknown error"
                filepath.unlink(missing_ok=True)
                return False, f"pg_dump failed: {stderr}", None

        size = filepath.stat().st_size
        size_human = f"{size / 1024 / 1024:.2f} MB" if size > 1024 * 1024 else f"{size / 1024:.1f} KB"
        return True, f"Backup created: {filename} ({size_human})", filepath

    except FileNotFoundError:
        return False, f"pg_dump not found at '{pg_dump}'. Install PostgreSQL client tools or check Postgres.app installation.", None
    except Exception as e:
        filepath.unlink(missing_ok=True)
        return False, f"Backup failed: {str(e)}", None
    finally:
        if pgpass_path:
            pgpass_path.unlink(missing_ok=True)


def list_backups() -> list[dict]:
    """List all available backups"""
    if not BACKUP_DIR.exists():
        return []

    backups = []
    for f in sorted(BACKUP_DIR.glob("*.sql.gz"), reverse=True):
        stat = f.stat()
        backups.append({
            "filename": f.name,
            "size_bytes": stat.st_size,
            "size_human": f"{stat.st_size / 1024 / 1024:.2f} MB" if stat.st_size > 1024 * 1024 else f"{stat.st_size / 1024:.1f} KB",
            "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })

    return backups


def get_backup_path(filename: str) -> Path | None:
    """Get full path to a backup file (for download)"""
    # Prevent directory traversal
    if Path(filename).name != filename:
        return None
    filepath = BACKUP_DIR / filename
    # Ensure resolved path stays within BACKUP_DIR
    try:
        filepath.resolve().relative_to(BACKUP_DIR.resolve())
    except ValueError:
        return None
    if filepath.exists() and filepath.is_file():
        return filepath
    return None


def delete_backup(filename: str) -> tuple[bool, str]:
    """Delete a backup file.

    Args:
        filename: Name of the backup file (e.g. 'mydb_2026-01-26_120000.sql.gz')

    Returns:
        (success, message) tuple
    """
    # Prevent directory traversal
    if Path(filename).name != filename:
        return False, "Invalid filename"

    filepath = BACKUP_DIR / filename
    # Ensure resolved path stays within BACKUP_DIR
    try:
        filepath.resolve().relative_to(BACKUP_DIR.resolve())
    except ValueError:
        return False, "Invalid filename"
    if not filepath.exists():
        return False, f"Backup not found: {filename}"

    if not filepath.suffix == ".gz" or not filepath.name.endswith(".sql.gz"):
        return False, "Not a valid backup file"

    try:
        filepath.unlink()
        log.info("Backup deleted", filename=filename)
        return True, f"Deleted {filename}"
    except Exception as e:
        log.error("Failed to delete backup", filename=filename, error=str(e))
        return False, f"Failed to delete: {str(e)}"


def restore_backup(config: ConnectionConfig, filename: str) -> tuple[bool, str]:
    """Restore a database from backup

    WARNING: This will overwrite the target database!
    """
    filepath = get_backup_path(filename)
    if not filepath:
        return False, f"Backup file not found: {filename}"

    psql = get_psql_path()

    cmd = [
        psql,
        "-h", config.host or "localhost",
        "-p", str(config.port),
        "-U", config.user or "postgres",
        "-d", config.database or "postgres",
    ]

    env, pgpass_path = _pg_env(config)

    try:
        # gunzip < file | psql
        with open(filepath, "rb") as f:
            gunzip_proc = subprocess.Popen(
                ["gunzip", "-c"],
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            psql_proc = subprocess.Popen(
                cmd,
                stdin=gunzip_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )

            gunzip_proc.stdout.close()
            stdout, stderr = psql_proc.communicate()

            if psql_proc.returncode != 0:
                return False, f"Restore failed: {stderr.decode()}"

        return True, f"Database restored from {filename}"

    except FileNotFoundError:
        return False, f"psql not found at '{psql}'. Install PostgreSQL client tools or check Postgres.app installation."
    except Exception as e:
        return False, f"Restore failed: {str(e)}"
    finally:
        if pgpass_path:
            pgpass_path.unlink(missing_ok=True)


def get_createdb_path() -> str:
    """Get the path to createdb binary"""
    return _find_pg_binary("createdb")


def create_database(config: ConnectionConfig, db_name: str, owner: str | None = None) -> tuple[bool, str]:
    """Create a new database on the PostgreSQL server

    Args:
        config: Connection config (uses host, port, user, password)
        db_name: Name for the new database
        owner: Optional owner for the database
    """
    createdb = get_createdb_path()

    cmd = [
        createdb,
        "-h", config.host or "localhost",
        "-p", str(config.port),
        "-U", config.user or "postgres",
    ]

    if owner:
        cmd.extend(["-O", owner])

    cmd.append(db_name)

    env, pgpass_path = _pg_env(config)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env
        )

        if result.returncode != 0:
            return False, f"Create database failed: {result.stderr}"

        return True, f"Database '{db_name}' created successfully"

    except FileNotFoundError:
        return False, f"createdb not found at '{createdb}'. Install PostgreSQL client tools."
    except Exception as e:
        return False, f"Create database failed: {str(e)}"
    finally:
        if pgpass_path:
            pgpass_path.unlink(missing_ok=True)


def create_database_from_backup(
    config: ConnectionConfig,
    filename: str,
    new_db_name: str,
    owner: str | None = None
) -> tuple[bool, str]:
    """Create a new database and restore from backup

    Args:
        config: Connection config for the PostgreSQL server
        filename: Backup file to restore from
        new_db_name: Name for the new database
        owner: Optional owner for the new database
    """
    # First create the new database
    success, message = create_database(config, new_db_name, owner)
    if not success:
        return False, message

    # Create a modified config pointing to the new database
    new_config = ConnectionConfig(
        id=config.id,
        name=config.name,
        type=config.type,
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=new_db_name,
    )

    # Restore the backup to the new database
    success, message = restore_backup(new_config, filename)
    if not success:
        return False, f"Database created but restore failed: {message}"

    return True, f"Database '{new_db_name}' created and restored from {filename}"
