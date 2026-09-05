"""Backup and restore functionality for PostgreSQL"""

import subprocess
import os
import tempfile
import stat
from pathlib import Path
from datetime import datetime, timezone

import structlog
from tusk.core.connection import ConnectionConfig, TUSK_DIR
from tusk.core.config import get_config

log = structlog.get_logger("backup")

BACKUP_DIR = TUSK_DIR / "backups"


def _resolve_tunnel(config: ConnectionConfig) -> tuple[str, int]:
    """For SSH-tunneled connections, return the local forward
    (`127.0.0.1`, local_port) that pg_dump / psql / pg_restore should
    connect to. For direct connections, return `(config.host,
    config.port)`. Used by every backup/restore call site so the
    binary tools work the same way regardless of deployment topology
    (Coolify-with-bastion vs local-postgres).

    The function is sync but `get_tunneled_dsn` is async; we bridge
    via `asyncio.run` if we're not already in a loop, or via a worker
    thread if we are. Either way the SSH tunnel registry is
    process-global so the resolved local port is valid for the
    pg_dump child process.
    """
    if not config.uses_ssh_tunnel:
        return (config.host or "localhost"), config.port

    import asyncio
    from urllib.parse import urlparse
    from tusk.core.ssh_tunnel import get_tunneled_dsn

    try:
        asyncio.get_running_loop()
        # Inside a loop already → spin a worker thread to avoid
        # "cannot run nested event loop".
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as ex:
            future = ex.submit(asyncio.run, get_tunneled_dsn(config))
            tunneled_dsn = future.result(timeout=30)
    except RuntimeError:
        tunneled_dsn = asyncio.run(get_tunneled_dsn(config))

    parsed = urlparse(tunneled_dsn)
    return (parsed.hostname or "127.0.0.1"), (parsed.port or config.port)


def _pg_env(
    config: ConnectionConfig,
    *,
    effective_host: str | None = None,
    effective_port: int | None = None,
) -> tuple[dict, Path | None]:
    """Create environment for pg_dump/psql with secure password handling.

    Uses a temporary .pgpass file instead of PGPASSWORD env var.
    Returns (env_dict, pgpass_path_or_None).
    Caller must delete pgpass_path when done.

    `effective_host` / `effective_port` override `config.host` / `config.port`
    in the pgpass entry. Used by the SSH-tunneled backup path so the
    pgpass record matches the local-forward address pg_dump actually
    connects to (`127.0.0.1:<localport>`), not the remote bastion-side
    address. Without this override, pg_dump on a tunneled connection
    fails with "no password supplied" because the pgpass file says one
    host and the connection ends up on another.
    """
    env = os.environ.copy()
    pgpass_path = None

    if config.password:
        # Create temp .pgpass file with restrictive permissions
        fd, pgpass_path = tempfile.mkstemp(prefix="tusk_pgpass_")
        pgpass_file = Path(pgpass_path)
        # Escape colons and backslashes in pgpass fields
        host_value = effective_host if effective_host else (config.host or "localhost")
        port_value = effective_port if effective_port else config.port
        host = host_value.replace("\\", "\\\\").replace(":", "\\:")
        port = str(port_value)
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


_VALID_FORMATS = ("plain", "custom", "directory")
_TABLE_NAME_RE = __import__("re").compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _format_to_extension(fmt: str) -> str:
    return {
        "plain": "sql.gz",
        "custom": "dump",
        "directory": "tar.gz",
    }[fmt]


def create_backup(
    config: ConnectionConfig,
    *,
    format: str = "plain",
    tables: list[str] | None = None,
    progress_path: Path | None = None,
    effective_host: str | None = None,
    effective_port: int | None = None,
    backup_dir: Path | str | None = None,
) -> tuple[bool, str, Path | None]:
    """Create a pg_dump backup of the database.

    Args:
        config: connection config
        format: pg_dump format — `plain` (gzipped SQL, default), `custom`
            (-Fc, single binary file, restored with pg_restore), or
            `directory` (-Fd, archived as tar.gz).
        tables: optional list of unqualified or schema-qualified table names
            to include (pg_dump `-t`). When provided, only these tables are
            dumped.
        progress_path: optional path to write progress messages into. Each
            line is one phase (e.g. `dumping`, `compressing`, `done`).
            The UI polls this file while the backup runs.
        backup_dir: carpeta de destino. Por defecto `~/.tusk/backups`; los
            backups programados pueden apuntar a otro sitio (un volumen
            montado, un NFS). Se crea si no existe.

    Returns: (success, message, filepath)
    """
    if format not in _VALID_FORMATS:
        return False, f"Invalid format: {format}", None
    out_dir = Path(backup_dir).expanduser() if backup_dir else BACKUP_DIR
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return False, f"Cannot create backup directory {out_dir}: {e}", None

    # Validate table names — pg_dump accepts patterns but we only allow
    # safe identifiers to avoid argv injection of `--option` flags.
    if tables:
        for t in tables:
            if not _TABLE_NAME_RE.match(t):
                return False, f"Invalid table name: {t}", None

    def _progress(msg: str) -> None:
        if progress_path:
            try:
                with open(progress_path, "a") as f:
                    f.write(msg + "\n")
            except OSError:
                pass

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
    ext = _format_to_extension(format)
    filename = f"{config.database}_{timestamp}.{ext}"
    filepath = out_dir / filename

    pg_dump = get_pg_dump_path()

    # If the caller (route handler) pre-resolved the tunnel, use those
    # values — that's the cross-loop-safe path. The internal
    # `_resolve_tunnel` fallback is for sync callers like
    # scheduler-driven backups that aren't already in an event loop.
    if effective_host is None or effective_port is None:
        try:
            effective_host, effective_port = _resolve_tunnel(config)
            if config.uses_ssh_tunnel:
                _progress(f"tunnel ready at {effective_host}:{effective_port}")
        except Exception as e:
            return False, f"SSH tunnel setup failed: {e}", None
    elif config.uses_ssh_tunnel:
        _progress(f"tunnel ready at {effective_host}:{effective_port} (pre-resolved)")

    cmd = [
        pg_dump,
        "-h", effective_host,
        "-p", str(effective_port),
        "-U", config.user or "postgres",
        "-d", config.database or "postgres",
    ]

    if format == "plain":
        cmd.append("--format=plain")
    elif format == "custom":
        cmd.append("--format=custom")
    elif format == "directory":
        cmd.append("--format=directory")

    if tables:
        for t in tables:
            cmd.extend(["-t", t])

    env, pgpass_path = _pg_env(config, effective_host=effective_host, effective_port=effective_port)
    _progress("dumping")

    tmp_dir: Path | None = None
    try:
        if format == "plain":
            # pg_dump | gzip > file
            with open(filepath, "wb") as f:
                dump_proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                )
                gzip_proc = subprocess.Popen(
                    ["gzip"],
                    stdin=dump_proc.stdout,
                    stdout=f,
                    stderr=subprocess.PIPE,
                )
                dump_proc.stdout.close()
                _, gzip_stderr = gzip_proc.communicate()
                dump_proc.wait()
                if dump_proc.returncode != 0:
                    stderr = dump_proc.stderr.read().decode() if dump_proc.stderr else "Unknown error"
                    filepath.unlink(missing_ok=True)
                    return False, f"pg_dump failed: {stderr}", None
                if gzip_proc.returncode != 0:
                    err = gzip_stderr.decode() if gzip_stderr else "gzip failed"
                    filepath.unlink(missing_ok=True)
                    return False, f"gzip failed: {err}", None
        elif format == "custom":
            # -Fc writes directly to a file via -f
            cmd.extend(["-f", str(filepath)])
            result = subprocess.run(cmd, capture_output=True, env=env)
            if result.returncode != 0:
                filepath.unlink(missing_ok=True)
                return False, f"pg_dump failed: {result.stderr.decode()}", None
        else:  # directory
            tmp_dir = out_dir / f".{filename}.staging"
            cmd.extend(["-f", str(tmp_dir)])
            result = subprocess.run(cmd, capture_output=True, env=env)
            if result.returncode != 0:
                import shutil as _shutil
                if tmp_dir.exists():
                    _shutil.rmtree(tmp_dir, ignore_errors=True)
                return False, f"pg_dump failed: {result.stderr.decode()}", None
            _progress("archiving")
            # Tar+gzip the staging directory into our final filepath.
            import tarfile
            with tarfile.open(filepath, "w:gz") as tar:
                tar.add(tmp_dir, arcname=tmp_dir.name)
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)
            tmp_dir = None

        size = filepath.stat().st_size
        # Even a totally empty database produces several hundred bytes
        # (SET search_path, encoding, role grants, etc). A sub-100-byte
        # gzipped output means pg_dump returned success but never
        # actually wrote anything to stdout — usually a silent client
        # version / protocol mismatch. Treat as failure so we don't
        # leave a misleading "verified" file in the backups dir.
        if size < 100:
            filepath.unlink(missing_ok=True)
            return False, (
                f"pg_dump returned success but produced an empty file "
                f"({size} bytes). Check that pg_dump's version matches the "
                f"server's PostgreSQL version."
            ), None
        size_human = f"{size / 1024 / 1024:.2f} MB" if size > 1024 * 1024 else f"{size / 1024:.1f} KB"

        _progress("hashing")
        _write_backup_metadata(filepath, config, size, fmt=format, tables=tables)
        _progress("done")

        return True, f"Backup created: {filename} ({size_human})", filepath

    except FileNotFoundError:
        return False, f"pg_dump not found at '{pg_dump}'. Install PostgreSQL client tools or check Postgres.app installation.", None
    except Exception as e:
        filepath.unlink(missing_ok=True)
        if tmp_dir and tmp_dir.exists():
            import shutil as _shutil
            _shutil.rmtree(tmp_dir, ignore_errors=True)
        return False, f"Backup failed: {str(e)}", None
    finally:
        if pgpass_path:
            pgpass_path.unlink(missing_ok=True)


def _write_backup_metadata(
    filepath: Path,
    config: ConnectionConfig,
    size: int,
    *,
    fmt: str = "plain",
    tables: list[str] | None = None,
) -> None:
    """Write a <backup>.meta.json alongside the backup file."""
    import hashlib
    import json

    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(chunk)

    meta = {
        "filename": filepath.name,
        "size_bytes": size,
        "sha256": sha.hexdigest(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "database": config.database,
        "host": config.host,
        "port": config.port,
        "tusk_version": _tusk_version(),
        "format": fmt,
        "tables": tables or [],
    }

    meta_path = filepath.with_suffix(filepath.suffix + ".meta.json")
    try:
        meta_path.write_text(json.dumps(meta, indent=2))
    except OSError:
        pass  # metadata is best-effort


def _tusk_version() -> str:
    try:
        import tusk
        return tusk.__version__
    except Exception:
        return "unknown"


def _read_backup_metadata(filepath: Path) -> dict | None:
    import json
    meta_path = filepath.with_suffix(filepath.suffix + ".meta.json")
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _is_backup_file(name: str) -> bool:
    return name.endswith(".sql.gz") or name.endswith(".dump") or name.endswith(".tar.gz")


def prune_backups(
    database: str,
    keep_last: int,
    backup_dir: Path | str | None = None,
) -> list[str]:
    """Borra los backups más antiguos de `database`, conservando los
    `keep_last` más recientes (y sus sidecars `.meta.json`).

    Pensado para el scheduler: sin esto un backup diario llena el volumen
    hasta que revienta. Devuelve los nombres borrados. `keep_last <= 0`
    significa "no rotar".
    """
    if keep_last <= 0:
        return []
    out_dir = Path(backup_dir).expanduser() if backup_dir else BACKUP_DIR
    if not out_dir.exists():
        return []
    prefix = f"{database}_"
    candidates = [
        f for f in out_dir.iterdir()
        if f.is_file() and f.name.startswith(prefix) and _is_backup_file(f.name)
    ]
    candidates.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    removed: list[str] = []
    for f in candidates[keep_last:]:
        try:
            f.unlink()
            f.with_suffix(f.suffix + ".meta.json").unlink(missing_ok=True)
            removed.append(f.name)
        except OSError as e:
            log.warning("backup_prune_failed", file=f.name, error=str(e))
    if removed:
        log.info("backup_pruned", database=database, kept=keep_last, removed=len(removed))
    return removed


def list_backups() -> list[dict]:
    """List all available backups. Prefers sidecar metadata when present."""
    if not BACKUP_DIR.exists():
        return []

    backups = []
    candidates: list[Path] = []
    for pattern in ("*.sql.gz", "*.dump", "*.tar.gz"):
        candidates.extend(BACKUP_DIR.glob(pattern))
    seen: set[str] = set()
    for f in sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True):
        if f.name in seen or not _is_backup_file(f.name):
            continue
        seen.add(f.name)
        stat = f.stat()
        meta = _read_backup_metadata(f)
        entry = {
            "filename": f.name,
            "size_bytes": stat.st_size,
            "size_human": f"{stat.st_size / 1024 / 1024:.2f} MB" if stat.st_size > 1024 * 1024 else f"{stat.st_size / 1024:.1f} KB",
            "created": (meta or {}).get("created_at") or datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": (meta or {}).get("sha256"),
            "database": (meta or {}).get("database"),
            "host": (meta or {}).get("host"),
            "tusk_version": (meta or {}).get("tusk_version"),
            "format": (meta or {}).get("format") or _format_from_filename(f.name),
            "tables": (meta or {}).get("tables") or [],
            "metadata_present": meta is not None,
        }
        backups.append(entry)

    return backups


def _format_from_filename(name: str) -> str:
    if name.endswith(".sql.gz"):
        return "plain"
    if name.endswith(".dump"):
        return "custom"
    if name.endswith(".tar.gz"):
        return "directory"
    return "plain"


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

    if not _is_backup_file(filepath.name):
        return False, "Not a valid backup file"

    # Clean up sidecar metadata alongside the backup.
    meta_path = filepath.with_suffix(filepath.suffix + ".meta.json")
    meta_path.unlink(missing_ok=True)

    try:
        filepath.unlink()
        log.info("Backup deleted", filename=filename)
        return True, f"Deleted {filename}"
    except Exception as e:
        log.error("Failed to delete backup", filename=filename, error=str(e))
        return False, f"Failed to delete: {str(e)}"


def restore_backup(config: ConnectionConfig, filename: str) -> tuple[bool, str]:
    """Restore a database from backup. Supports plain (.sql.gz),
    custom (-Fc, .dump) and directory (-Fd, archived as .tar.gz) formats.

    WARNING: This will overwrite the target database!
    """
    filepath = get_backup_path(filename)
    if not filepath:
        return False, f"Backup file not found: {filename}"

    fmt = _format_from_filename(filename)
    try:
        effective_host, effective_port = _resolve_tunnel(config)
    except Exception as e:
        return False, f"SSH tunnel setup failed: {e}"
    env, pgpass_path = _pg_env(config, effective_host=effective_host, effective_port=effective_port)

    try:
        if fmt == "plain":
            psql = get_psql_path()
            cmd = [
                psql,
                "-h", effective_host,
                "-p", str(effective_port),
                "-U", config.user or "postgres",
                "-d", config.database or "postgres",
            ]
            with open(filepath, "rb") as f:
                gunzip_proc = subprocess.Popen(
                    ["gunzip", "-c"],
                    stdin=f,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                psql_proc = subprocess.Popen(
                    cmd,
                    stdin=gunzip_proc.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                )
                gunzip_proc.stdout.close()
                stdout, stderr = psql_proc.communicate()
                if psql_proc.returncode != 0:
                    return False, f"Restore failed: {stderr.decode()}"
            return True, f"Database restored from {filename}"

        # custom / directory: pg_restore
        pg_restore = _find_pg_binary("pg_restore")
        cmd = [
            pg_restore,
            "-h", effective_host,
            "-p", str(effective_port),
            "-U", config.user or "postgres",
            "-d", config.database or "postgres",
            "--no-owner",
            "--no-privileges",
        ]

        if fmt == "custom":
            cmd.append(str(filepath))
            result = subprocess.run(cmd, capture_output=True, env=env)
            if result.returncode != 0:
                return False, f"Restore failed: {result.stderr.decode()}"
            return True, f"Database restored from {filename}"

        # directory format: extract tar.gz to temp dir then pg_restore -Fd
        import tempfile as _tempfile
        import tarfile as _tarfile
        import shutil as _shutil
        with _tempfile.TemporaryDirectory(prefix="tusk_restore_") as td:
            with _tarfile.open(filepath, "r:gz") as tar:
                tar.extractall(td)
            # The tar contains exactly one top-level dir
            entries = [Path(td) / p for p in os.listdir(td)]
            if len(entries) != 1 or not entries[0].is_dir():
                return False, "Invalid directory backup archive"
            cmd.extend(["-F", "d", str(entries[0])])
            result = subprocess.run(cmd, capture_output=True, env=env)
            if result.returncode != 0:
                return False, f"Restore failed: {result.stderr.decode()}"
            return True, f"Database restored from {filename}"

    except FileNotFoundError as e:
        return False, f"PostgreSQL client tool not found: {e}"
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
    try:
        effective_host, effective_port = _resolve_tunnel(config)
    except Exception as e:
        return False, f"SSH tunnel setup failed: {e}"

    cmd = [
        createdb,
        "-h", effective_host,
        "-p", str(effective_port),
        "-U", config.user or "postgres",
    ]

    if owner:
        cmd.extend(["-O", owner])

    cmd.append(db_name)

    env, pgpass_path = _pg_env(config, effective_host=effective_host, effective_port=effective_port)

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
