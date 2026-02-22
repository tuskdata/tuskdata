"""Download Manager — programmatic file downloads with caching and scheduling.

Provides infrastructure for downloading files from URLs, with:
- ETag/Last-Modified caching (skip re-download if unchanged)
- Streaming for large files
- Auto-detection and decompression (zip, gz, tar)
- Optional conversion to Parquet
- Storage backends (local + SSH/SFTP)
- Post-download hooks for plugins
- APScheduler integration for cron-based scheduling
"""

from __future__ import annotations

import os
import uuid
import gzip
import shutil
import zipfile
import tarfile
from datetime import datetime
from pathlib import Path

import httpx
import msgspec
import structlog

log = structlog.get_logger("downloads")

TUSK_DIR = Path.home() / ".tusk"
DOWNLOADS_DIR = TUSK_DIR / "downloads"
SOURCES_FILE = TUSK_DIR / "download_sources.json"


# ============================================================================
# Models
# ============================================================================

class StorageBackend(msgspec.Struct):
    """Where to store downloaded files.

    type="local": save to local disk (default).
    type="ssh": save to remote VM via SFTP.
    """
    id: str = "local"
    type: str = "local"           # "local" | "ssh"
    path: str = ""                # Local: ~/.tusk/downloads/  SSH: /data/tusk/
    # SSH-only fields
    host: str = ""
    port: int = 22
    user: str = ""
    key_path: str = ""


class DownloadSource(msgspec.Struct):
    """A downloadable and schedulable data source."""
    id: str
    name: str
    url: str
    category: str = ""            # "geo", "government", "custom"
    schedule: str = ""            # Cron expression: "0 3 * * 0"
    format: str = "auto"          # "csv", "json", "xlsx", "zip", "pbf", "auto"
    encoding: str = "utf-8"
    convert_to_parquet: bool = False
    post_download_hook: str = ""  # Plugin hook ID, e.g. "intel:parse_dgii_rnc"
    storage_backend: str = ""     # ID of backend, "" = default local
    enabled: bool = True
    description: str = ""
    # Cache / tracking
    last_downloaded: str = ""
    last_etag: str = ""
    last_size: int = 0
    last_status: str = "never"    # "ok" | "failed" | "unchanged" | "never"


class DownloadRun(msgspec.Struct):
    """Record of a download execution."""
    id: str
    source_id: str
    started_at: str
    completed_at: str = ""
    status: str = "running"       # "running" | "completed" | "failed" | "skipped"
    bytes_downloaded: int = 0
    output_path: str = ""
    error: str = ""
    trigger: str = "manual"       # "manual" | "scheduled"


# ============================================================================
# Persistence (JSON file)
# ============================================================================

_sources: dict[str, DownloadSource] = {}
_backends: dict[str, StorageBackend] = {}
_runs: list[DownloadRun] = []


def _ensure_dirs():
    """Ensure download directories exist."""
    TUSK_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


def load_sources() -> dict[str, DownloadSource]:
    """Load download sources from JSON file."""
    global _sources, _backends
    if not SOURCES_FILE.exists():
        return _sources

    try:
        with open(SOURCES_FILE, "rb") as f:
            data = msgspec.json.decode(f.read())

        for s in data.get("sources", []):
            src = msgspec.convert(s, DownloadSource)
            _sources[src.id] = src

        for b in data.get("backends", []):
            backend = msgspec.convert(b, StorageBackend)
            _backends[backend.id] = backend

        log.info("Download sources loaded", count=len(_sources))
    except Exception as e:
        log.error("Failed to load download sources", error=str(e))

    return _sources


def save_sources():
    """Save download sources to JSON file."""
    _ensure_dirs()
    data = {
        "sources": [msgspec.to_builtins(s) for s in _sources.values()],
        "backends": [msgspec.to_builtins(b) for b in _backends.values()],
    }
    with open(SOURCES_FILE, "wb") as f:
        f.write(msgspec.json.encode(data))


def get_sources() -> list[DownloadSource]:
    """Get all download sources."""
    if not _sources:
        load_sources()
    return list(_sources.values())


def get_source(source_id: str) -> DownloadSource | None:
    """Get a download source by ID."""
    if not _sources:
        load_sources()
    return _sources.get(source_id)


def add_source(source: DownloadSource) -> DownloadSource:
    """Add a new download source."""
    _sources[source.id] = source
    save_sources()
    return source


def update_source(source_id: str, **kwargs) -> DownloadSource | None:
    """Update a download source."""
    if source_id not in _sources:
        return None
    old = _sources[source_id]
    # Build updated source
    data = msgspec.to_builtins(old)
    data.update(kwargs)
    updated = msgspec.convert(data, DownloadSource)
    _sources[source_id] = updated
    save_sources()
    return updated


def delete_source(source_id: str) -> bool:
    """Delete a download source."""
    if source_id in _sources:
        del _sources[source_id]
        save_sources()
        return True
    return False


def get_backends() -> list[StorageBackend]:
    """Get all storage backends."""
    if not _backends:
        load_sources()
    # Always include default local backend
    if "local" not in _backends:
        _backends["local"] = StorageBackend(
            id="local", type="local", path=str(DOWNLOADS_DIR)
        )
    return list(_backends.values())


def add_backend(backend: StorageBackend) -> StorageBackend:
    """Add a new storage backend."""
    _backends[backend.id] = backend
    save_sources()
    return backend


def delete_backend(backend_id: str) -> bool:
    """Delete a storage backend (can't delete 'local')."""
    if backend_id == "local":
        return False
    if backend_id in _backends:
        del _backends[backend_id]
        save_sources()
        return True
    return False


def get_runs(source_id: str | None = None, limit: int = 50) -> list[DownloadRun]:
    """Get download run history."""
    if source_id:
        runs = [r for r in _runs if r.source_id == source_id]
    else:
        runs = list(_runs)
    return sorted(runs, key=lambda r: r.started_at, reverse=True)[:limit]


def _save_run(run: DownloadRun):
    """Save a download run to history."""
    # Update existing or add new
    for i, r in enumerate(_runs):
        if r.id == run.id:
            _runs[i] = run
            return
    _runs.append(run)
    # Keep only last 200 runs
    if len(_runs) > 200:
        _runs[:] = _runs[-200:]


# ============================================================================
# Download Engine
# ============================================================================

def _now_iso() -> str:
    """Return current timestamp as ISO string."""
    return datetime.now().isoformat()


def _generate_id() -> str:
    """Generate a short unique ID."""
    return uuid.uuid4().hex[:12]


def _resolve_output_path(source: DownloadSource, backend: StorageBackend | None) -> Path:
    """Determine local output path for a download."""
    base_dir = DOWNLOADS_DIR
    if backend and backend.type == "local" and backend.path:
        base_dir = Path(backend.path).expanduser()
    base_dir.mkdir(parents=True, exist_ok=True)

    # Derive filename from URL
    url_path = source.url.rstrip("/").split("/")[-1].split("?")[0]
    if not url_path or url_path == "":
        url_path = f"{source.id}.download"

    return base_dir / url_path


def _decompress(file_path: Path) -> Path:
    """Decompress a file if it's compressed. Returns path to decompressed file."""
    suffix = file_path.suffix.lower()

    if suffix == ".gz" and not str(file_path).endswith(".tar.gz"):
        out_path = file_path.with_suffix("")
        with gzip.open(file_path, "rb") as f_in:
            with open(out_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        file_path.unlink()
        log.info("Decompressed gz", output=str(out_path))
        return out_path

    if suffix == ".zip":
        out_dir = file_path.parent / file_path.stem
        out_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(file_path, "r") as zf:
            zf.extractall(out_dir)
        file_path.unlink()
        # Return the directory or first file
        files = list(out_dir.iterdir())
        if len(files) == 1:
            return files[0]
        log.info("Decompressed zip", output=str(out_dir), files=len(files))
        return out_dir

    if str(file_path).endswith((".tar.gz", ".tgz")):
        out_dir = file_path.parent / file_path.stem.replace(".tar", "")
        out_dir.mkdir(exist_ok=True)
        with tarfile.open(file_path, "r:gz") as tf:
            tf.extractall(out_dir, filter="data")
        file_path.unlink()
        log.info("Decompressed tar.gz", output=str(out_dir))
        return out_dir

    return file_path


def _convert_to_parquet(file_path: Path, encoding: str, fmt: str) -> Path:
    """Convert a file to Parquet format using Polars."""
    try:
        import polars as pl

        if fmt == "auto":
            suffix = file_path.suffix.lower()
            fmt = {"csv": "csv", ".json": "json", ".xlsx": "xlsx"}.get(suffix, "csv")

        if fmt == "csv":
            df = pl.read_csv(file_path, encoding=encoding)
        elif fmt == "json":
            df = pl.read_json(file_path)
        else:
            log.warning("Cannot convert format to parquet", format=fmt)
            return file_path

        out_path = file_path.with_suffix(".parquet")
        df.write_parquet(out_path)
        log.info("Converted to parquet", rows=df.height, output=str(out_path))
        return out_path

    except Exception as e:
        log.error("Parquet conversion failed", error=str(e))
        return file_path


async def download_file(
    source: DownloadSource,
    storage: StorageBackend | None = None,
    on_progress: callable | None = None,
) -> DownloadRun:
    """Download a file with ETag caching, streaming, and post-processing.

    Args:
        source: The download source configuration
        storage: Optional storage backend override
        on_progress: Optional callback(downloaded_bytes, total_bytes)

    Returns:
        DownloadRun with status and output path
    """
    _ensure_dirs()

    run = DownloadRun(
        id=_generate_id(),
        source_id=source.id,
        started_at=_now_iso(),
    )
    _save_run(run)

    # Resolve storage backend
    if not storage and source.storage_backend:
        storage = _backends.get(source.storage_backend)

    headers = {}
    if source.last_etag:
        headers["If-None-Match"] = source.last_etag

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(300, connect=30),
        ) as client:
            async with client.stream("GET", source.url, headers=headers) as resp:
                if resp.status_code == 304:
                    run.status = "skipped"
                    run.completed_at = _now_iso()
                    source.last_status = "unchanged"
                    save_sources()
                    _save_run(run)
                    log.info("Download skipped (unchanged)", source=source.name)
                    return run

                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                output = _resolve_output_path(source, storage)
                downloaded = 0

                with open(output, "wb") as f:
                    async for chunk in resp.aiter_bytes(8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if on_progress and total:
                            on_progress(downloaded, total)

                source.last_etag = resp.headers.get("etag", "")

        # Post-processing pipeline
        if output.suffix in (".zip", ".gz") or str(output).endswith((".tar.gz", ".tgz")):
            output = _decompress(output)

        if source.convert_to_parquet and source.format != "pbf":
            output = _convert_to_parquet(output, source.encoding, source.format)

        if source.post_download_hook:
            from tusk.core.download_hooks import run_post_hook
            output = await run_post_hook(source.post_download_hook, output)

        if storage and storage.type == "ssh":
            await _upload_to_remote(output, storage)

        run.status = "completed"
        run.bytes_downloaded = downloaded
        run.output_path = str(output)

        source.last_downloaded = _now_iso()
        source.last_size = downloaded
        source.last_status = "ok"
        save_sources()

        log.info(
            "Download completed",
            source=source.name,
            bytes=downloaded,
            output=str(output),
        )

    except Exception as e:
        run.status = "failed"
        run.error = str(e)
        source.last_status = "failed"
        save_sources()
        log.error("Download failed", source=source.name, error=str(e))

    run.completed_at = _now_iso()
    _save_run(run)
    return run


# ============================================================================
# Remote Storage (SSH/SFTP)
# ============================================================================

async def _upload_to_remote(local_path: Path, storage: StorageBackend) -> str:
    """Copy downloaded file to remote VM via SFTP."""
    try:
        import asyncssh

        async with asyncssh.connect(
            host=storage.host,
            port=storage.port,
            username=storage.user,
            client_keys=[storage.key_path] if storage.key_path else None,
            known_hosts=None,
        ) as conn:
            async with conn.start_sftp_client() as sftp:
                remote_dir = storage.path or "/data/tusk/downloads"
                await sftp.makedirs(remote_dir, exist_ok=True)
                remote_path = f"{remote_dir}/{local_path.name}"
                await sftp.put(str(local_path), remote_path)
                log.info("Uploaded to remote", remote=remote_path)
                return remote_path

    except ImportError:
        log.error("asyncssh not installed — cannot upload to SSH backend")
        raise ValueError("asyncssh is required for SSH storage. Install with: pip install asyncssh")


async def test_ssh_backend(backend: StorageBackend) -> dict:
    """Test SSH connection to a storage backend."""
    try:
        import asyncssh

        async with asyncssh.connect(
            host=backend.host,
            port=backend.port,
            username=backend.user,
            client_keys=[backend.key_path] if backend.key_path else None,
            known_hosts=None,
        ) as conn:
            result = await conn.run("echo ok", check=True)
            return {"success": True, "message": f"Connected to {backend.host}"}

    except ImportError:
        return {"success": False, "error": "asyncssh not installed"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# ============================================================================
# Scheduler Integration
# ============================================================================

def schedule_downloads():
    """Register scheduled downloads with APScheduler.

    Called from app.py on_startup to set up cron jobs for sources
    that have a schedule configured.
    """
    try:
        from tusk.core.scheduler import get_scheduler
        import asyncio

        scheduler = get_scheduler()

        for source in get_sources():
            if not source.schedule or not source.enabled:
                continue

            job_id = f"download_{source.id}"

            # Parse cron expression (minute hour day month day_of_week)
            parts = source.schedule.split()
            if len(parts) != 5:
                log.warning("Invalid cron expression", source=source.name, cron=source.schedule)
                continue

            try:
                scheduler.add_job(
                    _run_scheduled_download,
                    "cron",
                    id=job_id,
                    replace_existing=True,
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4],
                    args=[source.id],
                )
                log.info("Scheduled download", source=source.name, cron=source.schedule)
            except Exception as e:
                log.error("Failed to schedule download", source=source.name, error=str(e))

    except ImportError:
        log.warning("APScheduler not available — scheduled downloads disabled")


def _run_scheduled_download(source_id: str):
    """Execute a scheduled download (called by APScheduler)."""
    import asyncio

    source = get_source(source_id)
    if not source or not source.enabled:
        return

    log.info("Running scheduled download", source=source.name)

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(download_file(source))
        else:
            asyncio.run(download_file(source))
    except Exception as e:
        log.error("Scheduled download failed", source=source.name, error=str(e))
