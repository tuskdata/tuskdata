"""File scanning and management for data files"""

import os
from pathlib import Path
from typing import Literal
import msgspec


def _allowed_roots() -> list[Path]:
    """Roots that user-supplied paths are allowed to resolve into.

    Always allowed: the user's home directory and ``/tmp``.

    Optional: if the ``TUSK_FILES_ROOT`` environment variable is set, that
    path becomes an additional allowed root. This lets a deployer (e.g. a
    Coolify or Docker mount) expose, say, ``/data`` to the app without
    widening the default allowlist.
    """
    roots = [Path.home().resolve(), Path("/tmp").resolve()]
    extra = os.environ.get("TUSK_FILES_ROOT")
    if extra:
        try:
            roots.append(Path(extra).expanduser().resolve())
        except Exception:
            pass
    return roots


def validate_user_path(p: str | Path) -> Path:
    """Resolve a user-supplied filesystem path and confirm it sits inside
    one of the allowed roots (home, /tmp, or ``$TUSK_FILES_ROOT``).

    Raises ``PermissionError`` if the resolved path escapes the allowlist or
    contains a non-whitelisted hidden component (preventing access to
    things like ``~/.ssh`` or ``~/.aws``).

    Used as a single chokepoint by every route that accepts a path from
    the client (browse, scan, file info/preview, folder add, etc.).
    """
    resolved = Path(p).expanduser().resolve()

    if not any(
        resolved == root or resolved.is_relative_to(root)
        for root in _allowed_roots()
    ):
        raise PermissionError("path outside allowed roots")

    # Block hidden components (defense in depth: keeps ~/.ssh, ~/.aws,
    # etc. out of reach even though they live under home).
    allowed_hidden = {".", "..", ".local", ".config", ".tusk"}
    for part in resolved.parts:
        if part.startswith(".") and part not in allowed_hidden:
            raise PermissionError(f"hidden path component not allowed: {part}")

    return resolved


class DataFile(msgspec.Struct):
    """Information about a data file"""
    path: str
    name: str
    extension: str
    size_bytes: int
    size_human: str
    file_type: Literal["parquet", "csv", "json", "sqlite", "tsv"]
    row_count: int | None = None
    tables: list[dict] | None = None  # For SQLite files


class DataFolder(msgspec.Struct):
    """A registered data folder"""
    path: str
    name: str
    files: list[DataFile]


# Supported extensions
SUPPORTED_EXTENSIONS = {
    ".parquet": "parquet",
    ".pq": "parquet",
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".jsonl": "json",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
    ".db": "sqlite",
}


def format_size(size_bytes: int) -> str:
    """Format file size in human readable format"""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes} B"


def format_rows(row_count: int | None) -> str:
    """Format row count in human readable format"""
    if row_count is None:
        return ""
    if row_count >= 1_000_000:
        return f"{row_count / 1_000_000:.1f}M rows"
    elif row_count >= 1_000:
        return f"{row_count / 1_000:.1f}K rows"
    else:
        return f"{row_count} rows"


def scan_directory(directory: str | Path) -> list[DataFile]:
    """Scan a directory for data files"""
    directory = Path(directory).expanduser().resolve()

    if not directory.exists():
        return []

    if not directory.is_dir():
        return []

    files = []

    try:
        for entry in directory.iterdir():
            if entry.is_file():
                ext = entry.suffix.lower()
                if ext in SUPPORTED_EXTENSIONS:
                    try:
                        stat = entry.stat()
                        files.append(DataFile(
                            path=str(entry),
                            name=entry.name,
                            extension=ext,
                            size_bytes=stat.st_size,
                            size_human=format_size(stat.st_size),
                            file_type=SUPPORTED_EXTENSIONS[ext],
                        ))
                    except OSError:
                        continue
    except PermissionError:
        pass

    # Sort by name
    files.sort(key=lambda f: f.name.lower())

    return files


def get_file_icon(file_type: str) -> str:
    """Get icon for file type"""
    icons = {
        "parquet": "📊",
        "csv": "📄",
        "tsv": "📄",
        "json": "📋",
        "sqlite": "🗃️",
    }
    return icons.get(file_type, "📁")


# Registered folders storage (in-memory, persisted to config)
_registered_folders: list[str] = []


def get_registered_folders() -> list[str]:
    """Get list of registered folder paths"""
    return _registered_folders.copy()


def add_folder(path: str) -> bool:
    """Add a folder to the registry"""
    expanded = str(Path(path).expanduser().resolve())
    if expanded not in _registered_folders:
        if Path(expanded).exists() and Path(expanded).is_dir():
            _registered_folders.append(expanded)
            return True
    return False


def remove_folder(path: str) -> bool:
    """Remove a folder from the registry"""
    expanded = str(Path(path).expanduser().resolve())
    if expanded in _registered_folders:
        _registered_folders.remove(expanded)
        return True
    return False


def load_folders_from_config(folders: list[str]):
    """Load folders from config"""
    global _registered_folders
    _registered_folders = [
        str(Path(f).expanduser().resolve())
        for f in folders
        if Path(f).expanduser().exists()
    ]
