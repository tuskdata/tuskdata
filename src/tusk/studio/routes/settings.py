"""Settings API routes"""

from pathlib import Path
from litestar import Controller, get, post
from litestar.params import Body

from tusk.core.config import get_config, update_config
from tusk.admin.backup import get_pg_dump_path, get_psql_path


class SettingsController(Controller):
    """Settings API"""

    path = "/api/settings"

    @get("/")
    async def get_settings(self) -> dict:
        """Get current settings"""
        config = get_config()

        return {
            "pg_bin_path": config.pg_bin_path,
            "pg_bin_path_detected": {
                "pg_dump": get_pg_dump_path(),
                "psql": get_psql_path(),
            },
            "server": {
                "host": config.host,
                "port": config.port,
            },
            "ui": {
                "theme": config.theme,
                "editor_font_size": config.editor_font_size,
            },
        }

    @post("/pg-bin-path")
    async def set_pg_bin_path(self, data: dict = Body()) -> dict:
        """Set PostgreSQL binaries path"""
        path = data.get("path", "").strip()

        if path:
            # Validate the path
            pg_dump = Path(path) / "pg_dump"
            if not pg_dump.exists():
                return {
                    "success": False,
                    "error": f"pg_dump not found at {pg_dump}",
                }

            update_config(pg_bin_path=path)
            return {
                "success": True,
                "message": f"PostgreSQL binaries path set to: {path}",
                "pg_dump": str(pg_dump),
            }
        else:
            # Clear custom path (use auto-detect)
            update_config(pg_bin_path=None)
            return {
                "success": True,
                "message": "Using auto-detected PostgreSQL binaries",
                "pg_dump": get_pg_dump_path(),
            }

    @get("/pg-bin-path/detect")
    async def detect_pg_paths(self) -> dict:
        """Detect available PostgreSQL binary paths"""
        from tusk.admin.backup import _get_pg_bin_search_paths

        import asyncio
        import subprocess

        def _probe_version(pg_dump_path: str) -> str:
            """Sync subprocess wrapped for asyncio.to_thread — running
            subprocess.run() directly in an async body blocks the event
            loop for up to `timeout` seconds. Caught by ASYNC221."""
            try:
                result = subprocess.run(
                    [pg_dump_path, "--version"],
                    capture_output=True, text=True, timeout=5,
                )
                return result.stdout.strip() if result.returncode == 0 else "unknown"
            except Exception:
                return "unknown"

        available = []
        for search_path in _get_pg_bin_search_paths():
            pg_dump = search_path / "pg_dump"
            if pg_dump.exists():
                version = await asyncio.to_thread(_probe_version, str(pg_dump))
                available.append({
                    "path": str(search_path),
                    "version": version,
                })

        return {
            "current": get_config().pg_bin_path,
            "detected": get_pg_dump_path(),
            "available": available,
        }
