"""API routes for Download Manager"""

import uuid
from litestar import Controller, Request, get, post, put, delete
from litestar.params import Body
from litestar.response import Template, Response

from tusk.core.logging import get_logger
from tusk.core.downloads import (
    DownloadSource,
    StorageBackend,
    get_sources,
    get_source,
    add_source,
    update_source,
    delete_source,
    get_backends,
    add_backend,
    delete_backend,
    get_runs,
    download_file,
    test_ssh_backend,
)
from tusk.studio.htmx import is_htmx, htmx_toast

log = get_logger("downloads_api")


class DownloadsController(Controller):
    """REST API for Download Manager"""

    path = "/api/downloads"

    # ===== Sources CRUD =====

    @get("/sources")
    async def list_sources(self, request: Request) -> list[dict] | Template:
        """List all download sources."""
        sources = get_sources()
        source_list = [_source_to_dict(s) for s in sources]

        if is_htmx(request):
            return Template(
                "partials/data/downloads.html",
                context={"sources": source_list},
            )
        return source_list

    @post("/sources")
    async def create_source(self, request: Request, data: dict = Body()) -> dict | Response:
        """Create a new download source."""
        source = DownloadSource(
            id=uuid.uuid4().hex[:12],
            name=data.get("name", ""),
            url=data.get("url", ""),
            category=data.get("category", "custom"),
            schedule=data.get("schedule", ""),
            format=data.get("format", "auto"),
            encoding=data.get("encoding", "utf-8"),
            convert_to_parquet=data.get("convert_to_parquet", False),
            post_download_hook=data.get("post_download_hook", ""),
            storage_backend=data.get("storage_backend", ""),
            enabled=data.get("enabled", True),
            description=data.get("description", ""),
        )

        if not source.name or not source.url:
            if is_htmx(request):
                return Response(
                    content="",
                    headers=htmx_toast("Name and URL are required", "error"),
                    status_code=422,
                )
            return {"error": "Name and URL are required"}

        add_source(source)
        log.info("Download source created", name=source.name)

        if is_htmx(request):
            sources = [_source_to_dict(s) for s in get_sources()]
            return Template(
                "partials/data/downloads.html",
                context={"sources": sources},
                headers=htmx_toast(f"Source '{source.name}' created", "success"),
            )
        return _source_to_dict(source)

    @put("/sources/{source_id:str}")
    async def update_src(self, request: Request, source_id: str, data: dict = Body()) -> dict | Response:
        """Update a download source."""
        updated = update_source(source_id, **data)
        if not updated:
            if is_htmx(request):
                return Response(
                    content="", headers=htmx_toast("Source not found", "error"), status_code=404,
                )
            return {"error": "Source not found"}

        if is_htmx(request):
            sources = [_source_to_dict(s) for s in get_sources()]
            return Template(
                "partials/data/downloads.html",
                context={"sources": sources},
                headers=htmx_toast(f"Source '{updated.name}' updated", "success"),
            )
        return _source_to_dict(updated)

    @delete("/sources/{source_id:str}", status_code=200)
    async def delete_src(self, request: Request, source_id: str) -> dict | Response:
        """Delete a download source."""
        source = get_source(source_id)
        name = source.name if source else source_id

        if delete_source(source_id):
            if is_htmx(request):
                sources = [_source_to_dict(s) for s in get_sources()]
                return Template(
                    "partials/data/downloads.html",
                    context={"sources": sources},
                    headers=htmx_toast(f"Source '{name}' deleted", "success"),
                )
            return {"deleted": True}

        if is_htmx(request):
            return Response(content="", headers=htmx_toast("Source not found", "error"), status_code=404)
        return {"error": "Source not found"}

    # ===== Trigger Download =====

    @post("/sources/{source_id:str}/download")
    async def trigger_download(self, request: Request, source_id: str) -> dict | Response:
        """Trigger a manual download."""
        source = get_source(source_id)
        if not source:
            if is_htmx(request):
                return Response(content="", headers=htmx_toast("Source not found", "error"), status_code=404)
            return {"error": "Source not found"}

        try:
            run = await download_file(source)

            if is_htmx(request):
                variant = "success" if run.status == "completed" else (
                    "info" if run.status == "skipped" else "error"
                )
                msg = {
                    "completed": f"Downloaded {run.bytes_downloaded:,} bytes",
                    "skipped": "File unchanged (ETag match)",
                    "failed": f"Failed: {run.error}",
                }.get(run.status, run.status)

                sources = [_source_to_dict(s) for s in get_sources()]
                return Template(
                    "partials/data/downloads.html",
                    context={"sources": sources},
                    headers=htmx_toast(msg, variant),
                )

            return {
                "run_id": run.id,
                "status": run.status,
                "bytes": run.bytes_downloaded,
                "output": run.output_path,
                "error": run.error,
            }

        except Exception as e:
            log.error("Download trigger failed", source=source.name, error=str(e))
            if is_htmx(request):
                return Response(content="", headers=htmx_toast(str(e), "error"), status_code=500)
            return {"error": str(e)}

    # ===== Download History =====

    @get("/runs")
    async def list_runs(self, source_id: str = "", limit: int = 50) -> list[dict]:
        """Get download run history."""
        runs = get_runs(source_id or None, limit)
        return [
            {
                "id": r.id,
                "source_id": r.source_id,
                "started_at": r.started_at,
                "completed_at": r.completed_at,
                "status": r.status,
                "bytes_downloaded": r.bytes_downloaded,
                "output_path": r.output_path,
                "error": r.error,
                "trigger": r.trigger,
            }
            for r in runs
        ]

    # ===== Storage Backends =====

    @get("/backends")
    async def list_backends(self) -> list[dict]:
        """List storage backends."""
        return [
            {
                "id": b.id,
                "type": b.type,
                "path": b.path,
                "host": b.host,
                "port": b.port,
                "user": b.user,
            }
            for b in get_backends()
        ]

    @post("/backends")
    async def create_backend(self, data: dict = Body()) -> dict:
        """Create a new storage backend."""
        backend = StorageBackend(
            id=data.get("id", uuid.uuid4().hex[:8]),
            type=data.get("type", "local"),
            path=data.get("path", ""),
            host=data.get("host", ""),
            port=int(data.get("port", 22)),
            user=data.get("user", ""),
            key_path=data.get("key_path", ""),
        )
        add_backend(backend)
        return {"id": backend.id, "type": backend.type}

    @delete("/backends/{backend_id:str}", status_code=200)
    async def delete_be(self, backend_id: str) -> dict:
        """Delete a storage backend."""
        if delete_backend(backend_id):
            return {"deleted": True}
        return {"error": "Cannot delete this backend"}

    @post("/backends/{backend_id:str}/test")
    async def test_backend(self, backend_id: str) -> dict:
        """Test SSH connection to a storage backend."""
        backends = {b.id: b for b in get_backends()}
        backend = backends.get(backend_id)
        if not backend:
            return {"success": False, "error": "Backend not found"}
        if backend.type != "ssh":
            return {"success": False, "error": "Only SSH backends can be tested"}
        return await test_ssh_backend(backend)


# ============================================================================
# Helpers
# ============================================================================

def _source_to_dict(s: DownloadSource) -> dict:
    """Convert DownloadSource to serializable dict."""
    return {
        "id": s.id,
        "name": s.name,
        "url": s.url,
        "category": s.category,
        "schedule": s.schedule,
        "format": s.format,
        "convert_to_parquet": s.convert_to_parquet,
        "enabled": s.enabled,
        "description": s.description,
        "last_downloaded": s.last_downloaded,
        "last_size": s.last_size,
        "last_status": s.last_status,
    }
