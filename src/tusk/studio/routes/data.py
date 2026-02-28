"""API routes for Data/ETL with Polars"""

import hashlib
import uuid
import tempfile
import os
import asyncio
import json
from datetime import datetime
from pathlib import Path
from litestar import Controller, Request, get, post, put, delete
from litestar.params import Body
from litestar.response import File, Stream, Template
from litestar.datastructures import UploadFile
import msgspec
import structlog

from tusk.engines import polars_engine
from tusk.engines.polars_engine import (
    Pipeline, DataSource, Transform,
    FilterTransform, SelectTransform, RenameTransform, SortTransform,
    GroupByTransform, AddColumnTransform, DropNullsTransform,
    LimitTransform, JoinTransform, ConcatTransform, DistinctTransform, WindowTransform,
    generate_code, execute_pipeline, get_schema, preview_file as polars_preview_file, get_osm_layers,
    export_to_csv, export_to_parquet, import_to_duckdb, import_to_postgres
)
from tusk.engines.duckdb_engine import DuckDBEngine
from tusk.core.connection import list_connections
from tusk.studio.htmx import is_htmx

log = structlog.get_logger()

# ─── Parquet cache for CSV/JSON files ────────────────────────────
CACHE_DIR = Path.home() / ".tusk" / "cache"


def _cache_key(file_path: Path) -> str:
    """Build cache key from file path + mtime + size."""
    stat = file_path.stat()
    raw = f"{file_path.resolve()}:{stat.st_mtime_ns}:{stat.st_size}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _get_cache_path(file_path: Path) -> Path | None:
    """Return existing Parquet cache path, or None if not cached / stale."""
    if file_path.suffix.lower() == ".parquet":
        return None
    try:
        cache = CACHE_DIR / f"{_cache_key(file_path)}.parquet"
        return cache if cache.exists() else None
    except Exception:
        return None


def _build_cache(file_path: Path, file_type: str, engine: DuckDBEngine) -> Path | None:
    """Convert CSV/JSON to Parquet cache. Returns cache path on success."""
    from tusk.engines.duckdb_engine import _safe_path, _escape_duckdb_string
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache = CACHE_DIR / f"{_cache_key(file_path)}.parquet"
        if cache.exists():
            return cache

        safe_fp = _safe_path(str(file_path))
        safe_cache = _escape_duckdb_string(str(cache.resolve()))
        if file_type in ("csv", "tsv"):
            engine.conn.execute(
                f"COPY (SELECT * FROM read_csv_auto('{safe_fp}', max_line_size=20000000)) "
                f"TO '{safe_cache}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        elif file_type == "json":
            engine.conn.execute(
                f"COPY (SELECT * FROM read_json_auto('{safe_fp}', maximum_object_size=134217728)) "
                f"TO '{safe_cache}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        else:
            return None

        log.info("Parquet cache built", source=str(file_path), cache=str(cache),
                 size_mb=round(cache.stat().st_size / 1048576, 1))
        return cache
    except Exception as e:
        log.warning("Failed to build parquet cache", error=str(e))
        return None


def _validate_file_path(path: str) -> Path:
    """Validate that a file path is safe (no traversal attacks)"""
    p = Path(path).expanduser().resolve()

    # Allow home directory and subdirectories
    home = Path.home().resolve()
    # Allow /tmp
    tmp = Path("/tmp").resolve()

    if not (str(p).startswith(str(home)) or str(p).startswith(str(tmp))):
        raise ValueError(f"Access denied: path must be under home directory or /tmp")

    # Block hidden directories (except common ones)
    for part in p.parts:
        if part.startswith(".") and part not in {".", "..", ".local", ".config"}:
            raise ValueError(f"Access denied: hidden path component '{part}'")

    return p


# Shared DuckDB engine for previews
_duckdb_engine = DuckDBEngine()

# In-memory pipeline storage (could be persisted to SQLite later)
_pipelines: dict[str, Pipeline] = {}

# Transform decoder for parsing transforms from JSON
transform_decoder = msgspec.json.Decoder(Transform)


class DataController(Controller):
    """API for Data/ETL pipelines"""

    path = "/api/data"

    @get("/files/schema")
    async def get_file_schema(self, path: str, osm_layer: str | None = None, engine: str = "auto") -> dict:
        """Get schema of a data file"""
        try:
            p = _validate_file_path(path)
        except ValueError as e:
            return {"error": str(e)}

        suffix = p.suffix.lower()

        # DuckDB schema detection for standard files
        if engine == "duckdb" and suffix not in (".pbf",):
            file_type = {".csv": "csv", ".tsv": "tsv", ".parquet": "parquet", ".json": "json"}.get(suffix)
            if file_type:
                result = _duckdb_engine.preview_file(str(p), file_type, 1)
                if not result.error:
                    return {
                        "columns": [{"name": c.name, "type": c.type} for c in result.columns],
                        "engine_used": "duckdb",
                    }

        # Default: Polars schema (or auto for OSM)
        schema = get_schema(str(p), osm_layer)
        schema["engine_used"] = "polars"
        return schema

    @get("/files/preview")
    async def preview_data_file(self, path: str, limit: int = 100, osm_layer: str | None = None, engine: str = "auto") -> dict:
        """Preview contents of a data file using DuckDB, Polars, or auto-select"""
        import time
        start = time.perf_counter()

        try:
            p = _validate_file_path(path)
        except ValueError as e:
            return {"error": str(e)}
        suffix = p.suffix.lower()

        # OSM files always use Polars (DuckDB Spatial handles this differently)
        if suffix == ".pbf" or str(p).endswith(".osm.pbf"):
            result = polars_preview_file(str(p), limit, osm_layer)
            elapsed = round((time.perf_counter() - start) * 1000, 2)
            result["engine_used"] = "polars"
            result["elapsed_ms"] = elapsed
            return result

        file_type = {".csv": "csv", ".tsv": "tsv", ".parquet": "parquet", ".json": "json"}.get(suffix)
        if not file_type:
            return {"error": f"Unsupported file type: {suffix}"}

        # Engine selection: "polars" forces Polars, "duckdb" forces DuckDB, "auto" = DuckDB with Polars fallback
        use_polars = engine == "polars"
        use_duckdb = engine in ("auto", "duckdb")

        if use_polars:
            result = polars_preview_file(str(p), limit, osm_layer)
            elapsed = round((time.perf_counter() - start) * 1000, 2)
            result["engine_used"] = "polars"
            result["elapsed_ms"] = elapsed
            return result

        # Check Parquet cache for CSV/JSON files
        cached = _get_cache_path(p)
        cache_hit = cached is not None
        read_path = str(cached) if cached else str(p)
        read_type = "parquet" if cached else file_type

        # DuckDB path (auto or explicit duckdb)
        result = _duckdb_engine.preview_file(read_path, read_type, limit)
        elapsed = round((time.perf_counter() - start) * 1000, 2)

        if result.error:
            if engine == "duckdb":
                return {"error": result.error, "engine_used": "duckdb", "elapsed_ms": elapsed}
            log.warning("DuckDB preview failed, falling back to Polars", error=result.error)
            fallback = polars_preview_file(str(p), limit, osm_layer)
            elapsed = round((time.perf_counter() - start) * 1000, 2)
            fallback["engine_used"] = "polars"
            fallback["engine_fallback"] = True
            fallback["elapsed_ms"] = elapsed
            return fallback

        # Build cache in background for uncached CSV/JSON (< 500 MB)
        if not cache_hit and file_type in ("csv", "tsv", "json"):
            try:
                if p.stat().st_size < 500_000_000:
                    _build_cache(p, file_type, _duckdb_engine)
            except Exception:
                pass

        log.info("DuckDB preview", path=str(p), rows=len(result.rows), ms=elapsed,
                 cached=cache_hit, engine=engine)
        return {
            "columns": [{"name": c.name, "type": c.type} for c in result.columns],
            "rows": result.rows,
            "row_count": len(result.rows),
            "engine_used": "duckdb",
            "elapsed_ms": elapsed,
            "cached": cache_hit,
        }

    @get("/osm/layers")
    async def get_osm_file_layers(self, path: str) -> dict:
        """Get available layers in an OSM file"""
        try:
            p = _validate_file_path(path)
        except ValueError as e:
            return {"error": str(e)}
        return get_osm_layers(str(p))

    @get("/pipelines")
    async def list_pipelines(self) -> dict:
        """List all pipelines"""
        return {
            "pipelines": [
                {
                    "id": p.id,
                    "name": p.name,
                    "sources": len(p.sources),
                    "transforms": len(p.transforms),
                    "created_at": p.created_at,
                    "updated_at": p.updated_at
                }
                for p in _pipelines.values()
            ]
        }

    @get("/pipelines/{pipeline_id:str}")
    async def get_pipeline(self, pipeline_id: str) -> dict:
        """Get a pipeline by ID"""
        pipeline = _pipelines.get(pipeline_id)
        if not pipeline:
            return {"error": "Pipeline not found"}
        return msgspec.to_builtins(pipeline)

    @post("/pipelines")
    async def create_pipeline(self, data: dict = Body()) -> dict:
        """Create a new pipeline"""
        pipeline_id = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat()

        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        pipeline = Pipeline(
            id=pipeline_id,
            name=data.get("name", "Untitled Pipeline"),
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id if sources else ""),
            created_at=now,
            updated_at=now
        )

        _pipelines[pipeline_id] = pipeline

        return {"id": pipeline_id, "name": pipeline.name}

    @put("/pipelines/{pipeline_id:str}")
    async def update_pipeline(self, pipeline_id: str, data: dict = Body()) -> dict:
        """Update a pipeline"""
        pipeline = _pipelines.get(pipeline_id)
        if not pipeline:
            return {"error": "Pipeline not found"}

        now = datetime.now().isoformat()

        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        updated_pipeline = Pipeline(
            id=pipeline_id,
            name=data.get("name", pipeline.name),
            sources=sources if sources else pipeline.sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", pipeline.output_source_id),
            created_at=pipeline.created_at,
            updated_at=now
        )

        _pipelines[pipeline_id] = updated_pipeline

        return {"id": pipeline_id, "updated": True}

    @delete("/pipelines/{pipeline_id:str}", status_code=200)
    async def delete_pipeline(self, pipeline_id: str) -> dict:
        """Delete a pipeline"""
        if pipeline_id in _pipelines:
            del _pipelines[pipeline_id]
            return {"deleted": True}
        return {"error": "Pipeline not found"}

    @post("/pipelines/{pipeline_id:str}/execute")
    async def run_pipeline(self, pipeline_id: str, data: dict = Body()) -> dict:
        """Execute a pipeline and return results"""
        pipeline = _pipelines.get(pipeline_id)
        if not pipeline:
            return {"error": "Pipeline not found"}

        limit = data.get("limit", 100)
        return execute_pipeline(pipeline, limit)

    @post("/pipelines/{pipeline_id:str}/code")
    async def get_pipeline_code(self, pipeline_id: str) -> dict:
        """Generate Python/Polars code for a pipeline"""
        pipeline = _pipelines.get(pipeline_id)
        if not pipeline:
            return {"error": "Pipeline not found"}

        code = generate_code(pipeline)
        return {"code": code}

    @post("/execute")
    async def execute_inline(self, data: dict = Body()) -> dict:
        """Execute a pipeline inline without saving"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        pipeline = Pipeline(
            id="temp",
            name="Inline",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        limit = data.get("limit", 100)
        return execute_pipeline(pipeline, limit)

    @post("/code")
    async def generate_inline_code(self, data: dict = Body()) -> dict:
        """Generate code for inline pipeline"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        pipeline = Pipeline(
            id="temp",
            name="Inline",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        code = generate_code(pipeline)
        return {"code": code}

    @post("/export/csv")
    async def export_csv(self, data: dict = Body()) -> File | dict:
        """Export pipeline results to CSV and return as download"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        pipeline = Pipeline(
            id="temp",
            name="Export",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        # Create temp file for export
        filename = data.get("filename", "export.csv")
        if not filename.endswith(".csv"):
            filename += ".csv"

        temp_path = Path(tempfile.gettempdir()) / f"tusk_export_{uuid.uuid4().hex[:8]}.csv"
        result = export_to_csv(pipeline, str(temp_path), data.get("limit"))

        if "error" in result:
            return result

        return File(
            path=temp_path,
            filename=filename,
            media_type="text/csv",
        )

    @post("/export/parquet")
    async def export_parquet(self, data: dict = Body()) -> File | dict:
        """Export pipeline results to Parquet and return as download"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        pipeline = Pipeline(
            id="temp",
            name="Export",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        # Create temp file for export
        filename = data.get("filename", "export.parquet")
        if not filename.endswith(".parquet"):
            filename += ".parquet"

        temp_path = Path(tempfile.gettempdir()) / f"tusk_export_{uuid.uuid4().hex[:8]}.parquet"
        result = export_to_parquet(pipeline, str(temp_path), data.get("limit"))

        if "error" in result:
            return result

        return File(
            path=temp_path,
            filename=filename,
            media_type="application/octet-stream",
        )

    @post("/import/duckdb")
    async def import_duckdb(self, data: dict = Body()) -> dict:
        """Import pipeline results to DuckDB table"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        table_name = data.get("table_name", "imported_data")
        db_path = data.get("db_path")  # None = in-memory

        pipeline = Pipeline(
            id="temp",
            name="Import",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        return import_to_duckdb(pipeline, table_name, db_path, data.get("limit"))

    @post("/import/postgres")
    async def import_postgres(self, data: dict = Body()) -> dict:
        """Import pipeline results to PostgreSQL table"""
        # Parse sources
        sources = []
        for s in data.get("sources", []):
            sources.append(DataSource(
                id=s.get("id", str(uuid.uuid4())[:8]),
                name=s.get("name", "Unnamed"),
                source_type=s.get("source_type", "csv"),
                path=s.get("path"),
                connection_id=s.get("connection_id"),
                query=s.get("query"),
                osm_layer=s.get("osm_layer")
            ))

        # Parse transforms
        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        table_name = data.get("table_name", "imported_data")
        connection_id = data.get("connection_id")

        if not connection_id:
            return {"error": "No connection_id provided"}

        pipeline = Pipeline(
            id="temp",
            name="Import",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id)
        )

        return await import_to_postgres(pipeline, table_name, connection_id, data.get("limit"))

    @post("/import/postgres/stream")
    async def import_postgres_stream(self, data: dict = Body()) -> Stream:
        """Import data to PostgreSQL with SSE progress streaming"""

        async def generate_sse():
            progress_queue = asyncio.Queue()

            async def progress_callback(current: int, total: int, message: str):
                await progress_queue.put({
                    "type": "progress",
                    "current": current,
                    "total": total,
                    "message": message
                })

            async def run_import():
                try:
                    # Parse sources
                    sources = []
                    for s in data.get("sources", []):
                        sources.append(DataSource(
                            id=s.get("id", str(uuid.uuid4())[:8]),
                            name=s.get("name", "Unnamed"),
                            source_type=s.get("source_type", "csv"),
                            path=s.get("path"),
                            connection_id=s.get("connection_id"),
                            query=s.get("query"),
                            osm_layer=s.get("osm_layer")
                        ))

                    # Parse transforms
                    transforms = []
                    for t in data.get("transforms", []):
                        transform = _parse_transform(t)
                        if transform:
                            transforms.append(transform)

                    if not sources:
                        await progress_queue.put({"type": "error", "error": "No sources provided"})
                        return

                    table_name = data.get("table_name", "imported_data")
                    connection_id = data.get("connection_id")

                    if not connection_id:
                        await progress_queue.put({"type": "error", "error": "No connection_id provided"})
                        return

                    pipeline = Pipeline(
                        id="temp",
                        name="Import",
                        sources=sources,
                        transforms=transforms,
                        output_source_id=data.get("output_source_id", sources[0].id)
                    )

                    result = await import_to_postgres(
                        pipeline,
                        table_name,
                        connection_id,
                        data.get("limit"),
                        progress_callback=progress_callback
                    )

                    if "error" in result:
                        await progress_queue.put({"type": "error", "error": result["error"]})
                    else:
                        await progress_queue.put({"type": "complete", "result": result})

                except Exception as e:
                    await progress_queue.put({"type": "error", "error": str(e)})

            # Start import in background
            import_task = asyncio.create_task(run_import())

            # Stream progress updates
            while True:
                try:
                    msg = await asyncio.wait_for(progress_queue.get(), timeout=0.5)
                    yield f"data: {json.dumps(msg)}\n\n"
                    if msg["type"] in ("complete", "error"):
                        break
                except asyncio.TimeoutError:
                    # Send keepalive
                    yield f": keepalive\n\n"
                    if import_task.done():
                        break

            await import_task

        return Stream(
            generate_sse(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    @get("/connections")
    async def get_connections(self) -> dict:
        """Get available PostgreSQL connections for import"""
        connections = list_connections()
        return {
            "connections": [
                {"id": c.id, "name": c.name, "type": c.type, "database": c.database}
                for c in connections
                if c.type == "postgres"
            ]
        }

    @post("/upload")
    async def upload_file(self, data: UploadFile) -> dict:
        """Upload a data file for processing"""
        # Allowed file extensions for data files
        ALLOWED_EXTENSIONS = {".csv", ".tsv", ".json", ".parquet", ".xlsx", ".xls",
                              ".geojson", ".gpkg", ".pbf", ".shp", ".zip", ".gz", ".tar"}
        MAX_UPLOAD_SIZE = 500 * 1024 * 1024  # 500 MB

        try:
            # Get uploads directory (create if needed)
            uploads_dir = Path(tempfile.gettempdir()) / "tusk_uploads"
            uploads_dir.mkdir(exist_ok=True)

            # Validate filename — strip path components to prevent traversal
            raw_filename = data.filename or "uploaded_file"
            filename = Path(raw_filename).name  # Strip any directory components
            if not filename:
                return {"error": "Invalid filename"}

            # Validate extension
            suffix = Path(filename).suffix.lower()
            if suffix not in ALLOWED_EXTENSIONS:
                return {"error": f"File type '{suffix}' not allowed. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"}

            # Generate unique filename to avoid conflicts
            unique_name = f"{uuid.uuid4().hex[:8]}_{filename}"
            file_path = uploads_dir / unique_name

            # Save the uploaded file with size limit check
            content = await data.read()
            if len(content) > MAX_UPLOAD_SIZE:
                return {"error": f"File too large ({len(content) / 1048576:.0f} MB). Maximum: {MAX_UPLOAD_SIZE / 1048576:.0f} MB"}

            with open(file_path, "wb") as f:
                f.write(content)

            return {
                "success": True,
                "path": str(file_path),
                "filename": filename,
                "size": len(content)
            }
        except Exception as e:
            return {"error": str(e)}

    # =========================================================================
    # Workspace Persistence
    # =========================================================================

    @post("/workspace/save")
    async def save_workspace(self, data: dict = Body()) -> dict:
        """Save current workspace state"""
        from tusk.core.workspace import workspace_state_from_dict, save_workspace as do_save
        state = workspace_state_from_dict(data)
        return do_save(state)

    @get("/workspace/load")
    async def load_workspace(self, name: str = "default") -> dict:
        """Load workspace state"""
        from tusk.core.workspace import load_workspace as do_load, workspace_state_to_dict
        state = do_load(name)
        if state is None:
            return {"datasets": [], "name": name}
        return workspace_state_to_dict(state)

    @get("/workspace/list")
    async def list_workspaces(self, request: Request) -> dict | Template:
        """List all saved workspaces"""
        from tusk.core.workspace import list_workspaces as do_list
        workspaces = do_list()
        if is_htmx(request):
            return Template("partials/data/saved-pipelines.html", context={"pipelines": workspaces})
        return {"workspaces": workspaces}

    @delete("/workspace/{name:str}", status_code=200)
    async def delete_workspace(self, name: str) -> dict:
        """Delete a workspace"""
        from tusk.core.workspace import delete_workspace as do_delete
        if do_delete(name):
            return {"success": True}
        return {"error": "Workspace not found"}

    # =========================================================================
    # Cluster Catalog
    # =========================================================================

    @get("/plugin-datasets")
    async def get_plugin_datasets(self, request: Request) -> dict | Template:
        """Get datasets exposed by plugins (queryable via DuckDB sqlite_scan)"""
        from tusk.plugins.registry import get_plugin_datasets as fetch_datasets
        datasets = fetch_datasets()
        if is_htmx(request):
            return Template("partials/data/plugin-datasets.html", context={"datasets": datasets})
        return {"datasets": datasets}

    @post("/materialize")
    async def materialize_to_parquet(self, data: dict = Body()) -> dict:
        """Materialize a database query or pipeline source to a Parquet file.

        Used by the cluster to convert PostgreSQL queries into files
        that DataFusion workers can read.

        Returns: {"path": "/path/to/file.parquet", "rows": N, "table_name": "..."}
        """
        source_type = data.get("source_type", "database")
        query = data.get("query")
        connection_id = data.get("connection_id")
        path = data.get("path")
        name = data.get("name", "materialized")

        # For file sources, just return the path — no materialization needed
        if source_type in ("csv", "tsv", "json", "parquet") and path:
            return {"path": path, "table_name": name, "source_type": source_type}

        # For database sources, run query via psycopg and save as Parquet
        if source_type == "database" and query:
            try:
                import polars as pl
                from tusk.engines import postgres
                from tusk.core.connection import get_connection

                config = get_connection(connection_id)
                if not config:
                    return {"error": f"Connection '{connection_id}' not found"}

                result = await postgres.execute_query(config, query)
                result_dict = result.to_dict()
                if result_dict.get("error"):
                    return {"error": result_dict["error"]}

                columns = [c["name"] for c in result_dict.get("columns", [])]
                rows = result_dict.get("rows", [])
                if not columns or not rows:
                    return {"error": "Query returned no data"}

                # Build Polars DataFrame from result
                col_data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                df = pl.DataFrame(col_data)

                # Save to cache dir
                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
                cache_file = CACHE_DIR / f"cluster_{safe_name}_{uuid.uuid4().hex[:8]}.parquet"
                df.write_parquet(cache_file)

                table_name = safe_name or "materialized"
                log.info("Materialized to Parquet",
                         source=name, path=str(cache_file),
                         rows=df.height, size_mb=round(cache_file.stat().st_size / 1048576, 1))
                return {
                    "path": str(cache_file),
                    "table_name": table_name,
                    "rows": df.height,
                    "source_type": "parquet",
                }
            except Exception as e:
                log.error("Materialization failed", error=str(e))
                return {"error": str(e)}

        # For OSM sources, load via DuckDB spatial and save as Parquet
        if source_type == "osm" and path:
            try:
                from tusk.engines.polars_engine import load_osm

                osm_layer = data.get("osm_layer", "all")
                df = load_osm(path, osm_layer)

                CACHE_DIR.mkdir(parents=True, exist_ok=True)
                safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
                cache_file = CACHE_DIR / f"cluster_{safe_name}_{uuid.uuid4().hex[:8]}.parquet"
                df.write_parquet(cache_file)

                table_name = safe_name or "osm_data"
                log.info("Materialized OSM to Parquet",
                         source=path, path=str(cache_file),
                         rows=df.height)
                return {
                    "path": str(cache_file),
                    "table_name": table_name,
                    "rows": df.height,
                    "source_type": "parquet",
                }
            except Exception as e:
                log.error("OSM materialization failed", error=str(e))
                return {"error": str(e)}

        return {"error": "Invalid source: provide query+connection_id or file path"}

    @get("/catalog")
    async def get_cluster_catalog(self) -> dict:
        """Get datasets enabled for Cluster (DataFusion tables).

        Returns tables that workers should register:
        {"tables": [{"name": "ventas", "path": "/data/ventas.parquet", "format": "parquet"}, ...]}
        """
        from tusk.core.workspace import get_cluster_catalog
        tables = get_cluster_catalog()
        return {"tables": tables}


def _parse_transform(t: dict) -> Transform | None:
    """Parse a transform dict into a Transform object"""
    transform_type = t.get("type")

    try:
        if transform_type == "filter":
            return FilterTransform(
                column=t["column"],
                operator=t["operator"],
                value=t.get("value")
            )
        elif transform_type == "select":
            return SelectTransform(columns=t["columns"])
        elif transform_type == "rename":
            return RenameTransform(mapping=t["mapping"])
        elif transform_type == "sort":
            return SortTransform(
                columns=t["columns"],
                descending=t.get("descending")
            )
        elif transform_type == "group_by":
            return GroupByTransform(
                by=t["by"],
                aggregations=t["aggregations"]
            )
        elif transform_type == "add_column":
            return AddColumnTransform(
                name=t["name"],
                expression=t["expression"]
            )
        elif transform_type == "drop_nulls":
            return DropNullsTransform(subset=t.get("subset"))
        elif transform_type == "limit":
            return LimitTransform(n=t["n"])
        elif transform_type == "join":
            return JoinTransform(
                right_source_id=t["right_source_id"],
                on=t.get("on"),
                left_on=t.get("left_on"),
                right_on=t.get("right_on"),
                how=t.get("how", "inner")
            )
        elif transform_type == "concat":
            return ConcatTransform(
                source_ids=t["source_ids"],
                how=t.get("how", "vertical")
            )
        elif transform_type == "distinct":
            return DistinctTransform(
                subset=t.get("subset"),
                keep=t.get("keep", "first")
            )
        elif transform_type == "window":
            return WindowTransform(
                function=t["function"],
                order_by=t["order_by"],
                partition_by=t.get("partition_by"),
                alias=t.get("alias", "window_col"),
                descending=t.get("descending", False),
                column=t.get("column"),
                offset=t.get("offset", 1),
            )
    except (KeyError, TypeError) as e:
        pass

    return None
