"""API routes for Data/ETL with Polars"""

import hashlib
import uuid
import tempfile
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from litestar import Controller, Request, get, post, put, delete
from litestar.params import Body
from litestar.response import File, Stream, Template
from litestar.datastructures import UploadFile
import msgspec
import structlog

from tusk.engines.polars_engine import short_dtype
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
from tusk.core.files import validate_user_path
from tusk.studio.htmx import is_htmx

log = structlog.get_logger()


def _audit_export(request, fmt: str, filename: str, result: dict) -> None:
    """Record a data export to the audit log, best-effort."""
    try:
        from tusk.core.auth import log_audit
        from tusk.studio.routes.base import get_request_user
        ip = request.client.host if request.client else None
        user = get_request_user(request)
        user_id = user.id if user else None
        details = msgspec.json.encode({
            "format": fmt,
            "filename": filename,
            "rows": result.get("row_count") or result.get("total_count"),
        }).decode()
        log_audit("data.export", user_id=user_id, resource=filename, details=details, ip_address=ip)
    except Exception as e:
        log.warning("audit_log_failed", error=str(e))

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
    """Back-compat wrapper around the shared path guard.

    Existing callers in this module raise ``ValueError`` on rejection; we
    translate ``PermissionError`` from the shared guard into ``ValueError``
    so their error-handling stays unchanged.
    """
    try:
        return validate_user_path(path)
    except PermissionError as e:
        raise ValueError(f"Access denied: {e}") from e


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
                result = await asyncio.to_thread(_duckdb_engine.preview_file, str(p), file_type, 1)
                if not result.error:
                    return {
                        "columns": [{"name": c.name, "type": c.type} for c in result.columns],
                        "engine_used": "duckdb",
                    }

        # Default: Polars schema (or auto for OSM)
        schema = await asyncio.to_thread(get_schema, str(p), osm_layer)
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
            result = await asyncio.to_thread(polars_preview_file, str(p), limit, osm_layer)
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
            result = await asyncio.to_thread(polars_preview_file, str(p), limit, osm_layer)
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
        result = await asyncio.to_thread(_duckdb_engine.preview_file, read_path, read_type, limit)
        elapsed = round((time.perf_counter() - start) * 1000, 2)

        if result.error:
            if engine == "duckdb":
                return {"error": result.error, "engine_used": "duckdb", "elapsed_ms": elapsed}
            log.warning("DuckDB preview failed, falling back to Polars", error=result.error)
            fallback = await asyncio.to_thread(polars_preview_file, str(p), limit, osm_layer)
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
        now = datetime.now(timezone.utc).isoformat()

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

        now = datetime.now(timezone.utc).isoformat()

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
        return await asyncio.to_thread(execute_pipeline, pipeline, limit)

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
        """Execute a pipeline inline without saving.

        The `engine` field picks the executor:
        - "polars" (default) — legacy Polars engine, no new transforms
        - "ibis" / "ibis+duckdb" — Ibis on DuckDB backend, supports case_when,
          unpivot, date_arithmetic
        - "ibis+polars" — Ibis on Polars backend
        """
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
        # v0.3.0: Ibis is the default; Polars is reachable via engine="polars".
        engine = (data.get("engine") or "ibis+duckdb").lower()
        if engine in ("auto", "duckdb", "ibis"):
            engine = "ibis+duckdb"

        if engine.startswith("ibis"):
            from tusk.engines.ibis_engine import execute_pipeline as ibis_execute, HAS_IBIS
            if not HAS_IBIS:
                log.warning("ibis_unavailable_fallback_to_polars")
                result = await asyncio.to_thread(execute_pipeline, pipeline, limit)
                if isinstance(result, dict):
                    result.setdefault("engine_used", "polars")
                    result["fallback"] = "ibis_missing"
                return result
            backend = "polars" if engine.endswith("polars") else "duckdb"
            try:
                df = await asyncio.to_thread(ibis_execute, pipeline, backend=backend, limit=limit)
                return _polars_df_to_dict(df, engine_used=f"ibis+{backend}")
            except Exception as e:
                log.error("ibis_execute_failed", error=str(e), falling_back="polars")
                result = await asyncio.to_thread(execute_pipeline, pipeline, limit)
                if isinstance(result, dict):
                    result.setdefault("engine_used", "polars")
                    result["fallback"] = f"ibis_error: {e}"
                return result

        # Explicit polars or unknown engine → legacy Polars executor
        result = await asyncio.to_thread(execute_pipeline, pipeline, limit)
        if isinstance(result, dict):
            result.setdefault("engine_used", "polars")
        return result

    @post("/profile")
    async def profile_pipeline(self, data: dict = Body()) -> dict:
        """Return per-column stats (null count, distinct, min/max/mean).

        Uses the Ibis engine on DuckDB. Falls back with an error if ibis is
        not installed.
        """
        from tusk.engines.ibis_engine import profile as ibis_profile, HAS_IBIS
        if not HAS_IBIS:
            return {"error": "ibis-framework is not installed"}

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

        transforms = []
        for t in data.get("transforms", []):
            transform = _parse_transform(t)
            if transform:
                transforms.append(transform)

        if not sources:
            return {"error": "No sources provided"}

        pipeline = Pipeline(
            id="temp",
            name="Profile",
            sources=sources,
            transforms=transforms,
            output_source_id=data.get("output_source_id", sources[0].id),
        )
        try:
            return await asyncio.to_thread(ibis_profile, pipeline, sample_limit=data.get("sample_limit", 10_000))
        except Exception as e:
            log.error("ibis_profile_failed", error=str(e))
            return {"error": str(e)}

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
    async def export_csv(self, request: Request, data: dict = Body()) -> File | dict:
        """Export pipeline results to CSV and return as download"""
        from tusk.core import rate_limit
        client_ip = request.client.host if request.client else "unknown"
        if not rate_limit.check_and_record("export", client_ip, max_attempts=20, window_seconds=60):
            return {"error": "Too many exports. Please wait a minute."}
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

        _audit_export(request, "csv", filename, result)

        return File(
            path=temp_path,
            filename=filename,
            media_type="text/csv",
        )

    @post("/export/parquet")
    async def export_parquet(self, request: Request, data: dict = Body()) -> File | dict:
        """Export pipeline results to Parquet and return as download"""
        from tusk.core import rate_limit
        client_ip = request.client.host if request.client else "unknown"
        if not rate_limit.check_and_record("export", client_ip, max_attempts=20, window_seconds=60):
            return {"error": "Too many exports. Please wait a minute."}
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

        _audit_export(request, "parquet", filename, result)

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
                    yield ": keepalive\n\n"
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
    async def upload_file(self, request: Request, data: UploadFile) -> dict:
        """Upload a data file for processing.

        Streams the upload to disk in 1 MiB chunks instead of buffering the
        whole body in memory. Hard-stops + cleans up the partial file the
        moment we cross MAX_UPLOAD_SIZE.
        """
        from tusk.core import rate_limit

        ALLOWED_EXTENSIONS = {".csv", ".tsv", ".json", ".parquet", ".xlsx", ".xls",
                              ".geojson", ".gpkg", ".pbf", ".shp", ".zip", ".gz", ".tar"}
        MAX_UPLOAD_SIZE = 500 * 1024 * 1024  # 500 MB
        CHUNK_SIZE = 1024 * 1024  # 1 MiB

        client_ip = request.client.host if request.client else "unknown"
        if not rate_limit.check_and_record("upload", client_ip, max_attempts=10, window_seconds=60):
            return {"error": "Too many uploads. Please wait a minute."}

        file_path: Path | None = None
        try:
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

            # Stream to disk. We do the actual write in a worker thread so
            # the event loop doesn't block on syscalls for big uploads.
            total = 0
            f = await asyncio.to_thread(open, file_path, "wb")
            try:
                while True:
                    chunk = await data.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_UPLOAD_SIZE:
                        await asyncio.to_thread(f.close)
                        try:
                            file_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        return {
                            "error": f"File too large. Maximum: {MAX_UPLOAD_SIZE / 1048576:.0f} MB"
                        }
                    await asyncio.to_thread(f.write, chunk)
            finally:
                await asyncio.to_thread(f.close)

            return {
                "success": True,
                "path": str(file_path),
                "filename": filename,
                "size": total
            }
        except Exception as e:
            # Cleanup partial file on any error
            if file_path is not None:
                try:
                    file_path.unlink(missing_ok=True)
                except Exception:
                    pass
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
        elif transform_type == "case_when":
            from tusk.engines.ibis_engine import CaseWhenTransform, CaseWhenBranch
            branches = [
                CaseWhenBranch(
                    column=b["column"],
                    operator=b["operator"],
                    value=b.get("value"),
                    result=b.get("result"),
                )
                for b in t.get("branches", [])
            ]
            return CaseWhenTransform(
                alias=t["alias"],
                branches=branches,
                default=t.get("default"),
            )
        elif transform_type == "unpivot":
            from tusk.engines.ibis_engine import UnpivotTransform
            return UnpivotTransform(
                id_cols=t["id_cols"],
                value_cols=t["value_cols"],
                variable_name=t.get("variable_name", "variable"),
                value_name=t.get("value_name", "value"),
            )
        elif transform_type == "date_arithmetic":
            from tusk.engines.ibis_engine import DateArithmeticTransform
            return DateArithmeticTransform(
                operation=t["operation"],
                column=t["column"],
                alias=t["alias"],
                unit=t.get("unit", "day"),
                amount=t.get("amount", 0),
                other_column=t.get("other_column"),
            )
    except (KeyError, TypeError):
        pass

    return None


def _polars_df_to_dict(df, engine_used: str = "polars") -> dict:
    """Shape a Polars DataFrame into the dict the frontend expects."""
    try:
        columns = [{"name": c, "type": short_dtype(df.schema[c])} for c in df.columns]
        rows = df.rows()
        return {
            "columns": columns,
            "rows": [list(r) for r in rows],
            "row_count": len(rows),
            "engine_used": engine_used,
        }
    except Exception as e:
        return {"error": f"Failed to serialize result: {e}"}


# ─── Explore (per-column profile) ─────────────────────────────────
import re as _re

_IDENT_RE = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _coerce_scalar(v):
    """Make a Polars scalar JSON-safe."""
    if v is None:
        return None
    if isinstance(v, (bool, int, float, str)):
        return v
    # datetimes, decimals, etc.
    try:
        return v.isoformat()
    except AttributeError:
        return str(v)


def _compute_profile(columns: list[str], rows: list[tuple]) -> list[dict]:
    """Compute per-column profile from raw Postgres rows using Polars.

    Returns one dict per column with: name, dtype, null_pct, distinct_count,
    distinct_pct, min, max, mean, std, top_values.
    """
    import polars as pl

    n = len(rows)
    if n == 0:
        return [
            {
                "name": c, "dtype": "unknown", "null_pct": 0.0,
                "distinct_count": 0, "distinct_pct": 0.0,
                "min": None, "max": None, "mean": None, "std": None,
                "top_values": [],
            }
            for c in columns
        ]

    # Build column-major dict — coerce non-trivial Python objects to strings so
    # Polars can pick a stable dtype (geometries, dicts, lists, etc.).
    col_data: dict[str, list] = {}
    for i, name in enumerate(columns):
        series = []
        for row in rows:
            v = row[i] if i < len(row) else None
            if v is None or isinstance(v, (bool, int, float, str)):
                series.append(v)
            else:
                # datetimes are fine — Polars handles them; everything else → str
                try:
                    if hasattr(v, "isoformat"):
                        series.append(v)
                    else:
                        series.append(str(v))
                except Exception:
                    series.append(None)
        col_data[name] = series

    try:
        df = pl.DataFrame(col_data, strict=False)
    except Exception:
        # Fallback: stringify everything
        df = pl.DataFrame({k: [None if x is None else str(x) for x in v] for k, v in col_data.items()})

    out: list[dict] = []
    for name in columns:
        s = df[name]
        dtype = str(s.dtype)
        null_count = int(s.null_count())
        null_pct = round(null_count / n * 100, 2) if n else 0.0
        distinct = int(s.n_unique())
        distinct_pct = round(distinct / n * 100, 2) if n else 0.0

        col_stats: dict = {
            "name": name,
            "dtype": dtype,
            "null_pct": null_pct,
            "null_count": null_count,
            "distinct_count": distinct,
            "distinct_pct": distinct_pct,
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "top_values": [],
        }

        is_numeric = s.dtype.is_numeric()

        if is_numeric:
            try:
                col_stats["min"] = _coerce_scalar(s.min())
                col_stats["max"] = _coerce_scalar(s.max())
                mean_v = s.mean()
                col_stats["mean"] = round(float(mean_v), 4) if mean_v is not None else None
                std_v = s.std()
                col_stats["std"] = round(float(std_v), 4) if std_v is not None else None
            except Exception:
                pass
        else:
            # Min/max for dates/strings is still useful
            try:
                col_stats["min"] = _coerce_scalar(s.min())
                col_stats["max"] = _coerce_scalar(s.max())
            except Exception:
                pass

        # Top 10 values by frequency (skip nulls).
        try:
            vc = s.drop_nulls().value_counts(sort=True).head(10)
            top: list[dict] = []
            # value_counts produces a 2-col frame: [name, "count"]
            for row in vc.iter_rows():
                value, count = row[0], row[1]
                top.append({"value": _coerce_scalar(value), "count": int(count)})
            col_stats["top_values"] = top
        except Exception:
            col_stats["top_values"] = []

        out.append(col_stats)

    return out


class ExploreController(Controller):
    """Per-column data profiling for a real Postgres table."""

    path = "/api/explore"

    @post("/profile")
    async def explore_profile(self, data: dict = Body()) -> dict:
        """Profile a real Postgres table.

        Body: `{connection_id, schema, table, sample_size?}`.
        Runs `SELECT * FROM "schema"."table" LIMIT N` against the connection
        and returns per-column stats computed by Polars.
        """
        from tusk.engines import postgres
        from tusk.core.connection import get_connection

        connection_id = data.get("connection_id")
        schema_name = data.get("schema")
        table_name = data.get("table")
        sample_size = data.get("sample_size") or 10_000

        if not connection_id or not schema_name or not table_name:
            return {"error": "connection_id, schema and table are required"}

        try:
            sample_size = int(sample_size)
        except (TypeError, ValueError):
            sample_size = 10_000
        sample_size = max(100, min(sample_size, 1_000_000))

        # Identifier whitelist — psycopg won't parametrize identifiers.
        if not _IDENT_RE.match(schema_name) or not _IDENT_RE.match(table_name):
            return {"error": "Invalid schema or table identifier"}

        config = get_connection(connection_id)
        if not config:
            return {"error": f"Connection '{connection_id}' not found"}
        if config.type != "postgres":
            return {"error": "Profile only supports Postgres connections"}

        sql = f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT {sample_size}'
        try:
            result = await postgres.execute_query(config, sql)
        except Exception as e:
            log.error("explore_profile_query_failed", error=str(e))
            return {"error": str(e)}

        result_dict = result.to_dict()
        if result_dict.get("error"):
            return {"error": result_dict["error"]}

        columns = [c["name"] for c in result_dict.get("columns", [])]
        rows = result_dict.get("rows", [])

        if not columns:
            return {"error": "Table has no columns"}

        try:
            stats = await asyncio.to_thread(_compute_profile, columns, rows)
        except Exception as e:
            log.error("explore_profile_compute_failed", error=str(e))
            return {"error": f"Profile computation failed: {e}"}

        spatial_cols: list[dict] = []
        try:
            from tusk.core.spatial import table_spatial

            spatial_cols = await table_spatial(config, schema_name, table_name)
        except Exception as e:  # noqa: BLE001 — never fail a profile over the spatial extra
            log.debug("explore_spatial_skipped", error=str(e))
        return {
            "schema": schema_name,
            "table": table_name,
            "sampled_rows": len(rows),
            "sample_size": sample_size,
            "columns": stats,
            "spatial": spatial_cols,
        }
