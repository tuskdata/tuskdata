"""API routes for Data/ETL with Polars"""

import uuid
import tempfile
import os
import asyncio
import json
from datetime import datetime
from pathlib import Path
from litestar import Controller, get, post, put, delete
from litestar.params import Body
from litestar.response import File, Stream
from litestar.datastructures import UploadFile
import msgspec
import structlog

from tusk.engines import polars_engine
from tusk.engines.polars_engine import (
    Pipeline, DataSource, Transform,
    FilterTransform, SelectTransform, RenameTransform, SortTransform,
    GroupByTransform, AddColumnTransform, DropNullsTransform,
    LimitTransform, JoinTransform,
    generate_code, execute_pipeline, get_schema, preview_file as polars_preview_file, get_osm_layers,
    export_to_csv, export_to_parquet, import_to_duckdb, import_to_postgres
)
from tusk.engines.duckdb_engine import DuckDBEngine
from tusk.core.connection import list_connections

log = structlog.get_logger()

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
    async def get_file_schema(self, path: str, osm_layer: str | None = None) -> dict:
        """Get schema of a data file"""
        return get_schema(path, osm_layer)

    @get("/files/preview")
    async def preview_data_file(self, path: str, limit: int = 100, osm_layer: str | None = None) -> dict:
        """Preview contents of a data file using DuckDB (fast) or Polars (for OSM)"""
        import time
        start = time.perf_counter()

        p = Path(path).expanduser()
        suffix = p.suffix.lower()

        # Use Polars for OSM files (DuckDB Spatial handles this differently)
        if suffix == ".pbf" or path.endswith(".osm.pbf"):
            return polars_preview_file(path, limit, osm_layer)

        # Use DuckDB for standard files (faster)
        file_type = {".csv": "csv", ".tsv": "tsv", ".parquet": "parquet", ".json": "json"}.get(suffix)
        if not file_type:
            return {"error": f"Unsupported file type: {suffix}"}

        result = _duckdb_engine.preview_file(str(p), file_type, limit)
        elapsed = round((time.perf_counter() - start) * 1000, 2)

        if result.error:
            log.warning("DuckDB preview failed, falling back to Polars", error=result.error)
            return polars_preview_file(path, limit, osm_layer)

        log.info("DuckDB preview successful", path=str(p), rows=len(result.rows), ms=elapsed)
        return {
            "columns": [{"name": c.name, "type": c.type} for c in result.columns],
            "rows": result.rows,
            "row_count": len(result.rows)
        }

    @get("/osm/layers")
    async def get_osm_file_layers(self, path: str) -> dict:
        """Get available layers in an OSM file"""
        return get_osm_layers(path)

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
        try:
            # Get uploads directory (create if needed)
            uploads_dir = Path(tempfile.gettempdir()) / "tusk_uploads"
            uploads_dir.mkdir(exist_ok=True)

            # Generate unique filename to avoid conflicts
            filename = data.filename or "uploaded_file"
            unique_name = f"{uuid.uuid4().hex[:8]}_{filename}"
            file_path = uploads_dir / unique_name

            # Save the uploaded file
            content = await data.read()
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
    async def list_workspaces(self) -> dict:
        """List all saved workspaces"""
        from tusk.core.workspace import list_workspaces as do_list
        return {"workspaces": do_list()}

    @delete("/workspace/{name:str}", status_code=200)
    async def delete_workspace(self, name: str) -> dict:
        """Delete a workspace"""
        from tusk.core.workspace import delete_workspace as do_delete
        if do_delete(name):
            return {"success": True}
        return {"error": "Workspace not found"}


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
    except (KeyError, TypeError) as e:
        print(f"Error parsing transform: {e}")

    return None
