"""BI plugin API controllers (JSON + HTMX responses)"""

import base64
import csv
import io
import json
import re

from litestar import Controller, Request, get, post, put, delete
from litestar.params import Body
from litestar.response import Response, Template
import msgspec

from tusk.studio.htmx import is_htmx, htmx_toast
from tusk.core.logging import get_logger

from tusk.bi.engine import BIQueryEngine

log = get_logger("tusk.bi.api")
_engine = BIQueryEngine()


class BIAPIController(Controller):
    """BI plugin API routes"""

    path = "/api/bi"

    # ─────────────────────────────────────────────────────────────
    # Data Sources
    # ─────────────────────────────────────────────────────────────

    @get("/sources")
    async def list_sources(self) -> Response:
        from tusk.bi.db import get_data_sources
        sources = get_data_sources()
        return Response(
            content=msgspec.json.encode({"sources": sources}),
            media_type="application/json",
        )

    @post("/sources/discover")
    async def discover_sources(self) -> Response:
        from tusk.bi.db import discover_plugin_sources
        count = discover_plugin_sources()
        return Response(
            content=msgspec.json.encode({"discovered": count}),
            media_type="application/json",
        )

    @get("/sources/{source_id:int}/tables")
    async def list_tables(self, source_id: int) -> Response:
        from tusk.bi.db import get_data_source
        source = get_data_source(source_id)
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Source not found"}),
                media_type="application/json",
                status_code=404,
            )
        tables = _engine.get_table_list(source["source_type"], source["connection_ref"])
        return Response(
            content=msgspec.json.encode({"tables": tables}),
            media_type="application/json",
        )

    @get("/sources/{source_id:int}/schema/{table:str}")
    async def table_schema(self, source_id: int, table: str) -> Response:
        from tusk.bi.db import get_data_source
        source = get_data_source(source_id)
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Source not found"}),
                media_type="application/json",
                status_code=404,
            )
        schema = _engine.get_table_schema(source["source_type"], source["connection_ref"], table)
        return Response(
            content=msgspec.json.encode({"columns": schema}),
            media_type="application/json",
        )

    @get("/sources/{source_id:int}/preview/{table:str}")
    async def table_preview(self, source_id: int, table: str) -> Response:
        from tusk.bi.db import get_data_source
        source = get_data_source(source_id)
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Source not found"}),
                media_type="application/json",
                status_code=404,
            )
        result = _engine.get_table_preview(source["source_type"], source["connection_ref"], table)
        return Response(
            content=msgspec.json.encode(result),
            media_type="application/json",
        )

    # ─────────────────────────────────────────────────────────────
    # Queries
    # ─────────────────────────────────────────────────────────────

    @get("/queries")
    async def list_queries(self, source_id: int | None = None, tag: str | None = None) -> Response:
        from tusk.bi.db import get_saved_queries
        queries = get_saved_queries(source_id=source_id, tag=tag)
        return Response(
            content=msgspec.json.encode({"queries": queries}),
            media_type="application/json",
        )

    @post("/queries")
    async def create_query(self, request: Request, data: dict = Body()) -> Response:
        from tusk.bi.db import create_saved_query

        name = data.get("name", "").strip()
        source_id = data.get("source_id")
        sql = data.get("sql", "").strip()

        if not name or not source_id or not sql:
            return Response(
                content=msgspec.json.encode({"error": "name, source_id, and sql are required"}),
                media_type="application/json",
                status_code=400,
            )

        query_id = create_saved_query(
            name=name,
            source_id=source_id,
            sql=sql,
            description=data.get("description", ""),
            chart_type=data.get("chart_type"),
            chart_config=json.dumps(data.get("chart_config", {})),
            tags=data.get("tags", ""),
        )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast(f"Query '{name}' saved", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True, "id": query_id}),
            media_type="application/json",
        )

    @get("/queries/{query_id:int}")
    async def get_query(self, query_id: int) -> Response:
        from tusk.bi.db import get_saved_query
        query = get_saved_query(query_id)
        if not query:
            return Response(
                content=msgspec.json.encode({"error": "Query not found"}),
                media_type="application/json",
                status_code=404,
            )
        return Response(
            content=msgspec.json.encode({"query": query}),
            media_type="application/json",
        )

    @put("/queries/{query_id:int}")
    async def update_query(self, request: Request, query_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import update_saved_query

        update_saved_query(
            query_id,
            name=data.get("name"),
            sql=data.get("sql"),
            description=data.get("description"),
            chart_type=data.get("chart_type"),
            chart_config=json.dumps(data["chart_config"]) if "chart_config" in data else None,
            tags=data.get("tags"),
        )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Query updated", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @delete("/queries/{query_id:int}", status_code=200)
    async def delete_query(self, request: Request, query_id: int) -> Response:
        from tusk.bi.db import delete_saved_query
        delete_saved_query(query_id)

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Query deleted", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @post("/queries/{query_id:int}/execute")
    async def execute_saved_query(self, query_id: int, data: dict = Body()) -> Response:
        """Execute a saved query"""
        from tusk.bi.db import get_saved_query, mark_query_executed

        query = get_saved_query(query_id)
        if not query:
            return Response(
                content=msgspec.json.encode({"error": "Query not found"}),
                media_type="application/json",
                status_code=404,
            )

        params = data.get("params") if data else None

        try:
            result = _engine.execute(
                source_type=query["source_type"],
                connection_ref=query["connection_ref"],
                sql=query["sql"],
                params=params,
            )
            mark_query_executed(query_id)
            return Response(
                content=msgspec.json.encode(result),
                media_type="application/json",
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

    @post("/queries/run")
    async def execute_adhoc(self, data: dict = Body()) -> Response:
        """Execute ad-hoc SQL without saving"""
        from tusk.bi.db import get_data_source

        source_id = data.get("source_id")
        sql = data.get("sql", "").strip()

        if not source_id or not sql:
            return Response(
                content=msgspec.json.encode({"error": "source_id and sql are required"}),
                media_type="application/json",
                status_code=400,
            )

        source = get_data_source(source_id)
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Source not found"}),
                media_type="application/json",
                status_code=404,
            )

        params = data.get("params")

        try:
            result = _engine.execute(
                source_type=source["source_type"],
                connection_ref=source["connection_ref"],
                sql=sql,
                params=params,
            )
            return Response(
                content=msgspec.json.encode(result),
                media_type="application/json",
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

    # ─────────────────────────────────────────────────────────────
    # CSV Export
    # ─────────────────────────────────────────────────────────────

    @get("/queries/{query_id:int}/export-csv")
    async def export_csv(self, query_id: int) -> Response:
        """Execute a saved query and return results as CSV download."""
        from tusk.bi.db import get_saved_query, get_data_source

        query = get_saved_query(query_id)
        if not query:
            return Response(
                content=msgspec.json.encode({"error": "Query not found"}),
                media_type="application/json",
                status_code=404,
            )

        source = get_data_source(query["source_id"])
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Data source not found"}),
                media_type="application/json",
                status_code=404,
            )

        try:
            result = _engine.execute(
                source_type=source["source_type"],
                connection_ref=source["connection_ref"],
                sql=query["sql"],
                limit=10000,
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(result.get("columns", []))
        for row in result.get("rows", []):
            writer.writerow(row)

        csv_bytes = output.getvalue().encode("utf-8")
        safe_name = re.sub(r'[^\w\-]', '_', query.get("name", "export"))
        return Response(
            content=csv_bytes,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}.csv"'},
        )

    @get("/queries/{query_id:int}/export-json")
    async def export_query_json(self, query_id: int) -> Response:
        """Execute a saved query and return results as a JSON download."""
        from tusk.bi.db import get_saved_query, get_data_source

        query = get_saved_query(query_id)
        if not query:
            return Response(
                content=msgspec.json.encode({"error": "Query not found"}),
                media_type="application/json",
                status_code=404,
            )
        source = get_data_source(query["source_id"])
        if not source:
            return Response(
                content=msgspec.json.encode({"error": "Data source not found"}),
                media_type="application/json",
                status_code=404,
            )
        try:
            result = _engine.execute(
                source_type=source["source_type"],
                connection_ref=source["connection_ref"],
                sql=query["sql"],
                limit=10000,
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )
        safe_name = re.sub(r'[^\w\-]', '_', query.get("name", "export"))
        return Response(
            content=msgspec.json.encode(result),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}.json"'},
        )

    @post("/cache/clear")
    async def clear_cache(self) -> dict:
        """Invalidate the in-memory BI query cache."""
        from tusk.bi.engine import clear_cache
        evicted = clear_cache()
        return {"cleared": evicted}

    # ─────────────────────────────────────────────────────────────
    # Chart Data
    # ─────────────────────────────────────────────────────────────

    @post("/queries/{query_id:int}/chart-data")
    async def get_chart_data(self, query_id: int, data: dict = Body()) -> Response:
        """Get pre-processed data for Chart.js"""
        from tusk.bi.db import get_saved_query, mark_query_executed
        from tusk.bi.charts import build_chart_config

        query = get_saved_query(query_id)
        if not query:
            return Response(
                content=msgspec.json.encode({"error": "Query not found"}),
                media_type="application/json",
                status_code=404,
            )

        try:
            result = _engine.execute(
                source_type=query["source_type"],
                connection_ref=query["connection_ref"],
                sql=query["sql"],
            )
            mark_query_executed(query_id)

            chart_config = json.loads(query.get("chart_config", "{}"))
            if data:
                chart_config.update(data)

            chart_data = build_chart_config(
                chart_type=chart_config.get("chart_type", query.get("chart_type", "bar")),
                columns=result["columns"],
                rows=result["rows"],
                config=chart_config,
            )

            return Response(
                content=msgspec.json.encode(chart_data),
                media_type="application/json",
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

    @post("/suggest-chart")
    async def suggest_chart(self, data: dict = Body()) -> Response:
        """Auto-suggest chart type + axes for a query result.

        Body: {source_type, connection_ref, sql} OR {columns, rows}.
        Returns {chart_type, x_column, y_column, group_by, columns,
        column_types} so the chart-builder UI can pre-fill the dropdowns
        the moment a query is entered — same zero-config feel as Explore.
        """
        from tusk.bi.charts import suggest_axes, infer_column_types

        columns = data.get("columns")
        rows = data.get("rows")
        # If raw columns/rows weren't passed, run the SQL to get a sample.
        if columns is None or rows is None:
            sql = (data.get("sql") or "").strip()
            if not sql:
                return Response(
                    content=msgspec.json.encode({"error": "sql or columns/rows required"}),
                    media_type="application/json",
                    status_code=400,
                )
            try:
                result = _engine.execute(
                    source_type=data.get("source_type", "postgres"),
                    connection_ref=data.get("connection_ref", ""),
                    sql=sql,
                    limit=200,
                )
                columns = result["columns"]
                rows = result["rows"]
            except Exception as e:
                return Response(
                    content=msgspec.json.encode({"error": str(e)}),
                    media_type="application/json",
                    status_code=400,
                )

        suggestion = suggest_axes(columns, rows)
        column_types = infer_column_types(columns, rows)
        return Response(
            content=msgspec.json.encode({
                **suggestion,
                "columns": columns,
                "column_types": column_types,
            }),
            media_type="application/json",
        )

    # ─────────────────────────────────────────────────────────────
    # Dashboards
    # ─────────────────────────────────────────────────────────────

    @get("/dashboards")
    async def list_dashboards(self) -> Response:
        from tusk.bi.db import get_dashboards
        dashboards = get_dashboards()
        return Response(
            content=msgspec.json.encode({"dashboards": dashboards}),
            media_type="application/json",
        )

    @post("/dashboards")
    async def create_dashboard(self, request: Request, data: dict = Body()) -> Response:
        from tusk.bi.db import create_dashboard

        name = data.get("name", "").strip()
        if not name:
            return Response(
                content=msgspec.json.encode({"error": "name is required"}),
                media_type="application/json",
                status_code=400,
            )

        dashboard_id = create_dashboard(
            name=name,
            description=data.get("description", ""),
            is_default=data.get("is_default", False),
        )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast(f"Dashboard '{name}' created", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True, "id": dashboard_id}),
            media_type="application/json",
        )

    @get("/dashboards/{dashboard_id:int}")
    async def get_dashboard(self, dashboard_id: int) -> Response:
        from tusk.bi.db import get_dashboard, get_widgets
        dashboard = get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                content=msgspec.json.encode({"error": "Dashboard not found"}),
                media_type="application/json",
                status_code=404,
            )
        widgets = get_widgets(dashboard_id)
        return Response(
            content=msgspec.json.encode({"dashboard": dashboard, "widgets": widgets}),
            media_type="application/json",
        )

    @put("/dashboards/{dashboard_id:int}")
    async def update_dashboard(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import update_dashboard

        update_dashboard(
            dashboard_id,
            name=data.get("name"),
            description=data.get("description"),
            is_default=data.get("is_default"),
            filters=json.dumps(data["filters"]) if "filters" in data else None,
            is_public=data.get("is_public"),
            refresh_interval_seconds=data.get("refresh_interval_seconds"),
        )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Dashboard updated", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @delete("/dashboards/{dashboard_id:int}", status_code=200)
    async def delete_dashboard(self, request: Request, dashboard_id: int) -> Response:
        from tusk.bi.db import delete_dashboard
        delete_dashboard(dashboard_id)

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Dashboard deleted", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @post("/dashboards/{dashboard_id:int}/clone")
    async def clone_dashboard(self, request: Request, dashboard_id: int) -> Response:
        from tusk.bi.db import clone_dashboard
        new_id = clone_dashboard(dashboard_id)
        if not new_id:
            return Response(
                content=msgspec.json.encode({"error": "Dashboard not found"}),
                media_type="application/json",
                status_code=404,
            )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Dashboard cloned", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True, "id": new_id}),
            media_type="application/json",
        )

    # ─────────────────────────────────────────────────────────────
    # Widgets
    # ─────────────────────────────────────────────────────────────

    @post("/dashboards/{dashboard_id:int}/widgets")
    async def create_widget(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import create_widget

        widget_id = create_widget(
            dashboard_id=dashboard_id,
            query_id=data.get("query_id"),
            widget_type=data.get("widget_type", "chart"),
            title=data.get("title", ""),
            config=json.dumps(data.get("config", {})),
            col_start=data.get("col_start", 1),
            col_span=data.get("col_span", 6),
            row_start=data.get("row_start", 1),
            row_span=data.get("row_span", 4),
            tab_id=data.get("tab_id"),
        )

        if is_htmx(request):
            return Response(
                content=b"",
                headers=htmx_toast("Widget added", "success"),
            )

        return Response(
            content=msgspec.json.encode({"success": True, "id": widget_id}),
            media_type="application/json",
        )

    @put("/widgets/{widget_id:int}")
    async def update_widget(self, request: Request, widget_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import update_widget

        update_widget(
            widget_id,
            title=data.get("title"),
            config=json.dumps(data["config"]) if "config" in data else None,
            col_start=data.get("col_start"),
            col_span=data.get("col_span"),
            row_start=data.get("row_start"),
            row_span=data.get("row_span"),
            query_id=data.get("query_id"),
            tab_id=data.get("tab_id"),
        )

        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Widget updated", "success"))

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @delete("/widgets/{widget_id:int}", status_code=200)
    async def delete_widget(self, request: Request, widget_id: int) -> Response:
        from tusk.bi.db import delete_widget
        delete_widget(widget_id)

        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Widget removed", "success"))

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @put("/dashboards/{dashboard_id:int}/layout")
    async def save_layout(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        """Bulk save widget positions from gridstack layout."""
        from tusk.bi.db import update_widget
        widgets = data.get("widgets", [])
        for w in widgets:
            update_widget(
                w["id"],
                col_start=w.get("col_start"), col_span=w.get("col_span"),
                row_start=w.get("row_start"), row_span=w.get("row_span"),
            )
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Layout saved", "success"))
        return Response(content=msgspec.json.encode({"success": True}), media_type="application/json")

    # ── Dashboard Variables ───────────────────────────────────

    @get("/dashboards/{dashboard_id:int}/variables")
    async def list_variables(self, dashboard_id: int) -> Response:
        from tusk.bi.db import get_dashboard_variables
        variables = get_dashboard_variables(dashboard_id)
        return Response(content=msgspec.json.encode({"variables": variables}), media_type="application/json")

    @post("/dashboards/{dashboard_id:int}/variables")
    async def create_variable(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import create_dashboard_variable
        var_id = create_dashboard_variable(
            dashboard_id=dashboard_id, name=data["name"],
            var_type=data.get("var_type", "text"), default_value=data.get("default_value", ""),
            options=data.get("options", ""), label=data.get("label", ""),
        )
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Variable added", "success"))
        return Response(content=msgspec.json.encode({"success": True, "id": var_id}), media_type="application/json")

    @delete("/variables/{var_id:int}", status_code=200)
    async def delete_variable(self, request: Request, var_id: int) -> Response:
        from tusk.bi.db import delete_dashboard_variable
        delete_dashboard_variable(var_id)
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Variable deleted", "success"))
        return Response(content=msgspec.json.encode({"success": True}), media_type="application/json")

    # ── Dashboard Tabs ─────────────────────────────────────────

    @get("/dashboards/{dashboard_id:int}/tabs")
    async def list_tabs(self, dashboard_id: int) -> Response:
        from tusk.bi.db import get_dashboard_tabs
        tabs = get_dashboard_tabs(dashboard_id)
        return Response(content=msgspec.json.encode({"tabs": tabs}), media_type="application/json")

    @post("/dashboards/{dashboard_id:int}/tabs")
    async def create_tab(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import create_dashboard_tab
        name = data.get("name", "").strip()
        if not name:
            return Response(content=msgspec.json.encode({"error": "name is required"}), media_type="application/json", status_code=400)
        tab_id = create_dashboard_tab(
            dashboard_id=dashboard_id,
            name=name,
            tab_order=data.get("tab_order", 0),
        )
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast(f"Tab '{name}' created", "success"))
        return Response(content=msgspec.json.encode({"success": True, "id": tab_id}), media_type="application/json")

    @put("/tabs/{tab_id:int}")
    async def update_tab(self, request: Request, tab_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import update_dashboard_tab
        update_dashboard_tab(
            tab_id,
            name=data.get("name"),
            tab_order=data.get("tab_order"),
        )
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Tab updated", "success"))
        return Response(content=msgspec.json.encode({"success": True}), media_type="application/json")

    @delete("/tabs/{tab_id:int}", status_code=200)
    async def delete_tab(self, request: Request, tab_id: int) -> Response:
        from tusk.bi.db import delete_dashboard_tab
        delete_dashboard_tab(tab_id)
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Tab deleted", "success"))
        return Response(content=msgspec.json.encode({"success": True}), media_type="application/json")

    # ── Export / Import ───────────────────────────────────────

    @get("/dashboards/{dashboard_id:int}/export")
    async def export_dashboard(self, dashboard_id: int) -> Response:
        from tusk.bi.db import export_dashboard
        data = export_dashboard(dashboard_id)
        if not data:
            return Response(content=msgspec.json.encode({"error": "Dashboard not found"}), media_type="application/json", status_code=404)
        return Response(
            content=msgspec.json.encode(data), media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=dashboard_{dashboard_id}.json"},
        )

    @post("/dashboards/import")
    async def import_dashboard(self, request: Request, data: dict = Body()) -> Response:
        from tusk.bi.db import import_dashboard
        try:
            dashboard_id = import_dashboard(data, source_id=data.get("source_id"))
            if is_htmx(request):
                return Response(content=b"", headers=htmx_toast("Dashboard imported", "success"))
            return Response(content=msgspec.json.encode({"success": True, "id": dashboard_id}), media_type="application/json")
        except Exception as e:
            return Response(content=msgspec.json.encode({"error": str(e)}), media_type="application/json", status_code=400)

    # ── Public Links ──────────────────────────────────────────

    @post("/dashboards/{dashboard_id:int}/public-link")
    async def create_public_link(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        import secrets as _secrets
        from tusk.bi.db import create_public_link
        token = _secrets.token_urlsafe(32)
        expires_at = data.get("expires_at") if data else None
        link_id = create_public_link(dashboard_id, token, expires_at)
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Public link created", "success"))
        return Response(content=msgspec.json.encode({"success": True, "id": link_id, "token": token}), media_type="application/json")

    @get("/public-links/{dashboard_id:int}")
    async def list_public_links(self, dashboard_id: int) -> Response:
        from tusk.bi.db import get_public_links
        links = get_public_links(dashboard_id)
        return Response(content=msgspec.json.encode({"links": links}), media_type="application/json")

    @delete("/public-links/{link_id:int}", status_code=200)
    async def delete_public_link(self, request: Request, link_id: int) -> Response:
        from tusk.bi.db import delete_public_link
        delete_public_link(link_id)
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Public link deleted", "success"))
        return Response(content=msgspec.json.encode({"success": True}), media_type="application/json")

    # ── Visual Query Builder ──────────────────────────────────

    @post("/query-builder/generate-sql")
    async def generate_sql(self, data: dict = Body()) -> Response:
        """Generate SQL from visual query builder selections."""
        table = data.get("table", "")
        columns = data.get("columns", [])
        aggregates = data.get("aggregates", [])
        filters = data.get("filters", [])
        group_by = data.get("group_by", [])
        order_by = data.get("order_by", [])
        limit_val = data.get("limit", 1000)

        if not table:
            return Response(content=msgspec.json.encode({"error": "table is required"}), media_type="application/json", status_code=400)

        # Validate table name
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', table):
            return Response(content=msgspec.json.encode({"error": "Invalid table name"}), media_type="application/json", status_code=400)

        select_parts: list[str] = []
        if aggregates:
            for agg in aggregates:
                func = agg.get("function", "COUNT").upper()
                if func not in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
                    continue
                col = agg.get("column", "*")
                alias = agg.get("alias", f"{func.lower()}_{col}")
                select_parts.append(f'{func}("{col}") AS "{alias}"')
            for gb in group_by:
                select_parts.insert(0, f'"{gb}"')
        elif columns:
            select_parts = [f'"{c}"' for c in columns]
        else:
            select_parts = ["*"]

        sql = f'SELECT {", ".join(select_parts)}\nFROM "{table}"'

        if filters:
            conditions = []
            valid_ops = ("=", "!=", ">", "<", ">=", "<=", "LIKE", "IS NULL", "IS NOT NULL")
            for f in filters:
                col = f.get("column", "")
                op = f.get("operator", "=")
                val = f.get("value", "")
                if op not in valid_ops:
                    continue
                if op in ("IS NULL", "IS NOT NULL"):
                    conditions.append(f'"{col}" {op}')
                elif op == "LIKE":
                    safe_val = re.sub(r"[^\w\s\-%_]", "", val)
                    conditions.append(f'"{col}" LIKE \'%{safe_val}%\'')
                else:
                    safe_val = re.sub(r"[^\w\s\-\.]", "", str(val))
                    conditions.append(f'"{col}" {op} \'{safe_val}\'')
            if conditions:
                sql += f'\nWHERE {" AND ".join(conditions)}'

        if group_by:
            group_cols = ", ".join(f'"{g}"' for g in group_by)
            sql += f"\nGROUP BY {group_cols}"

        if order_by:
            parts = []
            for o in order_by:
                col = o.get("column", "")
                direction = "DESC" if o.get("direction", "ASC").upper() == "DESC" else "ASC"
                parts.append(f'"{col}" {direction}')
            sql += f'\nORDER BY {", ".join(parts)}'

        if limit_val:
            sql += f"\nLIMIT {int(limit_val)}"

        return Response(content=msgspec.json.encode({"sql": sql}), media_type="application/json")

    # ─────────────────────────────────────────────────────────────
    # Widget export + cross-filter helpers
    # ─────────────────────────────────────────────────────────────

    @get("/widgets/{widget_id:int}/export")
    async def export_widget(self, widget_id: int, format: str = "csv") -> Response:
        """Download the widget's current rows as CSV or JSON."""
        from tusk.bi.db import get_widget, get_saved_query, get_data_source

        widget = get_widget(widget_id)
        if not widget:
            return Response(
                content=msgspec.json.encode({"error": "Widget not found"}),
                media_type="application/json",
                status_code=404,
            )

        query = get_saved_query(widget["query_id"]) if widget.get("query_id") else None
        if query:
            source = get_data_source(query["source_id"])
            sql = query["sql"]
        else:
            source = get_data_source(widget["source_id"])
            sql = widget.get("query_sql", "")

        if not source or not sql:
            return Response(
                content=msgspec.json.encode({"error": "Widget has no executable query"}),
                media_type="application/json",
                status_code=400,
            )

        try:
            result = _engine.execute(
                source_type=source["source_type"],
                connection_ref=source["connection_ref"],
                sql=sql,
                limit=10000,
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

        safe_name = re.sub(r'[^\w\-]', '_', widget.get("title") or f"widget_{widget_id}")

        if format == "json":
            return Response(
                content=msgspec.json.encode(result),
                media_type="application/json",
                headers={"Content-Disposition": f'attachment; filename="{safe_name}.json"'},
            )

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(result.get("columns", []))
        for row in result.get("rows", []):
            writer.writerow(row)
        return Response(
            content=output.getvalue().encode("utf-8"),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}.csv"'},
        )

    # ─────────────────────────────────────────────────────────────
    # Widget Rendering (updated with variables + time filter)
    # ─────────────────────────────────────────────────────────────

    @get("/widgets/{widget_id:int}/render")
    async def render_widget(self, request: Request, widget_id: int) -> Template | Response:
        """Render widget as HTML partial (for HTMX loading in dashboards)"""
        from tusk.bi.db import get_widget

        widget = get_widget(widget_id)
        if not widget:
            return Response(
                content=b'<div class="text-red-400 text-xs p-2">Widget not found</div>',
                media_type="text/html",
                status_code=404,
            )

        data = None
        error = None
        chart_config = None

        # Parse RLS clauses from embed context
        rls_clauses = None
        rls_param = request.query_params.get("rls", "")
        if rls_param:
            try:
                rls_json = base64.urlsafe_b64decode(rls_param).decode()
                rls_clauses = json.loads(rls_json)
                if not isinstance(rls_clauses, dict):
                    rls_clauses = None
            except Exception:
                rls_clauses = None

        # Execute query if widget has one
        if widget.get("query_sql") and widget.get("source_id"):
            from tusk.bi.db import get_data_source
            source = get_data_source(widget["source_id"])
            if source:
                sql = widget["query_sql"]

                # Apply dashboard variables from query params (parameterized)
                variables = {k.replace("var_", ""): v for k, v in request.query_params.items() if k.startswith("var_")}
                variable_params: dict = {}
                if variables:
                    sql, variable_params = _engine.apply_variables(sql, variables)

                # Time filter params
                time_from = request.query_params.get("time_from")
                time_to = request.query_params.get("time_to")

                try:
                    data = _engine.execute(
                        source_type=source["source_type"],
                        connection_ref=source["connection_ref"],
                        sql=sql,
                        params=variable_params or None,
                        limit=500,
                        rls_clauses=rls_clauses,
                    )
                    # Apply time filter if requested
                    if (time_from or time_to) and data and data.get("columns"):
                        filtered_sql = _engine.apply_time_filter(sql, time_from, time_to, data["columns"])
                        if filtered_sql != sql:
                            data = _engine.execute(
                                source_type=source["source_type"],
                                connection_ref=source["connection_ref"],
                                sql=filtered_sql,
                                limit=500,
                            )
                    # Check if engine returned an error dict
                    if data and data.get("error"):
                        error = data["error"]
                        data = None
                except Exception as e:
                    error = str(e)

        # Select partial template based on widget type
        widget_type = widget.get("widget_type", "chart")
        partial_map = {
            "chart": "plugins/bi/partials/chart.html",
            "table": "plugins/bi/partials/table.html",
            "stat": "plugins/bi/partials/stat.html",
            "map": "plugins/bi/partials/map.html",
            "text": "plugins/bi/partials/text.html",
            "pivot": "plugins/bi/partials/pivot.html",
            "top_n": "plugins/bi/partials/top_n.html",
            "funnel": "plugins/bi/partials/funnel.html",
        }
        template_name = partial_map.get(widget_type, partial_map["chart"])

        # Parse widget config (stored as JSON string in DB)
        config = widget.get("config") or "{}"
        if isinstance(config, str):
            try:
                config = json.loads(config)
            except (json.JSONDecodeError, TypeError):
                config = {}

        # Build chart config if chart widget
        if widget_type == "chart" and data and not error:
            from tusk.bi.charts import build_chart_config, suggest_axes
            # Chart type precedence: what the user pinned on the widget →
            # the saved query's chart_type → the auto-detect suggestion
            # (date on X ⇒ line) → bar. It used to fall straight to "bar",
            # so time series rendered as bars.
            chart_type = config.get("chart_type")
            if not chart_type and widget.get("query_id"):
                from tusk.bi.db import get_saved_query
                q = get_saved_query(widget["query_id"])
                chart_type = (q or {}).get("chart_type")
            if not chart_type:
                chart_type = suggest_axes(data.get("columns", []), data.get("rows", [])).get("chart_type")
            chart_config = build_chart_config(
                chart_type=chart_type or "bar",
                columns=data.get("columns", []),
                rows=data.get("rows", []),
                config=config,
            )

        # Build pivot data if pivot widget
        pivot_data = None
        if widget_type == "pivot" and data and not error:
            pivot_data = _engine.pivot_data(
                columns=data.get("columns", []),
                rows=data.get("rows", []),
                row_field=config.get("row_field", data["columns"][0] if data.get("columns") else ""),
                col_field=config.get("col_field", data["columns"][1] if len(data.get("columns", [])) > 1 else ""),
                value_field=config.get("value_field", data["columns"][2] if len(data.get("columns", [])) > 2 else ""),
                agg_func=config.get("agg_func", "sum"),
            )

        # Load sparkline for stat widgets.
        # Priority:
        #   1. sparkline_sql in widget config → run it now, take the
        #      *last column* of every row in order. This is the explicit
        #      "I have my own time-series query" path.
        #   2. Snapshot history of the widget's main query — automatic
        #      sparkline from scheduled-query history.
        sparkline_values = None
        if widget_type == "stat":
            sparkline_sql = (config or {}).get("sparkline_sql", "").strip() if isinstance(config, dict) else ""
            if sparkline_sql and widget.get("source_id"):
                from tusk.bi.db import get_data_source
                spark_source = get_data_source(widget["source_id"])
                if spark_source:
                    try:
                        spark_data = _engine.execute(
                            source_type=spark_source["source_type"],
                            connection_ref=spark_source["connection_ref"],
                            sql=sparkline_sql,
                            limit=200,
                        )
                        if spark_data and not spark_data.get("error") and spark_data.get("rows"):
                            values: list[float] = []
                            for row in spark_data["rows"]:
                                if not row:
                                    continue
                                v = row[-1]
                                if v is None:
                                    continue
                                try:
                                    values.append(float(v))
                                except (ValueError, TypeError):
                                    pass
                            if values:
                                sparkline_values = values
                    except Exception as e:
                        log.debug("sparkline_sql failed", widget_id=widget_id, error=str(e))

            if sparkline_values is None and widget.get("query_id"):
                from tusk.bi.db import get_snapshots
                snapshots = get_snapshots(widget["query_id"], limit=20)
                for s in reversed(snapshots):
                    v = s.get("value")
                    if v is not None:
                        try:
                            sparkline_values = sparkline_values or []
                            sparkline_values.append(float(v))
                        except (ValueError, TypeError):
                            pass

        return Template(template_name, context={
            "widget": widget,
            "widget_id": widget_id,
            "data": data,
            "error": error,
            "chart_config": chart_config,
            "pivot_data": pivot_data,
            "config": config or {},
            "sparkline_values": sparkline_values or [],
            "delta": config.get("delta") if config else None,
            "delta_type": config.get("delta_type") if config else None,
        })

    # ─────────────────────────────────────────────────────────────
    # Snapshots
    # ─────────────────────────────────────────────────────────────

    @get("/queries/{query_id:int}/snapshots")
    async def list_snapshots(self, query_id: int, limit: int = 50) -> Response:
        from tusk.bi.db import get_snapshots
        snapshots = get_snapshots(query_id, limit)
        return Response(
            content=msgspec.json.encode({"snapshots": snapshots}),
            media_type="application/json",
        )

    @get("/queries/{query_id:int}/sparkline")
    async def query_sparkline(self, query_id: int, limit: int = 20) -> Response:
        """Get last N snapshot values for sparkline visualization"""
        from tusk.bi.db import get_snapshots
        snapshots = get_snapshots(query_id, limit)
        values = [s.get("value") for s in reversed(snapshots) if s.get("value") is not None]
        return Response(
            content=msgspec.json.encode({"values": values}),
            media_type="application/json",
        )

    @post("/queries/{query_id:int}/schedule")
    async def create_schedule(self, request: Request, query_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import create_schedule

        cron_expr = data.get("cron_expr", "").strip()
        if not cron_expr:
            return Response(
                content=msgspec.json.encode({"error": "cron_expr is required"}),
                media_type="application/json",
                status_code=400,
            )

        schedule_id = create_schedule(
            query_id=query_id,
            cron_expr=cron_expr,
            max_snapshots=data.get("max_snapshots", 100),
        )

        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Schedule created", "success"))

        return Response(
            content=msgspec.json.encode({"success": True, "id": schedule_id}),
            media_type="application/json",
        )

    @delete("/schedules/{schedule_id:int}", status_code=200)
    async def delete_schedule(self, request: Request, schedule_id: int) -> Response:
        from tusk.bi.db import delete_schedule
        delete_schedule(schedule_id)

        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Schedule deleted", "success"))

        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )

    @get("/schedules")
    async def list_schedules(self) -> Response:
        from tusk.bi.db import get_schedules
        schedules = get_schedules()
        return Response(
            content=msgspec.json.encode({"schedules": schedules}),
            media_type="application/json",
        )

    @put("/schedules/{schedule_id:int}/toggle")
    async def toggle_schedule(self, request: Request, schedule_id: int, data: dict = Body()) -> Response:
        from tusk.bi.db import toggle_schedule
        enabled = data.get("enabled", True)
        toggle_schedule(schedule_id, enabled)
        status_text = "enabled" if enabled else "disabled"
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast(f"Schedule {status_text}", "success"))
        return Response(
            content=msgspec.json.encode({"success": True, "enabled": enabled}),
            media_type="application/json",
        )

    # ─────────────────────────────────────────────────────────────
    # Dashboard Provisioning
    # ─────────────────────────────────────────────────────────────

    @post("/dashboards/provision")
    async def provision_dashboard(self, request: Request, data: dict = Body()) -> Response:
        from tusk.bi.db import provision_dashboard
        try:
            dashboard_id = provision_dashboard(data, source_id=data.get("source_id"))
            if is_htmx(request):
                return Response(content=b"", headers=htmx_toast("Dashboard provisioned", "success"))
            return Response(
                content=msgspec.json.encode({"success": True, "id": dashboard_id}),
                media_type="application/json",
            )
        except Exception as e:
            return Response(
                content=msgspec.json.encode({"error": str(e)}),
                media_type="application/json",
                status_code=400,
            )

    # ─────────────────────────────────────────────────────────────
    # Embed Tokens (UI management)
    # ─────────────────────────────────────────────────────────────

    @get("/embed-tokens/{dashboard_id:int}")
    async def list_embed_tokens(self, dashboard_id: int) -> Response:
        from tusk.bi.db import get_embed_tokens
        tokens = get_embed_tokens(dashboard_id)
        return Response(
            content=msgspec.json.encode({"tokens": tokens}),
            media_type="application/json",
        )

    @post("/embed-tokens/{dashboard_id:int}")
    async def create_embed_token_ui(self, request: Request, dashboard_id: int, data: dict = Body()) -> Response:
        """Generate embed token from the dashboard UI (requires auth via session)."""
        from datetime import datetime, timedelta
        from tusk.bi.db import get_dashboard, create_embed_token
        from tusk.bi.routes.embed import _generate_embed_token

        dashboard = get_dashboard(dashboard_id)
        if not dashboard:
            return Response(
                content=msgspec.json.encode({"error": "Dashboard not found"}),
                media_type="application/json",
                status_code=404,
            )

        rls_clauses = data.get("rls_clauses", {})
        expires_in = data.get("expires_in_seconds", 3600)
        app_id = data.get("app_id", "")
        expires_at = (datetime.now() + timedelta(seconds=expires_in)).isoformat()

        token = _generate_embed_token(dashboard_id, rls_clauses, expires_at, app_id)

        create_embed_token(
            dashboard_id=dashboard_id,
            token=token,
            rls_clauses=json.dumps(rls_clauses),
            expires_at=expires_at,
            app_id=app_id,
        )

        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost:8000"))
        embed_url = f"{scheme}://{host}/embed/dashboard/{dashboard_id}?token={token}"

        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Embed token created", "success"))

        return Response(
            content=msgspec.json.encode({
                "success": True, "token": token,
                "embed_url": embed_url, "expires_at": expires_at,
            }),
            media_type="application/json",
        )

    @delete("/embed-tokens/{token_id:int}", status_code=200)
    async def revoke_embed_token(self, request: Request, token_id: int) -> Response:
        from tusk.bi.db import delete_embed_token
        delete_embed_token(token_id)
        if is_htmx(request):
            return Response(content=b"", headers=htmx_toast("Embed token revoked", "success"))
        return Response(
            content=msgspec.json.encode({"success": True}),
            media_type="application/json",
        )
