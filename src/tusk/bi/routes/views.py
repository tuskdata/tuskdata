"""BI plugin page controllers (full HTML pages)"""

from litestar import Request, Response, get
from litestar.response import Template

from tusk.studio.routes.base import TuskController, get_base_context


def _sidebar_context(current_page: str, current_dashboard_id: int | None = None) -> dict:
    """Build shared sidebar context for all BI views."""
    from tusk.bi import __version__
    from tusk.bi.db import get_dashboards
    return {
        "bi_version": __version__,
        "dashboards": get_dashboards(),
        "current_page": current_page,
        "current_dashboard_id": current_dashboard_id,
    }


class BIPageController(TuskController):
    """BI plugin page routes"""

    path = "/bi"

    @get("/", name="bi:index")
    async def index(self, request: Request) -> Template:
        """Default BI page — show default dashboard or overview"""
        from tusk.bi.db import get_default_dashboard

        default = get_default_dashboard()
        if default:
            from tusk.bi.db import get_widgets, get_dashboard_variables, get_dashboard_tabs
            widgets = get_widgets(default["id"])
            variables = get_dashboard_variables(default["id"])
            tabs = get_dashboard_tabs(default["id"])
            return self.render(
                "plugins/bi/dashboard.html",
                active_page="bi",
                dashboard=default,
                widgets=widgets,
                variables=variables,
                tabs=tabs,
                active_tab_id=None,
                **_sidebar_context("dashboard_view", default["id"]),
            )

        from tusk.bi.db import get_overview_stats, get_connected_apps, get_query_volume_7d, get_recent_dashboards
        return self.render(
            "plugins/bi/overview.html",
            active_page="bi",
            stats=get_overview_stats(),
            connected_apps=get_connected_apps(),
            query_volume=get_query_volume_7d(),
            recent_dashboards=get_recent_dashboards(),
            **_sidebar_context("overview"),
        )

    @get("/overview", name="bi:overview")
    async def overview(self, request: Request) -> Template:
        """BI Engine overview page"""
        from tusk.bi.db import get_overview_stats, get_connected_apps, get_query_volume_7d, get_recent_dashboards
        return self.render(
            "plugins/bi/overview.html",
            active_page="bi",
            stats=get_overview_stats(),
            connected_apps=get_connected_apps(),
            query_volume=get_query_volume_7d(),
            recent_dashboards=get_recent_dashboards(),
            **_sidebar_context("overview"),
        )

    @get("/queries", name="bi:queries")
    async def query_list(self, request: Request) -> Template:
        """Query gallery"""
        from tusk.bi.db import get_saved_queries
        queries = get_saved_queries()
        return self.render(
            "plugins/bi/query_list.html",
            active_page="bi",
            queries=queries,
            **_sidebar_context("queries"),
        )

    @get("/queries/new", name="bi:query_new")
    async def query_new(self, request: Request) -> Template:
        """New query editor"""
        from tusk.bi.db import get_data_sources
        sources = get_data_sources()
        return self.render(
            "plugins/bi/query_editor.html",
            active_page="bi",
            query=None,
            sources=sources,
            **_sidebar_context("query_editor"),
        )

    @get("/queries/{query_id:int}", name="bi:query_edit")
    async def query_edit(self, request: Request, query_id: int) -> Template:
        """Edit existing query"""
        from tusk.bi.db import get_saved_query, get_data_sources
        query = get_saved_query(query_id)
        sources = get_data_sources()
        return self.render(
            "plugins/bi/query_editor.html",
            active_page="bi",
            query=query,
            sources=sources,
            **_sidebar_context("query_editor"),
        )

    @get("/explore", name="bi:explore")
    async def explore(self, request: Request) -> Template:
        """Data explorer"""
        from tusk.bi.db import get_data_sources
        sources = get_data_sources()
        return self.render(
            "plugins/bi/explore.html",
            active_page="bi",
            sources=sources,
            **_sidebar_context("explore"),
        )

    @get("/dashboards", name="bi:dashboards")
    async def dashboard_list(self, request: Request) -> Template:
        """Dashboard list"""
        return self.render(
            "plugins/bi/dashboard_list.html",
            active_page="bi",
            **_sidebar_context("dashboards"),
        )

    @get("/dashboards/{dashboard_id:int}", name="bi:dashboard_view")
    async def dashboard_view(self, request: Request, dashboard_id: int, tab_id: int | None = None) -> Template:
        """View a dashboard, optionally filtered by tab"""
        from tusk.bi.db import get_dashboard, get_widgets, get_dashboard_variables, get_dashboard_tabs
        dashboard = get_dashboard(dashboard_id)
        widgets = get_widgets(dashboard_id) if dashboard else []
        variables = get_dashboard_variables(dashboard_id) if dashboard else []
        tabs = get_dashboard_tabs(dashboard_id) if dashboard else []

        # Filter widgets by tab_id if provided
        active_tab_id = tab_id
        if active_tab_id is not None:
            widgets = [w for w in widgets if w.get("tab_id") == active_tab_id or w.get("tab_id") is None]

        return self.render(
            "plugins/bi/dashboard.html",
            active_page="bi",
            dashboard=dashboard,
            widgets=widgets,
            variables=variables,
            tabs=tabs,
            active_tab_id=active_tab_id,
            **_sidebar_context("dashboard_view", dashboard_id),
        )

    @get("/dashboards/{dashboard_id:int}/edit", name="bi:dashboard_edit")
    async def dashboard_edit(self, request: Request, dashboard_id: int) -> Template:
        """Edit a dashboard"""
        from tusk.bi.db import get_dashboard, get_widgets, get_saved_queries, get_dashboard_variables, get_dashboard_tabs, get_schedules, get_data_sources
        dashboard = get_dashboard(dashboard_id)
        widgets = get_widgets(dashboard_id) if dashboard else []
        queries = get_saved_queries()
        variables = get_dashboard_variables(dashboard_id) if dashboard else []
        tabs = get_dashboard_tabs(dashboard_id) if dashboard else []
        schedules = get_schedules()
        sources = get_data_sources()
        return self.render(
            "plugins/bi/dashboard_edit.html",
            active_page="bi",
            dashboard=dashboard,
            widgets=widgets,
            queries=queries,
            variables=variables,
            tabs=tabs,
            schedules=schedules,
            sources=sources,
            **_sidebar_context("dashboard_view", dashboard_id),
        )

    @get("/query-builder", name="bi:query_builder")
    async def query_builder(self, request: Request) -> Template:
        """Visual query builder"""
        from tusk.bi.db import get_data_sources
        sources = get_data_sources()
        return self.render(
            "plugins/bi/query_builder.html",
            active_page="bi",
            sources=sources,
            **_sidebar_context("query_builder"),
        )

    @get("/dashboards/{dashboard_id:int}/view", name="bi:dashboard_view_vars")
    async def dashboard_view_with_vars(self, request: Request, dashboard_id: int, tab_id: int | None = None) -> Template:
        """View a dashboard with variables loaded"""
        from tusk.bi.db import get_dashboard, get_widgets, get_dashboard_variables, get_dashboard_tabs
        dashboard = get_dashboard(dashboard_id)
        widgets = get_widgets(dashboard_id) if dashboard else []
        variables = get_dashboard_variables(dashboard_id) if dashboard else []
        tabs = get_dashboard_tabs(dashboard_id) if dashboard else []

        active_tab_id = tab_id
        if active_tab_id is not None:
            widgets = [w for w in widgets if w.get("tab_id") == active_tab_id or w.get("tab_id") is None]

        return self.render(
            "plugins/bi/dashboard.html",
            active_page="bi",
            dashboard=dashboard,
            widgets=widgets,
            variables=variables,
            tabs=tabs,
            active_tab_id=active_tab_id,
            **_sidebar_context("dashboard_view", dashboard_id),
        )

    @get("/public/{token:str}", name="bi:public_dashboard")
    async def public_dashboard(self, token: str) -> Template | Response:
        """Public (no-auth) dashboard view"""
        from tusk.bi.db import get_public_link_by_token, get_dashboard, get_widgets
        link = get_public_link_by_token(token)
        if not link:
            return Response(content=b"Dashboard not found or link expired", media_type="text/html", status_code=404)

        if link.get("expires_at"):
            from datetime import datetime
            try:
                expires = datetime.fromisoformat(link["expires_at"])
                if datetime.now() > expires:
                    return Response(content=b"This link has expired", media_type="text/html", status_code=410)
            except (ValueError, TypeError):
                pass

        dashboard = get_dashboard(link["dashboard_id"])
        if not dashboard:
            return Response(content=b"Dashboard not found", media_type="text/html", status_code=404)

        widgets = get_widgets(link["dashboard_id"])
        ctx = get_base_context(active_page="bi")
        ctx.update(dashboard=dashboard, widgets=widgets, is_public=True, token=token)
        return Template(template_name="plugins/bi/dashboard_public.html", context=ctx)
