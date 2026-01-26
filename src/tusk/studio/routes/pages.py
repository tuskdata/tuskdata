"""Page routes for Tusk Studio"""

from litestar import Controller, get
from litestar.response import Template, Response

from tusk.core.deps import get_available_features


def get_base_context(active_page: str) -> dict:
    """Get base context with features for all pages"""
    return {
        "active_page": active_page,
        "features": get_available_features(),
    }


class PageController(Controller):
    """Serves HTML pages"""

    path = "/"

    @get("/")
    async def index(self) -> Template:
        """Main studio page"""
        return Template("index.html", context=get_base_context("studio"))

    @get("/admin")
    async def admin(self) -> Template:
        """Admin dashboard page"""
        return Template("admin.html", context=get_base_context("admin"))

    @get("/data")
    async def data(self) -> Template:
        """Data/ETL pipeline builder page"""
        return Template("data.html", context=get_base_context("data"))

    @get("/cluster")
    async def cluster(self) -> Template:
        """Cluster dashboard page"""
        return Template("cluster.html", context=get_base_context("cluster"))

    @get("/login")
    async def login(self) -> Template:
        """Login page"""
        return Template("login.html", context=get_base_context("login"))

    @get("/users")
    async def users(self) -> Template:
        """User management page (admin)"""
        return Template("users.html", context=get_base_context("users"))

    @get("/profile")
    async def profile(self) -> Template:
        """User profile page"""
        return Template("profile.html", context=get_base_context("profile"))

    @get("/favicon.ico")
    async def favicon(self) -> Response:
        """Return a simple SVG favicon"""
        # Mammoth emoji as SVG favicon
        svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
            <text y=".9em" font-size="90">🦣</text>
        </svg>'''
        return Response(
            content=svg,
            media_type="image/svg+xml",
            headers={"Cache-Control": "public, max-age=86400"}
        )
