"""Page routes for Tusk Studio"""

from litestar import get, Request
from litestar.response import Template, Response

from tusk.core.auth import get_session, get_user_by_id, get_user_groups, get_user_permissions
from tusk.core.config import get_config
from tusk.core.connection import list_connections
from tusk.studio.routes.base import TuskController

SESSION_COOKIE = "tusk_session"


class PageController(TuskController):
    """Serves HTML pages"""

    path = "/"

    @get("/")
    async def index(self) -> Template:
        """Main studio page"""
        return self.render("index.html", active_page="studio")

    @get("/admin")
    async def admin(self) -> Template:
        """Admin dashboard page"""
        conns = list_connections()
        pg_conns = [
            {"id": c.id, "name": c.name}
            for c in conns if c.type == "postgres"
        ]
        return self.render("admin.html", active_page="admin", pg_connections=pg_conns)

    @get("/data")
    async def data(self) -> Template:
        """Data/ETL pipeline builder page"""
        return self.render("data.html", active_page="data")

    @get("/login")
    async def login(self) -> Template:
        """Login page"""
        return self.render("login.html", active_page="login")

    @get("/users")
    async def users(self) -> Template:
        """User management page (admin)"""
        return self.render("users.html", active_page="users")

    @get("/profile")
    async def profile(self, request: Request) -> Template:
        """User profile page — server-rendered with user data"""
        config = get_config()
        user = None
        groups = []
        permissions = []

        if config.auth_mode == "multi":
            session_id = request.cookies.get(SESSION_COOKIE)
            if session_id:
                session = get_session(session_id)
                if session:
                    user = get_user_by_id(session.user_id)
                    if user:
                        groups = [{"id": g.id, "name": g.name} for g in get_user_groups(user.id)]
                        permissions = list(get_user_permissions(user.id))

        return self.render(
            "profile.html",
            active_page="profile",
            profile_user=user,
            profile_groups=groups,
            profile_permissions=permissions,
        )

    @get("/favicon.ico")
    async def favicon(self) -> Response:
        """Return a simple SVG favicon"""
        # Mammoth emoji as SVG favicon
        svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
            <text y=".9em" font-size="90">&#129443;</text>
        </svg>'''
        return Response(
            content=svg,
            media_type="image/svg+xml",
            headers={"Cache-Control": "public, max-age=86400"}
        )
