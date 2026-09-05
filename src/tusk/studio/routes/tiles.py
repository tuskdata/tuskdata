"""Vector tiles + TileJSON for saved queries.

Map clients cannot send custom headers for tile requests, so in
multi-user mode the URL carries a personal API token (``?token=tusk_…``);
the browser session works too for Tusk's own pages. Single-user mode is
open like the rest of the app.
"""

from __future__ import annotations

from litestar import Controller, Request, get
from litestar.response import Response

from tusk.core import tiles
from tusk.core.config import get_config
from tusk.core.connection import get_connection
from tusk.core.history import QueryHistory
from tusk.studio.routes.base import get_request_user


def _authorized(request: Request) -> tuple[bool, str | None]:
    """(ok, token) — token echoed into TileJSON so the URLs keep working."""
    token = request.query_params.get("token")
    if get_config().auth_mode != "multi":
        return True, None
    if get_request_user(request):
        return True, token
    if token:
        from tusk.core.api_tokens import verify_token

        if verify_token(token):
            return True, token
    return False, None


def _load(query_id: int):
    saved = QueryHistory().get_saved_query(query_id)
    if not saved:
        return None, None, "saved query not found"
    conn = get_connection(saved.connection_id or "")
    if not conn or conn.type != "postgres":
        return None, None, "the saved query is not on a PostgreSQL connection"
    from tusk.studio.routes.mcp_tools import is_read_only_sql

    ok, reason = is_read_only_sql(saved.sql)
    if not ok:
        return None, None, f"only read-only queries can be served as tiles: {reason}"
    return saved, conn, None


class TilesController(Controller):
    path = "/api/tiles"
    tags = ["tiles"]

    @get("/{query_id:int}/tilejson")
    async def tilejson(self, request: Request, query_id: int) -> Response:
        ok, token = _authorized(request)
        if not ok:
            return Response(content={"error": "authentication required (?token=tusk_...)"}, status_code=401)
        saved, conn, err = _load(query_id)
        if err:
            return Response(content={"error": err}, status_code=404)
        try:
            shape = await tiles.describe_query(conn, saved.sql)
        except ValueError as exc:
            return Response(content={"error": str(exc)}, status_code=400)
        base = f"{request.url.scheme}://{request.url.netloc}"
        return Response(content=tiles.tilejson(query_id, saved.name, shape, base, token), headers={"Cache-Control": "max-age=60"})

    @get("/{query_id:int}/{z:int}/{x:int}/{y:int}")
    async def tile(self, request: Request, query_id: int, z: int, x: int, y: int) -> Response:
        ok, _ = _authorized(request)
        if not ok:
            return Response(content=b"", status_code=401)
        saved, conn, err = _load(query_id)
        if err:
            return Response(content={"error": err}, status_code=404)
        try:
            data = await tiles.render_tile(conn, saved.sql, z, x, y)
        except ValueError as exc:
            return Response(content={"error": str(exc)}, status_code=400)
        if not data:
            return Response(content=b"", status_code=204, headers={"Cache-Control": "max-age=60"})
        return Response(
            content=data,
            media_type="application/vnd.mapbox-vector-tile",
            headers={"Cache-Control": "max-age=60", "Access-Control-Allow-Origin": "*"},
        )
