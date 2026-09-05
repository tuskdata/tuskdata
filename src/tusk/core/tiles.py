"""Vector tiles (Mapbox Vector Tile) from a saved query.

``GET /api/tiles/{query_id}/{z}/{x}/{y}`` runs the saved query inside
``ST_AsMVT`` so any MapLibre / Mapbox / deck.gl client can draw the
query's geometry as a live layer, straight from PostGIS, without a GeoJSON
round trip. ``/api/tiles/{query_id}/tilejson`` is the TileJSON that points at
it (with the layer's extent as bounds).

The query is wrapped, never modified: it has to be a read-only statement
with one geometry column. Everything else in the SELECT becomes feature
properties. The geometry column is found by asking PostgreSQL for the
result's column types (``LIMIT 0``), so aliases and functions are fine.
"""

from __future__ import annotations

import time

from tusk.core.connection import ConnectionConfig
from tusk.core.logging import get_logger

log = get_logger("tiles")

EXTENT = 4096
BUFFER = 64
LAYER = "query"
_shape_cache: dict[str, tuple[float, dict]] = {}
_SHAPE_TTL = 300


def _strip(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


async def describe_query(conn: ConnectionConfig, sql: str) -> dict:
    """Column names, the geometry column, its SRID and the extent of the result."""
    from tusk.engines.postgres import execute_query

    key = f"{conn.id}:{hash(sql)}"
    hit = _shape_cache.get(key)
    if hit and time.time() - hit[0] < _SHAPE_TTL:
        return hit[1]
    inner = _strip(sql)
    oid_res = await execute_query(conn, "SELECT oid FROM pg_type WHERE typname IN ('geometry', 'geography')")
    if oid_res.error:
        raise ValueError(oid_res.error)
    geo_oids = {str(r[0]) for r in oid_res.rows}
    if not geo_oids:
        raise ValueError("PostGIS is not installed on this connection")
    probe = await execute_query(conn, f"SELECT * FROM ({inner}) AS _q LIMIT 0")
    if probe.error:
        raise ValueError(probe.error)
    columns = [c.name for c in probe.columns]
    geom = next((c.name for c in probe.columns if str(c.type) in geo_oids), None)
    if not geom:
        raise ValueError("the query returns no geometry column")
    ext = await execute_query(
        conn,
        f'SELECT ST_SRID(g::geometry), ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM '
        f'(SELECT (SELECT "{geom}" FROM ({inner}) AS _s WHERE "{geom}" IS NOT NULL LIMIT 1) AS g, '
        f'ST_Extent(ST_Transform("{geom}"::geometry, 4326)) AS e FROM ({inner}) AS _q) AS _x',
    )
    srid, bounds = 4326, None
    if ext.error:
        log.debug("tiles_extent_failed", error=ext.error)
    elif ext.rows and ext.rows[0][0] is not None:
        row = ext.rows[0]
        srid = int(row[0] or 4326)
        if row[1] is not None:
            bounds = [float(row[1]), float(row[2]), float(row[3]), float(row[4])]
    shape = {"columns": columns, "geometry": geom, "srid": srid or 4326, "bounds": bounds}
    _shape_cache[key] = (time.time(), shape)
    return shape


def tile_sql(sql: str, geom: str, columns: list[str], z: int, x: int, y: int) -> str:
    inner = _strip(sql)
    props = ", ".join(f'"{c}"' for c in columns if c != geom)
    props = (props + ", ") if props else ""
    return (
        f"WITH bounds AS (SELECT ST_TileEnvelope({z}, {x}, {y}) AS geom, "
        f"ST_TileEnvelope({z}, {x}, {y}, margin => (64.0 / 4096)) AS geom_margin), "
        f"mvtgeom AS (SELECT {props}"
        f'ST_AsMVTGeom(ST_Transform(_q."{geom}"::geometry, 3857), bounds.geom, {EXTENT}, {BUFFER}, true) AS geom '
        f"FROM ({inner}) AS _q, bounds "
        f'WHERE _q."{geom}" IS NOT NULL AND ST_Transform(_q."{geom}"::geometry, 3857) && bounds.geom_margin) '
        f"SELECT ST_AsMVT(mvtgeom.*, '{LAYER}', {EXTENT}, 'geom') FROM mvtgeom"
    )


async def render_tile(conn: ConnectionConfig, sql: str, z: int, x: int, y: int) -> bytes:
    """One tile as MVT bytes (empty bytes when nothing intersects)."""
    from tusk.engines.postgres import execute_query

    if not (0 <= z <= 24) or not (0 <= x < 2**z) or not (0 <= y < 2**z):
        raise ValueError("tile coordinates out of range")
    shape = await describe_query(conn, sql)
    res = await execute_query(conn, tile_sql(sql, shape["geometry"], shape["columns"], z, x, y))
    if res.error:
        raise ValueError(res.error)
    if not res.rows or res.rows[0][0] is None:
        return b""
    data = res.rows[0][0]
    return bytes(data) if not isinstance(data, bytes) else data


def tilejson(query_id: int, name: str, shape: dict, base_url: str, token: str | None = None) -> dict:
    suffix = f"?token={token}" if token else ""
    return {
        "tilejson": "3.0.0",
        "name": name,
        "scheme": "xyz",
        "tiles": [f"{base_url.rstrip('/')}/api/tiles/{query_id}/{{z}}/{{x}}/{{y}}{suffix}"],
        "vector_layers": [{"id": LAYER, "fields": {c: "" for c in shape["columns"] if c != shape["geometry"]}}],
        "bounds": shape.get("bounds") or [-180, -85.0511, 180, 85.0511],
        "minzoom": 0,
        "maxzoom": 22,
    }
