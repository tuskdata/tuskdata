"""Vector tiles from a saved query and the H3 density grid."""

from __future__ import annotations

import asyncio
import os
import uuid

import psycopg
import pytest

from tusk.core import h3grid, tiles
from tusk.core.connection import ConnectionConfig

ADMIN_DSN = os.environ.get("TUSK_TEST_PG_DSN", "postgresql://postgres@localhost:5432/postgres")


def test_h3_aggregate_counts_and_polygons():
    pts = [(18.47, -69.94), (18.4701, -69.9401), (18.5, -69.9), (None, 1.0)]
    fc = h3grid.aggregate(pts, 8)
    assert fc["type"] == "FeatureCollection" and fc["resolution"] == 8
    assert fc["points"] == 3 and fc["skipped"] == 1
    assert fc["cells"] == 2 and fc["max_count"] == 2
    ring = fc["features"][0]["geometry"]["coordinates"][0]
    assert len(ring) == 7 and ring[0] == ring[-1]        # closed hexagon
    assert -70 < ring[0][0] < -69 and 18 < ring[0][1] < 19  # [lon, lat]
    assert h3grid.aggregate([], 3)["resolution"] == h3grid.MIN_RES


def test_points_sql_shapes():
    assert "ST_Centroid" in h3grid.points_sql("public", "t", "geom", None, None)
    assert '"lat"::double precision' in h3grid.points_sql("public", "t", None, "lat", "lon")


@pytest.fixture(scope="module")
def postgis_points():
    name = f"tusk_test_tiles_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except Exception:
        pytest.skip("no local PostgreSQL")
    admin.execute(f'CREATE DATABASE "{name}"')
    try:
        with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}", autocommit=True) as c:
            try:
                c.execute("CREATE EXTENSION postgis")
            except Exception:
                pytest.skip("PostGIS not available")
            c.execute("""
                CREATE TABLE shops (id serial PRIMARY KEY, name text, geom geometry(Point, 4326));
                INSERT INTO shops (name, geom) VALUES
                  ('a', ST_SetSRID(ST_MakePoint(-69.94, 18.47), 4326)),
                  ('b', ST_SetSRID(ST_MakePoint(-69.92, 18.48), 4326));
            """)
        yield ConnectionConfig(id="t-tiles", name="tiles", type="postgres", host="localhost", port=5432,
                               database=name, user="postgres", password="")
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


def test_tile_from_query(postgis_points):
    sql = "SELECT id, name AS label, geom FROM shops"
    shape = asyncio.run(tiles.describe_query(postgis_points, sql))
    assert shape["geometry"] == "geom" and shape["srid"] == 4326 and shape["columns"] == ["id", "label", "geom"]
    assert shape["bounds"] and shape["bounds"][0] <= -69.94 and shape["bounds"][2] >= -69.92
    # z/x/y covering the first point (Santo Domingo) at zoom 10
    import math
    z, lon, lat = 10, -69.94, 18.47
    x = int((lon + 180) / 360 * 2**z)
    y = int((1 - math.log(math.tan(math.radians(lat)) + 1 / math.cos(math.radians(lat))) / math.pi) / 2 * 2**z)
    data = asyncio.run(tiles.render_tile(postgis_points, sql, z, x, y))
    assert data and b"query" in data and b"label" in data
    # a tile on the other side of the world is empty
    assert asyncio.run(tiles.render_tile(postgis_points, sql, 10, 10, 10)) == b""
    tj = tiles.tilejson(7, "shops", shape, "http://x", token="tusk_abc")
    assert tj["tiles"] == ["http://x/api/tiles/7/{z}/{x}/{y}?token=tusk_abc"]
    assert "label" in tj["vector_layers"][0]["fields"] and "geom" not in tj["vector_layers"][0]["fields"]
    with pytest.raises(ValueError):
        asyncio.run(tiles.render_tile(postgis_points, "SELECT id FROM shops", 1, 0, 0))
