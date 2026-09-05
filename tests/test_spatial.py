"""Spatial grounding: place tokens, place tables, prompt rendering, and an
end-to-end run against a local PostGIS when one is available."""

from __future__ import annotations

import asyncio
import os
import uuid

import psycopg
import pytest

from tusk.core import spatial
from tusk.core.connection import ConnectionConfig

CATALOG = {
    "geo_area": {
        "cols": [{"name": "id", "type": "uuid", "nn": True}, {"name": "name", "type": "character varying", "nn": True},
                 {"name": "level", "type": "integer", "nn": False}, {"name": "geom", "type": "geometry", "nn": False}],
        "pks": ["id"], "fks": [],
    },
    "osm_poi": {
        "cols": [{"name": "id", "type": "bigint", "nn": True}, {"name": "kind", "type": "text", "nn": False},
                 {"name": "tags", "type": "jsonb", "nn": False}, {"name": "lat", "type": "double precision", "nn": False},
                 {"name": "lon", "type": "double precision", "nn": False}],
        "pks": ["id"], "fks": [],
    },
    "orders": {"cols": [{"name": "id", "type": "integer", "nn": True}, {"name": "name", "type": "text", "nn": False}], "pks": ["id"], "fks": []},
}


def test_candidate_place_tokens_keeps_proper_nouns_only():
    toks = spatial.candidate_place_tokens(
        "Quiero que me enseñes todos los restaurantes vegetarianos que estén en el sector Piantini del Distrito Nacional"
    )
    assert "Piantini" in toks
    assert "Distrito Nacional" in toks
    assert "Quiero" not in toks
    assert spatial.candidate_place_tokens("show me all orders") == []


def test_place_tables_prefers_polygons_then_points_then_latlon():
    info = spatial.SpatialInfo(postgis="3.6.1", geometry={"geo_area": [{"column": "geom", "kind": "geometry", "type": "MULTIPOLYGON", "srid": 4326, "dims": 2}]},
                               latlon={"osm_poi": {"lat": "lat", "lon": "lon"}})
    places = spatial.place_tables(CATALOG, info)
    assert places[0] == ("geo_area", "name", "geom")
    # orders has a name but nothing spatial → not a place table
    assert all(t != "orders" for t, _, _ in places)


def test_render_spatial_section_mentions_everything():
    info = spatial.SpatialInfo(postgis="3.6.1", extensions={"postgis": "3.6.1", "h3": "4.1"},
                               geometry={"geo_area": [{"column": "geom", "kind": "geometry", "type": "MULTIPOLYGON", "srid": 4326, "dims": 2}]},
                               latlon={"osm_poi": {"lat": "lat", "lon": "lon"}})
    text = spatial.render_spatial_section(
        info,
        {"osm_poi": {"tags": ["amenity: cafe | restaurant", "diet:vegetarian: yes"]}},
        [{"token": "Piantini", "table": "geo_area", "name_col": "name", "geom_col": "geom", "value": "PIANTINI", "extra": {"level": 8}}],
    )
    assert "PostGIS 3.6.1" in text and "h3" in text
    assert "geo_area.geom: geometry MULTIPOLYGON SRID 4326" in text
    assert "osm_poi: lat/lon columns lat, lon" in text
    assert "amenity: cafe | restaurant" in text
    assert "\"Piantini\" → geo_area.name = 'PIANTINI' [level=8]" in text
    assert spatial.render_spatial_section(spatial.SpatialInfo(), {}, []) == ""


# ── Integration: real PostGIS if the local server has it ─────────────────

ADMIN_DSN = os.environ.get("TUSK_TEST_PG_DSN", "postgresql://postgres@localhost:5432/postgres")


@pytest.fixture(scope="module")
def postgis_db():
    name = f"tusk_test_spatial_{uuid.uuid4().hex[:8]}"
    try:
        admin = psycopg.connect(ADMIN_DSN, autocommit=True)
    except Exception:
        pytest.skip("no local PostgreSQL")
    try:
        admin.execute(f'CREATE DATABASE "{name}"')
        with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{name}", autocommit=True) as c:
            try:
                c.execute("CREATE EXTENSION postgis")
            except Exception:
                admin.execute(f'DROP DATABASE "{name}"')
                pytest.skip("PostGIS not available")
            c.execute("""
                CREATE TABLE areas (id serial PRIMARY KEY, name varchar NOT NULL, level int, geom geometry(MultiPolygon, 4326));
                INSERT INTO areas (name, level, geom) VALUES
                  ('PIANTINI', 8, ST_Multi(ST_GeomFromText('POLYGON((-69.95 18.46,-69.93 18.46,-69.93 18.48,-69.95 18.48,-69.95 18.46))', 4326))),
                  ('NACO', 8, ST_Multi(ST_GeomFromText('POLYGON((-69.93 18.46,-69.91 18.46,-69.91 18.48,-69.93 18.48,-69.93 18.46))', 4326)));
                CREATE TABLE pois (id bigserial PRIMARY KEY, kind text, tags jsonb, lat double precision, lon double precision);
                INSERT INTO pois (kind, tags, lat, lon) VALUES
                  ('node', '{"amenity":"restaurant","diet:vegetarian":"yes","name":"Verde"}', 18.47, -69.94),
                  ('node', '{"amenity":"cafe","name":"Cafe X"}', 18.47, -69.92),
                  ('node', '{"amenity":"restaurant","cuisine":"pizza"}', 18.47, -69.94);
            """)
        yield name
    finally:
        admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        admin.close()


def test_grounding_end_to_end(postgis_db):
    conn = ConnectionConfig(id="t-spatial", name="spatial test", type="postgres", host="localhost", port=5432,
                            database=postgis_db, user="postgres", password="")
    from tusk.core.catalog import fetch_catalog

    async def run():
        catalog = await fetch_catalog(conn, with_indexes=False)
        info = await spatial.fetch_spatial(conn, catalog)
        profiles = await spatial.profile_columns(conn, catalog)
        spatial.remember_catalog_columns(conn.id, catalog)
        places = spatial.place_tables(catalog, info)
        found = await spatial.lookup_places(conn, places, spatial.candidate_place_tokens("restaurantes vegetarianos en Piantini"))
        return catalog, info, profiles, places, found

    catalog, info, profiles, places, found = asyncio.run(run())
    assert info.postgis and info.geometry["areas"][0]["type"] == "MULTIPOLYGON" and info.geometry["areas"][0]["srid"] == 4326
    assert info.latlon["pois"] == {"lat": "lat", "lon": "lon"}
    tags = profiles["pois"]["tags"]
    assert any(line.startswith("amenity:") and "restaurant" in line for line in tags)
    assert any(line.startswith("diet:vegetarian") for line in tags)
    assert "kind" not in profiles.get("pois", {})  # single-valued columns are noise, not a filter
    assert places[0][0] == "areas"
    assert found and found[0]["value"] == "PIANTINI" and found[0]["extra"] == {"level": 8}


def test_spatial_health_and_geo_import(postgis_db, tmp_path):
    """Health report lists the geometry columns; a GeoJSON import ends up as a
    real geometry column with a GIST index."""
    import json

    from tusk.core.connection import add_connection, delete_connection
    from tusk.engines.polars_engine import DataSource, Pipeline, import_to_postgres

    conn = ConnectionConfig(id="t-spatial-h", name="spatial health", type="postgres", host="localhost", port=5432,
                            database=postgis_db, user="postgres", password="")
    health = asyncio.run(spatial.spatial_health(conn))
    assert health["postgis"]
    areas = next(c for c in health["columns"] if c["table"] == "areas")
    assert areas["srid"] == 4326 and areas["invalid"] == 0 and areas["extent"]
    assert not areas["has_index"]                      # small table → no finding either
    assert not [f for f in health["findings"] if f["kind"] == "missing_index"]

    geojson = tmp_path / "shops.geojson"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"name": "Verde"}, "geometry": {"type": "Point", "coordinates": [-69.94, 18.47]}},
        {"type": "Feature", "properties": {"name": "Cafe"}, "geometry": {"type": "Point", "coordinates": [-69.92, 18.47]}},
    ]}))
    import tusk.core.connection as conn_mod

    monkeypatch_file = tmp_path / "connections.toml"  # never the user's file
    original = conn_mod.CONN_FILE
    conn_mod.CONN_FILE = monkeypatch_file
    add_connection(conn, persist=False)
    try:
        pipeline = Pipeline(id="p", name="geo", sources=[DataSource(id="s", name="shops", source_type="geo", path=str(geojson))],
                            transforms=[], output_source_id="s")
        out = asyncio.run(import_to_postgres(pipeline, "shops", conn.id))
    finally:
        delete_connection(conn.id)
        conn_mod.CONN_FILE = original
    assert out.get("success"), out
    assert out["geometry"] and out["geometry"]["column"] == "geom" and out["geometry"]["srid"] == 4326
    with psycopg.connect(ADMIN_DSN.rsplit("/", 1)[0] + f"/{postgis_db}") as c:
        assert c.execute("SELECT count(*) FROM shops WHERE geom IS NOT NULL").fetchone()[0] == 2
        assert c.execute("SELECT ST_SRID(geom) FROM shops LIMIT 1").fetchone()[0] == 4326
        assert c.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'shops' AND indexdef ILIKE '%gist%'").fetchone()
        # Explore-style per-table view sees it too
    cols = asyncio.run(spatial.table_spatial(conn, "public", "shops"))
    assert cols and cols[0]["has_index"] and cols[0]["column"] == "geom"
