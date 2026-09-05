"""Aggregate points into H3 cells (pure Python, no database extension).

Explore uses it to draw a density map of a table's points: pick a
resolution, every row becomes a cell, cells become hexagon polygons with a
count. Works for a PostGIS point column or a lat/lon pair, on any
PostgreSQL — ``h3-pg`` is not required.
"""

from __future__ import annotations

from collections import Counter

MAX_POINTS = 200_000
MIN_RES, MAX_RES = 4, 11


def aggregate(points: list[tuple[float, float]], resolution: int) -> dict:
    """``points`` are (lat, lon). Returns a GeoJSON FeatureCollection of
    hexagons with ``count`` (and ``h3`` index) as properties, plus totals."""
    import h3

    resolution = max(MIN_RES, min(MAX_RES, int(resolution)))
    counts: Counter[str] = Counter()
    skipped = 0
    for lat, lon in points:
        if lat is None or lon is None:
            skipped += 1
            continue
        try:
            counts[h3.latlng_to_cell(float(lat), float(lon), resolution)] += 1
        except (ValueError, TypeError):
            skipped += 1
    features = []
    for cell, n in counts.items():
        ring = [[lon, lat] for lat, lon in h3.cell_to_boundary(cell)]
        ring.append(ring[0])
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {"h3": cell, "count": n},
        })
    max_count = max(counts.values(), default=0)
    return {
        "type": "FeatureCollection",
        "features": features,
        "resolution": resolution,
        "cells": len(features),
        "points": len(points) - skipped,
        "skipped": skipped,
        "max_count": max_count,
    }


def points_sql(schema: str, table: str, geom_col: str | None, lat_col: str | None, lon_col: str | None, limit: int = MAX_POINTS) -> str:
    """SQL that yields (lat, lon) for the table. Non-point geometries use
    their centroid, so polygons and lines get a hexagon too."""
    tq = f'"{schema}"."{table}"'
    if geom_col:
        g = f'"{geom_col}"'
        return (
            f"SELECT ST_Y(c), ST_X(c) FROM (SELECT ST_Centroid(ST_Transform({g}::geometry, 4326)) AS c "
            f"FROM {tq} WHERE {g} IS NOT NULL LIMIT {int(limit)}) s"
        )
    return f'SELECT "{lat_col}"::double precision, "{lon_col}"::double precision FROM {tq} WHERE "{lat_col}" IS NOT NULL AND "{lon_col}" IS NOT NULL LIMIT {int(limit)}'
