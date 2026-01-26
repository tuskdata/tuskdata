"""Geo utilities for detecting and converting geographic data using msgspec"""

from __future__ import annotations
import re
from typing import Any, Union
import msgspec


# Type alias for coordinates (use list for msgspec compatibility)
Position = list[float]


# ============================================================================
# GeoJSON Structs (based on msgspec example)
# ============================================================================

class Point(msgspec.Struct, tag=True):
    """GeoJSON Point geometry"""
    coordinates: Position


class MultiPoint(msgspec.Struct, tag=True):
    """GeoJSON MultiPoint geometry"""
    coordinates: list[Position]


class LineString(msgspec.Struct, tag=True):
    """GeoJSON LineString geometry"""
    coordinates: list[Position]


class MultiLineString(msgspec.Struct, tag=True):
    """GeoJSON MultiLineString geometry"""
    coordinates: list[list[Position]]


class Polygon(msgspec.Struct, tag=True):
    """GeoJSON Polygon geometry"""
    coordinates: list[list[Position]]


class MultiPolygon(msgspec.Struct, tag=True):
    """GeoJSON MultiPolygon geometry"""
    coordinates: list[list[list[Position]]]


class GeometryCollection(msgspec.Struct, tag=True):
    """GeoJSON GeometryCollection"""
    geometries: list["Geometry"]


# Union of all geometry types
Geometry = Union[
    Point, MultiPoint, LineString, MultiLineString,
    Polygon, MultiPolygon, GeometryCollection
]


class Feature(msgspec.Struct, tag=True):
    """GeoJSON Feature"""
    geometry: Geometry | None = None
    properties: dict | None = None
    id: str | int | None = None


class FeatureCollection(msgspec.Struct, tag=True):
    """GeoJSON FeatureCollection"""
    features: list[Feature]


# GeoJSON type union
GeoJSON = Geometry | Feature | FeatureCollection

# Decoder/encoder for GeoJSON
geojson_decoder = msgspec.json.Decoder(GeoJSON)
geojson_encoder = msgspec.json.Encoder()


def loads(data: bytes | str) -> GeoJSON:
    """Parse GeoJSON from JSON string/bytes"""
    if isinstance(data, str):
        data = data.encode()
    return geojson_decoder.decode(data)


def dumps(obj: GeoJSON) -> bytes:
    """Serialize GeoJSON to JSON bytes"""
    return geojson_encoder.encode(obj)


# ============================================================================
# Detection utilities
# ============================================================================

# Patterns for detecting geometry columns
GEOMETRY_COLUMN_NAMES = {
    "geom", "geometry", "the_geom", "shape", "geo", "location",
    "point", "polygon", "linestring", "multipoint", "multipolygon",
    "multilinestring", "geometrycollection", "wkb_geometry"
}

GEOMETRY_TYPE_NAMES = {
    "geometry", "geography", "point", "polygon", "linestring",
    "multipoint", "multipolygon", "multilinestring", "geometrycollection"
}

# WKT pattern
WKT_PATTERN = re.compile(
    r'^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(',
    re.IGNORECASE
)

# EWKT pattern (PostGIS extended WKT with SRID)
EWKT_PATTERN = re.compile(
    r'^SRID=\d+;(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(',
    re.IGNORECASE
)


def is_geometry_column(name: str, type_name: str) -> bool:
    """Check if a column is likely a geometry column based on name and type"""
    name_lower = name.lower()
    type_lower = type_name.lower()

    # Check if type indicates geometry
    for geo_type in GEOMETRY_TYPE_NAMES:
        if geo_type in type_lower:
            return True

    # Check if name indicates geometry
    if name_lower in GEOMETRY_COLUMN_NAMES:
        return True

    # Check for common suffixes/prefixes
    if name_lower.endswith("_geom") or name_lower.endswith("_geometry"):
        return True
    if name_lower.startswith("geom_") or name_lower.startswith("geometry_"):
        return True

    return False


def detect_geometry_columns(columns: list[dict], rows: list[tuple]) -> list[int]:
    """Detect which columns contain geometry data.

    Returns list of column indices that contain geometry.
    """
    geo_indices = []

    for i, col in enumerate(columns):
        col_name = col.get("name", "")
        col_type = col.get("type", "")

        # First check by type/name
        if is_geometry_column(col_name, col_type):
            geo_indices.append(i)
            continue

        # Then check actual values in first few rows
        if rows:
            for row in rows[:5]:  # Check first 5 rows
                if i < len(row) and row[i] is not None:
                    value = row[i]
                    if is_geometry_value(value):
                        geo_indices.append(i)
                        break

    return geo_indices


def is_geometry_value(value: Any) -> bool:
    """Check if a value looks like geometry data"""
    if value is None:
        return False

    if isinstance(value, (bytes, memoryview)):
        # Could be WKB (Well-Known Binary)
        if len(value) >= 5:
            return True

    if isinstance(value, str):
        value = value.strip()

        # Check for WKT
        if WKT_PATTERN.match(value):
            return True

        # Check for EWKT (PostGIS)
        if EWKT_PATTERN.match(value):
            return True

        # Check for GeoJSON
        if value.startswith("{") and '"type"' in value:
            try:
                loads(value)
                return True
            except Exception:
                pass

        # Check for hex-encoded WKB (starts with 01 or 00)
        if len(value) >= 10 and value[:2] in ("01", "00"):
            try:
                bytes.fromhex(value[:10])
                return True
            except ValueError:
                pass

    return False


# ============================================================================
# WKT Parsing
# ============================================================================

def parse_wkt(wkt: str) -> Geometry | None:
    """Parse WKT to GeoJSON geometry"""
    if not wkt:
        return None

    wkt = wkt.strip()

    # Handle EWKT (remove SRID prefix)
    if wkt.upper().startswith("SRID="):
        wkt = wkt.split(";", 1)[1] if ";" in wkt else wkt

    wkt_upper = wkt.upper()

    try:
        if wkt_upper.startswith("POINT"):
            coords = _extract_coords(wkt)
            if coords and len(coords) >= 1:
                return Point(coordinates=coords[0])

        elif wkt_upper.startswith("LINESTRING"):
            coords = _extract_coords(wkt)
            if coords:
                return LineString(coordinates=coords)

        elif wkt_upper.startswith("POLYGON"):
            rings = _extract_polygon_rings(wkt)
            if rings:
                return Polygon(coordinates=rings)

        elif wkt_upper.startswith("MULTIPOINT"):
            coords = _extract_coords(wkt)
            if coords:
                return MultiPoint(coordinates=coords)

        elif wkt_upper.startswith("MULTILINESTRING"):
            lines = _extract_multi_coords(wkt)
            if lines:
                return MultiLineString(coordinates=lines)

        elif wkt_upper.startswith("MULTIPOLYGON"):
            polygons = _extract_multipolygon_coords(wkt)
            if polygons:
                return MultiPolygon(coordinates=polygons)

    except Exception:
        pass

    return None


def _extract_coords(wkt: str) -> list[list[float]]:
    """Extract coordinates from WKT string"""
    start = wkt.find("(")
    end = wkt.rfind(")")
    if start == -1 or end == -1:
        return []

    content = wkt[start + 1:end].strip()
    content = content.replace("(", "").replace(")", "")

    coords = []
    for part in content.split(","):
        part = part.strip()
        if not part:
            continue
        nums = part.split()
        if len(nums) >= 2:
            try:
                coord = [float(nums[0]), float(nums[1])]
                if len(nums) >= 3:
                    coord.append(float(nums[2]))
                coords.append(coord)
            except ValueError:
                continue

    return coords


def _extract_polygon_rings(wkt: str) -> list[list[list[float]]]:
    """Extract polygon rings from WKT"""
    start = wkt.find("(")
    end = wkt.rfind(")")
    if start == -1 or end == -1:
        return []

    content = wkt[start + 1:end]

    rings = []
    depth = 0
    current = ""

    for char in content:
        if char == "(":
            depth += 1
            if depth == 1:
                current = ""
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                coords = []
                for part in current.split(","):
                    part = part.strip()
                    if part:
                        nums = part.split()
                        if len(nums) >= 2:
                            try:
                                coords.append([float(nums[0]), float(nums[1])])
                            except ValueError:
                                continue
                if coords:
                    rings.append(coords)
                current = ""
                continue

        if depth >= 1:
            current += char

    return rings


def _extract_multi_coords(wkt: str) -> list[list[list[float]]]:
    """Extract multi-linestring coordinates"""
    start = wkt.find("(")
    end = wkt.rfind(")")
    if start == -1 or end == -1:
        return []

    content = wkt[start + 1:end]

    lines = []
    depth = 0
    current = ""

    for char in content:
        if char == "(":
            depth += 1
            if depth == 1:
                current = ""
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                coords = []
                for part in current.split(","):
                    part = part.strip()
                    if part:
                        nums = part.split()
                        if len(nums) >= 2:
                            try:
                                coords.append([float(nums[0]), float(nums[1])])
                            except ValueError:
                                continue
                if coords:
                    lines.append(coords)
                current = ""
                continue

        if depth >= 1:
            current += char

    return lines


def _extract_multipolygon_coords(wkt: str) -> list[list[list[list[float]]]]:
    """Extract multipolygon coordinates"""
    start = wkt.find("((")
    end = wkt.rfind("))")
    if start == -1 or end == -1:
        return []

    content = wkt[start:end + 2]

    polygons = []
    current_polygon = []
    depth = 0
    ring_content = ""

    i = 0
    while i < len(content):
        char = content[i]

        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 1 and ring_content:
                coords = []
                for part in ring_content.split(","):
                    part = part.strip()
                    if part:
                        nums = part.split()
                        if len(nums) >= 2:
                            try:
                                coords.append([float(nums[0]), float(nums[1])])
                            except ValueError:
                                continue
                if coords:
                    current_polygon.append(coords)
                ring_content = ""
            elif depth == 0:
                if current_polygon:
                    polygons.append(current_polygon)
                current_polygon = []
        elif depth >= 2:
            ring_content += char

        i += 1

    return polygons


# ============================================================================
# Conversion to GeoJSON
# ============================================================================

def geometry_to_geojson(value: Any) -> Geometry | None:
    """Convert a geometry value to GeoJSON Geometry"""
    if value is None:
        return None

    # Already a Geometry struct
    if isinstance(value, (Point, MultiPoint, LineString, MultiLineString,
                          Polygon, MultiPolygon, GeometryCollection)):
        return value

    if isinstance(value, str):
        value = value.strip()

        # Try parsing as GeoJSON string
        if value.startswith("{"):
            try:
                result = loads(value)
                if isinstance(result, (Point, MultiPoint, LineString, MultiLineString,
                                       Polygon, MultiPolygon, GeometryCollection)):
                    return result
                elif isinstance(result, Feature):
                    return result.geometry
            except Exception:
                pass

        # Try parsing as WKT/EWKT
        result = parse_wkt(value)
        if result:
            return result

    return None


def rows_to_geojson(columns: list[dict], rows: list[tuple], geo_column_idx: int) -> FeatureCollection:
    """Convert query results to GeoJSON FeatureCollection"""
    features = []

    col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]

    for row in rows:
        if geo_column_idx >= len(row):
            continue

        geom = geometry_to_geojson(row[geo_column_idx])
        if not geom:
            continue

        # Build properties from other columns
        properties = {}
        for i, val in enumerate(row):
            if i == geo_column_idx:
                continue

            col_name = col_names[i] if i < len(col_names) else f"col_{i}"

            # Convert value to JSON-serializable format
            if val is None:
                properties[col_name] = None
            elif isinstance(val, (int, float, str, bool)):
                properties[col_name] = val
            elif isinstance(val, (bytes, memoryview)):
                properties[col_name] = "(binary)"
            else:
                properties[col_name] = str(val)

        features.append(Feature(geometry=geom, properties=properties))

    return FeatureCollection(features=features)


def to_dict(obj: GeoJSON) -> dict:
    """Convert GeoJSON msgspec Struct to dict for JSON serialization"""
    return msgspec.to_builtins(obj)
