"""Spatial grounding for the AI Copilot and the MCP tools.

Three things a model cannot see in a plain catalog, and that decide whether
"vegetarian restaurants in Piantini" becomes SQL or a shrug:

* **Spatial catalog** — is PostGIS there, which columns are geometry or
  geography, with what type and SRID, which tables carry a lat/lon pair,
  is ``h3`` installed. ``USER-DEFINED`` tells the model nothing.
* **Column profiles** — for ``jsonb`` and low-cardinality text columns, the
  keys and values that actually occur (``amenity: restaurant | cafe``,
  ``diet:vegetarian``). Sampled, cached per connection.
* **Gazetteer** — tables that look like places (a polygon or point plus a
  name column); capitalised words in the question are looked up there so
  the prompt can say *Piantini → geo_administrative_area.name = 'PIANTINI'*.

Everything here is read-only and best-effort: any failure degrades to "no
spatial section", never to an error in the Copilot.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

from tusk.core.connection import ConnectionConfig
from tusk.core.logging import get_logger

log = get_logger("spatial")

PROFILE_TTL_SECONDS = 600
SAMPLE_ROWS = 5000
TOP_VALUES = 20
MAX_PROFILED_COLUMNS = 40

_SPATIAL_EXT_SQL = """
    SELECT extname, extversion FROM pg_extension
    WHERE extname IN ('postgis', 'postgis_topology', 'postgis_raster', 'h3', 'h3_postgis', 'pgrouting', 'pg_trgm')
"""

_GEOMETRY_COLUMNS_SQL = """
    SELECT f_table_schema, f_table_name, f_geometry_column, type, srid, coord_dimension, 'geometry' AS kind
    FROM geometry_columns
    UNION ALL
    SELECT f_table_schema, f_table_name, f_geography_column, type, srid, coord_dimension, 'geography'
    FROM geography_columns
    ORDER BY 1, 2, 3
"""


@dataclass
class SpatialInfo:
    postgis: str | None = None
    extensions: dict[str, str] = field(default_factory=dict)
    # {table: [{"column", "kind", "type", "srid", "dims"}]}
    geometry: dict[str, list[dict]] = field(default_factory=dict)
    # {table: {"lat": col, "lon": col}}
    latlon: dict[str, dict[str, str]] = field(default_factory=dict)

    @property
    def enabled(self) -> bool:
        return bool(self.postgis or self.geometry or self.latlon)

    @property
    def h3(self) -> bool:
        return any(k.startswith("h3") for k in self.extensions)


def _qualified(schema: str, table: str) -> str:
    return table if schema in ("public", None, "") else f"{schema}.{table}"


async def fetch_spatial(conn: ConnectionConfig, catalog: dict[str, dict] | None = None) -> SpatialInfo:
    """Spatial catalog for a PostgreSQL connection. ``catalog`` (the shape
    from :func:`tusk.core.catalog.fetch_catalog`) lets us spot lat/lon pairs
    without another round trip."""
    from tusk.engines.postgres import execute_query

    info = SpatialInfo()
    if conn.type != "postgres":
        return info
    try:
        ext = await execute_query(conn, _SPATIAL_EXT_SQL)
        if not ext.error:
            info.extensions = {r[0]: r[1] for r in ext.rows}
            info.postgis = info.extensions.get("postgis")
        if info.postgis:
            geo = await execute_query(conn, _GEOMETRY_COLUMNS_SQL)
            if not geo.error:
                for schema, table, column, gtype, srid, dims, kind in geo.rows:
                    info.geometry.setdefault(_qualified(schema, table), []).append(
                        {"column": column, "kind": kind, "type": gtype, "srid": srid, "dims": dims}
                    )
    except Exception as exc:  # noqa: BLE001 — grounding is best-effort
        log.warning("spatial_catalog_failed", error=str(exc))

    if catalog:
        for tname, t in catalog.items():
            names = {c["name"].lower(): c["name"] for c in t.get("cols", [])}
            lat = next((names[n] for n in ("lat", "latitude", "latitud") if n in names), None)
            lon = next((names[n] for n in ("lon", "lng", "long", "longitude", "longitud") if n in names), None)
            if lat and lon:
                info.latlon[tname] = {"lat": lat, "lon": lon}
    return info


# ── Column profiles ─────────────────────────────────────────────────────

_profile_cache: dict[str, tuple[float, dict[str, dict[str, list[str]]]]] = {}

_PROFILE_TYPES = {"jsonb", "json"}
_TEXT_TYPES = {"text", "character varying", "varchar", "citext"}
_SKIP_TEXT_COLUMNS = {"id", "uuid", "name", "email", "phone", "address", "description", "notes", "comment", "comments",
                      "title", "slug", "url", "token", "password", "hash", "code", "key", "path", "body", "content"}


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _quote_table(tname: str) -> str:
    if "." in tname:
        schema, table = tname.split(".", 1)
        return f"{_quote(schema)}.{_quote(table)}"
    return _quote(tname)


def _profilable_columns(
    catalog: dict[str, dict],
    only_tables: set[str] | None,
    first: set[str] | None = None,
) -> list[tuple[str, str, str]]:
    """Columns worth sampling, most informative first: jsonb tag bags (OSM
    style) before free text, and tables in ``first`` (the spatial ones)
    before the rest, so the cap never drops the column that matters."""
    out: list[tuple[str, str, str]] = []
    for tname, t in catalog.items():
        if only_tables is not None and tname not in only_tables:
            continue
        for c in t.get("cols", []):
            ctype = c["type"].lower()
            cname = c["name"].lower()
            if ctype in _PROFILE_TYPES:
                out.append((tname, c["name"], "json"))
            elif ctype in _TEXT_TYPES and cname not in _SKIP_TEXT_COLUMNS and not cname.endswith("_id"):
                out.append((tname, c["name"], "text"))
    first = first or set()
    out.sort(key=lambda x: (0 if x[2] == "json" else 1, 0 if x[0] in first else 1, x[0], x[1]))
    return out[:MAX_PROFILED_COLUMNS]


async def profile_columns(
    conn: ConnectionConfig,
    catalog: dict[str, dict],
    *,
    only_tables: set[str] | None = None,
    first: set[str] | None = None,
) -> dict[str, dict[str, list[str]]]:
    """Top keys/values per jsonb and categorical text column, per table.

    Returns ``{table: {column: ["amenity: restaurant | cafe | bar", ...]}}``.
    Text columns are kept only when they look categorical (≤ 30 distinct
    values in the sample). Cached for ten minutes per connection.
    """
    from tusk.engines.postgres import execute_query

    key = f"{conn.id}:{','.join(sorted(only_tables)) if only_tables else '*'}"
    hit = _profile_cache.get(key)
    if hit and time.time() - hit[0] < PROFILE_TTL_SECONDS:
        return hit[1]

    out: dict[str, dict[str, list[str]]] = {}
    for tname, cname, kind in _profilable_columns(catalog, only_tables, first):
        tq, cq = _quote_table(tname), _quote(cname)
        try:
            if kind == "json":
                # Per key: how many rows carry it (n), how many distinct values
                # (nd) and its 8 most common values. Categorical keys (2-15
                # distinct values: amenity, cuisine, diet:*, takeaway…) come
                # first — those are the ones a WHERE clause is written on.
                # Names, addresses, contacts, URLs and dates are skipped: they
                # are unique per row and only eat the budget.
                sql = f"""
                    WITH raw AS (SELECT {cq}::jsonb AS x FROM {tq} WHERE {cq} IS NOT NULL LIMIT {SAMPLE_ROWS}),
                    -- Importers often store a JSON *string* inside the jsonb column
                    -- ('"{{\\"amenity\\": ...}}"'); unwrap it when it is valid JSON.
                    s AS (SELECT CASE WHEN jsonb_typeof(x) = 'string' AND pg_input_is_valid(x #>> '{{}}', 'jsonb')
                                      THEN (x #>> '{{}}')::jsonb ELSE x END AS v FROM raw),
                    kv AS (SELECT k, left(e.value #>> '{{}}', 40) AS val
                           FROM s, jsonb_each(s.v) AS e(k, value) WHERE jsonb_typeof(s.v) = 'object'),
                    counted AS (SELECT k, val, count(*) AS c FROM kv GROUP BY k, val),
                    ranked AS (SELECT k, val, c, row_number() OVER (PARTITION BY k ORDER BY c DESC, val) AS rn,
                                      sum(c) OVER (PARTITION BY k) AS n, count(*) OVER (PARTITION BY k) AS nd
                               FROM counted)
                    SELECT k, max(n) AS n, max(nd) AS nd,
                           string_agg(val, ' | ' ORDER BY rn) FILTER (WHERE rn <= 8) AS vals
                    FROM ranked
                    WHERE k !~* '^(name|official_name|alt_name|old_name|brand|operator|addr:|contact:|website|phone|email|opening_hours|check_date|source|note|description|fixme|ref|image|url|wiki)'
                      AND k !~* '(:en|:es|:fr|wikidata|wikipedia)$'
                    GROUP BY k
                    ORDER BY (CASE WHEN max(nd) BETWEEN 2 AND 15 THEN 0 ELSE 1 END), max(n) DESC
                    LIMIT 25
                """
                res = await execute_query(conn, sql)
                if res.error or not res.rows:
                    continue
                lines = []
                for k, n, nd, vals in res.rows:
                    more = f" (+{nd - 8} more)" if nd and nd > 8 else ""
                    lines.append(f"{k}: {vals}{more}" if vals else str(k))
                out.setdefault(tname, {})[cname] = lines
            else:
                sql = f"""
                    SELECT {cq}::text AS v, count(*) AS n
                    FROM (SELECT {cq} FROM {tq} WHERE {cq} IS NOT NULL LIMIT {SAMPLE_ROWS}) s
                    GROUP BY 1 ORDER BY n DESC LIMIT 31
                """
                res = await execute_query(conn, sql)
                if res.error or not res.rows or len(res.rows) > 30:
                    continue
                vals = [str(r[0]).strip() for r in res.rows[:TOP_VALUES] if str(r[0]).strip()]
                # Categorical means short, repeated labels — not URLs, notes or blanks.
                if len(vals) < 2 or any("://" in v for v in vals) or sum(map(len, vals)) / len(vals) > 30:
                    continue
                out.setdefault(tname, {})[cname] = [" | ".join(v[:40] for v in vals)]
        except Exception as exc:  # noqa: BLE001
            log.debug("profile_failed", table=tname, column=cname, error=str(exc))
            continue
    _profile_cache[key] = (time.time(), out)
    return out


# ── Gazetteer ───────────────────────────────────────────────────────────

_NAME_COLUMNS = ("name", "nombre", "label", "title", "toponimia", "name_alt", "nom")
_STOP_WORDS = {
    "quiero", "muestra", "muéstrame", "dame", "lista", "todos", "todas", "cuales", "cuáles", "donde", "dónde",
    "show", "list", "give", "find", "which", "where", "what", "select", "the", "los", "las", "del", "de", "en", "que",
    "restaurantes", "restaurants", "vegetariano", "vegetarianos", "vegetarian", "tipo", "sector", "distrito", "district",
}


def place_tables(catalog: dict[str, dict], spatial: SpatialInfo) -> list[tuple[str, str, str]]:
    """Tables that can name a place: a geometry (polygon preferred) or a
    lat/lon pair, plus a name-like text column. Returns (table, name_col, geom_col)."""
    out: list[tuple[str, str, str, int]] = []
    for tname, t in catalog.items():
        cols = {c["name"].lower(): c["name"] for c in t.get("cols", [])}
        name_col = next((cols[n] for n in _NAME_COLUMNS if n in cols), None)
        if not name_col:
            continue
        geoms = spatial.geometry.get(tname, [])
        if geoms:
            g = sorted(geoms, key=lambda x: 0 if "POLYGON" in (x["type"] or "").upper() else 1)[0]
            rank = 0 if "POLYGON" in (g["type"] or "").upper() else 1
            out.append((tname, name_col, g["column"], rank))
        elif tname in spatial.latlon:
            out.append((tname, name_col, "", 2))
    out.sort(key=lambda x: x[3])
    return [(t, n, g) for t, n, g, _ in out]


def candidate_place_tokens(prompt: str) -> list[str]:
    """Proper-noun candidates from the question: capitalised phrases, the
    segments between joiners (de/del/la/el), and single capitalised words.
    'sector Piantini del Distrito Nacional' → ['Piantini del Distrito Nacional',
    'Distrito Nacional', 'Piantini', 'Nacional', 'Distrito']. Longest first,
    so the lookup tries the most specific form before falling back."""
    cap = r"[A-ZÁÉÍÓÚÑ][\wáéíóúñ]+"
    joiner = r"(?:de\s+|del\s+|la\s+|el\s+|las\s+|los\s+)?"
    found: list[str] = []
    for m in re.finditer(rf"\b({cap}(?:\s+{joiner}{cap}){{0,3}})", prompt):
        phrase = m.group(1).strip()
        found.append(phrase)
        for seg in re.split(r"\s+(?:de|del|la|el|las|los)\s+", phrase):
            found.append(seg.strip())
        found.extend(re.findall(cap, phrase))
    seen: set[str] = set()
    out: list[str] = []
    for t in sorted(found, key=len, reverse=True):
        key = t.lower()
        if len(t) < 3 or key in seen:
            continue
        if all(w.lower() in _STOP_WORDS for w in t.split()):
            continue
        seen.add(key)
        out.append(t)
    return out[:8]


async def lookup_places(
    conn: ConnectionConfig,
    places: list[tuple[str, str, str]],
    tokens: list[str],
    *,
    per_token: int = 3,
) -> list[dict]:
    """Case-insensitive lookup of each token in each place table.
    Returns [{"token", "table", "name_col", "geom_col", "value", "extra"}]."""
    from tusk.engines.postgres import execute_query

    found: list[dict] = []
    if not places or not tokens:
        return found
    matched: list[str] = []
    seen_rows: set[tuple] = set()
    for token in tokens:  # longest first (see candidate_place_tokens)
        if any(token.lower() in m.lower() and token.lower() != m.lower() for m in matched):
            continue  # "Nacional" adds nothing once "Distrito Nacional" matched
        for tname, name_col, geom_col in places:
            tq, nq = _quote_table(tname), _quote(name_col)
            extra_cols = [c for c in ("level", "type", "kind", "admin_level", "category") if c in {x.lower() for x in _table_cols(conn, tname)}]
            extra_sel = ", " + ", ".join(_quote(c) for c in extra_cols) if extra_cols else ""
            sql = f"SELECT {nq}{extra_sel} FROM {tq} WHERE {nq} ILIKE %s ORDER BY length({nq}) LIMIT {per_token}"
            try:
                res = await execute_query(conn, sql, params=(f"%{token}%",))
            except Exception as exc:  # noqa: BLE001
                log.debug("gazetteer_failed", table=tname, error=str(exc))
                continue
            if res.error or not res.rows:
                continue
            matched.append(token)
            for row in res.rows:
                key = (tname, row[0], tuple(row[1:]))
                if key in seen_rows:
                    continue
                seen_rows.add(key)
                found.append({
                    "token": token, "table": tname, "name_col": name_col, "geom_col": geom_col,
                    "value": row[0], "extra": dict(zip(extra_cols, row[1:])) if extra_cols else {},
                })
            break  # first table with a hit wins for this token
    return found


_cols_cache: dict[str, list[str]] = {}


def remember_catalog_columns(conn_id: str, catalog: dict[str, dict]) -> None:
    for tname, t in catalog.items():
        _cols_cache[f"{conn_id}:{tname}"] = [c["name"] for c in t.get("cols", [])]


def _table_cols(conn: ConnectionConfig, tname: str) -> list[str]:
    return _cols_cache.get(f"{conn.id}:{tname}", [])


# ── Prompt rendering ────────────────────────────────────────────────────

CHEATSHEET = (
    "PostGIS cheat sheet: points from lat/lon → ST_SetSRID(ST_MakePoint(lon, lat), 4326); "
    "inside an area → ST_Contains(area.geom, point) or ST_Intersects; "
    "distance in metres → ST_DWithin(a::geography, b::geography, metres); "
    "text WKT → ST_GeomFromText(wkt, 4326). Keep the geometry column (or lat, lon) in the SELECT "
    "so the result can be drawn on a map; never cast geometry to text."
)


def render_spatial_section(
    spatial: SpatialInfo,
    profiles: dict[str, dict[str, list[str]]],
    places_found: list[dict],
    *,
    sanitize=lambda s: s,
) -> str:
    """The `### Spatial` block for the Copilot prompt. Empty string when the
    connection has nothing spatial."""
    lines: list[str] = []
    if spatial.postgis or spatial.geometry or spatial.latlon:
        lines.append("### Spatial")
        if spatial.postgis:
            ext = ", ".join(sorted(k for k in spatial.extensions if k != "postgis"))
            lines.append(f"PostGIS {spatial.postgis} is installed" + (f" (also: {ext})" if ext else "") + ".")
        for tname, geoms in sorted(spatial.geometry.items()):
            for g in geoms:
                lines.append(f"- {sanitize(tname)}.{sanitize(g['column'])}: {g['kind']} {g['type']} SRID {g['srid']}")
        for tname, ll in sorted(spatial.latlon.items()):
            lines.append(f"- {sanitize(tname)}: lat/lon columns {sanitize(ll['lat'])}, {sanitize(ll['lon'])}")
        lines.append(CHEATSHEET)
    if profiles:
        lines.append("\n### Column values (sampled)")
        for tname, cols in profiles.items():
            for cname, vals in cols.items():
                lines.append(f"- {sanitize(tname)}.{sanitize(cname)}:")
                for v in vals[:TOP_VALUES]:
                    lines.append(f"    {sanitize(v)}")
    if places_found:
        lines.append("\n### Places mentioned in the question")
        for p in places_found:
            extra = ", ".join(f"{k}={v}" for k, v in p["extra"].items()) if p["extra"] else ""
            where = f"{sanitize(p['table'])}.{sanitize(p['name_col'])} = '{sanitize(str(p['value']))}'"
            geom = f" (geometry column: {sanitize(p['geom_col'])})" if p["geom_col"] else " (use its lat/lon)"
            lines.append(f"- \"{sanitize(p['token'])}\" → {where}{' [' + extra + ']' if extra else ''}{geom}")
        lines.append("Filter with these exact values; join spatially against the matching geometry.")
    return "\n".join(lines)


# ── Spatial health (Admin / Explore) ───────────────────────────────────

_HEALTH_SQL = """
    WITH geo AS (
        SELECT f_table_schema AS schema, f_table_name AS tbl, f_geometry_column AS col, type, srid
        FROM geometry_columns
    ),
    idx AS (
        SELECT n.nspname AS schema, c.relname AS tbl, a.attname AS col
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_am am ON am.oid = ic.relam
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey)
        WHERE am.amname IN ('gist', 'spgist', 'brin')
    )
    SELECT geo.schema, geo.tbl, geo.col, geo.type, geo.srid,
           EXISTS (SELECT 1 FROM idx WHERE idx.schema = geo.schema AND idx.tbl = geo.tbl AND idx.col = geo.col) AS has_index,
           COALESCE((SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = geo.schema AND c.relname = geo.tbl), 0) AS approx_rows
    FROM geo
    ORDER BY 1, 2, 3
"""

INVALID_SAMPLE_ROWS = 20000


async def spatial_health(conn: ConnectionConfig, *, check_validity: bool = True) -> dict:
    """What an admin wants to know about PostGIS on this database.

    Returns ``{"postgis": version|None, "columns": [...], "findings": [...]}``
    where each column has type, SRID, index presence, approximate rows,
    invalid-geometry count on a sample, and findings are the actionable
    ones: missing spatial index (with the CREATE INDEX to run), SRID 0,
    invalid geometries.
    """
    from tusk.engines.postgres import execute_query

    out: dict = {"postgis": None, "columns": [], "findings": []}
    if conn.type != "postgres":
        return out
    ext = await execute_query(conn, "SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
    if ext.error or not ext.rows:
        return out
    out["postgis"] = ext.rows[0][0]
    res = await execute_query(conn, _HEALTH_SQL)
    if res.error:
        out["error"] = res.error
        return out
    for schema, tbl, col, gtype, srid, has_index, approx_rows in res.rows:
        tname = _qualified(schema, tbl)
        entry = {
            "table": tname, "column": col, "type": gtype, "srid": srid,
            "has_index": bool(has_index), "approx_rows": int(approx_rows or 0),
            "invalid": None, "extent": None,
        }
        tq, cq = _quote_table(tname), _quote(col)
        if check_validity:
            try:
                v = await execute_query(
                    conn,
                    f"SELECT count(*) FILTER (WHERE NOT ST_IsValid({cq})), "
                    f"ST_AsText(ST_Extent({cq})) FROM (SELECT {cq} FROM {tq} WHERE {cq} IS NOT NULL LIMIT {INVALID_SAMPLE_ROWS}) s",
                )
                if not v.error and v.rows:
                    entry["invalid"] = int(v.rows[0][0] or 0)
                    entry["extent"] = v.rows[0][1]
            except Exception as exc:  # noqa: BLE001
                log.debug("spatial_validity_failed", table=tname, error=str(exc))
        out["columns"].append(entry)
        if not has_index and entry["approx_rows"] >= 1000:
            idx_name = f"{tbl}_{col}_gist"
            out["findings"].append({
                "kind": "missing_index", "severity": "warning", "table": tname, "column": col,
                "message": f"{tname}.{col} has no spatial index (~{entry['approx_rows']:,} rows): every ST_Contains / ST_DWithin scans the table.",
                "fix": f'CREATE INDEX CONCURRENTLY {_quote(idx_name)} ON {tq} USING GIST ({cq});',
            })
        if not srid:
            out["findings"].append({
                "kind": "srid_zero", "severity": "warning", "table": tname, "column": col,
                "message": f"{tname}.{col} has SRID 0: distances and joins with 4326 data will be wrong or fail.",
                "fix": f"SELECT UpdateGeometrySRID('{schema}', '{tbl}', '{col}', 4326);  -- if the data is lon/lat",
            })
        if entry["invalid"]:
            out["findings"].append({
                "kind": "invalid", "severity": "error", "table": tname, "column": col,
                "message": f"{tname}.{col}: {entry['invalid']:,} invalid geometries in the first {INVALID_SAMPLE_ROWS:,} rows (ST_IsValid).",
                "fix": f"UPDATE {tq} SET {cq} = ST_MakeValid({cq}) WHERE NOT ST_IsValid({cq});",
            })
    return out


async def table_spatial(conn: ConnectionConfig, schema: str, table: str) -> list[dict]:
    """Spatial columns of one table with SRID, type, extent, invalid count and
    index presence — the Explore card."""
    health = await spatial_health(conn, check_validity=True)
    tname = _qualified(schema, table)
    return [c for c in health.get("columns", []) if c["table"] == tname]
