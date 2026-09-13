"""Check the joins of a generated query against the foreign keys.

The Copilot's EXPLAIN dry run (0.4.45) catches columns that do not exist;
it cannot catch a join between two columns that both exist but were never
meant to be joined (`orders.product_id = products.id` where the real path
is through `order_items`). This module finds every ``a.x = b.y`` pair in a
SELECT, resolves the aliases, and asks the catalog whether a foreign key
links the two columns. No SQL parser: a regex pass is enough for the
shapes a model writes, and anything it cannot resolve is reported as
``unknown`` and never counted against the query.

Statuses per join:

* ``fk``            a foreign key links the two columns (either direction)
* ``no_fk``         both columns exist, same type family, no foreign key
* ``type_mismatch`` both columns exist and their types cannot match
                    (integer = text, uuid = integer …): almost surely wrong
* ``unknown``       a side could not be resolved (CTE, subquery, unknown
                    table or column): not the checker's business
"""

from __future__ import annotations

import re

_IDENT = r'"[^"]+"|[A-Za-z_][\w$]*'
_QUALIFIED = rf"(?:(?:{_IDENT})\.)?(?:{_IDENT})"
_KEYWORDS = {
    "on", "where", "join", "left", "right", "inner", "full", "cross", "natural", "using",
    "group", "order", "limit", "having", "union", "except", "intersect", "lateral", "as",
    "set", "returning", "window", "fetch", "offset", "tablesample", "select", "with",
    "values", "and", "or", "not", "outer", "for",
}
_TABLE_RE = re.compile(
    rf"\b(from|join)\s+(?P<table>{_QUALIFIED})(?:\s+(?:as\s+)?(?P<alias>{_IDENT}))?",
    re.IGNORECASE,
)
_CTE_RE = re.compile(rf"(?:\bwith\s+(?:recursive\s+)?|,\s*)(?P<name>{_IDENT})\s*(?:\([^)]*\))?\s+as\s*\(", re.IGNORECASE)
_EQ_RE = re.compile(rf"(?P<lq>{_IDENT})\.(?P<lc>{_IDENT})\s*=\s*(?P<rq>{_IDENT})\.(?P<rc>{_IDENT})")
_USING_RE = re.compile(rf"\bjoin\s+(?P<table>{_QUALIFIED})(?:\s+(?:as\s+)?(?P<alias>{_IDENT}))?\s+using\s*\((?P<cols>[^)]*)\)", re.IGNORECASE)

_FAMILIES = (
    ("int", ("smallint", "integer", "bigint", "smallserial", "serial", "bigserial", "oid")),
    ("num", ("numeric", "decimal", "real", "double precision", "money")),
    ("text", ("text", "character varying", "character", "varchar", "char", "citext", "name", "bpchar")),
    ("uuid", ("uuid",)),
    ("time", ("timestamp", "date", "time")),
    ("bool", ("boolean",)),
    ("json", ("json", "jsonb")),
    ("bytes", ("bytea",)),
)
_COMPATIBLE = {("int", "num"), ("num", "int")}


def _norm(ident: str) -> str:
    ident = ident.strip()
    if ident.startswith('"') and ident.endswith('"'):
        return ident[1:-1]
    return ident.lower()


def _norm_table(name: str) -> str:
    parts = [_norm(p) for p in re.findall(_IDENT, name)]
    if len(parts) == 2 and parts[0] == "public":
        return parts[1]
    return ".".join(parts)


def type_family(pg_type: str) -> str:
    t = pg_type.lower().strip()
    if t.endswith("[]"):
        return "array"
    for fam, names in _FAMILIES:
        if any(t == n or t.startswith(n + "(") or t.startswith(n + " ") for n in names):
            return fam
    return t


def parse_aliases(sql: str) -> tuple[dict[str, str], set[str]]:
    """``{alias_or_table: table}`` for every FROM/JOIN table, plus CTE names."""
    ctes = {_norm(m.group("name")) for m in _CTE_RE.finditer(sql)}
    aliases: dict[str, str] = {}
    for m in _TABLE_RE.finditer(sql):
        table = _norm_table(m.group("table"))
        alias = m.group("alias")
        aliases.setdefault(table.split(".")[-1], table)
        aliases.setdefault(table, table)
        if alias and _norm(alias) not in _KEYWORDS:
            aliases[_norm(alias)] = table
    return aliases, ctes


def parse_joins(sql: str) -> list[dict]:
    """Every ``a.x = b.y`` and ``JOIN t USING (c)`` in the statement, with
    aliases resolved. Sides that resolve to a CTE or an unknown alias keep
    ``table=None``."""
    aliases, ctes = parse_aliases(sql)

    def side(q: str, c: str) -> dict:
        q, c = _norm(q), _norm(c)
        table = aliases.get(q)
        if table in ctes or (table and table.split(".")[-1] in ctes):
            table = None
        return {"ref": f"{q}.{c}", "table": table, "col": c}

    joins: list[dict] = []
    for m in _EQ_RE.finditer(sql):
        joins.append({"text": m.group(0), "left": side(m.group("lq"), m.group("lc")), "right": side(m.group("rq"), m.group("rc"))})
    seen_tables: list[str] = []
    for m in _TABLE_RE.finditer(sql):
        seen_tables.append(_norm_table(m.group("table")))
    for m in _USING_RE.finditer(sql):
        table = _norm_table(m.group("table"))
        earlier = [t for t in seen_tables[: seen_tables.index(table)] if t != table] if table in seen_tables else []
        for col in m.group("cols").split(","):
            col = _norm(col)
            if not col:
                continue
            joins.append({
                "text": f"JOIN {table} USING ({col})",
                "left": {"ref": f"{table}.{col}", "table": table, "col": col},
                "right": {"ref": f"?.{col}", "table": None, "col": col, "candidates": earlier},
            })
    return joins


def _has_fk(catalog: dict, a_table: str, a_col: str, b_table: str, b_col: str) -> bool:
    for src, sc, dst, dc in ((a_table, a_col, b_table, b_col), (b_table, b_col, a_table, a_col)):
        for fk in catalog.get(src, {}).get("fks", []):
            if fk["col"] == sc and _norm_table(fk["to_table"]) == dst and fk["to_col"] == dc:
                return True
    return False


def _col_type(catalog: dict, table: str | None, col: str) -> str | None:
    if not table:
        return None
    for c in catalog.get(table, {}).get("cols", []):
        if c["name"] == col:
            return c["type"]
    return None


def check_joins(sql: str, catalog: dict) -> list[dict]:
    """Classify every join of ``sql`` against a ``fetch_catalog`` snapshot."""
    findings: list[dict] = []
    for j in parse_joins(sql):
        left, right = j["left"], j["right"]
        # USING: pick the earlier table that has the column
        if right["table"] is None and right.get("candidates"):
            for cand in reversed(right["candidates"]):
                if _col_type(catalog, cand, right["col"]) is not None:
                    right = {**right, "table": cand, "ref": f"{cand}.{right['col']}"}
                    break
        lt, rt = _col_type(catalog, left["table"], left["col"]), _col_type(catalog, right["table"], right["col"])
        item = {
            "join": j["text"],
            "left": f"{left['table']}.{left['col']}" if left["table"] else left["ref"],
            "right": f"{right['table']}.{right['col']}" if right["table"] else right["ref"],
        }
        if lt is None or rt is None:
            item.update(status="unknown", detail="")
        elif _has_fk(catalog, left["table"], left["col"], right["table"], right["col"]):
            item.update(status="fk", detail="foreign key")
        else:
            lf, rf = type_family(lt), type_family(rt)
            if lf == rf or (lf, rf) in _COMPATIBLE:
                item.update(status="no_fk", detail=f"no foreign key between {item['left']} and {item['right']}")
            else:
                item.update(status="type_mismatch", detail=f"{item['left']} is {lt}, {item['right']} is {rt}: these cannot match")
        findings.append(item)
    return findings


def warnings(findings: list[dict]) -> list[dict]:
    return [f for f in findings if f["status"] in ("no_fk", "type_mismatch")]


def render_fks(catalog: dict, tables: set[str]) -> str:
    """The foreign keys touching ``tables``, one per line, for a retry prompt."""
    lines: list[str] = []
    for tname, t in sorted(catalog.items()):
        for fk in t.get("fks", []):
            if tname in tables or _norm_table(fk["to_table"]) in tables:
                lines.append(f"{tname}.{fk['col']} -> {fk['to_table']}.{fk['to_col']}")
    return "\n".join(lines) if lines else "(none of these tables has a foreign key)"
