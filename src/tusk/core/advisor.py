"""Advisor: what a DBA would tell you after ten minutes with your database.

Everything here reads catalog and statistics views — nothing is applied.
Each finding carries the statement to run, so the fix is a copy away, and
a severity so the list reads top-down:

* foreign keys without an index (every join and every cascade scans);
* tables scanned sequentially far more than by index, with real size;
* indexes never used since the last stats reset, by size;
* duplicate indexes (same table, same leading columns);
* dead tuples piling up (autovacuum not keeping up);
* big tables never analysed;
* the top queries by total time from ``pg_stat_statements`` when installed,
  with the sequential scans their generic plan shows (PostgreSQL 16+).

The optional AI pass (``/api/admin/{conn}/advisor/ai``) reads this same
report and writes the two-paragraph version for a human.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from tusk.core.connection import ConnectionConfig
from tusk.core.logging import get_logger

log = get_logger("advisor")

BIG_TABLE_ROWS = 10_000
UNUSED_INDEX_MIN_BYTES = 1024 * 1024
DEAD_TUPLE_RATIO = 0.2
TOP_QUERIES = 10
PLAN_QUERIES = 5


@dataclass
class Finding:
    kind: str
    severity: str  # error | warning | info
    title: str
    detail: str
    fix: str = ""
    table: str = ""
    impact: str = ""


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    top_queries: list[dict] = field(default_factory=list)
    stats_available: bool = False
    pg_version: str = ""
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "findings": [asdict(f) for f in self.findings],
            "top_queries": self.top_queries,
            "stats_available": self.stats_available,
            "pg_version": self.pg_version,
            "error": self.error,
            "counts": {s: sum(1 for f in self.findings if f.severity == s) for s in ("error", "warning", "info")},
        }


# ── Catalog checks ─────────────────────────────────────────────────────

_FK_WITHOUT_INDEX = """
    SELECT n.nspname, c.conrelid::regclass::text AS tbl, a.attname,
           c.confrelid::regclass::text AS ref_tbl,
           COALESCE(s.n_live_tup, 0) AS rows_,
           pg_total_relation_size(c.conrelid) AS bytes_
    FROM pg_constraint c
    JOIN pg_class cl ON cl.oid = c.conrelid
    JOIN pg_namespace n ON n.oid = cl.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.conrelid
    WHERE c.contype = 'f'
      AND array_length(c.conkey, 1) = 1
      AND NOT EXISTS (
          SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid AND i.indkey[0] = a.attnum
      )
    ORDER BY rows_ DESC
"""

_SEQ_SCAN_HEAVY = """
    SELECT schemaname, relname, seq_scan, seq_tup_read, idx_scan, n_live_tup,
           pg_total_relation_size(relid) AS bytes_
    FROM pg_stat_user_tables
    WHERE n_live_tup >= %(rows)s AND seq_scan > 50 AND seq_scan > COALESCE(idx_scan, 0) * 3
    ORDER BY seq_tup_read DESC
    LIMIT 10
"""

_UNUSED_INDEXES = """
    SELECT s.schemaname, s.relname, s.indexrelname, s.idx_scan,
           pg_relation_size(s.indexrelid) AS bytes_
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON i.indexrelid = s.indexrelid
    WHERE s.idx_scan = 0 AND NOT i.indisprimary AND NOT i.indisunique
      AND pg_relation_size(s.indexrelid) >= %(min_bytes)s
    ORDER BY bytes_ DESC
    LIMIT 10
"""

_DUPLICATE_INDEXES = """
    SELECT n.nspname, t.relname, array_agg(i.indexrelid::regclass::text ORDER BY i.indexrelid) AS names,
           i.indkey::text AS cols
    FROM pg_index i
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY n.nspname, t.relname, i.indkey::text, i.indrelid
    HAVING count(*) > 1
"""

_DEAD_TUPLES = """
    SELECT schemaname, relname, n_live_tup, n_dead_tup, last_autovacuum, last_vacuum
    FROM pg_stat_user_tables
    WHERE n_live_tup >= %(rows)s AND n_dead_tup > n_live_tup * %(ratio)s
    ORDER BY n_dead_tup DESC
    LIMIT 10
"""

_NEVER_ANALYZED = """
    SELECT schemaname, relname, n_live_tup
    FROM pg_stat_user_tables
    WHERE last_analyze IS NULL AND last_autoanalyze IS NULL AND n_live_tup >= %(rows)s
    ORDER BY n_live_tup DESC
    LIMIT 10
"""


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _q(schema: str, table: str) -> str:
    return table if schema in ("public", "", None) else f"{schema}.{table}"


async def analyze(conn: ConnectionConfig, *, with_plans: bool = True) -> Report:
    """Run every check. Never raises: a failing check is skipped with a log line."""
    from tusk.engines.postgres import execute_query

    report = Report()
    if conn.type != "postgres":
        report.error = "the Advisor only reads PostgreSQL"
        return report

    async def run(sql: str, params: dict | None = None):
        res = await execute_query(conn, sql % params if params else sql)
        if res.error:
            raise RuntimeError(res.error)
        return res.rows

    try:
        report.pg_version = (await run("SHOW server_version"))[0][0]
    except Exception as exc:  # noqa: BLE001
        report.error = str(exc)
        return report

    # 1. FKs without an index
    try:
        for schema, tbl, col, ref_tbl, rows, size in await run(_FK_WITHOUT_INDEX):
            sev = "warning" if rows >= BIG_TABLE_ROWS else "info"
            report.findings.append(Finding(
                kind="fk_no_index", severity=sev, table=tbl,
                title=f"{tbl}.{col} → {ref_tbl}: foreign key without an index",
                detail=f"Joins on {col} and deletes on {ref_tbl} scan {tbl} ({rows:,} rows, {_human(size)}).",
                fix=f'CREATE INDEX CONCURRENTLY "{tbl.split(".")[-1]}_{col}_idx" ON {tbl} ({col});',
                impact=f"{rows:,} rows",
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_fk_check_failed", error=str(exc))

    # 2. Sequential-scan-heavy tables
    try:
        for schema, tbl, seq, seq_rows, idx, live, size in await run(_SEQ_SCAN_HEAVY, {"rows": BIG_TABLE_ROWS}):
            name = _q(schema, tbl)
            report.findings.append(Finding(
                kind="seq_scan", severity="warning", table=name,
                title=f"{name}: {seq:,} sequential scans vs {idx or 0:,} index scans",
                detail=f"{seq_rows:,} rows read sequentially on a {_human(size)} table ({live:,} rows). "
                       "Something filters or joins on a column with no usable index.",
                fix=f"-- find the column: look at the WHERE/JOIN of the queries on {name} in the top list below\n"
                    f"-- then: CREATE INDEX CONCURRENTLY ON {name} (<column>);",
                impact=f"{seq_rows:,} rows read",
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_seqscan_check_failed", error=str(exc))

    # 3. Unused indexes
    try:
        for schema, tbl, idx, scans, size in await run(_UNUSED_INDEXES, {"min_bytes": UNUSED_INDEX_MIN_BYTES}):
            name = _q(schema, tbl)
            report.findings.append(Finding(
                kind="unused_index", severity="info", table=name,
                title=f"{idx} on {name} has never been used ({_human(size)})",
                detail="Zero index scans since the last statistics reset. It still costs writes and space. "
                       "Check it is not needed for a rare report before dropping.",
                fix=f'DROP INDEX CONCURRENTLY "{idx}";',
                impact=_human(size),
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_unused_index_check_failed", error=str(exc))

    # 4. Duplicate indexes
    try:
        for schema, tbl, names, cols in await run(_DUPLICATE_INDEXES):
            name = _q(schema, tbl)
            names = list(names) if not isinstance(names, str) else names.strip("{}").split(",")
            report.findings.append(Finding(
                kind="duplicate_index", severity="info", table=name,
                title=f"{name}: {len(names)} indexes on the same columns",
                detail=", ".join(names) + " cover the same key; one is enough unless they differ in type or predicate.",
                fix=f"DROP INDEX CONCURRENTLY {names[-1]};  -- after checking the definitions",
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_duplicate_index_check_failed", error=str(exc))

    # 5. Dead tuples
    try:
        for schema, tbl, live, dead, last_auto, last_vac in await run(_DEAD_TUPLES, {"rows": BIG_TABLE_ROWS, "ratio": DEAD_TUPLE_RATIO}):
            name = _q(schema, tbl)
            pct = 100.0 * dead / max(1, live)
            report.findings.append(Finding(
                kind="dead_tuples", severity="warning", table=name,
                title=f"{name}: {dead:,} dead tuples ({pct:.0f}% of live rows)",
                detail=f"Autovacuum is not keeping up (last autovacuum: {last_auto or 'never'}). "
                       "Scans read the dead rows too.",
                fix=f"VACUUM (ANALYZE) {name};\n-- or lower autovacuum_vacuum_scale_factor for this table",
                impact=f"{pct:.0f}% dead",
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_dead_tuples_check_failed", error=str(exc))

    # 6. Never analysed
    try:
        for schema, tbl, live in await run(_NEVER_ANALYZED, {"rows": BIG_TABLE_ROWS}):
            name = _q(schema, tbl)
            report.findings.append(Finding(
                kind="no_stats", severity="warning", table=name,
                title=f"{name} has never been analysed ({live:,} rows)",
                detail="The planner is guessing row counts for this table; plans can be badly off.",
                fix=f"ANALYZE {name};",
            ))
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_analyze_check_failed", error=str(exc))

    # 7. Top queries + generic plans
    try:
        from tusk.admin.monitoring import get_slow_queries

        slow = await get_slow_queries(conn, limit=TOP_QUERIES, order_by="total_time")
        if isinstance(slow, list):
            report.stats_available = True
            for q in slow:
                d = q if isinstance(q, dict) else {k: getattr(q, k) for k in ("query", "calls", "total_time_ms", "mean_time_ms", "rows", "hit_ratio")}
                report.top_queries.append({
                    "query": (d.get("query") or "")[:600], "calls": d.get("calls"),
                    "total_time_ms": round(d.get("total_time_ms") or 0, 1), "mean_time_ms": round(d.get("mean_time_ms") or 0, 1),
                    "rows": d.get("rows"), "hit_ratio": round(d.get("hit_ratio") or 0, 1), "seq_scans": [],
                })
            if with_plans:
                await _annotate_plans(conn, report)
    except Exception as exc:  # noqa: BLE001
        log.debug("advisor_top_queries_failed", error=str(exc))

    order = {"error": 0, "warning": 1, "info": 2}
    report.findings.sort(key=lambda f: (order.get(f.severity, 3), f.title))
    return report


async def _annotate_plans(conn: ConnectionConfig, report: Report) -> None:
    """EXPLAIN (GENERIC_PLAN) the top SELECTs and note sequential scans on
    big tables. PostgreSQL 16+; silently skipped elsewhere."""
    from tusk.engines.postgres import execute_query

    for entry in report.top_queries[:PLAN_QUERIES]:
        sql = entry["query"].strip().rstrip(";")
        if not sql.lower().startswith(("select", "with")) or len(sql) > 4000:
            continue
        res = await execute_query(conn, f"EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}")
        if res.error or not res.rows:
            continue
        plan = res.rows[0][0]
        if isinstance(plan, str):
            try:
                plan = json.loads(plan)
            except ValueError:
                continue
        root = plan[0]["Plan"] if isinstance(plan, list) else plan.get("Plan", plan)
        scans: list[dict] = []

        def walk(node: dict) -> None:
            if node.get("Node Type") == "Seq Scan":
                scans.append({"table": node.get("Relation Name"), "filter": node.get("Filter"), "rows": node.get("Plan Rows")})
            for child in node.get("Plans", []) or []:
                walk(child)

        walk(root)
        entry["seq_scans"] = [s for s in scans if (s.get("rows") or 0) >= 1000]
        for s in entry["seq_scans"]:
            report.findings.append(Finding(
                kind="plan_seq_scan", severity="warning", table=s["table"] or "",
                title=f"Top query scans {s['table']} sequentially ({s['rows']:,} rows est.)",
                detail=f"Filter: {s['filter'] or '(none)'} — query: {sql[:160]}…",
                fix=f"-- index the filtered column(s) of {s['table']}: {s['filter'] or ''}",
                impact=f"{entry['total_time_ms']:,} ms total",
            ))


def render_for_ai(report: Report) -> str:
    """Compact text version of the report for the AI summary."""
    lines = [f"PostgreSQL {report.pg_version}", f"Findings: {len(report.findings)}"]
    for f in report.findings[:20]:
        lines.append(f"- [{f.severity}] {f.title}. {f.detail} Fix: {f.fix.splitlines()[0] if f.fix else '-'}")
    if report.top_queries:
        lines.append("Top queries by total time:")
        for q in report.top_queries[:8]:
            lines.append(f"- {q['total_time_ms']} ms total, {q['calls']} calls, {q['mean_time_ms']} ms mean: {q['query'][:200]}")
    elif not report.stats_available:
        lines.append("pg_stat_statements is not installed: no query statistics.")
    return "\n".join(lines)
