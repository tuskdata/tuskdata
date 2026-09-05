"""CLI commands for BI plugin"""

import sys


def handle_bi_cli(args: list[str]) -> None:
    """Handle BI CLI commands.

    Usage:
        tusk bi sources         - List data sources
        tusk bi queries         - List saved queries
        tusk bi dashboards      - List dashboards
        tusk bi run <sql>       - Execute ad-hoc SQL
        tusk bi status          - Show BI summary
        tusk bi export <id|all> [--out DIR] [--json]   - Dashboards as YAML files
        tusk bi import <file>...                        - Load dashboards from files
    """
    if not args:
        _print_help()
        return

    command = args[0]
    rest = args[1:]

    if command == "sources":
        _cmd_sources()
    elif command == "queries":
        _cmd_queries()
    elif command == "dashboards":
        _cmd_dashboards()
    elif command == "run":
        _cmd_run(rest)
    elif command == "status":
        _cmd_status()
    elif command == "export":
        _cmd_export(rest)
    elif command == "import":
        _cmd_import(rest)
    elif command in ("help", "-h", "--help"):
        _print_help()
    else:
        print(f"Unknown command: {command}")
        _print_help()
        sys.exit(1)


def _print_help() -> None:
    print("""
Tusk BI - Business Intelligence & Analytics

Usage:
    tusk bi <command> [options]

Commands:
    sources         List data sources
    queries         List saved queries
    dashboards      List dashboards
    run <sql>       Execute ad-hoc SQL against DuckDB
    status          Show BI summary
    export <id|all> Write dashboards as YAML (--out DIR, --json for JSON)
    import <file>.. Create dashboards from YAML/JSON files (idempotent by name)
""")


def _cmd_sources() -> None:
    from tusk.bi.db import get_data_sources
    sources = get_data_sources()
    if not sources:
        print("No data sources found. Start Tusk Studio to auto-discover.")
        return
    print(f"Data Sources ({len(sources)}):")
    for s in sources:
        plugin = f" [plugin: {s['plugin_id']}]" if s.get("plugin_id") else ""
        print(f"  {s['id']}. {s['name']} ({s['source_type']}){plugin}")


def _cmd_queries() -> None:
    from tusk.bi.db import get_saved_queries
    queries = get_saved_queries()
    if not queries:
        print("No saved queries.")
        return
    print(f"Saved Queries ({len(queries)}):")
    for q in queries:
        chart = f" [{q['chart_type']}]" if q.get("chart_type") else ""
        print(f"  {q['id']}. {q['name']}{chart} (source: {q.get('source_name', '?')})")


def _cmd_dashboards() -> None:
    from tusk.bi.db import get_dashboards
    dashboards = get_dashboards()
    if not dashboards:
        print("No dashboards.")
        return
    print(f"Dashboards ({len(dashboards)}):")
    for d in dashboards:
        default = " [default]" if d.get("is_default") else ""
        prebuilt = " [prebuilt]" if d.get("is_prebuilt") else ""
        print(f"  {d['id']}. {d['name']}{default}{prebuilt}")


def _cmd_run(args: list[str]) -> None:
    if not args:
        print("Error: sql required")
        print("Usage: tusk bi run \"SELECT ...\"")
        sys.exit(1)

    sql = " ".join(args)

    from tusk.bi.engine import BIQueryEngine
    engine = BIQueryEngine()

    try:
        result = engine.execute("duckdb", ":memory:", sql)
        columns = result.get("columns", [])
        rows = result.get("rows", [])

        if not columns:
            print("No results")
            return

        # Simple table output
        col_widths = [max(len(str(c)), max((len(str(row[i])) for row in rows), default=0)) for i, c in enumerate(columns)]
        col_widths = [min(w, 40) for w in col_widths]

        header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(columns))
        print(header)
        print("-" * len(header))
        for row in rows:
            print(" | ".join(str(v)[:40].ljust(col_widths[i]) for i, v in enumerate(row)))

        print(f"\n{len(rows)} rows")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def _cmd_status() -> None:
    from tusk.bi.db import get_data_sources, get_saved_queries, get_dashboards

    sources = get_data_sources()
    queries = get_saved_queries()
    dashboards = get_dashboards()

    print("BI Status")
    print("=" * 40)
    print(f"Data sources:    {len(sources)}")
    print(f"Saved queries:   {len(queries)}")
    print(f"Dashboards:      {len(dashboards)}")


# ── Dashboards as files ─────────────────────────────────────────────────


def _cmd_export(args: list[str]) -> None:
    """`tusk bi export <dashboard id|all> [--out DIR] [--json]`.

    Writes one file per dashboard (`<slug>.yaml`) containing everything
    `import` needs: the dashboard, its widgets and their queries.
    """
    from pathlib import Path

    from tusk.bi.db import export_dashboard, get_dashboards

    if not args:
        print("Usage: tusk bi export <dashboard id|all> [--out DIR] [--json]")
        sys.exit(2)
    out_dir = Path(".")
    as_json = "--json" in args
    if "--out" in args:
        out_dir = Path(args[args.index("--out") + 1])
    out_dir.mkdir(parents=True, exist_ok=True)
    targets = [d["id"] for d in get_dashboards()] if args[0] == "all" else [int(args[0])]
    for dashboard_id in targets:
        data = export_dashboard(dashboard_id)
        if not data:
            print(f"dashboard {dashboard_id}: not found")
            continue
        name = (data.get("dashboard") or data).get("name") or f"dashboard-{dashboard_id}"
        slug = "".join(c if c.isalnum() else "-" for c in name.lower()).strip("-") or f"dashboard-{dashboard_id}"
        path = out_dir / f"{slug}.{'json' if as_json else 'yaml'}"
        path.write_text(_dump(data, as_json))
        print(f"wrote {path}")


def _cmd_import(args: list[str]) -> None:
    """`tusk bi import <file>...` — YAML or JSON produced by `export`.
    A dashboard with the same name is replaced, so applying a file twice
    does not duplicate it."""
    from pathlib import Path

    from tusk.bi.db import delete_dashboard, get_dashboards, import_dashboard

    files = [a for a in args if not a.startswith("--")]
    if not files:
        print("Usage: tusk bi import <file>...")
        sys.exit(2)
    for f in files:
        path = Path(f)
        data = _load(path.read_text(), path.suffix.lower() == ".json")
        name = (data.get("dashboard") or data).get("name")
        existing = [d for d in get_dashboards() if name and d["name"] == name]
        for d in existing:
            delete_dashboard(d["id"])
        dashboard_id = import_dashboard(data, source_id=data.get("source_id"))
        print(f"{path}: dashboard '{name}' → id {dashboard_id}{' (replaced)' if existing else ''}")


def _dump(data: dict, as_json: bool) -> str:
    if as_json:
        import json

        return json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    import yaml

    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True)


def _load(text: str, as_json: bool) -> dict:
    if as_json:
        import json

        return json.loads(text)
    import yaml

    return yaml.safe_load(text)
