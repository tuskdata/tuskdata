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
