"""Pre-built dashboard definitions for common use cases"""

import json

import structlog
log = structlog.get_logger()


def ensure_prebuilt_dashboards() -> None:
    """Create pre-built dashboards for installed plugins.

    Only creates dashboards if the corresponding plugin is installed
    AND the dashboard doesn't already exist.
    """
    from tusk.bi.db import get_dashboards

    existing = {d["name"] for d in get_dashboards()}

    try:
        from tusk.plugins.registry import get_plugin
    except ImportError:
        return

    if get_plugin("tusk-security") and "Security Overview" not in existing:
        _create_security_dashboard()

    if get_plugin("tusk-cluster") and "Cluster Monitor" not in existing:
        _create_cluster_dashboard()

    # Always available — runs on in-memory DuckDB (VALUES literals), so
    # it works on any install regardless of which connections exist.
    # Wrapped: a failure here must not abort plugin init.
    if "Chart Gallery (Demo)" not in existing:
        try:
            _create_demo_dashboard()
        except Exception as e:
            log.warning("demo dashboard create failed", error=str(e))


def _get_or_create_duckdb_source() -> int:
    """Return a data source id backed by in-process DuckDB.

    DuckDB queries ignore connection_ref and run in-memory, so this is
    the one source guaranteed to work everywhere — perfect for the demo.
    """
    from tusk.bi.db import get_data_sources, create_data_source
    for s in get_data_sources():
        if s.get("source_type") == "duckdb":
            return s["id"]
    return create_data_source(
        name="DuckDB (in-process)",
        source_type="duckdb",
        connection_ref=":memory:",
    )


def _create_demo_dashboard() -> None:
    """Chart Gallery — one widget per chart type, on synthetic data.

    Doubles as a smoke test for the auto-axis detection (charts.py
    `suggest_axes`): most widgets save NO chart_config, so the renderer
    has to pick the right X (dimension) and Y (measure) itself. Several
    queries deliberately put an `id` column first to prove the detector
    skips it instead of plotting primary keys.
    """
    from tusk.bi.db import create_dashboard, create_saved_query, create_widget

    source_id = _get_or_create_duckdb_source()

    dash_id = create_dashboard(
        name="Chart Gallery (Demo)",
        description="One widget per chart type on synthetic data. "
                    "Most widgets rely on auto-detected axes.",
        is_prebuilt=True,
    )

    def q(name: str, sql: str, chart_type: str | None = None, cfg: dict | None = None) -> int:
        return create_saved_query(
            name=name, source_id=source_id, sql=sql,
            chart_type=chart_type,
            chart_config=json.dumps(cfg) if cfg else "{}",
            tags="demo,prebuilt",
        )

    # 1. Stat — single big number. Auto-detects "stat" from 1 row/1 num.
    q1 = q("Demo · Total Revenue", "SELECT 482300 AS total_revenue", chart_type="stat")
    create_widget(dashboard_id=dash_id, query_id=q1, widget_type="stat",
                  title="Total Revenue", col_start=1, col_span=3, row_start=1, row_span=2)

    q2 = q("Demo · Active Users", "SELECT 12840 AS active_users", chart_type="stat")
    create_widget(dashboard_id=dash_id, query_id=q2, widget_type="stat",
                  title="Active Users", col_start=4, col_span=3, row_start=1, row_span=2)

    q3 = q("Demo · Conversion %", "SELECT 3.7 AS conversion_pct", chart_type="stat")
    create_widget(dashboard_id=dash_id, query_id=q3, widget_type="stat",
                  title="Conversion %", col_start=7, col_span=3, row_start=1, row_span=2)

    q4 = q("Demo · Open Tickets", "SELECT 47 AS open_tickets", chart_type="stat")
    create_widget(dashboard_id=dash_id, query_id=q4, widget_type="stat",
                  title="Open Tickets", col_start=10, col_span=3, row_start=1, row_span=2)

    # 2. Bar — id first on purpose; auto must pick region/sales, not id.
    q5 = q("Demo · Sales by Region", """
        SELECT * FROM (VALUES
            (1, 'North', 420), (2, 'South', 310),
            (3, 'East', 280), (4, 'West', 190)
        ) AS t(id, region, sales)
    """, chart_type="bar")
    create_widget(dashboard_id=dash_id, query_id=q5, widget_type="chart",
                  title="Sales by Region (auto axes)", col_start=1, col_span=6, row_start=3, row_span=4)

    # 3. Line — temporal X auto-detected → line over time.
    q6 = q("Demo · Daily Signups", """
        SELECT * FROM (VALUES
            (DATE '2026-05-01', 120), (DATE '2026-05-02', 145),
            (DATE '2026-05-03', 132), (DATE '2026-05-04', 178),
            (DATE '2026-05-05', 196), (DATE '2026-05-06', 210),
            (DATE '2026-05-07', 188)
        ) AS t(day, signups)
    """, chart_type="line")
    create_widget(dashboard_id=dash_id, query_id=q6, widget_type="chart",
                  title="Daily Signups (auto line)", col_start=7, col_span=6, row_start=3, row_span=4)

    # 4. Pie — explicit (pie is a user choice, not auto-suggested).
    q7 = q("Demo · Traffic by Channel", """
        SELECT * FROM (VALUES
            ('Direct', 45), ('Organic', 30),
            ('Referral', 15), ('Social', 10)
        ) AS t(channel, pct)
    """, chart_type="pie", cfg={"chart_type": "pie", "x_column": "channel", "y_column": "pct"})
    create_widget(dashboard_id=dash_id, query_id=q7, widget_type="chart",
                  title="Traffic by Channel", col_start=1, col_span=4, row_start=7, row_span=4)

    # 5. Horizontal bar — many categories; auto picks horizontal_bar.
    q8 = q("Demo · Top Products", """
        SELECT * FROM (VALUES
            ('Widget A', 980), ('Widget B', 870), ('Widget C', 760),
            ('Widget D', 650), ('Widget E', 540), ('Widget F', 430),
            ('Widget G', 390), ('Widget H', 350), ('Widget I', 300),
            ('Widget J', 260)
        ) AS t(product, units)
    """, chart_type="bar")
    create_widget(dashboard_id=dash_id, query_id=q8, widget_type="chart",
                  title="Top Products (auto horizontal)", col_start=5, col_span=4, row_start=7, row_span=4)

    # 6. Grouped bar — auto-detect should find region as group_by.
    q9 = q("Demo · Quarterly by Region", """
        SELECT * FROM (VALUES
            ('Q1', 'North', 100), ('Q1', 'South', 80),
            ('Q2', 'North', 130), ('Q2', 'South', 95),
            ('Q3', 'North', 150), ('Q3', 'South', 110),
            ('Q4', 'North', 170), ('Q4', 'South', 140)
        ) AS t(quarter, region, sales)
    """, chart_type="bar")
    create_widget(dashboard_id=dash_id, query_id=q9, widget_type="chart",
                  title="Quarterly by Region (auto group)", col_start=9, col_span=4, row_start=7, row_span=4)

    # 7. Table — raw rows.
    q10 = q("Demo · Recent Orders", """
        SELECT * FROM (VALUES
            (1001, 'Alice', 'shipped', 248.50),
            (1002, 'Bob', 'pending', 99.00),
            (1003, 'Carol', 'shipped', 432.75),
            (1004, 'Dave', 'cancelled', 0.00),
            (1005, 'Eve', 'shipped', 156.20)
        ) AS t(order_id, customer, status, total)
    """)
    create_widget(dashboard_id=dash_id, query_id=q10, widget_type="table",
                  title="Recent Orders", col_start=1, col_span=12, row_start=11, row_span=4)

    log.info("Demo chart-gallery dashboard created", dashboard_id=dash_id, widgets=10)


def _get_source_id_for_plugin(plugin_id: str) -> int | None:
    """Get the data source ID for a plugin."""
    from tusk.bi.db import get_data_sources
    sources = get_data_sources()
    for s in sources:
        if s.get("plugin_id") == plugin_id:
            return s["id"]
    return None


def _create_security_dashboard() -> None:
    """Create security overview dashboard"""
    from tusk.bi.db import (
        create_dashboard, create_saved_query, create_widget,
    )

    source_id = _get_source_id_for_plugin("tusk-security")
    if not source_id:
        log.info("Security plugin source not found, skipping prebuilt dashboard")
        return

    dash_id = create_dashboard(
        name="Security Overview",
        description="Security scan results and vulnerability overview",
        is_prebuilt=True,
    )

    # Stat: Total code issues
    q1 = create_saved_query(
        name="Total Code Issues",
        source_id=source_id,
        sql="SELECT COUNT(*) as total FROM code_issues",
        chart_type="stat",
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q1, widget_type="stat",
                  title="Code Issues", col_start=1, col_span=3, row_start=1, row_span=2)

    # Stat: Vulnerable dependencies
    q2 = create_saved_query(
        name="Vulnerable Dependencies",
        source_id=source_id,
        sql="SELECT COUNT(DISTINCT package_name) as total FROM dependency_vulns",
        chart_type="stat",
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q2, widget_type="stat",
                  title="Vulnerable Packages", col_start=4, col_span=3, row_start=1, row_span=2)

    # Stat: Network hosts
    q3 = create_saved_query(
        name="Network Hosts Found",
        source_id=source_id,
        sql="SELECT COUNT(DISTINCT ip_address) as total FROM network_hosts",
        chart_type="stat",
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q3, widget_type="stat",
                  title="Network Hosts", col_start=7, col_span=3, row_start=1, row_span=2)

    # Stat: DNS blocked
    q4 = create_saved_query(
        name="DNS Blocked Queries",
        source_id=source_id,
        sql="SELECT COUNT(*) as total FROM dns_queries WHERE blocked = 1",
        chart_type="stat",
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q4, widget_type="stat",
                  title="DNS Blocked", col_start=10, col_span=3, row_start=1, row_span=2)

    # Bar: Issues by severity
    q5 = create_saved_query(
        name="Issues by Severity",
        source_id=source_id,
        sql="SELECT severity, COUNT(*) as count FROM code_issues GROUP BY severity ORDER BY count DESC",
        chart_type="bar",
        chart_config=json.dumps({"x_column": "severity", "y_column": "count"}),
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q5, widget_type="chart",
                  title="Issues by Severity", col_start=1, col_span=6, row_start=3, row_span=4)

    # Pie: Vulnerabilities by package
    q6 = create_saved_query(
        name="Vulns by Package",
        source_id=source_id,
        sql="SELECT package_name, COUNT(*) as count FROM dependency_vulns GROUP BY package_name ORDER BY count DESC LIMIT 10",
        chart_type="pie",
        chart_config=json.dumps({"x_column": "package_name", "y_column": "count"}),
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q6, widget_type="chart",
                  title="Vulns by Package", col_start=7, col_span=6, row_start=3, row_span=4)

    # Table: Recent scans
    q7 = create_saved_query(
        name="Recent Scans",
        source_id=source_id,
        sql="SELECT scan_id, scan_type, status, started_at FROM scan_history ORDER BY started_at DESC LIMIT 10",
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q7, widget_type="table",
                  title="Recent Scans", col_start=1, col_span=6, row_start=7, row_span=4)

    # Bar: Top blocked domains
    q8 = create_saved_query(
        name="Top Blocked Domains",
        source_id=source_id,
        sql="SELECT domain, COUNT(*) as count FROM dns_queries WHERE blocked = 1 GROUP BY domain ORDER BY count DESC LIMIT 10",
        chart_type="horizontal_bar",
        chart_config=json.dumps({"x_column": "domain", "y_column": "count"}),
        tags="security,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q8, widget_type="chart",
                  title="Top Blocked Domains", col_start=7, col_span=6, row_start=7, row_span=4)

    log.info("Security dashboard created", dashboard_id=dash_id, widgets=8)


def _create_cluster_dashboard() -> None:
    """Create cluster monitoring dashboard"""
    from tusk.bi.db import (
        create_dashboard, create_saved_query, create_widget,
    )

    source_id = _get_source_id_for_plugin("tusk-cluster")
    if not source_id:
        log.info("Cluster plugin source not found, skipping prebuilt dashboard")
        return

    dash_id = create_dashboard(
        name="Cluster Monitor",
        description="Distributed query cluster monitoring",
        is_prebuilt=True,
    )

    # Stat: Total jobs
    q1 = create_saved_query(
        name="Total Jobs",
        source_id=source_id,
        sql="SELECT COUNT(*) as total FROM jobs",
        chart_type="stat",
        tags="cluster,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q1, widget_type="stat",
                  title="Total Jobs", col_start=1, col_span=4, row_start=1, row_span=2)

    # Stat: Completed jobs
    q2 = create_saved_query(
        name="Completed Jobs",
        source_id=source_id,
        sql="SELECT COUNT(*) as total FROM jobs WHERE status = 'completed'",
        chart_type="stat",
        tags="cluster,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q2, widget_type="stat",
                  title="Completed", col_start=5, col_span=4, row_start=1, row_span=2)

    # Stat: Failed jobs
    q3 = create_saved_query(
        name="Failed Jobs",
        source_id=source_id,
        sql="SELECT COUNT(*) as total FROM jobs WHERE status = 'failed'",
        chart_type="stat",
        tags="cluster,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q3, widget_type="stat",
                  title="Failed", col_start=9, col_span=4, row_start=1, row_span=2)

    # Bar: Jobs by status
    q4 = create_saved_query(
        name="Jobs by Status",
        source_id=source_id,
        sql="SELECT status, COUNT(*) as count FROM jobs GROUP BY status",
        chart_type="bar",
        chart_config=json.dumps({"x_column": "status", "y_column": "count"}),
        tags="cluster,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q4, widget_type="chart",
                  title="Jobs by Status", col_start=1, col_span=6, row_start=3, row_span=4)

    # Table: Recent jobs
    q5 = create_saved_query(
        name="Recent Cluster Jobs",
        source_id=source_id,
        sql="SELECT id, sql, status, worker_id, created_at FROM jobs ORDER BY created_at DESC LIMIT 10",
        tags="cluster,prebuilt",
    )
    create_widget(dashboard_id=dash_id, query_id=q5, widget_type="table",
                  title="Recent Jobs", col_start=7, col_span=6, row_start=3, row_span=4)

    log.info("Cluster dashboard created", dashboard_id=dash_id, widgets=5)
