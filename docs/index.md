# Tusk Studio

PostgreSQL admin and SQL studio with an AI copilot and light analytics. One process, one browser tab, state in `~/.tusk`.

## What it is

The tool you open every day to work with your Postgres:

- **A SQL editor** with tabs, schema autocomplete, results as table / chart / map, and an AI Copilot grounded in the real catalog.
- **A schema map** of tables and foreign keys, with Schema Watch and Data Contracts to catch drift.
- **A table profiler** that samples a table and emits per-column statistics.
- **A PostgreSQL admin console** for processes, locks, settings, backups, roles, extensions.
- **Dashboards** built in, plus scheduled jobs and notifications.
- **An MCP server** so agents can query your connections read-only with your permissions.

You install one wheel. You run one process. You point a browser at it.

## When it's a fit

**Good fit**: small-to-medium businesses running one or a few Postgres databases, internal-tool teams that want dashboards + admin without spinning up Metabase + pgAdmin + Datadog, anyone tired of `docker-compose up` with 6 sidecars.

**Not the right fit**: high-availability multi-replica deployments (single-process by design — [ADR 0001](https://github.com/tuskdata/tuskdata/blob/main/specs/architecture/adrs/0001-single-process-by-default.md)), warehouse-scale analytics (use Snowflake / BigQuery / Trino), heavy ETL orchestration (use Airflow / dbt).

## Top-level features

| Tab | What it does | Doc page |
|---|---|---|
| **Home** | Activity summary, recent queries, AI suggestions | [home.md](features/home.md) |
| **Studio** | Multi-tab SQL editor with results table, charts, maps | [studio.md](features/studio.md) |
| **Schema** | Interactive ER diagram of your tables and foreign keys | [schema.md](features/schema.md) |
| **Explore** | Auto-profile any table — per-column stats, histograms, distinct counts | [explore.md](features/explore.md) |
| **Scheduled** | Backups, vacuum/analyze, queries, pipelines and schema checks on a cron | [scheduled.md](features/scheduled.md) |
| **Data** | Visual Polars ETL: files, Postgres, open data → transforms → export/import/pipeline | [data.md](features/data.md) |
| **Admin** | PostgreSQL admin: processes, locks, settings, backups, roles | [admin.md](features/admin.md) |
| **Analytics** | Dashboards and charts, built in | [analytics.md](features/analytics.md) |
| **Notifications** | Slack, Discord, Telegram, email or webhook for job, backup, schema and contract events | [notifications.md](features/notifications.md) |
| **Schema Watch** | Snapshot + diff of a connection's schema, notify on changes | [schema-watch.md](features/schema-watch.md) |
| **Data Contracts** | Freeze the schema as a contract; be told when something breaks it | [data-contracts.md](features/data-contracts.md) |
| **AI Copilot** | Plain-language to SQL grounded in the real catalog, PostGIS-aware, place-name lookup; explains plans | [copilot.md](features/copilot.md) |
| **MCP server** | Let Claude Code, Cursor or any MCP client query your connections, read-only | [mcp.md](features/mcp.md) |
| **Desktop window** | `tusk app`: Studio in a native window (preview) | [desktop.md](features/desktop.md) |
| **Users & tokens** | Single vs multi-user mode, sessions, personal API tokens, audit log | [auth.md](features/auth.md) |

## Get started

- **Install**: `pip install tuskdata[all]` then `tusk studio`.
- **Deploy on K8s**: see [deployment/kubernetes.md](deployment/kubernetes.md).
- **Read the architecture**: [ADR 0001 — single-process by default](https://github.com/tuskdata/tuskdata/blob/main/specs/architecture/adrs/0001-single-process-by-default.md).
