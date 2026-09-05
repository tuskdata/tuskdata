# Tusk Studio

A single-binary data platform for PostgreSQL teams. SQL editor + schema explorer + admin console + analytics + ETL, all in one container with no broker, no Redis, no docker-compose with five services.

## What it is

Tusk Studio combines what would normally be 4-5 separate tools:

- **A SQL editor** with multi-tab, schema autocomplete, results table/map/chart, AI Copilot.
- **A schema visualizer** that draws your tables + foreign keys as an interactive graph.
- **An auto-explore profile tool** that samples a table and emits per-column statistics.
- **A PostgreSQL admin console** for processes, locks, settings, backups, roles, extensions.
- **A BI engine** for dashboards, embedded analytics, scheduled snapshots.
- **A cluster mode** (via the `tusk-cluster` plugin) for distributed query execution.

You install one wheel. You run one process. You point a browser at it.

## When it's a fit

**Good fit**: small-to-medium businesses running one or a few Postgres databases, internal-tool teams that want dashboards + admin without spinning up Metabase + pgAdmin + Datadog, anyone tired of `docker-compose up` with 6 sidecars.

**Not the right fit (yet)**: high-availability multi-replica deployments (single-process by design — [ADR 0001](https://github.com/tuskdata/tuskdata/blob/main/specs/architecture/adrs/0001-single-process-by-default.md)), petabyte-scale lakehouse warehouses (use Snowflake / BigQuery / Trino), real-time CDC pipelines (on the roadmap, not 0.x).

## Top-level features

| Tab | What it does | Doc page |
|---|---|---|
| **Home** | Activity summary, recent queries, AI suggestions | [home.md](features/home.md) |
| **Studio** | Multi-tab SQL editor with results table, charts, maps | [studio.md](features/studio.md) |
| **Schema** | Interactive ER diagram of your tables and foreign keys | [schema.md](features/schema.md) |
| **Explore** | Auto-profile any table — per-column stats, histograms, distinct counts | [explore.md](features/explore.md) |
| **Scheduled** | Cron-style query/pipeline scheduler with snapshot history | (later) |
| **Data** | ETL pipelines, file upload, export | (later) |
| **Admin** | PostgreSQL admin: processes, locks, settings, backups, roles | [admin.md](features/admin.md) |
| **Analytics** | Dashboards + embedded analytics SDK (tusk-bi plugin) | [analytics.md](features/analytics.md) |
| **Schema Watch** | Snapshot + diff of a connection's schema, notify on changes | [schema-watch.md](features/schema-watch.md) |
| **Data Contracts** | Freeze the schema as a contract; be told when something breaks it | [data-contracts.md](features/data-contracts.md) |
| **MCP server** | Let Claude Code, Cursor or any MCP client query your connections, read-only | [mcp.md](features/mcp.md) |
| **Users & tokens** | Single vs multi-user mode, sessions, personal API tokens, audit log | [auth.md](features/auth.md) |
| **Cluster** | Distributed query execution (tusk-cluster plugin) | (later) |

## Get started

- **Install**: `pip install tuskdata[all]` then `tusk studio`.
- **Deploy on K8s**: see [deployment/kubernetes.md](deployment/kubernetes.md).
- **Read the architecture**: [ADR 0001 — single-process by default](https://github.com/tuskdata/tuskdata/blob/main/specs/architecture/adrs/0001-single-process-by-default.md).
