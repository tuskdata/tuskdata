# Tusk

**PostgreSQL admin and SQL studio with an AI copilot and light analytics — one process, one browser tab.**

Tusk is what you open every day to work with your Postgres: a multi-tab SQL editor, a schema map, a table profiler, the admin console (processes, locks, backups, roles, settings), dashboards, scheduled jobs with notifications, and an AI copilot grounded in your real schema. Agents can use it too: Tusk is an MCP server, read-only, with the same permissions as the person whose token they carry.

[Documentation](https://tuskdata.github.io/tuskdata/) · [Changelog](CHANGELOG.md) · [Roadmap](specs/roadmap/now.md)

> Built with [Claude Code](https://claude.ai) (Anthropic).

![Tusk Studio](docs/screenshots/studio.png)

## What's inside

| Tab | What it does |
|---|---|
| **Studio** | Multi-tab SQL editor (CodeMirror 6, autocomplete from your schema), results as table / chart / map, EXPLAIN with an AI reading of the plan, saved queries and history, per-connection colours, one-click table preview. |
| **Schema** | Interactive diagram of tables and foreign keys. Hosts **Schema Watch** (snapshot + diff of the catalog, notifications on drift) and **Data Contracts** (freeze the schema you depend on; be told when it breaks). |
| **Explore** | Profile any table: per-column stats, null rates, distinct counts, histograms. |
| **Admin** | Server dashboard, active queries with kill, locks and blocking chains, settings, extensions, roles, VACUUM/ANALYZE/REINDEX, backup and restore with `pg_dump`. |
| **Analytics** | Dashboards and charts on top of your connections, with auto-detected chart types. Built in since 0.4.36. |
| **Scheduled** | Backups (with retention), VACUUM/ANALYZE, queries with saved results, pipelines and schema checks on a cron, interval or one-shot trigger. |
| **Notifications** | Slack, Discord, Telegram, email or webhook for job, backup, schema and contract events. |
| **Data** | Small visual ETL with Polars: files, Postgres, open data → filter / join / aggregate → export, import into Postgres or save as a pipeline. |
| **AI Copilot** | Ask in plain language, get SQL grounded in the actual catalog; explain a plan; runs against your own provider (Ollama, OpenAI-compatible, Anthropic). |
| **MCP server** | `POST /mcp`: Claude Code, Cursor or any MCP client can list connections, read schemas, run read-only queries and check schema changes — audited, permission-scoped. |
| **Users & tokens** | Single-user mode by default; multi-user mode with groups, sessions, personal API tokens and an audit log. |
| **Desktop window** | `tusk app` opens Studio in a native window (preview). |

Screenshots of every page are in the [docs](https://tuskdata.github.io/tuskdata/) and in [`docs/screenshots/`](docs/screenshots/README.md).

## Install

Python 3.13.

```bash
pip install "tuskdata[all]"        # Studio + PostgreSQL + admin tooling (recommended)
pip install "tuskdata[studio]"     # Web UI only
pip install "tuskdata[app]"        # + native window (pywebview)

tusk studio                        # http://127.0.0.1:8000
```

From source:

```bash
git clone https://github.com/tuskdata/tuskdata.git
cd tuskdata
uv pip install -e ".[all]"
```

Container: `docker compose up --build` uses the in-repo [Dockerfile](Dockerfile) (`python:3.13-slim`, port 8000, healthcheck on `/api/health`). Tusk runs as **one process** with its state in `~/.tusk`; see [Deploying on Kubernetes](docs/deployment/kubernetes.md) and [ADR 0001](specs/architecture/adrs/0001-single-process-by-default.md) for why there is exactly one replica.

## Quick start

```bash
tusk studio                         # local, single-user, binds 127.0.0.1
tusk studio --host 0.0.0.0 -p 3000  # on a server (put it behind TLS)
tusk app                            # native window (preview)
```

Add a connection on the Home page, open Studio, run a query. `⌘K` / `Ctrl+K` jumps anywhere.

### Multi-user mode

```bash
tusk auth enable
tusk auth init                      # admin user + default groups
tusk studio
```

Personal API tokens (for MCP clients, scripts, CI) are created in **Profile → API tokens** or with `tusk auth token create <user> <name> --expires-days 90`. A token is the user: same connections, same permissions.

### Connect an AI agent

```bash
claude mcp add --transport http tusk http://127.0.0.1:8000/mcp
# multi-user: add --header "Authorization: Bearer tusk_..."
```

Tools: `list_connections`, `get_schema`, `run_query` (read-only, capped), `explain_query`, `schema_changes`, `contract_status`, `list_saved_queries`, `run_saved_query`. Every call lands in the audit log.

## CLI

```
tusk studio [--host H] [--port N] [--pg-bin-path P]   Start the web studio
tusk app [--url URL] [--port N]                        Studio in a native window (preview)
tusk config show | set KEY VALUE                       Configuration
tusk users list | create | delete | reset-password     User management (multi-user)
tusk auth enable | disable | init                      Auth mode
tusk auth token create | list | revoke                 Personal API tokens
tusk ai stats [--days N] [--verbose]                   Copilot hit-rate report
tusk plugins                                           Installed plugins
tusk features                                          Installed optional features
tusk version
```

## Where things live

```
~/.tusk/
├── config.toml          # settings (also editable in Settings → Studio)
├── connections.toml     # saved connections
├── auth.db              # users, groups, sessions, API tokens (multi-user)
├── scheduler.db         # scheduled jobs and runs
├── schema_watch.db      # schema snapshots, changes, contracts
├── stats_history.db     # admin dashboard history
├── backups/             # pg_dump output
├── scheduled_results/   # saved query results from jobs
├── workspaces/          # Data page state
└── plugins/             # per-plugin SQLite + TOML (tusk_bi.db, …)
```

Environment knobs worth knowing: `TUSK_ADMIN_ALLOW_LAN=1` (admin endpoints from a trusted LAN in single-user mode), `TUSK_ALLOW_PRIVATE_WEBHOOKS=1` (webhooks to private addresses, dev only).

## Plugins

Analytics ships inside the package as a built-in plugin; the same mechanism is open to third parties. A plugin is a Python package exposing a `TuskPlugin` subclass through the `tusk.plugins` entry point; it gets a tab, routes, templates, static files, isolated SQLite storage, CLI commands, scheduled-job kinds and notification events.

```python
from pathlib import Path
from tusk.plugins.base import TuskPlugin

class MyPlugin(TuskPlugin):
    name = "tusk-myplugin"
    version = "0.1.0"
    tab_label = "My Plugin"
    tab_icon = "puzzle"                      # Lucide icon

    def get_templates_path(self) -> Path | None:
        return Path(__file__).parent / "templates"

    def get_route_handlers(self) -> list:
        from .routes import MyPageController
        return [MyPageController]
```

```toml
[project.entry-points."tusk.plugins"]
myplugin = "my_plugin:MyPlugin"
```

Plugin templates extend `base.html` and can use the macro library in `templates/components/` (`badge`, `modal`, `alert`, `empty_state`, `stat_card`, form fields, status dots, map and pipeline canvas). The macros carry no colours: they map to the design tokens, so they follow the light/dark theme.

`tusk-cluster` (distributed queries with DataFusion + Arrow Flight) exists as a separate plugin and is currently paused.

## Tech stack

Litestar 2 · Granian · MiniJinja · msgspec · Tailwind · Alpine.js + HTMX · Lucide · CodeMirror 6 · MapLibre GL · psycopg 3 · DuckDB · Polars · structlog · litestar-mcp.

## Development

```bash
uv pip install -e ".[all]" --group dev
pytest tests/ -q                            # unit + API tests; e2e need `playwright install chromium`
python scripts/demo_db.py                   # synthetic demo database (needs local Postgres)
python scripts/docs_screenshots.py          # regenerate docs/screenshots/
mkdocs serve                                # docs at http://127.0.0.1:8000/tuskdata/
```

Code, comments, commits and docs are in English. Bugs found while a release is being prepared go into that release, not a hotfix.

## License

MIT — © 2026 Jearel Alcantara
