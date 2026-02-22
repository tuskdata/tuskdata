# Tusk

Modern Data Platform — SQL Client, PostgreSQL Admin, Analytics Engine, ETL Pipeline Builder & Plugin System

> **Built with Claude**: This project was developed with [Claude Code](https://claude.ai) (Anthropic's AI assistant).

## Features

### SQL Client (Studio)
- Multi-connection support (PostgreSQL, SQLite, DuckDB)
- Tabbed SQL editor with CodeMirror 6 (syntax highlighting, autocomplete)
- Schema browser with FK/PK indicators
- Query history and saved queries with folders
- Results grid with sortable, resizable columns
- Export to CSV/JSON
- Keyboard shortcuts (Ctrl+Enter, Ctrl+S, Ctrl+N, etc.)

### PostgreSQL Admin
- Server statistics dashboard with auto-refresh
- Active queries monitor with kill button
- Locks monitor with blocking visualization
- Backup/Restore with pg_dump/pg_restore
- Table maintenance (VACUUM, ANALYZE, REINDEX)
- Extensions manager
- Roles & users management
- Database settings viewer
- Replication status and slow query analysis

### Analytics Engine (DuckDB)
- In-process DuckDB for analytical queries
- File browser with Parquet, CSV, JSON, SQLite support
- Drag & drop file loading
- Export results to Parquet
- Engine selector (PostgreSQL/DuckDB)

### Data/ETL (Polars + DuckDB)
- Visual transform pipeline builder (filter, select, sort, group by, rename, drop nulls, limit, join)
- Engine selector: Auto/DuckDB/Polars with performance metrics
- OSM/PBF file support via Polars
- Auto-generated Polars code
- Export to CSV/Parquet, import to DuckDB/PostgreSQL
- Drag & drop file upload

### Geo Integration
- Auto-detect geometry columns (WKT, GeoJSON, EWKT, Hex WKB)
- Map visualization with MapLibre GL
- Points, lines, polygons rendering
- Feature popups and hover tooltips
- Export to GeoJSON

### Cluster Mode (Plugin)
- Distributed query processing with DataFusion
- Scheduler + Worker architecture via Arrow Flight
- Job persistence to SQLite with retry support
- Real-time cluster dashboard
- Connect to remote schedulers or start local cluster from UI

### User Management
- Single mode (no auth) and multi-user mode
- Session-based authentication
- 24 permissions across 6 categories
- Default groups (Administrators, Data Engineers, Analysts, Viewers)
- User management UI and CLI commands

## Installation

```bash
# Core only
pip install tuskdata

# With PostgreSQL support
pip install tuskdata[postgres]

# With full web UI (recommended)
pip install tuskdata[studio]

# Everything
pip install tuskdata[all]
```

Or install from source:

```bash
git clone https://github.com/tuskdata/tuskdata.git
cd tuskdata
pip install -e ".[all]"
```

## Quick Start

```bash
# Start the web studio
tusk studio
# Open http://127.0.0.1:8000

# Start with options
tusk studio --host 0.0.0.0 --port 3000

# Start cluster (dev mode, 3 workers)
tusk cluster --workers 3
```

## CLI Commands

```bash
tusk studio [options]     # Start the web studio
tusk config [options]     # Manage configuration
tusk scheduler [options]  # Start the cluster scheduler
tusk worker [options]     # Start a cluster worker
tusk cluster [options]    # Start local cluster (dev mode)
tusk users [subcommand]   # User management
tusk auth [subcommand]    # Authentication management
tusk plugins              # List installed plugins
tusk version              # Show version
```

## Authentication

### Single Mode (Default)
No authentication required. All features accessible.

### Multi-User Mode
```bash
tusk auth enable     # Enable auth mode
tusk auth init       # Create admin user and default groups
tusk studio          # Start studio (login required)
```

Default credentials: `admin` / `admin`

```bash
tusk users list                  # List all users
tusk users create john --admin   # Create admin user
tusk users create jane           # Create regular user
tusk users reset-password john   # Reset password
```

## Plugin System

Tusk has an extensible plugin architecture. Plugins can add new pages, API endpoints, CLI commands, datasets, templates, static files, and reusable components.

### Installing Plugins

```bash
pip install tusk-security    # Security analysis plugin
pip install tusk-cluster     # Distributed query plugin
tusk plugins                 # List installed plugins
tusk studio                  # Plugins auto-register on startup
```

### How Plugins Work

Plugins are Python packages that register via `pyproject.toml` entry points. On startup, Tusk:

1. **Discovers** plugins via `importlib.metadata.entry_points()`
2. **Checks** version compatibility
3. **Copies** plugin templates to `templates/plugins/{id}/`
4. **Copies** plugin static files to `static/plugins/{id}/`
5. **Mounts** plugin routes alongside core routes
6. **Calls** `on_startup()` lifecycle hook

Each plugin gets:
- **Sidebar tab** with icon and label
- **Isolated SQLite storage** at `~/.tusk/plugins/{id}.db`
- **TOML config file** at `~/.tusk/plugins/{id}.toml`
- **Template directory** accessible as `plugins/{id}/`
- **Static files** served at `/static/plugins/{id}/`
- **Dataset integration** — plugin tables queryable via DuckDB

### Creating a Plugin

```python
# my_plugin/__init__.py
from tusk.plugins.base import TuskPlugin
from pathlib import Path

class MyPlugin(TuskPlugin):
    @property
    def name(self) -> str:
        return "tusk-myplugin"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def tab_label(self) -> str:
        return "My Plugin"

    @property
    def tab_icon(self) -> str:
        return "puzzle"  # Lucide icon name

    @property
    def requires_storage(self) -> bool:
        return True

    def get_templates_path(self) -> Path | None:
        return Path(__file__).parent / "templates"

    def get_static_path(self) -> Path | None:
        return Path(__file__).parent / "static"

    def get_route_handlers(self) -> list:
        from .routes import MyPageController, MyAPIController
        return [MyPageController, MyAPIController]

    def get_datasets(self) -> list[dict]:
        return [{"name": "items", "table": "items", "description": "Plugin items"}]

    def get_cli_commands(self) -> dict:
        return {"myplugin": self.handle_cli}

    async def on_startup(self) -> None:
        from .db import init_database
        init_database()
```

Register in `pyproject.toml`:
```toml
[project.entry-points."tusk.plugins"]
myplugin = "my_plugin:MyPlugin"
```

### Plugin Templates & Components

Plugins can use all core UI components via MiniJinja macros:

```html
{# Plugin template — extends base.html like any core page #}
{% extends "base.html" %}
{% from "components/feedback.html" import badge, modal, alert %}
{% from "components/card.html" import stat_card, info_card %}
{% from "components/map.html" import map_assets, map_container, carto_dark_style %}
{% from "components/htmx.html" import htmx_poll, htmx_tabs %}

{% block content %}
    {{ stat_card(label="Total Items", value=42, icon="box", color="blue") }}

    {% call modal(id="create-item", title="Create Item", icon="plus") %}
        <form>...</form>
    {% endcall %}
{% endblock %}
```

Plugins can also create their own reusable components:

```html
{# Plugin-specific component at plugins/bi/components/chart.html #}
{% macro bar_chart(data, x, y, height="300px") %}
<div class="chart-container" style="height: {{ height }}">...</div>
{% endmacro %}
```

Other templates (core or plugin) can import these:
```html
{% from "plugins/bi/components/chart.html" import bar_chart %}
{{ bar_chart(data=sales, x="month", y="revenue") }}
```

### Plugin Static Files

Plugins with `get_static_path()` get their static files served automatically:

```
my_plugin/
├── __init__.py
├── static/           # get_static_path() points here
│   ├── chart.js      # Served at /static/plugins/myplugin/chart.js
│   └── styles.css    # Served at /static/plugins/myplugin/styles.css
└── templates/
    └── dashboard.html
```

```html
{# In plugin template #}
<script src="/static/plugins/myplugin/chart.js"></script>
```

## Component Library

Tusk includes a MiniJinja macro library for consistent UI across core pages and plugins.

| File | Macros |
|------|--------|
| `components/card.html` | `stat_card()`, `info_card()`, `simple_card()`, `loading_card()`, `metric_row()` |
| `components/table.html` | `data_table()`, `simple_table()`, `key_value_table()` |
| `components/forms.html` | `text_input()`, `select_input()`, `checkbox()`, `toggle()`, `button()`, `icon_button()`, `form_group()` |
| `components/feedback.html` | `badge()`, `severity_badge()`, `status_badge()`, `alert()`, `empty_state()`, `modal()`, `confirmation_dialog()`, `loading_spinner()`, `progress_bar()`, `tooltip()` |
| `components/htmx.html` | `htmx_table()`, `htmx_poll()`, `htmx_tabs()`, `htmx_search()`, `htmx_form()` |
| `components/map.html` | `map_assets()`, `map_container()`, `map_dark_styles()`, `carto_dark_style()` |

## Architecture

### Project Structure
```
src/tusk/
├── cli.py              # CLI entry point
├── core/               # Core functionality
│   ├── config.py       # Global configuration
│   ├── connection.py   # Connection registry
│   ├── auth.py         # Authentication system
│   ├── files.py        # File scanning
│   ├── geo.py          # GeoJSON/WKT utilities
│   ├── history.py      # Query history
│   ├── logging.py      # Structlog setup
│   ├── scheduler.py    # Task scheduler
│   └── result.py       # QueryResult struct
├── engines/            # Query engines
│   ├── duckdb_engine.py    # DuckDB analytics
│   ├── polars_engine.py    # Polars ETL
│   ├── postgres.py         # PostgreSQL
│   └── sqlite.py           # SQLite
├── admin/              # PostgreSQL admin modules
│   ├── stats.py        # Server statistics
│   ├── processes.py    # Active queries
│   ├── backup.py       # Backup/restore
│   ├── extensions.py   # Extensions manager
│   ├── roles.py        # Role management
│   ├── settings.py     # Settings viewer
│   └── maintenance.py  # Table maintenance
├── plugins/            # Plugin system
│   ├── base.py         # TuskPlugin abstract base class
│   ├── registry.py     # Discovery via entry_points
│   ├── storage.py      # Per-plugin SQLite storage
│   ├── config.py       # Per-plugin TOML config
│   └── templates.py    # Template & static file loader
└── studio/             # Web UI
    ├── app.py           # Litestar application
    ├── routes/          # API & page controllers
    ├── static/          # JS, CSS, plugin statics
    └── templates/       # HTML templates
        ├── base.html        # Base layout (loads Alpine, HTMX, Tailwind, Lucide)
        ├── components/      # Reusable MiniJinja macros
        │   ├── card.html
        │   ├── table.html
        │   ├── forms.html
        │   ├── feedback.html
        │   ├── htmx.html
        │   └── map.html
        ├── partials/        # HTMX partial responses
        └── plugins/         # Plugin templates (copied at startup)
```

### Tech Stack

| Layer | Technology |
|-------|-----------|
| Web Framework | Litestar 2.x |
| Server | Granian |
| Templates | MiniJinja |
| Serialization | msgspec (Structs) |
| CSS | Tailwind CSS |
| Interactivity | Alpine.js + HTMX |
| Icons | Lucide |
| Maps | MapLibre GL 4.1 |
| DataFrames | Polars |
| PostgreSQL | psycopg 3.x (async) |
| Analytics | DuckDB |
| Distributed | Arrow Flight + DataFusion |
| Logging | structlog |

## Configuration

```
~/.tusk/
├── config.toml          # Global settings
├── connections.toml     # Saved connections
├── history.db           # Query history (SQLite)
├── auth.db              # Users/groups (multi-user mode)
├── backups/             # Database backups
└── plugins/             # Plugin storage
    ├── security.db      # Plugin SQLite databases
    ├── security.toml    # Plugin config files
    └── ...
```

```bash
tusk config show
tusk config set pg_bin_path /usr/local/pgsql/bin
tusk config set port 3000
tusk config set auth_mode multi
```

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl+Enter | Execute query |
| Ctrl+S | Save query |
| Ctrl+N / Ctrl+T | New tab |
| Ctrl+W | Close tab |
| Ctrl+Space | Autocomplete |
| F5 | Refresh schema |
| Escape | Cancel query |

## Known Limitations

1. **Cluster Mode**: Requires scheduler/workers to be running. Local cluster spawns subprocesses.
2. **Auth System**: Sessions stored in SQLite. Server restart does not invalidate sessions.
3. **CDN Dependencies**: Frontend libraries (Tailwind, Alpine, HTMX, MapLibre, Lucide) loaded via CDN. No offline mode yet.
4. **Large Files**: Performance may degrade with files larger than 500MB.

## License

MIT
