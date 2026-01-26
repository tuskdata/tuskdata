# Tusk

Modern Data Platform - SQL Client, PostgreSQL Admin, Analytics Engine & Distributed Query Processing

> **⚠️ Experimental Project**: This is an experimental project built for learning and exploration purposes. Use at your own risk in production environments.
>
> **🤖 Built with Claude**: This project was developed with significant assistance from [Claude](https://claude.ai) (Anthropic's AI assistant), demonstrating the potential of human-AI collaboration in software development.

## Features

### SQL Client (Studio)
- Multi-connection support (PostgreSQL, SQLite, DuckDB)
- Tabbed SQL editor with CodeMirror 6
- Syntax highlighting and autocomplete
- Schema browser with FK/PK indicators
- Query history with persistence
- Saved queries with folders
- Results grid with sortable, resizable columns
- Export to CSV/JSON
- Keyboard shortcuts (Ctrl+Enter, Ctrl+S, etc.)
- Light/Dark theme toggle

### PostgreSQL Admin
- Server statistics dashboard with auto-refresh
- Active queries monitor with kill button
- Locks monitor with blocking visualization
- Backup/Restore with pg_dump/pg_restore
- Table maintenance (VACUUM, ANALYZE, REINDEX)
- Extensions manager (install/uninstall)
- **Roles & Users management** (create, edit, delete roles)
- **Database settings viewer** (important pg_settings)

### Analytics Engine (DuckDB)
- DuckDB integration for analytical queries
- File browser for data files
- Parquet, CSV, JSON, SQLite support
- Drag & drop file loading
- Export results to Parquet
- Engine selector (PostgreSQL/DuckDB)

### Data/ETL (Polars)
- Visual transform pipeline builder
- OSM/PBF file support
- 8 transform types (filter, select, sort, group by, rename, drop nulls, limit, join)
- Auto-generated Polars code
- Export to CSV/Parquet
- Import to DuckDB/PostgreSQL
- Drag & drop file upload

### Geo Integration
- Auto-detect geometry columns
- Map visualization with MapLibre GL
- Points, lines, polygons rendering
- Feature popups on click
- Export to GeoJSON

### Cluster Mode
- Distributed query processing
- Scheduler + Worker architecture
- Arrow Flight for data transfer
- DataFusion query execution
- Real-time cluster dashboard
- Connect to remote schedulers from UI
- Start/Stop local cluster from UI (single-node mode)

### User Management
- Single mode (no auth) and Multi-user mode
- User authentication with sessions
- 24 permissions across 6 categories
- Default groups (Administrators, Data Engineers, Analysts, Viewers)
- User management UI for admins
- **Profile page** for users to manage their account
- **Group assignment UI** with checkboxes
- CLI commands for user management

## Installation

```bash
# Core only
pip install tuskdata

# With PostgreSQL support
pip install tuskdata[postgres]

# With full web UI (recommended)
pip install tuskdata[studio]

# Everything (studio + admin + cluster)
pip install tuskdata[all]
```

Or install from source:

```bash
git clone https://github.com/tuskdata/tuskdata.git
cd tuskdata
pip install -e ".[all]"
```

## Quick Start

### Start the Web Studio
```bash
tusk studio
# Open http://127.0.0.1:8000
```

### Start with Options
```bash
tusk studio --host 0.0.0.0 --port 3000
```

### Start Cluster (Dev Mode)
```bash
# Start scheduler + 3 workers
tusk cluster --workers 3
```

### Start Components Separately
```bash
# Terminal 1: Scheduler
tusk scheduler --port 8814

# Terminal 2: Worker
tusk worker --scheduler localhost:8814 --port 8815

# Terminal 3: Another Worker
tusk worker --scheduler localhost:8814 --port 8816
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
tusk version              # Show version
tusk help                 # Show help
```

### Studio Options
```
--host HOST           Host to bind to (default: 127.0.0.1)
--port, -p PORT       Port to bind to (default: 8000)
--pg-bin-path PATH    Path to PostgreSQL binaries
```

### Scheduler Options
```
--host HOST           Host to bind to (default: 0.0.0.0)
--port, -p PORT       Port to bind to (default: 8814)
```

### Worker Options
```
--scheduler HOST:PORT Scheduler address (default: localhost:8814)
--host HOST           Host to bind to (default: 0.0.0.0)
--port, -p PORT       Port to bind to (default: 8815)
```

### Cluster Options (Dev Mode)
```
--workers, -w N       Number of workers (default: 3)
```

## Authentication

Tusk supports two modes:

### Single Mode (Default)
No authentication required. All features accessible.

### Multi-User Mode
Enable multi-user authentication:

```bash
# Enable auth mode
tusk auth enable

# Initialize (create admin user and default groups)
tusk auth init

# Start studio
tusk studio
```

Default credentials: `admin` / `admin`

### User Management CLI
```bash
tusk users list                     # List all users
tusk users create john --admin      # Create admin user
tusk users create jane              # Create regular user
tusk users reset-password john      # Reset password
tusk users delete john              # Delete user
```

## Configuration

Configuration files are stored in `~/.tusk/`:

```
~/.tusk/
├── config.toml       # Global settings
├── connections.toml  # Saved connections
├── history.db        # Query history (SQLite)
├── auth.db           # Users/groups (SQLite, multi-user mode)
└── backups/          # Database backups
```

### View Configuration
```bash
tusk config show
```

### Set Configuration
```bash
tusk config set pg_bin_path /usr/local/pgsql/bin
tusk config set port 3000
tusk config set auth_mode multi
```

## Usage

### Data Page - Creating Pipelines

1. **Select a Data Source**: Click "Select Data File" to browse and select a CSV, Parquet, JSON, or OSM/PBF file. You can also drag & drop files directly.

2. **Add Transforms**: Click "Add Transform" to add operations like filter, sort, group by, etc.

3. **Preview Results**: See real-time preview of your data with applied transforms.

4. **Export**: Export results to CSV, Parquet, or import directly to DuckDB/PostgreSQL.

5. **View Code**: Click "View Code" to see the auto-generated Polars code.

### Studio Page - Running Queries

1. **Add a Connection**: Click "+" in the Connections sidebar to add PostgreSQL, SQLite, or DuckDB connections.

2. **Browse Schema**: Expand tables in the Schema panel to see columns, types, and keys.

3. **Write Queries**: Use the SQL editor with autocomplete (Ctrl+Space).

4. **Execute**: Press Ctrl+Enter or click "Run" to execute queries.

5. **Export**: Export results to CSV or JSON.

### Admin Page - PostgreSQL Management

1. **Select a Server**: Choose a PostgreSQL connection from the sidebar.

2. **Monitor**: View real-time stats, active queries, and locks.

3. **Maintain**: Run VACUUM, ANALYZE, or REINDEX on tables.

4. **Manage Extensions**: Install or uninstall PostgreSQL extensions.

5. **Manage Roles**: Create, edit, or delete database roles.

## Architecture

### Project Structure
```
src/tusk/
├── cli.py           # CLI entry point
├── core/            # Core functionality
│   ├── config.py    # Global configuration
│   ├── connection.py# Connection registry
│   ├── auth.py      # Authentication system
│   ├── files.py     # File scanning
│   ├── geo.py       # GeoJSON/WKT utilities
│   ├── history.py   # Query history
│   ├── logging.py   # Structlog setup
│   └── result.py    # QueryResult dataclass
├── engines/         # Query engines
│   ├── duckdb_engine.py  # DuckDB
│   ├── polars_engine.py  # Polars ETL
│   ├── postgres.py       # PostgreSQL
│   └── sqlite.py         # SQLite
├── admin/           # PostgreSQL admin
│   ├── stats.py     # Server stats
│   ├── processes.py # Active queries
│   ├── backup.py    # Backup/restore
│   ├── extensions.py# Extensions
│   ├── roles.py     # Role management
│   ├── settings.py  # Settings viewer
│   └── maintenance.py # Table maintenance
├── cluster/         # Distributed processing
│   ├── models.py    # Job/Worker models
│   ├── scheduler.py # Arrow Flight scheduler
│   └── worker.py    # Arrow Flight worker
└── studio/          # Web UI
    ├── app.py       # Litestar app
    ├── routes/      # API endpoints
    ├── static/      # Static files (JS)
    └── templates/   # HTML templates
```

### Technologies
- **Web Framework**: Litestar + Granian
- **Database**: PostgreSQL (psycopg3), SQLite, DuckDB
- **ETL**: Polars
- **Distributed**: Arrow Flight + DataFusion
- **Frontend**: TailwindCSS, CodeMirror 6, MapLibre GL, Lucide Icons
- **Serialization**: msgspec
- **Logging**: structlog

## Dependencies

```toml
litestar >= 2.0
granian >= 2.0
psycopg[binary] >= 3.0
msgspec >= 0.18
duckdb >= 1.0
polars >= 1.0
pyarrow >= 17.0
datafusion >= 51.0
structlog >= 24.0
psutil >= 5.9
```

## Development

```bash
# Clone the repository
git clone https://github.com/tuskdata/tuskdata.git
cd tuskdata

# Install in development mode with all features
pip install -e ".[all]"

# Run studio
tusk studio

# Or directly
python -m tusk.cli studio
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

1. **Cluster Mode**: Requires scheduler/workers to be running. The "Start Local Cluster" spawns a subprocess which may have permission issues on some systems.

2. **Auth System**: In multi-user mode, sessions are stored in SQLite. Server restart does not invalidate sessions.

3. **State Persistence**: Data page state uses localStorage which is browser-specific.

4. **Large Files**: Performance may degrade with files larger than 500MB. Use appropriate limit settings.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT
