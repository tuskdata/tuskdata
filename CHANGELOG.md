# Changelog

All notable changes to Tusk will be documented in this file.

## [0.7.0] - 2026-01-25

### Code Architecture Improvements

#### Frontend Code Separation
- **JavaScript**: All inline JS extracted to separate files in `/static/`
  - `studio.js` - Studio page (SQL client)
  - `admin.js` - Admin page (PostgreSQL admin)
  - `cluster.js` - Cluster page (distributed queries)
  - `data.js` - Data page (ETL pipelines)
  - `profile.js` - Profile page
  - `users.js` - User management page
- **CSS**: Common styles moved to `/static/styles.css`
- Templates now only contain HTML structure with external references
- Standardized sidebar width to 256px (w-64) across all pages

#### Response Compression
- Added zstd compression support via Litestar CompressionConfig
- Responses larger than 500 bytes are automatically compressed
- Uses Python 3.14 stdlib zstd support

### DuckDB Enhancements

#### Spatial Extension Support
- DuckDB Spatial extension now auto-installs at startup
- Enables geospatial functions: ST_Point, ST_Distance, ST_Within, etc.
- Full WKT/WKB/GeoJSON support for geometry data

#### Extension Manager UI
- New Extension Manager modal in Data page (puzzle icon button)
- List all installed and available DuckDB extensions
- Install new extensions with one click
- Load installed extensions that aren't loaded
- Shows extension status (loaded/installed) and description
- API endpoints: GET /api/duckdb/extensions, POST /api/duckdb/extensions/{name}/install, POST /api/duckdb/extensions/{name}/load

### Scheduled Tasks (APScheduler)

#### Task Scheduler Service
- New `tusk.core.scheduler` module with APScheduler integration
- AsyncIOScheduler for non-blocking task execution
- Support for cron-based and interval-based schedules
- Job management: pause, resume, run now, delete

#### Scheduled Task Types
- **Backup**: Automatic pg_dump at scheduled times
- **VACUUM**: Scheduled VACUUM (regular or FULL) for table maintenance
- **ANALYZE**: Scheduled ANALYZE for query planner statistics

#### Scheduler UI (Admin Page)
- New "Scheduled Tasks" section in Admin page
- Add scheduled backup, vacuum, or analyze tasks
- Configure schedule: hour, minute, day of week
- View all scheduled jobs with next run time
- Pause/resume individual jobs
- Run job immediately (manual trigger)
- Delete scheduled jobs
- API endpoints: GET/POST /api/scheduler/jobs, POST /api/scheduler/jobs/{id}/pause|resume|run, DELETE /api/scheduler/jobs/{id}

### PostgreSQL Admin Enhancements

#### Roles Management (Phase 2)
- **List Roles**: View all PostgreSQL roles with attributes
- **Create Role**: Create new roles with login/superuser/createdb/createrole options
- **Edit Role**: Modify role attributes and password
- **Delete Role**: Remove roles from database
- UI in Admin page under "Roles & Users" section

#### Database Settings Viewer (Phase 2)
- View PostgreSQL configuration settings
- Toggle between important settings and all settings
- Shows setting name, current value, category, and description
- Highlights settings requiring restart
- UI in Admin page under "Database Settings" section

#### Auto-Refresh Configuration
- Dropdown to select refresh interval: Off, 5s, 10s, 30s, 60s
- Applies to stats and processes sections

### User Management Enhancements (Phase 7)

#### Profile Page
- New `/profile` route
- View and edit display name and email
- Change password with current password verification
- View assigned groups and permissions

#### Group Assignment UI
- Improved user edit modal with group checkboxes
- Select multiple groups for a user
- Real-time updates to group membership

#### CLI Commands
- `tusk users list` - List all users
- `tusk users create USERNAME` - Create new user
- `tusk users reset-password USERNAME` - Reset user password
- `tusk users delete USERNAME` - Delete user
- `tusk auth enable` - Enable multi-user auth
- `tusk auth disable` - Disable auth (single mode)

### UX Improvements

#### Toast Notifications
- Global toast notification system in base.html
- Success, error, warning, and info variants
- Auto-dismiss after 4 seconds with close button
- Replaces browser alert() dialogs throughout the app

#### Drag & Drop File Upload
- Drop files directly onto Data page to load them
- Supports CSV, Parquet, JSON, OSM/PBF files
- Visual drop overlay with feedback

#### Resizable Columns
- Data grid columns can be resized by dragging
- Minimum width of 80px enforced

### Technical

#### New Files
- `/src/tusk/admin/roles.py` - PostgreSQL role management
- `/src/tusk/admin/settings.py` - PostgreSQL settings viewer
- `/src/tusk/studio/static/styles.css` - Common CSS
- `/src/tusk/studio/static/admin.js` - Admin page JS
- `/src/tusk/studio/static/cluster.js` - Cluster page JS
- `/src/tusk/studio/static/data.js` - Data page JS
- `/src/tusk/studio/static/profile.js` - Profile page JS
- `/src/tusk/studio/static/users.js` - Users page JS
- `/src/tusk/studio/templates/profile.html` - Profile page template

#### Configuration
- CLAUDE.md updated with rules for JS/CSS separation
- CLAUDE.md updated with sidebar width standardization rules

---

## [0.6.0] - 2026-01-25

### Phase 7: User Management & Access Control - Complete

#### Added
- **Auth System**: Complete authentication infrastructure
  - Single mode (default, no auth) and Multi-user mode
  - User model with SQLite storage (~/.tusk/users.db)
  - Password hashing with SHA-256 + salt
  - Session management with secure tokens
  - 24-hour session lifetime (configurable)
- **Permissions System**: Fine-grained access control
  - 24 permissions across 6 categories
  - Connections, Queries, Admin, Data/ETL, Cluster, System
  - Default groups: Administrators, Data Engineers, Analysts, Viewers
- **Login Page**: Clean login UI at /login
  - Auto-redirect when auth enabled
  - Remember redirect URL after login
- **User Management Page**: Admin UI at /users
  - Users table with search
  - Create/edit user modals
  - Password reset
  - Groups tab with permission overview
- **User Menu**: Header dropdown when authenticated
  - Shows user avatar and name
  - Quick access to User Management (admins)
  - Sign out button
- **API Endpoints**: Complete REST API
  - `/api/auth/status` - Check auth status
  - `/api/auth/login` - Login with credentials
  - `/api/auth/logout` - Logout and clear session
  - `/api/users/*` - User CRUD operations
  - `/api/groups/*` - Group management

#### Configuration
```toml
[auth]
mode = "multi"              # Enable multi-user mode
session_lifetime = 86400    # 24 hours
allow_registration = false
```

#### Technical
- New `tusk.core.auth` module with User, Group, Session models
- New `tusk.studio.routes.auth` with AuthController, UsersController, GroupsController
- New templates: login.html, users.html
- Updated base.html with user menu
- Auth database: ~/.tusk/users.db

---

## [0.5.0] - 2026-01-25

### Phase 6: Cluster Mode - Complete

#### Added
- **CLI Commands**: New cluster management commands
  - `tusk scheduler` - Start the cluster scheduler
  - `tusk worker` - Start a cluster worker
  - `tusk cluster` - Start local dev cluster (scheduler + N workers)
- **Cluster Tab**: New navigation tab for cluster management
- **Scheduler**: Arrow Flight-based job distribution server
  - Accept job submissions via Arrow Flight
  - Distribute work to registered workers
  - Track job progress and status
  - Worker registration and health monitoring
- **Worker**: DataFusion query executor
  - Register with scheduler on startup
  - Execute SQL queries with DataFusion
  - Return results via Arrow Flight
  - Health metrics (CPU, memory) via psutil
  - Heartbeat loop for status reporting
- **Cluster Dashboard UI**: Real-time cluster monitoring
  - Connect to remote scheduler (host:port form)
  - Disconnect from scheduler button
  - Start/Stop local cluster from UI (single-node mode)
  - Select number of workers (1-4) for local cluster
  - Worker list with status indicators (idle/busy/offline)
  - CPU/Memory metrics per worker
  - Active jobs with progress bars
  - Job history table
  - Job submission form
  - Cancel job functionality
  - Auto-refresh every 3 seconds
- **Job Model**: Complete job lifecycle management
  - Status tracking (pending/running/completed/failed/cancelled)
  - Progress tracking with stages
  - Job cancellation
  - Error handling

#### Technical
- New `tusk.cluster` module:
  - `scheduler.py` - FlightSchedulerServer with Arrow Flight
  - `worker.py` - FlightWorkerServer with DataFusion
  - `models.py` - Job, WorkerInfo, ClusterStatus msgspec Structs
- New `tusk.studio.routes.cluster` - Cluster API endpoints
- New `tusk.studio.templates.cluster.html` - Cluster dashboard
- Added `psutil>=5.9` dependency for metrics
- Multiprocessing support for local dev cluster

---

## [0.4.0] - 2026-01-25

### Phase 4: Data/ETL with Polars - Complete

#### Added
- **Data Tab**: New navigation tab for ETL pipeline building
- **File Browser**: Navigate local filesystem to select data files
- **Source Support**: CSV, Parquet, JSON, and OSM/PBF files
- **OSM Integration**: Load OpenStreetMap data using DuckDB ST_ReadOSM
  - Support for nodes, ways, relations, or all data layers
  - Automatic geometry column creation for points
- **Transform Pipeline**: Visual transform builder with 8 transform types:
  - **Filter**: Filter rows by column conditions (eq, ne, gt, lt, contains, is_null, etc.)
  - **Select**: Select specific columns
  - **Sort**: Sort by columns (ascending/descending)
  - **Group By**: Group by columns with aggregations (sum, mean, min, max, count)
  - **Rename**: Rename columns
  - **Drop Nulls**: Remove rows with null values
  - **Limit**: Limit number of rows
  - **Join**: Join with another file (inner, left, right, outer)
- **Quick Transforms**: Sidebar shortcuts for common transforms
- **View Code**: Generate Polars Python code from pipeline
- **Preview**: Real-time data preview with limit selector (100/500/1000/5000 rows)
- **Export CSV**: Export pipeline results to CSV file
- **Export Parquet**: Export pipeline results to Parquet file
- **Import to DuckDB**: Load data into DuckDB table (in-memory or file)
- **Import to PostgreSQL**: Load data into PostgreSQL table
- **Save Pipeline**: Save pipelines to localStorage with custom names
- **Load Pipeline**: Load and restore saved pipelines
- **State Persistence**: Data page state persists across navigation
- **Geo Detection**: Auto-detect geometry columns in Data page
- **Map Modal**: Full-screen map visualization with MapLibre GL
- **Geo Popups**: Click on map features to see properties
- **Export GeoJSON**: Export geo data to GeoJSON format
- **Structured Logging**: Added structlog for better debugging

#### Technical
- New `tusk.engines.polars_engine` module with Pipeline/Transform models
- New `tusk.studio.routes.data` controller for Data API
- New `tusk.studio.templates.data.html` template
- New `tusk.core.logging` module with structlog configuration
- Export functions: `export_to_csv()`, `export_to_parquet()`
- Import functions: `import_to_duckdb()`, `import_to_postgres()`
- Pipeline code generation: `generate_code()`
- OSM loading: `load_osm()` with ST_ReadOSM

---

## [0.3.0] - 2025-01-25

### Phase 5: Geo Integration

#### Added
- **Geo Detection**: Auto-detect geometry columns (WKT, GeoJSON, PostGIS)
- **Map Button**: Shows when geometry columns are detected in results
- **Map View**: Full-screen map modal with MapLibre GL JS
- **Dark Basemap**: CARTO dark tiles for consistent dark theme
- **Points**: Orange circles for Point/MultiPoint geometries
- **Lines**: Green lines for LineString/MultiLineString geometries
- **Polygons**: Purple fill with outline for Polygon/MultiPolygon geometries
- **Popups**: Click on features to see properties
- **Auto-fit**: Map automatically fits to data bounds
- **Export GeoJSON**: Download results as GeoJSON file
- **WKT Parser**: Parse WKT/EWKT strings to GeoJSON
- **msgspec GeoJSON**: Type-safe GeoJSON structs with msgspec

#### Technical
- New `tusk.core.geo` module with msgspec Structs for GeoJSON
- WKT parsing for Point, LineString, Polygon, Multi* types
- MapLibre GL JS 4.1.0 for map rendering
- CARTO dark basemap tiles

---

## [0.2.0] - 2025-01-25

### Phase 3: Analytics Engine (DuckDB)

#### Added
- **DuckDB Engine**: In-memory analytics engine with Parquet, CSV, JSON, SQLite support
- **File Browser**: Sidebar section to browse registered data folders
- **Add Folder**: Register folders containing data files to browse
- **Auto-detect Types**: Automatically detect Parquet, CSV, TSV, JSON, SQLite files
- **File Preview**: Modal to preview file contents with schema info
- **Insert Query**: Generate DuckDB query from file preview
- **Engine Selector**: Toggle between PostgreSQL and DuckDB engines
- **Parquet Files**: Read Parquet files with `read_parquet()`
- **CSV Files**: Read CSV/TSV files with `read_csv_auto()`
- **JSON Files**: Read JSON/JSONL files with `read_json_auto()`
- **SQLite Files**: Read SQLite databases with `sqlite_scan()`
- **Export to Parquet**: Export query results to Parquet format (DuckDB only)
- **Federated Queries**: Backend support for attaching Postgres connections to DuckDB

#### Technical
- New `DuckDBEngine` class in `tusk.engines.duckdb_engine`
- New `FilesController` and `DuckDBController` in `tusk.studio.routes.files`
- New `tusk.core.files` module for file scanning and folder management
- DuckDB extensions: parquet, postgres_scanner, sqlite

---

## [0.1.1] - 2025-01-24

### Data Grid Improvements (Phase 1)

#### Added
- **Results**: Sort by column (click header to toggle asc/desc)
- **Results**: Filter results with quick search box
- **Results**: Pagination (100 rows per page with navigation)
- **Results**: Export to CSV with proper escaping
- **Results**: Export to JSON
- **Results**: NULL values displayed as styled badge
- **Results**: Boolean values displayed as checkmark/x
- **Results**: JSON values with purple highlight
- **Results**: Long values truncated with tooltip
- **Results**: Sticky header when scrolling
- **Connections**: Test Connection button (works before saving)
- **Schema**: Refresh button (also F5 keyboard shortcut)
- **Schema**: Primary key indicator (key icon)
- **UI**: Loading spinner while query is running
- **UI**: Run button shows spinner and disables during execution
- **UI**: Lucide icons in navigation
- **UI**: Improved navigation with pill-style tabs

### PostgreSQL Admin Improvements (Phase 2)

#### Added
- **Processes**: Highlight slow queries (> 10 seconds) with warning badge
- **Processes**: Highlight "idle in transaction" state with red styling
- **Processes**: Visual indicator (left border + background) for queries needing attention
- **Locks Monitor**: View all active locks in the database
- **Locks Monitor**: Identify blocking locks with blocked/blocking PIDs
- **Locks Monitor**: View waiting queries and their lock modes
- **Locks Monitor**: Visualize lock chains (blocked to blocking relationship)
- **Locks Monitor**: Toggle between blocking-only and all locks view
- **Locks Monitor**: Kill blocker button to terminate blocking processes
- **Table Maintenance**: View table bloat with dead tuples count
- **Table Maintenance**: VACUUM button for individual tables
- **Table Maintenance**: VACUUM FULL button (with lock warning)
- **Table Maintenance**: ANALYZE button to update table statistics
- **Table Maintenance**: REINDEX button to rebuild table indexes
- **Table Maintenance**: Visual indicators for tables needing maintenance
- **Table Maintenance**: Last vacuum/analyze dates displayed

### Saved Queries & Tab Persistence (Phase 1)

#### Added
- **Saved Queries**: Save queries with custom names
- **Saved Queries**: Organize queries in folders
- **Saved Queries**: Load saved query into editor (click)
- **Saved Queries**: Edit saved query name and folder
- **Saved Queries**: Delete saved queries
- **Saved Queries**: Keyboard shortcut Ctrl+S to save current query
- **Saved Queries**: Sidebar section showing all saved queries
- **Tab Persistence**: Tabs now persist between page loads (localStorage)
- **Tab Persistence**: Query content preserved when navigating to Admin and back
- **Tab Persistence**: Auto-save tabs every 5 seconds and on page unload

### Quick Wins (Phase 1 & 2 Polish)

#### Added
- **Query Cancellation**: Cancel running queries with Escape key or Cancel button
- **Query Cancellation**: Run button transforms to Cancel button while query runs
- **Last Connection**: Auto-select last used connection on page load (localStorage)
- **Foreign Keys**: FK columns show link icon with reference info in schema browser
- **Foreign Keys**: Hover tooltip shows referenced table.column

---

## [0.1.0] - 2025-01-24

### Phase 1: SQL Client Basico - "Ya lo puedo usar"

#### Added
- **CLI**: `tusk studio` command to start the web server
- **CLI**: `tusk config show` to view current configuration
- **CLI**: `tusk config set KEY VALUE` to update configuration
- **Core**: Connection management with in-memory registry
- **Core**: Connection persistence to `~/.tusk/connections.toml`
- **Core**: Global configuration in `~/.tusk/config.toml`
- **Engines**: PostgreSQL engine with psycopg3 async
- **Engines**: SQLite engine with stdlib sqlite3
- **Studio**: Litestar + Granian web server
- **Studio**: Main page with dark theme (GitHub-style)
- **UI**: Connection manager (add PostgreSQL/SQLite connections)
- **UI**: Schema browser in sidebar (tables + columns)
- **UI**: SQL editor with CodeMirror 6
- **UI**: Syntax highlighting for SQL
- **UI**: Autocomplete for tables AND columns
- **UI**: Query execution with Ctrl+Enter
- **UI**: Results table with row count and execution time
- **UI**: Error display for failed queries

### Phase 1 (continued): Query History & Connection Management

#### Added
- **Core**: Query history persistence to SQLite (`~/.tusk/history.db`)
- **API**: `GET /api/history` - List recent queries
- **API**: `DELETE /api/history/{id}` - Delete history entry
- **API**: `DELETE /api/history` - Clear all history
- **API**: `PUT /api/connections/{id}` - Edit existing connection
- **API**: `GET /api/connections/{id}` - Get connection details for editing
- **API**: `GET /api/connections/{id}/databases` - List databases on PostgreSQL server
- **API**: `POST /api/connections/{id}/clone` - Clone connection to another database
- **UI**: History panel in sidebar showing recent queries
- **UI**: Click on history entry to load query in editor
- **UI**: Clear history button
- **UI**: Edit connection button (pencil icon)
- **UI**: Browse databases button for PostgreSQL connections
- **UI**: Databases modal showing all databases on server with sizes
- **UI**: Quick-connect to other databases on same server

### Phase 2: PostgreSQL Admin - "Mejor que pgAdmin"

#### Added
- **Admin**: Server statistics dashboard
  - Connection count (current/max)
  - Active queries count
  - Cache hit ratio
  - Database size
  - Uptime
  - PostgreSQL version
- **Admin**: Active processes list from pg_stat_activity
- **Admin**: Kill query button (pg_terminate_backend)
- **Admin**: Auto-refresh every 5 seconds
- **Admin**: Backup with pg_dump (gzipped)
- **Admin**: List backups with download links
- **Admin**: Restore from backup (pg_restore)
- **Admin**: Extension manager
  - List installed extensions with versions
  - Show available extensions (toggle)
  - Install extension with one click
  - Uninstall extension (with CASCADE option if needed)
- **Config**: PostgreSQL binaries path configuration
  - Auto-detection of Postgres.app, Homebrew, Linux paths
  - CLI: `tusk config set pg_bin_path /path/to/bin`
  - CLI: `tusk studio --pg-bin-path /path/to/bin`
  - UI: Settings modal with path picker
- **UI**: Admin tab in navigation
- **UI**: Settings modal (gear icon in header)
- **UI**: Extensions section in Admin page

---

## Technical Details

### Project Structure
```
src/tusk/
├── __init__.py
├── cli.py                 # CLI entry point
├── core/
│   ├── __init__.py
│   ├── config.py          # Global configuration
│   ├── connection.py      # Connection registry
│   ├── files.py           # File scanning for data files
│   ├── geo.py             # GeoJSON/WKT utilities
│   ├── history.py         # Query history (SQLite)
│   ├── logging.py         # Structlog configuration
│   └── result.py          # QueryResult dataclass
├── engines/
│   ├── __init__.py
│   ├── duckdb_engine.py   # DuckDB (analytics, Parquet, CSV)
│   ├── polars_engine.py   # Polars (ETL, transforms)
│   ├── postgres.py        # PostgreSQL (psycopg3 async)
│   └── sqlite.py          # SQLite (stdlib)
├── admin/
│   ├── __init__.py
│   ├── stats.py           # Server statistics
│   ├── processes.py       # Active queries
│   ├── backup.py          # pg_dump/restore
│   └── extensions.py      # Extension management
├── cluster/
│   ├── __init__.py
│   ├── models.py          # Job, Worker, ClusterStatus
│   ├── scheduler.py       # Arrow Flight scheduler
│   └── worker.py          # Arrow Flight worker + DataFusion
└── studio/
    ├── __init__.py
    ├── app.py             # Litestar application
    ├── routes/
    │   ├── __init__.py
    │   ├── api.py         # Connections, query, history API
    │   ├── admin.py       # Admin API
    │   ├── cluster.py     # Cluster API
    │   ├── data.py        # Data/ETL API
    │   ├── files.py       # Files & DuckDB API
    │   ├── pages.py       # HTML pages
    │   └── settings.py    # Settings API
    ├── templates/
    │   ├── base.html      # Base template
    │   ├── index.html     # Studio page
    │   ├── admin.html     # Admin page
    │   ├── cluster.html   # Cluster dashboard
    │   └── data.html      # Data/ETL page
    └── static/
        └── studio.js      # Studio JavaScript module
```

### Configuration Files
- `~/.tusk/config.toml` - Global settings (pg_bin_path, server, ui)
- `~/.tusk/connections.toml` - Saved connections
- `~/.tusk/history.db` - Query history (SQLite)
- `~/.tusk/backups/` - Database backups

### Dependencies
- litestar >= 2.0
- granian >= 2.0
- psycopg[binary] >= 3.0
- msgspec >= 0.18
- jinja2 >= 3.0
- tomli-w >= 1.0
- duckdb >= 1.0
- polars >= 1.0
- pyarrow >= 17.0
- datafusion >= 51.0
- structlog >= 24.0
- minijinja >= 2.0
- psutil >= 5.9
