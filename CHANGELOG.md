# Changelog

All notable changes to Tusk will be documented in this file.

## [0.2.0] - 2026-02-22

### HTMX Migration & Plugin System

#### HTMX + Alpine.js Migration
- Migrated from vanilla JS to HTMX for server-driven interactivity
- Removed ~5,000 lines of vanilla JS (`admin.js`, `cluster.js`, `profile.js`, `users.js`)
- New HTMX helper module (`studio/htmx.py`) for partial responses
- HTMX partials in `templates/partials/` for admin, cluster, data, studio, and users

#### Server-Side Pagination
- `/api/query` now accepts `page` and `page_size` parameters
- Returns `total_count`, `page`, `page_size` in response
- Frontend auto-detects and uses server-side pagination
- New `postgres.execute_query_paginated()` function
- Fixes memory issues with >50k row queries

#### Map Data Endpoint
- `/api/query/map-data` for optimized geometry fetching
- Only fetches geometry column + ID (lightweight)
- Supports `simplify_tolerance` and `max_features`
- Allows displaying all geometries even when table is paginated

#### MiniJinja Component Library
- Reusable macros in `templates/components/`:
  - `card.html` — stat cards, info cards, metric rows
  - `table.html` — data tables, simple tables, key-value tables
  - `forms.html` — inputs, selects, checkboxes, toggles, buttons
  - `feedback.html` — badges, alerts, modals, confirmation dialogs, spinners
  - `htmx.html` — HTMX-powered tables, polls, tabs, search, forms
  - `map.html` — MapLibre assets, containers, dark styles
  - `pipeline.html` — pipeline visualization
  - `status.html` — status indicators

#### Plugin System
- Plugin discovery via `pyproject.toml` entry_points (`tusk.plugins`)
- `TuskPlugin` base class with lifecycle hooks
- Per-plugin SQLite storage via `get_plugin_db_path()`
- Template and static file management per plugin
- Plugin routes and CLI commands registration

#### Download Manager
- Async file export system with background processing
- Download hooks for progress tracking
- New routes: `studio/routes/downloads.py`

#### Cluster Decoupling
- `tusk-cluster` extracted to separate package
- Install via `pip install tuskdata[cluster]`
- Cluster tab only shows when plugin is installed

#### Tests
- 8 test modules: auth, connection, downloads, health, pipeline, plugin registry, polars safe eval
- Test configuration via `conftest.py`

#### Other
- Added `LICENSE` (MIT)
- Added `Makefile` for common tasks
- Added vendor scripts (`scripts/vendor.sh`, `scripts/install-tailwind.sh`)

### Breaking Changes
- `tuskdata[cluster]` now requires the external `tusk-cluster` package

---

## [0.1.2] - 2026-01-25

### UX Improvements

#### Search & Filter
- **Schema Search**: Filter tables in schema browser by name
- **History Search**: Search through query history
- **Row Counts**: Show estimated row count per table in schema browser

#### Navigation
- **Ctrl+Tab**: Switch between editor tabs (Shift+Ctrl+Tab for reverse)
- **Connection Status**: Visual indicators for connection health (online/offline/connecting)
- **Cluster Tab Conditional**: Only shows when `[cluster]` feature is installed

#### Map Enhancements
- **Hover Tooltips**: Show feature name/tag on hover in Studio map view
- **Fixed Map Click**: Click handlers now work correctly on all geometry layers

### PostgreSQL Admin

#### Logs Viewer
- View PostgreSQL server logs (requires superuser or pg_read_server_files)
- Filter logs by level (ERROR, WARNING, FATAL, LOG)
- Shows log settings and current log file path

### Package & Distribution

#### PyPI Metadata
- Added `readme`, `license`, `authors`, `keywords`, `classifiers`
- Added project URLs (Homepage, Repository, Issues)
- Package description now shows on PyPI

#### Bug Fixes
- Added missing `apscheduler>=3.10` to `[studio]` dependencies
- Fixed MapLibre event handlers for individual layer clicks

---

## [0.1.1] - 2026-01-25

### Bug Fixes
- Fix missing `apscheduler` dependency
- Hide Cluster tab when `[cluster]` feature is not installed

---

## [0.1.0] - 2026-01-24

### Initial Release

#### SQL Client
- **CLI**: `tusk studio` to start the web server, `tusk config` for configuration
- **Engines**: PostgreSQL (psycopg3 async), SQLite, DuckDB (analytics), Polars (ETL)
- **SQL Editor**: CodeMirror 6 with syntax highlighting and autocomplete (tables + columns)
- **Query Execution**: Ctrl+Enter to run, Escape to cancel
- **Results Grid**: Sortable columns, filtering, pagination, CSV/JSON export
- **Schema Browser**: Tables, columns, primary keys, foreign keys, row counts
- **Query History**: Persistent history with search (SQLite-backed)
- **Saved Queries**: Save/load queries with folders, Ctrl+S shortcut
- **Tab Persistence**: Editor tabs persist across page loads (localStorage)
- **Connection Manager**: Add/edit/test PostgreSQL and SQLite connections
- **Database Browser**: List databases on a server, quick-connect to other databases

#### PostgreSQL Admin
- **Server Stats**: Connection count, active queries, cache hit ratio, DB size, uptime
- **Process Monitor**: Active queries with slow query highlighting, kill button
- **Lock Monitor**: Active locks, blocking chains, kill blocker
- **Table Maintenance**: Bloat detection, VACUUM, ANALYZE, REINDEX per table
- **Backup/Restore**: pg_dump (gzipped), restore from backup
- **Extension Manager**: Install/uninstall PostgreSQL extensions
- **Roles Management**: Create/edit/delete PostgreSQL roles
- **Database Settings**: View and filter PostgreSQL configuration
- **Logs Viewer**: View server logs with level filtering
- **Scheduled Tasks**: APScheduler for automated backup, VACUUM, ANALYZE
- **Auto-Refresh**: Configurable refresh interval (5s/10s/30s/60s)

#### DuckDB Analytics
- **Engine Selector**: Toggle between PostgreSQL and DuckDB
- **File Support**: Parquet, CSV/TSV, JSON/JSONL, SQLite via DuckDB
- **File Browser**: Register data folders, auto-detect file types, preview files
- **Spatial Extension**: Auto-install, ST_Point, ST_Distance, ST_Within, etc.
- **Extension Manager**: Install/load DuckDB extensions from UI
- **Export to Parquet**: Export query results to Parquet format

#### Data/ETL with Polars
- **Data Tab**: Visual ETL pipeline builder
- **File Browser**: Navigate filesystem to select data files
- **Source Support**: CSV, Parquet, JSON, OSM/PBF files
- **8 Transform Types**: Filter, Select, Sort, Group By, Rename, Drop Nulls, Limit, Join
- **Quick Transforms**: Sidebar shortcuts for common operations
- **View Code**: Generate Polars Python code from pipeline
- **Preview**: Real-time data preview (100/500/1000/5000 rows)
- **Export**: CSV, Parquet, GeoJSON
- **Import**: Load into DuckDB or PostgreSQL tables
- **Save/Load Pipelines**: Persist pipelines to localStorage

#### Geo Integration
- **Auto-Detection**: Detect geometry columns (WKT, GeoJSON, PostGIS)
- **Map View**: Full-screen MapLibre GL JS modal with CARTO dark basemap
- **Geometry Support**: Points, Lines, Polygons, Multi* types
- **Popups**: Click features to see properties
- **Auto-fit**: Map fits to data bounds
- **WKT Parser**: Parse WKT/EWKT strings to GeoJSON
- **msgspec GeoJSON**: Type-safe GeoJSON structs

#### Cluster Mode
- **Arrow Flight**: Scheduler/worker architecture for distributed queries
- **DataFusion**: SQL query execution on workers
- **Cluster Dashboard**: Real-time monitoring, worker status, job management
- **Local Dev Cluster**: Start scheduler + N workers from UI or CLI
- **Job Lifecycle**: Submit, track progress, cancel, view results

#### User Management
- **Auth System**: Single mode (no auth) and multi-user mode
- **User Model**: SQLite storage, SHA-256 + salt password hashing, session tokens
- **Permissions**: 24 permissions across 6 categories, 4 default groups
- **Login Page**: Auto-redirect, remember URL after login
- **User Management UI**: CRUD users, group assignment, password reset
- **Profile Page**: Edit display name, email, change password
- **CLI**: `tusk users` and `tusk auth` commands

#### UX
- **Dark Theme**: GitHub-style dark UI
- **Toast Notifications**: Success/error/warning/info with auto-dismiss
- **Drag & Drop**: Drop files onto Data page to load them
- **Resizable Columns**: Drag to resize data grid columns
- **Lucide Icons**: Consistent iconography throughout

#### Frontend Architecture
- All inline JS extracted to separate files in `/static/`
- Common styles in `/static/styles.css`
- zstd response compression via Litestar
- Standardized sidebar width (256px)
