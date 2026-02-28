# Tusk TODO

> Based on detailed specs in `/tusk/specs/phase-*.md`

---

## Phase 1: Complete SQL Client - DONE ✅

> **Goal**: SQL client you can actually use every day
> **Success**: Can replace pgAdmin/DBeaver for basic queries

### 1. Connections (Full CRUD)
- [x] Create connection (PostgreSQL, SQLite)
- [x] Edit existing connection
- [x] Delete connection (with confirmation)
- [x] Test connection before saving
- [x] Remember last used connection
- [x] Different icon per type (PG, SQLite)
- [x] Show status (online/offline/connecting)
- [ ] Multiple connections can be active simultaneously

### 2. SQL Editor (Tabs)
- [x] Multiple query tabs
- [x] Create new tab (+)
- [x] Close tab (x)
- [x] Persist tabs between sessions
- [x] CodeMirror with syntax highlighting
- [x] Autocomplete (tables, columns, keywords)
- [x] Line numbers
- [ ] Close tab with confirmation if unsaved changes
- [ ] Rename tab (double click)
- [ ] Tab shows * if unsaved changes
- [ ] Error highlighting
- [ ] Bracket matching
- [ ] Multi-cursor (Ctrl+D)

### 3. Results (Real Data Grid)
- [x] Table with sticky headers
- [x] Sort by column (click header)
- [x] Quick text filter
- [x] Pagination (100 rows per page)
- [x] Show total row count
- [x] NULL as gray badge
- [x] Boolean as check/x
- [x] JSON with syntax highlight
- [x] Expand long cell (tooltip on hover)
- [ ] Filter by specific column
- [ ] Select rows (checkbox)
- [ ] Select/copy cells
- [x] Resize columns (drag)
- [ ] Human-readable date formatting
- [ ] Numbers with separators

### 4. Export
- [x] Export to CSV
- [x] Export to JSON
- [ ] Export selection or all
- [ ] Copy as INSERT statements
- [ ] Copy as CSV to clipboard

### 5. Query History
- [x] Save queries to SQLite
- [x] Show in sidebar
- [x] Click to load in editor
- [x] Show timestamp and duration
- [x] Clear history
- [x] Search in history

### 6. Saved Queries (Favorites)
- [x] Save query with name
- [x] Organize in folders
- [x] Load saved query
- [x] Edit name
- [x] Delete

### 7. Schema Browser
- [x] Expandable tree view
- [x] View columns with types
- [x] View primary keys
- [x] View foreign keys
- [x] Click column → add to query
- [x] Refresh schema (F5)
- [ ] Click table → INSERT template
- [x] Search in schema
- [x] View row count per table

### 8. Keyboard Shortcuts
- [x] Ctrl+Enter - Execute query
- [x] Ctrl+S - Save query
- [x] Ctrl+N/T - New tab
- [x] Ctrl+W - Close tab
- [x] Ctrl+Space - Autocomplete
- [x] F5 - Refresh schema
- [x] Escape - Cancel query
- [x] Ctrl+Tab - Next tab
- [ ] Ctrl+/ - Comment line

### 9. UX
- [x] Loading spinner
- [x] Cancel running query
- [x] Friendly error messages
- [x] Dark mode (default)
- [x] Toast notifications (global component)
- [ ] Resizable panels
- [x] Light mode toggle
- [ ] Responsive design

---

## Phase 2: PostgreSQL Admin - DONE ✅

> **Goal**: Complete PostgreSQL administration
> **Success**: Handle a "slow database" incident using only Tusk

### 1. Server Dashboard
- [x] Active connections vs max
- [x] Active queries
- [x] Cache hit ratio
- [x] Database size
- [x] Uptime
- [x] PostgreSQL version
- [x] Manual refresh
- [x] Configurable auto-refresh (Off/5s/10s/30s/60s)
- [ ] Trend graphs

### 2. Active Queries Monitor
- [x] List of running queries
- [x] Show: PID, user, database, duration, state, query
- [x] Highlight slow queries (> 10s)
- [x] Highlight idle in transaction
- [x] Kill individual query
- [x] Refresh button
- [ ] Kill all queries from a user
- [ ] Filter by database/user

### 3. Locks Monitor
- [x] View active locks
- [x] Identify blocking locks
- [x] View waiting queries
- [x] Visualize lock chains

### 4. Backup & Restore
- [x] pg_dump with UI
- [x] List existing backups
- [x] Download backup
- [x] Restore with UI
- [ ] Select format (plain, custom, directory)
- [ ] Select specific tables
- [ ] Progress bar during backup
- [x] Scheduled backups (APScheduler)

### 5. Table Maintenance
- [x] VACUUM table
- [x] VACUUM FULL table
- [x] ANALYZE table
- [x] REINDEX table
- [x] View table bloat
- [x] View dead tuples
- [x] Schedule maintenance (APScheduler)

### 6. Extensions Manager
- [x] List installed extensions
- [x] List available extensions
- [x] Install extension
- [x] Uninstall extension
- [x] View current vs available version

### 7. User/Role Management
- [x] List roles
- [x] Create role
- [x] Edit permissions
- [x] Change password
- [ ] View grants

### 8. Database Settings
- [x] View current configuration (important settings)
- [ ] Modify settings
- [ ] Compare with defaults

### 9. Logs Viewer
- [x] View recent log lines
- [x] Filter by level
- [ ] Search in logs

---

## Phase 3: Analytics Engine (DuckDB) - DONE ✅

> **Goal**: Integrated DuckDB, federated queries, Parquet files

- [x] DuckDB engine
- [x] Open Parquet files directly
- [x] Open CSV files
- [x] Open JSON files
- [x] Open SQLite files
- [x] Federated queries (Postgres + Parquet)
- [x] Auto-detect file types
- [x] File browser in sidebar
- [x] Engine selector (Postgres/DuckDB)
- [x] File preview modal
- [x] Insert query from file preview
- [x] Export results to Parquet
- [x] Drag & drop files

---

## Phase 4: Data/ETL with Polars - DONE ✅

> **Goal**: Visual transformations with generated Polars code

- [x] "Data" tab in UI
- [x] Dataset browser (local files)
- [x] File browser with navigation
- [x] OSM/PBF file support (ST_ReadOSM)
- [x] Transform pipeline builder
- [x] Filter transform
- [x] Select columns transform
- [x] Sort transform
- [x] Group by with aggregations
- [x] Rename columns transform
- [x] Drop nulls transform
- [x] Limit transform
- [x] Join transform (with file browser)
- [x] Quick transforms shortcuts
- [x] Auto-generated Polars code (View Code)
- [x] Real-time preview
- [x] Limit selector (100/500/1000/5000)
- [x] Export to CSV
- [x] Export to Parquet
- [x] Import to DuckDB (in-memory or file)
- [x] Import to PostgreSQL
- [x] Save/load pipelines (localStorage)
- [x] State persistence between page navigations
- [x] Structured logging with structlog

---

## Phase 5: Geo Integration - DONE ✅

> **Goal**: Detect geometries and show on map

### In Studio Page (index.html)
- [x] Auto-detect geometry columns
- [x] "Geo detected" badge
- [x] "Map" button
- [x] Map panel (MapLibre GL JS)
- [x] Points, lines, polygons
- [x] Popup with data on click
- [x] Export to GeoJSON

### In Data Page (data.html)
- [x] Auto-detect geometry columns
- [x] Map modal (full screen)
- [x] Points with click popups
- [x] Polygons with click popups
- [x] Auto-fit bounds
- [x] Export to GeoJSON
- [x] Dark basemap (CARTO)

### Completed
- [x] DuckDB Spatial extension (auto-installed at startup)
- [x] DuckDB Extension Manager UI

---

## Phase 6: Cluster Mode - DONE ✅

> **Goal**: Distributed queries with multiple workers

### CLI Commands
- [x] `tusk scheduler` - Start scheduler process
- [x] `tusk worker` - Start worker process
- [x] `tusk cluster` - Start all-in-one (dev mode)

### Scheduler
- [x] Accept job submissions
- [x] Plan query execution
- [x] Distribute work to workers
- [x] Track job progress
- [x] Report results
- [ ] Handle worker failures (graceful)

### Worker
- [x] Register with scheduler
- [x] Execute DataFusion queries
- [x] Return results via Arrow Flight
- [x] Report health/metrics (CPU, Memory)
- [x] Heartbeat loop

### UI (Cluster Tab)
- [x] Worker list with status indicators
- [x] CPU/Memory metrics per worker
- [x] Active jobs with progress bars
- [x] Job history table
- [x] Cancel job button
- [x] Connect to remote scheduler (host:port form)
- [x] Disconnect from scheduler
- [x] Start/Stop local cluster from UI (single-node mode)

### Job Model
- [x] Job submission API
- [x] Job status tracking (pending/running/completed/failed/cancelled)
- [x] Job progress (stages)
- [x] Job cancellation

---

## Phase 7: User Management & Access Control - DONE ✅

> **Goal**: Multi-user support with authentication and authorization
> See full spec: `/specs/phase-7.md`

### Modes
- [x] Single Mode (default, no auth - current behavior)
- [x] Multi-User Mode (federated, with auth)

### Core Auth (7.1)
- [x] User model and SQLite storage
- [x] Password hashing (SHA-256 with salt)
- [x] Session management
- [x] Login/logout API
- [x] Auth middleware (permission checks)
- [x] Config for auth mode (auth_mode: single/multi)

### Login UI (7.2)
- [x] Login page template (/login)
- [x] User menu in header (dropdown)
- [x] Logout functionality

### User Management (7.3)
- [x] Users API endpoints (CRUD)
- [x] Users admin page (/users)
- [x] Create/edit user modals
- [x] Password reset

### Groups & Permissions (7.4)
- [x] Group model and storage
- [x] 24 permissions defined
- [x] Default groups (Administrators, Data Engineers, Analysts, Viewers)
- [x] Groups API endpoints
- [x] Groups tab in users page
- [x] Assign users to groups UI (checkbox interface)

### Profile & CLI (7.5)
- [x] Profile page (/profile)
- [x] Change password from profile
- [x] View groups and permissions
- [x] CLI: `tusk users list`
- [x] CLI: `tusk users create`
- [x] CLI: `tusk users reset-password`
- [x] CLI: `tusk users delete`
- [x] CLI: `tusk auth enable/disable`

### Pending
- [ ] Audit logging UI

---

## Phase 8: Plugin System & Component Library - DONE ✅

> **Goal**: Extensible plugin architecture for Tusk
> **Success**: tusk-cluster works as external plugin, component library used by all pages
> See full spec: `/specs/phase-8.md`

### Plugin System Core
- [x] `tusk/plugins/__init__.py`
- [x] `tusk/plugins/base.py` - TuskPlugin ABC
- [x] `tusk/plugins/registry.py` - Discovery via entry_points
- [x] `tusk/plugins/storage.py` - SQLite helpers (per-plugin DBs)
- [x] `tusk/plugins/config.py` - TOML helpers
- [x] `tusk/plugins/templates.py` - Template copying

### Core Integration
- [x] `tusk/studio/routes/base.py` - TuskController base class
- [x] Modify `app.py` - Plugin lifecycle hooks
- [x] Modify `cli.py` - Plugin commands
- [x] Modify `templates/base.html` - Plugin tabs

### Controller Migration
- [x] Migrate `PageController` to TuskController
- [x] `AdminController` - API only, no changes needed
- [x] `DataController` - API only, no changes needed
- [x] `FilesController` - API only, no changes needed
- [x] Test all existing routes still work

### Cluster → Plugin Migration (tusk-cluster)
- [x] Create `tusk-cluster/` package structure
- [x] Move `tusk/cluster/` code to plugin
- [x] Create `ClusterPlugin` class
- [x] Register via entry_points
- [x] Remove cluster from core
- [x] Test cluster works as external plugin

### Component Library
- [x] `templates/components/table.html`
- [x] `templates/components/card.html`
- [x] `templates/components/forms.html`
- [x] `templates/components/feedback.html`
- [x] `static/js/ui.js`

---

## Phase 9: Security Plugin (tusk-security) - IN PROGRESS 🚧

> **Goal**: Security scanning plugin with code analysis, dependency audit, network scanning
> **Requires**: Phase 8 completed
> See full spec: `/specs/phase-9.md`
> **Note**: Developed as external plugin in separate repository

### Plugin Core
- [x] Plugin scaffold (pyproject.toml, entry_points, SecurityPlugin class)
- [x] Data models (msgspec Structs)
- [x] SQLite schema and queries

### Scanners
- [x] Bandit - Code analysis (Python)
- [x] pip-audit - Dependency vulnerabilities
- [x] TCP Scanner - Network scanning (no root, asyncio)
- [ ] ZMap - Network scanning (optional, requires sudo)
- [x] AdGuard Home API - DNS visibility
- [x] CycloneDX - SBOM generation

### Routes & Templates
- [x] Page controllers and API endpoints
- [x] 6 UI pages (Dashboard, Code, Deps, Network, DNS, SBOM)
- [x] Shared sidebar macro with Alpine.js modals

### Pending
- [ ] CLI commands (scan, audit, network, sbom)
- [ ] Tests

---

## v0.2.1: Security Hardening + ETL Overhaul — DONE ✅

> **Goal**: Fix critical security issues, make ETL actually usable for multi-source workflows
> **Target**: Production-safe core, real multi-source pipelines

### P0 — Security Fixes (must ship)
- [x] Password hashing: SHA-256 → argon2/bcrypt (`core/auth.py`)
- [x] Fix SQL injection in role management (`admin/roles.py`)
- [x] Fix directory traversal in backup delete (`admin/backup.py`)
- [x] Fix PGPASSWORD exposure in environment (`admin/backup.py`)
- [x] Fix SQL injection in DuckDB file paths (`duckdb_engine.py`)
- [x] Fix path traversal in downloads (`core/downloads.py`)
- [x] Fix command injection in post_download_hook (`routes/downloads.py`)
- [x] Add file upload validation (size + type) (`routes/data.py`)
- [x] Add CSRF protection on POST/PUT/DELETE endpoints
- [x] Add rate limiting on login endpoint (`routes/auth.py`)
- [x] Add auth checks to admin endpoints (`routes/admin.py`)
- [x] Add auth checks to cluster endpoints (`cluster/routes/api.py`)
- [x] Fix ZIP extraction path traversal (`core/downloads.py`)
- [x] Fix OSM SQL injection in polars_engine.py
- [x] Fix XSS in MapLibre popups and tab names

### P1 — ETL Pipeline Overhaul
- [x] Multi-source pipelines: chained joins (A JOIN B → result JOIN C)
- [x] UNION / APPEND multiple sources
- [x] Multiple aggregations per group_by (UI fix — backend already supports it)
- [x] DISTINCT / dedup transform
- [x] Preview right table in join UI (show columns)
- [x] Window functions (row_number, rank, dense_rank, lag, lead, cum_sum, cum_max, cum_min)

### P2 — Bug Fixes
- [x] Fix missing `log` import in `admin/backup.py` (RuntimeError)
- [ ] Fix missing `import msgspec` in tusk-ci `api.py`
- [ ] Fix missing `_parse_discovery()` in tusk-ci
- [x] Fix `connection.py` auto-save on add/delete/update (changes lost on restart)
- [x] Fix bare `except:` in `workspace.py` (catches KeyboardInterrupt)

### P3 — Stability & Polish
- [x] Connection pooling for PostgreSQL (psycopg_pool)
- [x] Query timeout enforcement (TUSK_QUERY_TIMEOUT env, default 5 min)
- [x] HTMX error states (htmx:sendError, htmx:responseError handlers)
- [x] CSRF double-submit cookie middleware
- [x] Session cleanup scheduler (hourly)
- [x] Temp export file cleanup scheduler (every 30 min)
- [ ] Clean empty catch blocks in frontend JS
- [ ] Fetch timeouts in frontend
- [ ] Fix memory leaks (setInterval, event listeners)
- [ ] Tests for engines and routes

---

## v0.3.0: Ibis Unified DataFrame API — PLANNED 📋

> **Goal**: Replace direct Polars dependency with Ibis as universal DataFrame API
> **Why**: Single pipeline definition that compiles to Polars, Pandas, DuckDB, DataFusion, or PostgreSQL

### Core
- [ ] Add `ibis-framework` as dependency
- [ ] Create `tusk/engines/ibis_engine.py` — unified pipeline execution layer
- [ ] Pipeline model targets Ibis expressions instead of Polars-specific code
- [ ] Backend selector: Polars, Pandas, DuckDB, DataFusion (per pipeline or global)
- [ ] Code generation outputs Ibis code (portable across backends)
- [ ] Lazy evaluation by default (Ibis is lazy-first)

### UI Changes
- [ ] Engine/backend selector dropdown in Data tab (Polars / Pandas / DuckDB / DataFusion)
- [ ] Show which backend is executing in status bar
- [ ] "View Code" generates Ibis Python code (works with any backend)

### Migration Path
- [ ] Keep `polars_engine.py` as fallback for v0.2.x compatibility
- [ ] Ibis engine as default, Polars engine as legacy option
- [ ] Existing saved pipelines auto-convert to Ibis format

### Benefits
- Pandas users can use their preferred backend
- DuckDB backend for large files (out-of-core processing)
- DataFusion backend for distributed execution (integrates with tusk-cluster)
- PostgreSQL backend for server-side execution (no data transfer)
- Single API, multiple execution targets

---

## Nice-to-haves (Deferred)

- [x] Drag & drop files (data.html)
- [x] Resize columns in data grid
- [x] Theme toggle (light/dark mode)
- [x] Toast notifications (global component)
- [x] Scheduled backups (APScheduler)
- [x] Database settings viewer
- [x] Logs viewer
- [x] DuckDB Spatial extension

---

## Future Considerations

### Internationalization (i18n)
> Multi-language support for the UI
- [ ] Translation system (i18next or similar)
- [ ] Translation files per language
- [ ] Language selector in UI
- [ ] Spanish as first additional language
- [ ] Auto-detect browser language

### Automated ETL Workflows
> Download files from URLs, extract, transform, load to DB
- [ ] Workflow definition model (steps, triggers)
- [ ] HTTP download step (using httpx, already installed)
- [ ] File extraction step (zip, gz, tar)
- [ ] Transform steps (reuse existing Polars transforms)
- [ ] Load/upsert to table step
- [ ] Workflow scheduler integration (APScheduler)
- [ ] UI for workflow builder

### Engine Selector
> Let users choose which engine to use for preview/transforms
- [ ] UI dropdown (DuckDB / Polars / Pandas)
- [ ] Common preview interface for all engines
- [ ] Pandas engine (optional dependency)
- [ ] Remember user preference

### Optional Dependencies
> Make installation lighter by separating features into extras
- [x] Reorganize pyproject.toml with optional-dependencies:
  - `tusk[studio]` → duckdb, polars, psycopg, litestar, minijinja
  - `tusk[cluster]` → datafusion, pyarrow
  - `tusk[admin]` → apscheduler
  - `tusk[postgres]` → psycopg
  - `tusk[all]` → everything
- [x] CLI commands check for required deps (`tusk.core.deps` module)
- [x] `tusk features` command to show installed features
- [ ] Conditional imports in modules (try/except or importlib)
- [ ] UI graceful degradation (disable features if deps missing)

---

## Files Created for Phase 6

```
src/tusk/cluster/
├── __init__.py       ✅
├── scheduler.py      ✅ Arrow Flight scheduler
├── worker.py         ✅ Arrow Flight worker with DataFusion
└── models.py         ✅ Job/Worker msgspec models

src/tusk/studio/
├── routes/cluster.py    ✅ Cluster API endpoints
└── templates/cluster.html ✅ Cluster dashboard UI
```
