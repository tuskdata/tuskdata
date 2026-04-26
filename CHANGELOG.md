# Changelog

All notable changes to Tusk will be documented in this file.

## [0.3.5] - 2026-04-26

### Added — SSH tunneling for PostgreSQL connections

Connections can now reach databases sitting behind a bastion host
without configuring system-level port-forwards. Six new optional fields
on `ConnectionConfig`:

- `ssh_host`, `ssh_port` (default 22), `ssh_user`
- `ssh_password` *or* `ssh_private_key` (PEM contents, paste in)
- `ssh_known_hosts` (optional pin; default = trust on first use)

`ssh_password` and `ssh_private_key` encrypt at rest with the same
Fernet key as the database password. The Studio's connection form gains
a collapsible "SSH Tunnel" panel. The DB host/port stay as the values
the bastion sees — typically `localhost` or a private IP.

Implementation: new `core/ssh_tunnel.py` opens an asyncssh session +
`forward_local_port` per connection, returns a `127.0.0.1:<random>` DSN
for psycopg. Tunnels are reused across queries (one per connection id),
torn down on shutdown. Wired through `engines/postgres.py` so every
query path — `execute_query`, `execute_query_paginated`,
`fetch_geometries`, `cancel_query`, `test_connection` — auto-tunnels
when configured.

### Changed — Studio results table styling

Tighter, denser layout that doesn't waste horizontal space:

- Cell padding reduced from `px-4 py-2` to `px-3 py-1`
- Font dropped to `text-xs` for the table body (cells stay readable, no
  more giant rows for short values)
- Column type (e.g. `int8`, `text`) shown next to the header name in
  small grey
- Header text non-uppercase, less aggressive color contrast
- Per-cell `max-w-xs` + `truncate`: long values clip with an ellipsis
  and reveal in the title tooltip
- Checkbox column sticks left when scrolling horizontally
- Sort indicator shifted to the right edge of the header in indigo
- Alternating row tint slightly stronger for scan-ability


### Fixed
- **Version badge stuck at v0.2.1.** `src/tusk/__init__.py` had a
  hardcoded `__version__ = "0.2.1"` that never moved when the
  pyproject version did. Now it resolves from
  `importlib.metadata.version("tuskdata")`, which makes drift between
  the two impossible.
- **Studio results table couldn't scroll horizontally** when the row
  had more columns than the viewport could fit. The `<table class="w-full">`
  was forcing the table to shrink-to-fit, masking the overflow trigger
  on the parent `overflow-x-auto` div. Replaced with
  `min-width: max-content` so the table grows wider than its container
  and the parent's horizontal scrollbar takes over.

---

## [0.3.3] - 2026-04-26

The deployment artifacts that referenced first-party plugin repositories
by name have moved out of this repository. The closed plugin set
(BI / CI / security) is now composed in the dedicated tuskdata-compose
deployment harness.

### Changed
- **Dockerfile** is now scoped to the public surface: builds TuskData
  core and optionally bundles the public tusk-cluster plugin
  (`WITH_CLUSTER=1` by default). No private repo references.
- **docker-compose.yml** no longer pins refs for the closed plugins.
- **.env.example** trimmed to public knobs only.

### Removed
- `docs/DEPLOY.md` and `scripts/release.sh` — moved to
  `tuskdata/tuskdata-compose`. The full-suite deploy walkthrough lives
  there now.

### How to deploy the full suite
Use the `tuskdata-compose` repo as the Coolify build target. Its
Dockerfile clones every component (tuskdata + every plugin) at the refs
you pin, with explicit auth modes for private repo access.

---

## [0.3.2] - 2026-04-25

Deploy-track release. No app behavior changes; the image build pipeline
is rewritten so Coolify (or anything pulling the repo) can build a
working image without committing wheel binaries first.

### Build pipeline

- **Plugin wheels are now built at image-build time** by cloning each
  plugin repo at a pinned ref. The Dockerfile takes
  `TUSK_BI_REF`/`TUSK_CI_REF`/`TUSK_SEC_REF`/`TUSK_CLUSTER_REF` build
  args (default to current stable tags) and clones from
  `${TUSK_PLUGINS_ORG}` (default `tuskdata`).
- **Auth options for private repos**: pass `--secret id=gh_token,src=...`
  for HTTPS-with-PAT, or `--ssh default` for an agent socket. Falls back
  to public HTTPS clone when neither is supplied.
- **`wheels/` is no longer in the build context.** The previous
  Dockerfile expected pre-built wheels there but `.gitignore` excluded
  them, so any fresh clone failed silently with missing plugins.

### Python version

- Base image bumped from `python:3.12-slim` to `python:3.13-slim`.
- `requires-python` is now `>=3.12`. 3.11 wheels for some deps
  (psycopg, ibis backends) are no longer pinned.

### Compose

- **Postgres sidecar moved behind `--profile pg`.** Default `up` runs
  just the Studio; bring your own Postgres for everything else. Adminer
  also profile-gated (`--profile dbtools`).
- **`TUSK_HOST_PORT`** env var (default `7000`) controls the host-side
  port mapping so the Coolify dashboard at `:8000` doesn't collide.
- All build args (plugin refs + plugins org) parameterized through `.env`.

### Release tooling

- **`scripts/release.sh`** is a portable (bash 3+) idempotent driver
  that walks TuskData + the four plugin repos, detects which have new
  commits since their last tag, bumps the patch number, tags, and
  pushes — printing the matching `TUSK_*_REF` block ready to paste into
  Coolify or `.env`. Default is dry-run; `--apply` to actually publish.

---

## [0.3.1] - 2026-04-25

Hotfix round on top of v0.3.0 after an independent audit. No new features.

### Security
- **`/api/query/cancel` now requires auth.** Previously a CSRF-tokened
  request could cancel anyone's in-flight query by guessing a `request_id`.
  Same guard as the admin panel (loopback in single-user, admin in
  multi-user).
- **DSN redaction in logs.** `engines/postgres.py` no longer logs the
  first 50 chars of a DSN (which could leak user/password substrings).
  URL-form DSNs are reduced to `scheme://host:port/db`; keyword form has
  `password=...` replaced with `password=***`.

### Bugs
- **tusk-bi placeholder mismatch on PostgreSQL.** `apply_variables()`
  emits `:name`, then `_apply_params()` rewrote them to `?` for every
  backend. psycopg requires `%s`, so any dashboard with variables on
  Postgres failed with "syntax error near ?". Fixed: the placeholder
  now matches the backend (`%s` for Postgres, `?` for SQLite/DuckDB).
- **PostgreSQL pool race.** First-hit creation of a pool for a new DSN
  was unguarded: two concurrent coroutines could each build a pool, with
  one leaked. Now serialized through an `asyncio.Lock`.
- **Frontend `minlength` on user-create / reset-password forms** was
  still 6, conflicting with the backend's 8-char + letter + digit policy.
  Updated to 8.
- **tusk-cluster lock coverage gaps.** v0.3.0 added `_state_lock` only
  to register/heartbeat/status. `list_workers` and `unregister_worker`
  now also take the lock so concurrent reads see consistent state.
- **UTC tzinfo in pitr + scheduler.** `pitr.py:266,269,321` parsed
  filenames as naïve datetimes and read mtimes as local; `scheduler.py:218`
  passed naïve `datetime.now()` to APScheduler. All upgraded to
  timezone-aware UTC.

### Deploy hardening
- **Dockerfile fail-fast on plugin install.** The `for w in wheels/...
  || true` loop hid wheels that didn't install. The image "succeeded"
  but a tab silently disappeared. The loop now `set -e`s and dies on
  first failure.
- **`docker-entrypoint.sh`** runs as root only long enough to chown
  `$HOME` to UID 1000 (the baked `tusk` user) before dropping privileges
  via `gosu`. This is the single biggest first-deploy footgun on Coolify
  / bind mounts where the host UID doesn't match the container's.
  Override with `TUSK_SKIP_CHOWN=1` if your orchestrator handles
  ownership.

### Tests
- Two new E2E tests assert the controller-level admin guard fires on a
  real HTTP request and that `/api/query/cancel` returns 401 from a
  non-loopback caller. Total: 186 passing (was 184).

### Plugin updates
- **tusk-bi 0.2.1**: placeholder fix.
- **tusk-cluster 0.2.1**: list_workers/unregister_worker lock coverage.

---

## [0.3.0] - 2026-04-20

### Ibis Unified DataFrame API (now default)

- **New engine `engines/ibis_engine.py`** — pipelines now run through Ibis on
  a DuckDB or Polars backend. **Ibis on DuckDB is now the default** for
  `/api/data/execute`; legacy `engine: "polars"` is still accepted and the
  executor falls back to Polars automatically if Ibis raises. Pipeline
  models (`DataSource`, `Pipeline`, transforms) are reused verbatim so saved
  pipelines work on either engine.
- **New transforms**: `case_when` (conditional column with branches + default),
  `unpivot` (wide → long MELT), `date_arithmetic` (add/sub/diff/extract/truncate
  with year/month/day/hour/minute/second/week units).
- **Column profiling endpoint** — `POST /api/data/profile` returns per-column
  null count, distinct count, min/max/mean via Ibis on DuckDB. UI button
  "Profile" in the data tab renders a summary table.
- **Engine selector in UI** — data tab dropdown now includes `Ibis · DuckDB`
  and `Ibis · Polars` alongside `Auto / DuckDB / Polars`.

### Admin Panel Expansion

- **Bulk query killers** — `POST /api/admin/{conn}/kill-by-user` and
  `/kill-by-database` terminate every matching active query in one call.
- **EXPLAIN plan viewer** — `POST /api/admin/{conn}/explain` returns the
  query plan as JSON; set `analyze: true` for EXPLAIN (ANALYZE, BUFFERS).
- **Session SET settings** — `POST /api/admin/{conn}/set-setting` applies
  a runtime SET for the current session (identifier regex-validated,
  value bound as parameter).
- **Backup sidecar metadata** — every `pg_dump` now writes
  `<backup>.sql.gz.meta.json` with real timestamp, size, sha256, source,
  and Tusk version. `list_backups()` prefers the sidecar over `st_mtime`.
- **Stats history for sparklines** — every `/admin/{conn}/stats` call
  records a point into `~/.tusk/stats_history.db` (pruned to 288 points,
  ~24h @ 5 min). New `GET /admin/{conn}/stats/history` feeds the
  sparkline renderers without extra scheduler work.
- `kill_query()` now parameterizes `pg_terminate_backend(pid)` instead of
  f-string interpolation.

### Studio Editor Polish (Phase 1)

- CodeMirror editor adds bracket matching, auto-close brackets, selection
  match highlighting, history + search + completion keymaps.
- `Ctrl+/` / `Cmd+/` toggles line comments.
- Tabs now support rename (double-click), dirty marker (`*` while the buffer
  differs from the saved content), and confirm-before-close on dirty tabs.
- Per-column filter inputs stack with the global filter and reset pagination
  on every keystroke.
- Row checkboxes with a page-scoped "select all" in the header; selection
  survives sort/filter changes via a stable row key.
- **Copy as INSERT** button generates parameterized SQL `INSERT` statements
  from selected rows (or the current page when nothing is checked) and
  writes them to the clipboard.
- Cell formatting: numbers get locale-aware thousands separators, ISO
  dates/datetimes render in short form, raw values remain in the `title`
  tooltip so copy-paste still yields the original text.

### Observability

- **Health endpoint with dependency status** — `/api/health` reports per
  component (`scheduler`, `plugins`, `ibis`) and returns `"ok"` or
  `"degraded"` so load balancers can route around broken instances.
- **Prometheus metrics endpoint** — `/api/metrics` is now text-format
  Prometheus exposition (`text/plain; version=0.0.4`). Exposes
  `tusk_build_info`, `tusk_connections_registered`, `tusk_queries_in_flight`,
  `tusk_rate_limit_buckets`, `tusk_scheduler_up`, `tusk_plugins_loaded`,
  `tusk_ibis_available`. Histograms left for v0.4.x.
- **Configurable logging via env** — `TUSK_LOG_LEVEL`
  (`debug|info|warning|error|critical`) and `TUSK_LOG_FORMAT`
  (`console` default, `json` for structured pipelines).

### Plugin updates shipped alongside (all at v0.2.0)

- **tusk-cluster**: user-or-worker guard, thread-lock on `_cluster_state`,
  worker registration validation, `TUSK_CLUSTER_SECRET` shared secret for
  worker endpoints, `TUSK_CLUSTER_TLS=1` toggles Flight to `grpc+tls://`.
- **tusk-security**: `AdGuardClient` is a context manager, migration v4 adds
  `scan_history(project_id)`, `scan_history(status)`,
  `dependency_vulns(severity)`, `code_issues(test_name)` indexes.
- **tusk-bi**: SQL injection closed in `apply_variables()` (now emits
  `:__tusk_var_N` bind params), module-level TTL query cache,
  `GET /api/bi/widgets/{id}/export?format=csv|json`,
  `GET /api/bi/queries/{id}/export-json`, `POST /api/bi/cache/clear`.
- **tusk-ci**: `GET /api/ci/webhook/info` with GitHub/curl examples,
  `POST /api/ci/cron/validate` with APScheduler validation, plain-English
  description, and next-run timestamp.

### Security Hardening

- **Connection passwords encrypted at rest** — `~/.tusk/connections.toml` now stores
  passwords with Fernet symmetric encryption. Key lives at `~/.tusk/.key` with
  `0600` perms. Plain-text legacy passwords are decrypted passthrough and
  re-saved encrypted on next write. New module: `core/crypto.py`.
- **Server-side query cancellation** — `Escape` now actually stops the backend
  query, not just the client fetch. PostgreSQL uses `pg_cancel_backend(pid)`.
  New endpoint `POST /api/query/cancel`. New module: `core/query_tracker.py`.
- **Admin auth fail-closed in single-user mode** — admin endpoints now refuse
  non-loopback requests unless multi-user auth is enabled. Closes exposure
  vector where a default deploy on `0.0.0.0` exposed admin to the network.
- **SQL injection hardening** — parameterized `pg_available_extensions` lookup
  (`admin/extensions.py`) and `pg_settings` category filter (`admin/settings.py`).
- **Password policy** — minimum raised from 6 to 8 chars, plus required letter
  and digit. Shared validator in `studio/routes/auth.py`.
- **UTC-aware timestamps** — all session, audit, workspace, history, download,
  and cluster timestamps now store `datetime.now(timezone.utc).isoformat()`.
  Fixes ambiguous session expiry and audit logs across time zones.

### Stability & Reliability

- **Pagination for DuckDB and SQLite** — `/api/query` now paginates every engine,
  not just PostgreSQL. Prevents OOM on 10M-row results.
- **HTMX error responses** — new `htmx_error()` helper sets `HX-Reswap: none` so
  failed actions no longer wipe the target element. Retrofitted across admin,
  auth, downloads, and notifications routes.
- **Rate limiting beyond login** — new `core/rate_limit.py` module. Uploads
  capped at 10/min/IP, exports at 20/min/IP.
- **Data export audit trail** — CSV and Parquet exports write entries to the
  audit log with user_id, format, filename, and row count (GDPR/compliance).
- **Temp file cleanup extended** — scheduler now also prunes `tusk_uploads/`
  older than 2 hours alongside `tusk_export_*`.

### Frontend

- **Shared fetch wrapper with timeout** — `static/tusk-fetch.js` exposes
  `tuskFetch()` and `tuskFetchJSON()` with a default 2-minute AbortController
  timeout so slow servers no longer hang the UI indefinitely.
- **Column-resize listener leak fixed** — `initColumnResize()` no longer stacks
  document-level `mousemove`/`mouseup` handlers on every re-render.
- **Query cancellation wired** — Studio sends a `request_id` on every query and
  posts to `/api/query/cancel` on Escape / Cancel.

### Dependencies

- Added `cryptography>=42.0` to core dependencies (Fernet symmetric encryption).
- Added `ibis-framework[duckdb,polars]>=10.0` to `studio` optional dependencies.

### Breaking Changes

- Password minimum raised to 8 characters with letter+digit requirement. Users
  with shorter passwords can still log in but will be required to update on
  next change.
- Connection file format now writes encrypted passwords. First load under
  v0.3.0 migrates existing plain-text passwords automatically — downgrading
  to v0.2.x will not be able to read them.

---

## [0.2.1] - 2026-02-22

### Security Hardening

#### Authentication & Session Security
- Password hashing upgraded to argon2id (with SHA-256 fallback)
- Rate limiting on login endpoint (5 attempts per 60 seconds per IP)
- CSRF protection via double-submit cookie middleware on all POST/PUT/DELETE/PATCH
- Automatic session cleanup of expired sessions (hourly scheduler job)
- Auth setup endpoint exempted from CSRF for initial configuration

#### SQL Injection Fixes
- Fixed f-string SQL injection in PostgreSQL role management (`admin/roles.py`)
- Fixed f-string SQL injection in DuckDB file path queries (`duckdb_engine.py`)
- Fixed f-string SQL injection in OSM file loading (`polars_engine.py`)

#### Path Traversal & File Safety
- Fixed directory traversal in backup delete (now uses `Path.name`)
- Fixed PGPASSWORD exposure in environment (now uses `.pgpass` file)
- Fixed path traversal in downloads module
- Fixed ZIP extraction path traversal (validates member paths)
- Fixed command injection in post_download_hook (now uses shlex)
- Added file upload validation (10MB limit, type whitelist)

#### Frontend Security
- Fixed XSS in MapLibre popups via `escHtml()` helper in studio.js and data.js
- Fixed XSS in data table headers and geo column rendering
- HTMX auto-sends CSRF token on all requests via `htmx:configRequest`
- Vanilla `fetch()` auto-injects CSRF header via global interceptor

### ETL Pipeline Overhaul

#### Multi-Source Pipelines
- Chained joins: result of A JOIN B can be used as input for JOIN C
- UNION/APPEND: ConcatTransform with vertical, diagonal, align modes
- Right table column preview in join UI (fetchRightTableColumns)

#### New Transforms
- **Distinct**: Remove duplicate rows with subset and keep options
- **Window Functions**: row_number, rank, dense_rank, lag, lead, cum_sum, cum_max, cum_min
  - Partition by and order by support
  - Configurable offset for lag/lead
- **Multi-aggregation Group By**: Dynamic multi-row aggregation UI (addAggRow)

### Notification System
- In-app notification center with bell icon in navbar
- Multi-channel support: in-app, email, webhook (extensible)
- Notification preferences per user (settings page)
- Event-driven architecture (query completion, backup, security alerts)
- Automatic retry for failed notifications (scheduler job)
- Old notification cleanup (scheduler job)
- Templates for partials: bell icon, notification list, settings

### Stability & Performance

- **PostgreSQL connection pooling** via `psycopg_pool` (1-10 connections per DSN)
- **Query timeout enforcement**: Configurable via `TUSK_QUERY_TIMEOUT` env (default 5 min)
- **Temp export file cleanup**: Scheduler removes `tusk_export_*` files older than 30 min
- **HTMX error handlers**: `htmx:sendError` and `htmx:responseError` for network errors
- **Connection TOML fix**: `to_dict()` no longer includes None values (TOML-serializable)

### Bug Fixes
- Fixed missing `log` import in `admin/backup.py`
- Fixed `connection.py` auto-save on add/delete/update
- Fixed bare `except:` in `workspace.py` (now catches Exception)
- Fixed connection `to_dict()` None values breaking TOML serialization

### Dependencies
- Added `psycopg_pool>=3.0` to postgres optional dependencies

---

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
