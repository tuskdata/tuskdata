# Tusk TODO

> Based on detailed specs in `/tusk/specs/phase-*.md`

---

## Phase 1: SQL Client Completo - DONE ✅

> **Goal**: SQL client que realmente puedas usar todos los días
> **Success**: Puedes reemplazar pgAdmin/DBeaver para queries básicos

### 1. Conexiones (CRUD Completo)
- [x] Crear conexión (PostgreSQL, SQLite)
- [x] Editar conexión existente
- [x] Eliminar conexión (con confirmación)
- [x] Test connection antes de guardar
- [x] Recordar última conexión usada
- [x] Icono diferente por tipo (PG, SQLite)
- [x] Mostrar estado (online/offline/connecting)
- [ ] Múltiples conexiones pueden estar activas

### 2. Editor SQL (Tabs)
- [x] Múltiples tabs de queries
- [x] Crear nuevo tab (+)
- [x] Cerrar tab (x)
- [x] Persistir tabs entre sesiones
- [x] CodeMirror con syntax highlighting
- [x] Autocomplete (tablas, columnas, keywords)
- [x] Líneas numeradas
- [ ] Cerrar tab con confirmación si hay cambios
- [ ] Renombrar tab (doble click)
- [ ] Tab muestra * si hay cambios sin guardar
- [ ] Highlight de errores
- [ ] Bracket matching
- [ ] Multi-cursor (Ctrl+D)

### 3. Resultados (Data Grid Real)
- [x] Tabla con headers sticky
- [x] Ordenar por columna (click en header)
- [x] Filtro rápido por texto
- [x] Paginación (100 rows por página)
- [x] Mostrar total de rows
- [x] NULL como badge gris
- [x] Boolean como check/x
- [x] JSON con syntax highlight
- [x] Expandir celda larga (tooltip on hover)
- [ ] Filtro por columna específica
- [ ] Seleccionar filas (checkbox)
- [ ] Seleccionar/copiar celdas
- [x] Resize de columnas (drag)
- [ ] Formateo de fechas legibles
- [ ] Números con separadores

### 4. Export
- [x] Export a CSV
- [x] Export a JSON
- [ ] Export selección o todo
- [ ] Copiar como INSERT statements
- [ ] Copiar como CSV al clipboard

### 5. Query History
- [x] Guardar queries en SQLite
- [x] Mostrar en sidebar
- [x] Click para cargar en editor
- [x] Mostrar timestamp y duración
- [x] Limpiar history
- [x] Buscar en history

### 6. Saved Queries (Favoritos)
- [x] Guardar query con nombre
- [x] Organizar en carpetas
- [x] Cargar query guardado
- [x] Editar nombre
- [x] Eliminar

### 7. Schema Browser
- [x] Tree view expandible
- [x] Ver columnas con tipos
- [x] Ver primary keys
- [x] Ver foreign keys
- [x] Click en columna → añadir al query
- [x] Refresh schema (F5)
- [ ] Click en tabla → INSERT template
- [x] Buscar en schema
- [x] Ver row count por tabla

### 8. Keyboard Shortcuts
- [x] Ctrl+Enter - Ejecutar query
- [x] Ctrl+S - Guardar query
- [x] Ctrl+N/T - Nuevo tab
- [x] Ctrl+W - Cerrar tab
- [x] Ctrl+Space - Autocomplete
- [x] F5 - Refresh schema
- [x] Escape - Cancelar query
- [x] Ctrl+Tab - Siguiente tab
- [ ] Ctrl+/ - Comentar línea

### 9. UX
- [x] Loading spinner
- [x] Cancelar query en ejecución
- [x] Mensaje de error amigable
- [x] Modo oscuro (default)
- [x] Toast notifications (global component)
- [ ] Resize de paneles
- [x] Modo claro toggle
- [ ] Responsive

---

## Phase 2: PostgreSQL Admin - DONE ✅

> **Goal**: Administración completa de PostgreSQL
> **Success**: Manejar un incidente de "base de datos lenta" usando solo Tusk

### 1. Dashboard de Servidor
- [x] Conexiones activas vs max
- [x] Queries activas
- [x] Cache hit ratio
- [x] Tamaño de base de datos
- [x] Uptime
- [x] Versión de PostgreSQL
- [x] Refresh manual
- [x] Auto-refresh configurable (Off/5s/10s/30s/60s)
- [ ] Gráficos de tendencia

### 2. Active Queries Monitor
- [x] Lista de queries en ejecución
- [x] Mostrar: PID, user, database, duration, state, query
- [x] Highlight queries lentas (> 10s)
- [x] Highlight idle in transaction
- [x] Kill query individual
- [x] Refresh button
- [ ] Kill todas las queries de un usuario
- [ ] Filtrar por database/user

### 3. Locks Monitor
- [x] Ver locks activos
- [x] Identificar bloqueos
- [x] Ver waiting queries
- [x] Visualizar cadena de bloqueos

### 4. Backup & Restore
- [x] pg_dump con UI
- [x] Lista de backups existentes
- [x] Download backup
- [x] Restore con UI
- [ ] Seleccionar formato (plain, custom, directory)
- [ ] Seleccionar tablas específicas
- [ ] Progress bar durante backup
- [x] Scheduled backups (APScheduler)

### 5. Table Maintenance
- [x] VACUUM table
- [x] VACUUM FULL table
- [x] ANALYZE table
- [x] REINDEX table
- [x] Ver table bloat
- [x] Ver dead tuples
- [x] Programar maintenance (APScheduler)

### 6. Extensions Manager
- [x] Lista de extensiones instaladas
- [x] Lista de extensiones disponibles
- [x] Instalar extensión
- [x] Desinstalar extensión
- [x] Ver versión actual vs disponible

### 7. User/Role Management
- [x] Lista de roles
- [x] Crear rol
- [x] Editar permisos
- [x] Cambiar password
- [ ] Ver grants

### 8. Database Settings
- [x] Ver configuración actual (important settings)
- [ ] Modificar settings
- [ ] Comparar con defaults

### 9. Logs Viewer
- [ ] Ver últimas líneas del log
- [ ] Filtrar por nivel
- [ ] Buscar en logs

---

## Phase 3: Analytics Engine (DuckDB) - DONE ✅

> **Goal**: DuckDB integrado, queries federados, archivos Parquet

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

### Pending
- [x] DuckDB Spatial extension (auto-installed at startup)
- [x] DuckDB Extension Manager UI

---

## Phase 6: Cluster Mode - IN PROGRESS 🚧

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

---

## Phase 7: User Management & Access Control - IN PROGRESS 🚧

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
  - `tusk[studio]` → duckdb, polars, psycopg, litestar, jinja2
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
