# Now — 2026-09-05

What's in flight or starting **this cycle (0.4.31 → 0.5.0)**. Higher-level than
the task list — this is the "what does the user-visible product gain in the
next weeks" view.

## Context reset (2026-09-05)

Three months idle, then a one-day revival: deps to Litestar 2.24 / Python
3.13 / Polars 1.44, tusk-bi charts fixed (widgets.js was never loaded), AI
grounding for large schemas, scheduled backups with destination + rotation,
Windows console, MCP server via litestar-mcp, prod restart loop on the
non-AVX VM fixed, plugin assets no longer wiped on worker recycle.
Shipped as 0.4.28 – 0.4.30.

Positioning agreed: **a modern pgAdmin with AI and lightweight analytics**,
not a generic data platform. tusk-ci and tusk-security are gone, tusk-cluster
is paused, Data/ETL is reduced (canvas stays, Ibis epics are dead), tusk-bi
stays lean and moves to core. The May roadmap items in `later/` (semantic
layer, CDC, HA, embedded SDK, license keys, notebooks) are parked.

**0.5.0 waits for the Apple Developer account** (needed to sign the desktop
app). Until then everything ships as 0.4.x — one feature per release, each
one deployable on its own, bugs found along the way go in the same release.

## 0.4.31 — API tokens + MCP for everyone (~3-4 days)

The foundation the rest builds on.
- Per-user API tokens: `api_tokens` table, `tusk auth token create|list|revoke`,
  `Authorization: Bearer` accepted by the session middleware. Scoped to the
  user's connections/permissions.
- MCP in multi-user mode via tokens. Every `tools/call` goes to the audit
  log (user, tool, connection, SQL).
- `run_query` on DuckDB/SQLite connections too; `list_saved_queries` +
  run a saved query as a tool; `get_schema` also exposed as an MCP resource.
- User identity in the HTTP access log.

## 0.4.32 — Schema Watch (~2-3 days)

Data Contracts, layer 1: the thing that evaluates them.
- Scheduled kind `schema_watch` per connection: snapshot of tables /
  columns / types / nullability / PK-FK / indexes (same catalog query the
  Copilot uses), stored in SQLite, diffed against the previous run.
- Diff → event `schema.changed` with the detail → Slack / webhook / in-app,
  through the notification channels that already work end-to-end.
- MCP tool `schema_changes(connection_id, since)`.

## 0.4.33 — Frozen contracts (~3-4 days)

Data Contracts, layer 2: contracts are inferred, not written.
- "Freeze contract" on a connection or a set of tables: the current
  snapshot becomes the expected schema. Rules that come for free: column
  exists, type unchanged, not made nullable, PK/FK intact.
- Contract shown as a table in the UI with ✅/⚠️ per table; every Schema
  Watch run evaluates it; violations raise `contract.violated`.
- Export to YAML is a button, not the workflow. Layer 3 (freshness, volume,
  uniqueness rules — where YAML finally earns its place) comes after real use.

## 0.4.34 — Studio ergonomics (~2 days)

- Colour tabs and editor header by the connected server's colour.
  Stops the DROP-on-prod-thinking-it's-dev class of accident.
- Row cap for "open table" from the schema tree: `LIMIT` from a setting
  instead of `SELECT *`.
- AI Insight on EXPLAIN: wire the Copilot to the existing EXPLAIN viewer.
- Custom XYZ tile provider for the map — small, we use geo.

## 0.4.35 — `tusk app` (preview) (~1 day)

- Optional extra `tuskdata[app]` (pywebview). `tusk app` boots Granian on a
  free port and opens a native window; `tusk app --url http://...` wraps a
  remote deploy instead. Labelled preview: no packaging, no signing, no
  auto-update until the Apple account exists.

## 0.4.36 — tusk-bi into core (~1-2 days, mechanical)

Decided in May (`later/tusk-bi-to-core.md`). Own release so any breakage
is isolatable. One wheel to deploy instead of two.

## 0.4.37 — docs site + component library (done, unreleased)

Shipped on main after 0.4.36: generated screenshots (`scripts/demo_db.py`,
`scripts/docs_screenshots.py`), Scheduled/Data/Notifications pages, GitHub
Pages workflow (needs Pages enabled once), the macro library rebuilt on the
design tokens with a render test, and three fixes found on the way
(orphaned granian on SIGTERM, job names, compact dtypes in Data preview).
Tag it as 0.4.37 with the next fix batch.

## 0.5.0 — when Apple approves

- Desktop packaging: PyInstaller/Briefcase, Developer ID + notarization,
  Windows signing, auto-update.
- Docs site live on GitHub Pages (workflow exists; enable Pages in repo settings).
- Test coverage ≥ 45% (35% today; `routes/data.py`, `routes/auth.py`,
  `admin/backup.py`, `engines/postgres.py` are the gap).
- Ibis moved to an optional extra (prod has run with `ibis: unavailable`
  for months; Data page falls back to Polars anyway).
- TODO.md / TODO.es.md rewritten to reality.

## Open nits (fold into whichever release touches the area)

- Horizontal scroll appears after confirmation modals in BI.
- Litestar 2.24 deprecates inferred path params (`{id:int}` → `FromPath[int]`);
  dozens of handlers, do before 3.0 lands (beta expected late 2026).
- Gradual adoption of litestar-htmx (installed, unused) when touching routes.
