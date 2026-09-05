# Now — 2026-09-06

What ships between here and 0.5.0, in order. Sizes are working days at
the current pace (one medium release per session day); the calendar
assumes 1-2 session days a week, so the whole list is roughly 4-5 months.

Positioning stays: **PostgreSQL admin and SQL studio with an AI copilot and
light analytics.** Rule of thumb: one geo item and one admin item per
cycle, never two geo cycles in a row.

Shipped this cycle: 0.4.28 – 0.4.37 (revival, deps, MCP + API tokens, Schema
Watch, Data Contracts, Studio ergonomics, `tusk app` preview, tusk-bi into
core, generated docs, component library on tokens, Schema diagrams that
scale).

## 0.4.38 — Schema navigation + one metadata store (~1 week)

- **Schema navigation**: search that jumps to and centres a table (reuse
  ⌘K), filter by schema/prefix, **only related** toggle that hides
  everything but the selected table's neighbourhood and re-lays it out.
- **`tusk.core.meta`**: one module owning every SQLite connection (today 55
  `sqlite3.connect` sites in 16 files, 9 `.db` files under `~/.tusk`).
  `connect(name)` plus a small dialect shim; no behaviour change; the
  existing suite is the net. Collapse the per-component files into
  `tusk.db` (+ `plugins/<id>.db`) with a one-time migration on boot. This
  is the cheap insurance that would make a Postgres meta backend a bounded
  job — the backend itself is not planned.

## 0.4.39 — Geo grounding for the Copilot (~1 week)

Baseline (2026-09-05, statuos_dev, qwen3.5:9b): "show me the vegetarian
restaurants in Piantini" → *no restaurant tables in this schema*. Three
blind spots, all grounding:

- **Spatial catalog**: PostGIS presence/version; per geometry column type
  and SRID from `geometry_columns`; geography; lat/lon pairs; `h3` if
  installed. A `### Spatial` prompt section with a short PostGIS cheat
  sheet (`ST_Contains`, `ST_DWithin` on geography for metres,
  `ST_SetSRID(ST_MakePoint(lon, lat), 4326)`, keep the geometry in SELECT).
- **Column profiles**: for `jsonb` and low-cardinality text columns, top-20
  keys/values from a 5k-row sample, stored with the Schema Watch snapshot.
- **Gazetteer**: tables that look like places (polygon + name column); at
  question time look up capitalised tokens of the prompt (`ILIKE` /
  `pg_trgm`) and inject the matches. Deterministic retrieval beats
  tool-calling with small models.
- **Map**: Copilot SQL keeps the geometry column and Studio opens the map
  tab; MCP `run_query` returns GeoJSON when a geometry column is present.

## 0.4.40 — Kubernetes for real + published roadmap tells the truth (~2-3 days)

Waits for the homelab k3s cluster.

- Publish `ghcr.io/tuskdata/tuskdata:<tag>` from `publish.yml` on tag.
- `TUSK_AUTH_MODE` (and `TUSK_PG_BIN_PATH`) as environment overrides.
- `deploy/k8s/` manifest (Namespace, StatefulSet 1 replica, PVC, Service,
  Traefik Ingress); `docs/deployment/kubernetes.md` corrected (image, env,
  the real `~/.tusk` files, no HA date); tested: PVC survives restart,
  `rollout restart` upgrade, liveness probe restarts a hung pod.
- Move `roadmap/later/*` to `roadmap/archive/` with a dated reason each;
  rewrite `next.md`; drop the public promises (Embedded SDK "0.7.x" in
  analytics.md, HA "0.9.x" in kubernetes.md); rewrite TODO.md / TODO.es.md.

## 0.4.41 — Alerts on a value + dashboards as files (~1 week)

- **Alert rules**: *when <value> <op> <threshold> [for <duration>] → notify
  <channel>*. Sources: a saved query, a dashboard widget, or an Admin
  metric (connections %, replication lag, bloat, disk). Reuses the
  notification channels and the scheduler tick. No write-back / reverse
  ETL.
- `tusk bi export` / `tusk bi import` in YAML (the JSON endpoint exists).
  No `tusk apply`, no published JSON Schema.

## 0.4.42 — Spatial health + geodata import (~1.5 weeks)

- **Spatial health** in Admin, Schema and Explore: geometry columns without
  a GIST index, SRID 0 or mixed, `ST_IsValid` counts, extent and a sample
  map in Explore, SRID/type on Schema cards, SRID/type changes in Schema
  Watch and Contracts.
- **Import to PostGIS** from Data: GeoJSON / GeoParquet / GPKG / Shapefile
  via DuckDB spatial → table with geometry column, SRID and GIST. One-click
  OSM (bbox download already exists) → PostGIS with `tags jsonb`.

## 0.4.43 — Advisor (~2 weeks)

Admin → Advisor: top queries from `pg_stat_statements` by total time,
missing-index suggestions from EXPLAIN, the existing AI plan insight
reading the result. Recommends, never applies. First to drop if time runs
out; the product does not limp without it.

## 0.4.44 — Vector tiles + H3 (~4 days)

- `/api/tiles/{saved_query_id}/{z}/{x}/{y}` with `ST_AsMVT`, so MapLibre
  clients (the territorial platform) consume Tusk layers directly.
- "Aggregate to H3 resolution N" on the Explore map when `h3-pg` exists.

## 0.5.0 — when Apple approves

Desktop packaging (own plan) plus a hygiene cut, no new features:

- Test coverage ≥ 45 % (35 % today; `routes/data.py`, `routes/auth.py`,
  `admin/backup.py`, `engines/postgres.py` are the gap).
- Litestar 2.24 path-param deprecations (`{id:int}` → `FromPath[int]`),
  before 3.0 lands (beta expected late 2026).
- Ibis to an optional extra (prod has run with `ibis: unavailable`).
- Docs complete for everything above.

## Out, and the docs say so

HA / multi-replica, Embedded SDK and license keys, semantic layer / OSI /
agentic anomaly, CDC, notebooks, tusk-cluster (paused), more chart types
in Analytics. Any of these comes back only when a named user asks for it.

## Open nits (fold into whichever release touches the area)

- Horizontal scroll appears after confirmation modals in BI.
- Copilot with a 9B model invents joins when the schema is thin
  (`orders.product_id` in the demo); geo grounding will not fix that one —
  a bigger model or a "verify columns" post-check would.
- Gradual adoption of litestar-htmx (installed, unused) when touching routes.
