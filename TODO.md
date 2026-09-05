# TODO

The plan lives in [`specs/roadmap/now.md`](specs/roadmap/now.md); this is
the short list of what is open right now. Done work is in
[`CHANGELOG.md`](CHANGELOG.md).

## Next up (0.4.41)

- [ ] Alert rules: *when <value> <op> <threshold> [for <duration>] → notify
      <channel>* on a saved query, a dashboard widget or an Admin metric.
- [ ] `tusk bi export` / `tusk bi import` (YAML).

## Then

- [ ] Spatial health in Admin / Schema / Explore (missing GIST, SRID 0,
      `ST_IsValid`, extent) — 0.4.42.
- [ ] Import GeoJSON / GeoParquet / GPKG / Shapefile and OSM into PostGIS
      from Data — 0.4.42.
- [ ] Advisor (`pg_stat_statements` + EXPLAIN + AI reading) — 0.4.43.
- [ ] Vector tiles from saved queries; H3 aggregation — 0.4.44.

## Hygiene before 0.5.0

- [ ] Test coverage ≥ 45 % (`routes/data.py`, `routes/auth.py`,
      `admin/backup.py`, `engines/postgres.py`).
- [ ] Litestar path-param deprecations (`{id:int}` → `FromPath[int]`).
- [ ] Ibis as an optional extra.
- [ ] Verify `deploy/k8s/tusk.yaml` on a real cluster (PVC survives restart,
      rollout upgrade, liveness restart of a hung pod).

## Known nits

- [ ] Horizontal scroll after confirmation modals in Analytics.
- [ ] Small models still invent a join on thin schemas; a "verify columns"
      pass after generation would catch it.
- [ ] litestar-htmx is installed but unused; adopt gradually.
