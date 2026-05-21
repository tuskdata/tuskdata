# Test coverage snapshot — 2026-05-19

Baseline before any test investment. Generated with `coverage run --source=src/tusk -m pytest tests/`.

## Headline number

**33% overall** (3,757 of 11,449 statements covered, 239 tests passing in 57s).

This is low. Industry "healthy" for a product approaching commercial is 60-75%. Going from 33% → 60% is a focused 4-6 weeks of effort once we know where to aim.

## Well-covered (≥80%) — touch with confidence

| Module | Stmts | Cover |
|---|---:|---:|
| `core/notifications.py` | 406 | 96% |
| `core/connection.py` | 145 | 94% |
| `core/query_tracker.py` | 35 | 94% |
| `core/logging.py` | 26 | 88% |
| `core/download_hooks.py` | 21 | 86% |
| `plugins/storage.py` | 36 | 83% |
| `core/crypto.py` | 37 | 81% |
| `core/url_guard.py` | 42 | 81% |
| `plugins/templates.py` | 47 | 81% |

These are the safest modules to refactor. Most are small/leaf.

## Dangerously thin (≤25%) on big modules — refactor risk = high

Rank-ordered by `(statements × (1 − coverage))` — the modules where the most untested lines live, weighted by absolute size:

| Module | Stmts | Cover | **Untested lines** |
|---|---:|---:|---:|
| `studio/routes/admin.py` | 892 | 17% | **741** |
| `studio/routes/data.py` | 719 | 21% | **568** |
| `engines/polars_engine.py` | 674 | 26% | **500** |
| `studio/routes/auth.py` | 403 | 18% | **331** |
| `admin/backup.py` | 358 | 12% | **316** |
| `engines/postgres.py` | 405 | 17% | **335** |
| `studio/routes/ai.py` | 316 | 14% | **272** |
| `studio/routes/cluster.py` | 269 | 0% | **269** |
| `core/geo.py` | 304 | 15% | **258** |
| `studio/routes/admin.py` (already counted) | — | — | — |
| `cli.py` | 248 | 0% | **248** |
| `engines/duckdb_engine.py` | 232 | 16% | **195** |

The pattern is loud: **`studio/routes/*` is where most untested lines live**. Same files that have the highest churn (admin, data, api, cluster). The CSRF middleware bug we shipped tonight is exactly this pattern.

## Completely uncovered (0%)

- `cli.py` — 248 stmts. No CLI tests at all.
- `studio/routes/cluster.py` — 269 stmts. Tested only indirectly via the plugin.
- `admin/stats_history.py` — 41 stmts. Stat snapshot ring buffer.
- `cluster/__init__.py` — 13 stmts. Tiny, plugin-only.

## Where to invest (recommendation)

For the **next 4 features** in the roadmap (Data Contracts → GitOps Dashboards → Alerts & Actions → Embedded SDK), the prep work that gives best ROI is:

1. **Bring `studio/routes/admin.py` from 17% → 50%** before adding any new admin endpoints. It's the largest untested surface and the most frequently bug-prone.
2. **Test `studio/app.py` middleware paths** (currently 63%, but the CSRF + correlation-id paths are untested — that's why tonight's bug shipped). Add a `test_middleware.py` that exercises each middleware with a TestClient and asserts the 4xx codes, not just the happy path.
3. **`cli.py` 0%** is acceptable for now (manual usage), but if we ship `tusk apply` for GitOps dashboards, the CLI gets a buyer use case and needs coverage. Plan a basic `test_cli.py` when 1.3 (GitOps) lands.
4. **Engines** (polars/postgres/duckdb): currently 17-26%. Risky for refactors but the tests we have catch the obvious regressions. Lower priority; defer until we add a real second user.

## Re-measure

Every PR that adds tests should report the new overall %. Every 0.X.0 release should snapshot here and note the delta vs this baseline.

| Date | Version | Overall | admin.py | Notes |
|---|---|---:|---:|---|
| 2026-05-19 | 0.4.13 | 33% | 17% | baseline |
| 2026-05-21 | 0.4.18 | **35%** | **31%** | +2pp global, +14pp on admin.py via `test_admin_routes.py` covering guards + branching. PG-execution paths still uncovered — pending CI PG service in 0.5.x. |
