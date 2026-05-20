# Architecture snapshot — 2026-05-19

Point-in-time view of the codebase. Re-snapshot every minor release (0.5.0, 0.6.0, …) and diff to track drift. Numbers come from raw `wc -l` on `.py` / `.html` / `.js` / `.css`; not a substitute for thinking, just a reference.

## Footprint by area

| Area | LOC | Files | Notes |
|---|---:|---:|---|
| `src/tusk/core/` | 7,737 | 26 | Cross-cutting: auth, config, connections, postgres pool, SSH tunnel, AI, notifications, scheduler, downloads, geo. Biggest single area. |
| `src/tusk/studio/` (routes+app) | 8,966 | 18 | HTTP layer — routes, app composition, HTMX helpers, middleware. Top of the funnel for new bugs. |
| `src/tusk/engines/` | 3,064 | 6 | Polars, DuckDB, Postgres, Ibis backends. |
| `src/tusk/admin/` | 3,028 | 11 | PostgreSQL-specific admin: backup, monitoring, processes, roles, extensions, settings, pitr, maintenance. |
| `src/tusk/plugins/` | 887 | 6 | Plugin system core (base class, registry, templates, storage). Small surface. |
| **Templates** | 13,970 | 112 | MiniJinja. Single biggest line-count category in the repo. |
| **Static (JS+CSS)** | 11,844 | (varies) | Alpine + HTMX + vendor (gridstack, chartjs, maplibre). |
| **Tests** | 3,425 | 15 files | Pytest + Playwright. Ratio of test LOC to source ≈ 14% — low. |

Total Python in core repo: ~24,148 LOC. Total all artifacts: ~53k.

## Largest single files (top 15)

```
1709  src/tusk/studio/routes/admin.py        ← split candidate
1384  src/tusk/studio/routes/data.py         ← split candidate
1164  src/tusk/engines/polars_engine.py
1122  src/tusk/studio/routes/api.py          ← split candidate (global API)
1070  src/tusk/core/scheduled_tasks.py
 949  src/tusk/core/auth.py
 866  src/tusk/engines/postgres.py
 786  src/tusk/studio/routes/auth.py
 773  src/tusk/core/notifications.py
 716  src/tusk/admin/backup.py
 657  src/tusk/studio/routes/ai.py
 607  src/tusk/core/downloads.py
 587  src/tusk/studio/app.py                 ← middleware lives here, bug-prone
 525  src/tusk/core/ai.py
 524  src/tusk/studio/routes/cluster.py
```

Anything above ~800 LOC is at the "single-file-doing-too-much" threshold. The three `routes/*.py` files at the top are particularly risky — they accumulate every new endpoint by default.

## Plugin sources (external repos)

| Plugin | Src LOC | Test LOC | Notes |
|---|---:|---:|---|
| `tusk-bi` (analytics) | 4,706 | 953 | Going to be promoted to core long-term; currently the most active plugin. |
| `tusk-security` (DNS/OSINT) | 6,028 | 789 | **Slated for removal** — see `roadmap/later/`. |
| `tusk-ci` (CI/CD) | 4,551 | 574 | **Slated for removal** — see `roadmap/later/`. |
| `tusk-cluster` | 1,992 | 314 | Stays as plugin. Smallest, most stable. |

## Hot spots (most-changed files in 2026)

By commit count since 2026-01-01:

```
20  src/tusk/studio/static/studio.js
16  src/tusk/studio/templates/base.html
14  src/tusk/studio/routes/api.py
13  src/tusk/studio/routes/admin.py
13  src/tusk/studio/app.py            ← CSRF bug lived here for months
13  src/tusk/engines/postgres.py
11  src/tusk/studio/templates/index.html
 9  src/tusk/studio/templates/admin.html
 8  src/tusk/admin/backup.py
 7  src/tusk/studio/templates/data.html
```

The pattern: **the studio (HTTP+UI) layer changes most.** The engine layer is comparatively stable. That's expected for a product still finding its UX shape, but it also means the studio layer has the lowest blast-radius-per-bug.

## What we have ✅

- Plugin system: discovery via entry points, isolated per-plugin SQLite storage, template + static asset isolation, dataset registration. Works.
- Multi-engine query (Polars, DuckDB, Postgres, Ibis) under one query API.
- AI Copilot wired into Studio with structured msgspec output (v0.4.8).
- Background-job system with persistent SQLite registry (v0.4.11).
- SSH tunnel infrastructure with connection pooling + fail-fast (v0.4.12).
- Schema drift detection (used for AI Copilot's grounding).
- Audit trail on exports + login/auth events.
- Single-user and multi-user auth modes.
- Embed tokens with RLS clauses (used by tusk-bi).
- CSRF + correlation-id + session-required middleware stack.

## What we DON'T have ❌ (or have weakly)

- **No CI workflow** in `.github/workflows/` other than the publish one we added today. No PR-time tests. → P1 in tech-debt.
- **No docs site** (mkdocs). README only. → P1 in tech-debt.
- **Marker test coverage is low** (~14% test LOC ratio). Many handlers untested. → P2 in tech-debt.
- **The studio routes files are too big**. Adding endpoints to existing files is the path of least resistance, which is making them hot spots for bugs. → P2 in tech-debt.
- **Middleware exception logging is silent**. The CSRF bug stayed hidden for months because Litestar's default exception handler doesn't log tracebacks at INFO. → P2 in tech-debt.
- **No license-key infrastructure**. If we go open-core, we need this before the first paid feature lands. → not P-anything yet, on the roadmap.
- **No data observability / contracts surface** as user-facing feature, despite having schema drift detection internally. → roadmap item, not debt.

## Module owners

Solo for now — owner is `jeasoft` everywhere. Note in case of future contributors.

## Re-audit cadence

Re-run the wc + churn queries before every 0.X.0 release. Compare with this baseline. Flag any module that grew >30% LOC or that joined the "top 15 largest" list since last snapshot.
