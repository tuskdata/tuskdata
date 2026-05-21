# Tech debt — 2026-05-19 (updated 2026-05-20)

Ranked by **impact × likelihood of biting us within 0.5.x-0.7.x**. Each item should either become an ADR + closed issue, or move to roadmap if it's actually a feature.

## P1 — fix in 0.5.x

### 1. ~~No CI workflow on PRs~~ **CLOSED** (v0.4.14)
`.github/workflows/ci.yml` runs pytest + ruff + coverage on push/PR with parallel Python 3.13 + 3.14 jobs.

### 2. ~~Middleware exception logging is silent~~ **CLOSED** (v0.4.15)
`_log_unhandled_exception` `after_exception` hook in `studio/app.py` logs every 5xx at ERROR with traceback + path + method + correlation_id; routine 4xx are silenced. Regression test in `tests/test_middleware.py`.

### 3. ~~AbstractMiddleware is deprecated (Litestar 2.15)~~ **CLOSED** (v0.4.17)
All four middlewares (RequestTimeout, Session, CorrelationID, CSRF) migrated to `litestar.middleware.ASGIMiddleware`. Passed as instances now, not classes.

### 4. ~~StaticFilesConfig is deprecated (Litestar 2.6)~~ **CLOSED** (v0.4.17)
Migrated to `create_static_files_router` in `studio/app.py`. Tests pass with zero DeprecationWarnings.

### 5b. Test isolation — Litestar app is a module-level singleton shared across test files
Multiple test files (`test_e2e.py`, `test_admin_routes.py`, `test_middleware.py`, `test_bi_v030_e2e.py`) all do `from tusk.studio.app import app` then `TestClient(app=app)`. The Litestar app object's lifespan state leaks across modules: when test file A's TestClient exits, its `on_shutdown` runs and partially closes the scheduler / lifespan task; when test file B's TestClient enters, it tries to start a new lifespan but the underlying ASGI receive coroutine has been cancelled. Symptom: `concurrent.futures._base.CancelledError` raised during TestClient `__exit__` on the *second* fixture to run.

Worked around in v0.4.19 by catching CancelledError in `test_e2e.py`'s fixture teardown — the test itself has already completed by then. **Proper fix**: each test file builds its own Litestar instance from scratch (route handlers + middleware passed in) instead of importing the singleton. That eliminates the cross-file state entirely. Tracked here, scheduled for 0.5.x when we touch the test infra to add the Postgres service container.

### 5. Test coverage — admin.py from 17% → 31%, target 50% (open, partial)
**Partial progress shipped in v0.4.18** (`tests/test_admin_routes.py`):
- admin.py: 17% → 31% (covered the routing logic, guards, wrong-type
  rejection, HTMX-vs-JSON branching for ~10 endpoints).
- Global: 33% → 35%.

What's left to reach 50% on admin.py: the actual SQL-execution
paths inside each handler. Those need a **real Postgres service
container in CI** (we don't run one yet). Adding it is the first
task in 0.5.x: extend `.github/workflows/ci.yml` with a `postgres:17`
service, then add integration tests that walk the happy paths
end-to-end against it. The framework-level paths are already
covered, so the integration tests can focus on engine output shape
without re-testing guards.

The four largest routes files still need a similar pass:
- `data.py 1384 LOC, ~21%`
- `api.py 1122 LOC, ~33%`
- `auth.py 786 LOC, ~18%`

Plus the long-term refactor: stop adding endpoints to these mega-files;
one Controller per logical area in its own file.

## P2 — fix in 0.6.x or 0.7.x

### 6. Three routes files exceed 1,000 LOC
`admin.py` (1,709), `data.py` (1,384), `api.py` (1,122). Too big to navigate, too many cross-handler dependencies, refactor risk grows monthly. **Fix**: split into `routes/admin/{processes,locks,backup,roles,extensions,settings,stats,maintenance}.py` with one Controller each.
- Effort: 2-3 days.

### 7. `polars_engine.py` is 1,164 LOC
Mixes I/O, schema introspection, eval safety, and chart rendering. **Fix**: split into `polars_engine/{io,schema,safe_eval,charts}.py`. Tests exist for safe_eval already.
- Effort: 2 days.

### 8. Silent `try/except: pass` blocks (10+ occurrences)
Each one is a potential silent-failure landmine. Many are legitimately fine ("close best-effort"), but several swallow real errors. **Fix**: audit each one, replace with explicit `log.debug("...", error=str(e))` so the failure is at least visible at DEBUG. Add a lint rule for naked `except: pass`.
- Effort: 1 day.

### 9. CLI is 248 stmts with 0% coverage
`tusk studio`, `tusk plugins`, `tusk version` etc. all manually tested. When we add `tusk apply` for GitOps dashboards (roadmap), the CLI becomes user-facing. **Fix**: add `tests/test_cli.py` covering the main commands via `subprocess` or click's test runner.
- Effort: 1 day. Bundle with GitOps feature work.

### 10. No docs site
README is the only public-facing doc. Enterprise won't touch a product without proper docs. **Fix**: mkdocs-material site, GitHub Pages hosting. Initial scope: install, first dashboard, plugin system, security, deployment.
- Effort: 2-3 weeks (mostly writing).

## P3 — opportunistic / cleanup

### 11. Many `Optional`/`Union` legacy typing
Python 3.10+ has `X | None` and `X | Y`. We're on 3.12. **Fix**: ruff rule `UP007` + a single PR doing the migration. Cosmetic but reduces noise.
- Effort: 1 hour with ruff.

### 12. `cli.py` uses `print(...)` everywhere
Fine for a CLI, but inconsistent with the rest of the codebase (`structlog`). Not worth changing unless we want CLI output to be machine-parseable. **Defer.**

### 13. Plugin templates rely on filesystem copy at startup
`plugins/templates.py` copies templates from each plugin's installed wheel to `studio/templates/plugins/<id>/` on boot. Works but means hot-reloading plugin templates requires app restart. **Defer** unless someone complains. When `tusk-bi` is promoted to core, this goes away.

## Not actually debt (but listed elsewhere as such)

- **No license-key infrastructure**: not debt yet — only needed when we ship the first paid feature. Tracked in `roadmap/next.md`.
- **No semantic layer**: feature, not debt. `roadmap/next.md`.
- **No reverse-ETL / activation**: feature. `roadmap/now.md`.

## Re-prioritize

This list should be reviewed before every 0.X.0 release. New items go into the right priority slot via PR. Closed items move out with a one-line reason.
