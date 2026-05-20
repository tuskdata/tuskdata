# Now — 2026-05-19

What's in flight or starting **this cycle (0.5.x)**. Higher-level than the TaskCreate task list — this is the "what does the user-visible product gain in the next 3 months" view.

## Just shipped (2026-05-18 → 2026-05-19)

- **v0.4.12** — SSH tunnel fails fast (10s `connect_timeout`, 30s broken-session cooldown, inline admin error banners).
- **v0.4.13** — CSRF middleware fixed (was returning 500 instead of 403, silently, since 0.3.x).
- **tusk-bi v0.3.0** — Phase 1 dashboards redesign: CSS-grid viewer, .dash-head chrome, Top-N + Funnel widgets, map bubbles style, sparkline tinted by trend, `is_public` + `refresh_interval_seconds` (Live + Public badges), General tab in editor settings.
- **PyPI publishing workflow** (OIDC trusted publishing, builds tagged `v*`).
- **Engineering specs structure** (`specs/{architecture,bugs,features,roadmap}/`) — first audit cycle done.

## 0.5.x — Engineering hygiene + Data Contracts (next 3-4 weeks)

Per the SMB-first focus + audit findings.

### Hygiene
- [ ] `.github/workflows/ci.yml` — run pytest + ruff on PRs and pushes (tech-debt P1 #1).
- [ ] Litestar middleware exception logging at ERROR with traceback regardless of debug flag (tech-debt P1 #2).
- [ ] Migrate `AbstractMiddleware` → `ASGIMiddleware`, `StaticFilesConfig` → `create_static_files_router` (tech-debt P1 #3, #4).
- [ ] Backfill basic happy-path tests for `studio/routes/admin.py` to reach 50% (tech-debt P1 #5).

### Feature
- [ ] **Data Contracts at connection level** — YAML contract per Postgres connection, schema-drift detection feeds it, Slack/in-app alert when contract violated, AI Copilot reads contract for grounding context. ~2 weeks of focused work. See [features/data-contracts.md](../features/data-contracts.md) for the spec (TODO: write it).

## 0.5.x — Process resilience (added 2026-05-20)

Distinct from HA. Single pod, single process — but the pod itself should
survive its own hangs and stuck subsystems. The SSH tunnel freeze on
2026-05-17 (see `bugs/2026-05-18-ssh-tunnel-hangs-admin.md`) made the
pod unresponsive without crashing it; nothing kicked it. K8s liveness
probe would have, if we had documented one.

Concrete work, ranked by impact-per-effort:

- [ ] **Granian worker timeout + recycle.** Set `--worker-timeout=30s`
  (or whatever the right number is per profile) so a worker that
  doesn't return inside the budget gets killed and respawned. Add
  `--max-requests=N` so any slow leak in a long-running worker gets
  cycled out periodically.
- [ ] **Request-level timeout middleware.** Litestar middleware that
  wraps the handler call in `asyncio.wait_for(coro, timeout=...)`.
  Per-route override via decorator; sensible default (e.g. 60s for
  read endpoints, longer for export/backup).
- [ ] **Job max-duration watchdog.** Background jobs in
  `tusk.core.jobs` already track `started_at`. A new periodic
  watchdog (every 30s) kills jobs past their declared `max_duration`
  and marks them `failed_timeout`. Default per kind: backup=1h,
  dns_scan=30m, query=10m, etc.
- [ ] **K8s deployment recipe** in `docs/deployment/kubernetes.md`
  with tuned liveness + readiness probes:
  - `livenessProbe`: GET `/api/health` every 30s, fail after 3 misses
    (~90s before the kubelet kills the pod — matches the worker
    timeout above).
  - `readinessProbe`: same endpoint, every 10s, more aggressive.
  - StatefulSet + PVC for `~/.tusk` (single replica until HA lands).
- [ ] **Lint rule: subprocess inside `async def`.** Ruff custom rule
  or grep-based pre-commit hook. Lesson #1 from the backup
  post-mortem (`bugs/2026-04-30-backups-hang-and-lie.md`) was "never
  call blocking I/O from an `async def` handler". Codify it.

None of these need a broker, a second container, or HA infra. All of
them help **even with `replicas: 1`**.

## 0.5.x exit criteria

- Overall test coverage ≥ 45% (from 33% baseline).
- All P1 tech-debt items closed.
- All process-resilience knobs from the section above shipped (Granian
  timeouts, request timeouts, job watchdog, documented K8s probes).
- Data Contracts shipped with docs page + sample contract YAML.
- mkdocs-material docs site live on GitHub Pages with: install, first
  connection, plugin system, security model.
- ADR 0001 in repo so the next person reading the codebase understands
  why we deliberately don't have Redis/brokers (and when we will).
