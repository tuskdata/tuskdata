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

## 0.5.x exit criteria

- Overall test coverage ≥ 45% (from 33% baseline).
- All P1 tech-debt items closed.
- Data Contracts shipped with docs page + sample contract YAML.
- mkdocs-material docs site live on GitHub Pages with: install, first connection, plugin system, security model.
