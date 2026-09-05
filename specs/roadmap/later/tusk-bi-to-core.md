# Later: Promote tusk-bi from external plugin into TuskData core

## Status

**Executed** 2026-09-05 in v0.4.36. Done as a *built-in plugin*: the package moved to
`src/tusk/bi` and the registry loads `BIPlugin` from core before entry points, so the
tab, templates, statics (`/static/plugins/bi/`), storage (`~/.tusk/plugins/tusk_bi.db`),
CLI and notification events are unchanged — no data migration, no URL changes. A stale
external `tusk-bi` wheel is skipped by the registry. The `tusk-bi` GitHub repo can be
archived; the compose deploy no longer ships its wheel.

## Why this matters

tusk-bi is no longer a "nice-to-have side feature". It's becoming the dashboards/analytics/embedded SDK story — central to the SMB pitch. A few signals that it should be in core, not in a separate plugin repo:

1. **Every SMB user wants dashboards.** It's not an extension; it's a primary use case. Shipping it as an external plugin means the install story is "core + this thing" instead of "everything you need".

2. **The mockup positions Dashboards / Notebooks at the top nav**, alongside Studio / Schema / Admin / Data. Top-nav features in core, plugin features under their own tab. The plugin model is for things that are genuinely optional (CDC scanner, security scanner, CI bot) — dashboards aren't.

3. **CI install matrix is simpler** when fewer plugins need to be wired up. The CSRF middleware test had to be rewritten this cycle to be hermetic precisely because `tusk-bi` isn't installed in CI — moving it to core makes test_bi_*.py first-class.

4. **The features in the 0.5.x-0.7.x roadmap** (Data Contracts, Alerts & Actions, GitOps dashboards, Embedded SDK) all touch BI in some way. Iterating across the boundary between two repos slows every change.

## When to execute

Right before **0.7.x** (Embedded SDK). By then we'll have:

- 0.5.x: Data Contracts shipping (touches contracts viewer in BI dashboards too)
- 0.6.x: Alerts & Actions + GitOps Dashboards (both deep-integrate with the dashboard editor)

If we wait until after 0.7.x, the Embedded SDK lives across two repos for its whole life. If we promote before, both 0.6.x features ship in a single repo. The cost of the promotion itself is ~1-2 days of mechanical work; we earn it back by 0.6.x's mid-point.

## What the promotion involves (mechanical, well-scoped)

1. `git mv` the source tree:
   - `/Users/jeasoft/Projects/Tusk/bi/src/tusk_bi/*.py` → `src/tusk/bi/*.py`
   - `/Users/jeasoft/Projects/Tusk/bi/src/tusk_bi/templates/bi/` → `src/tusk/studio/templates/bi/`
   - `/Users/jeasoft/Projects/Tusk/bi/src/tusk_bi/static/bi/` → `src/tusk/studio/static/bi/`
   - `/Users/jeasoft/Projects/Tusk/bi/tests/test_bi.py` → `tests/test_bi.py`

2. Update imports in moved code: `from tusk_bi.*` → `from tusk.bi.*`.

3. Drop the plugin entry-point in the old `pyproject.toml`; **archive** the `tusk-bi` GitHub repo as read-only (keep the git history for reference, no new commits).

4. Add `bi` to the route controllers list in `tusk/studio/app.py`. Remove plugin discovery for tusk-bi.

5. Migrate the SQLite storage path: old plugin used `~/.tusk/plugins/tusk-bi.db`, new core position uses `~/.tusk/bi.db`. Migration runs on first 0.7.x boot — copy file if old exists.

6. Update `tuskdata-compose/wheels/` — drop the `tusk_bi-*.whl`, just one wheel (tuskdata core) covers it.

7. Update CHANGELOG + docs.

## What we DON'T change

- The plugin **system itself** stays. Other plugins (`tusk-cluster` for sure) still exist as external repos. The plugin system is the right pattern for opt-in extensions; it just isn't the right pattern for "the second-most-used feature of the product".

- Public APIs of BI (the `/api/bi/...` routes, embed token shape, dashboard JSON export format) stay identical — only the import path inside Python changes.

- The mockup labels the top-nav tab "Analytics" (`tab_label` in the current plugin) — keep that name.

## Risk + rollback

Low risk: the move is mechanical. Rollback would be `git revert` + re-publishing the `tusk-bi` wheel. Worth doing this in its own release commit so any breakage is isolatable.

## Related specs

- [`later/postgres-cdc.md`](postgres-cdc.md) — if/when we add CDC, the BI viewer is one of the first consumers (live dashboards).
- [`later/tusk-cluster-improvements.md`](tusk-cluster-improvements.md) — tusk-cluster STAYS as a plugin even after BI moves to core; it's genuinely optional.
- [`now.md`](../now.md) — 0.5.x doesn't touch this; ship hygiene + Data Contracts first.
