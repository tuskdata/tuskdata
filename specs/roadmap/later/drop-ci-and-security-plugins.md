# Cleanup: drop tusk-ci + tusk-security (OSINT) plugins

## Status

**Executed** 2026-05-20 alongside v0.4.15. Wheels removed from `tuskdata-compose/wheels/`; Coolify rebuilds without them. Source repos remain unchanged on GitHub. Originally scheduled "after 0.5.x" but pulled in early — the cleanup turned out to be 15 minutes of work and aligned naturally with the v0.4.15 release that needed a compose bump anyway.

This file remains as a record of the decision; the cleanup is no longer pending.

## Rationale

Focus on the core product. tusk-ci and tusk-security are both reasonable products in their own right, but they pull attention away from the SMB data play (dashboards + admin + contracts + embed).

The mockup's "Security" tab is **app-level RBAC + audit + secrets vault**, not the OSINT/DNS scanner that tusk-security currently is. Same name, very different product. The new in-core Security feature ships in 0.7.x+.

## Action plan

When we execute (post-0.5.x):

1. **No data migration.** SMB users today are limited to internal use; no external dependency exists. The plugin tables (`security_scans`, `ci_runs`, etc. inside the plugins' own SQLite DBs) get dropped along with the plugin install.
2. Remove the wheels from `tuskdata-compose/wheels/`.
3. Update `tuskdata-compose/Dockerfile` to stop installing them.
4. Archive (not delete) the source repos `tusk-security` and `tusk-ci` on GitHub — read-only, in case someone forks.
5. Document the deprecation in the next release CHANGELOG.

## When this becomes "Now"

After 0.5.x ships. The cleanup doesn't unblock anything in 0.5.x — Data Contracts and engineering hygiene don't touch the plugins at all.

If asked "why not now" during 0.5.x: removing these plugins now means redoing the install matrix and breaking any test that loads them. Cheaper to do it once after the audit cycle is done.
