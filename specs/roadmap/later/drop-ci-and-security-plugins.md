# Cleanup: drop tusk-ci + tusk-security (OSINT) plugins

## Status

**Decided** 2026-05-18 in conversation. Not yet executed. Lives in `later/` because it's housekeeping that happens **after** 0.5.x — there's no value in removing them mid-cycle.

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
