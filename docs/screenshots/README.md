# Screenshots

This directory holds the public-facing screenshots referenced by the docs in `docs/features/*.md`.

## How to regenerate

Screenshots are generated, not hand-made. `scripts/demo_db.py` creates a
synthetic `tusk_demo` PostgreSQL database (customers, products, orders,
events); `scripts/docs_screenshots.py` boots a throwaway Tusk on port 8900
with that single connection and shoots every page with Playwright at
1440×900, 2x:

```bash
.venv/bin/python scripts/demo_db.py            # once
.venv/bin/python scripts/docs_screenshots.py   # or --only studio,schema
```

Stop any other local Tusk first: `admin.png` shows `pg_stat_activity`, and
another instance's pools would put its database names in the shot.

| File | Page |
|---|---|
| `home.png` | `features/home.md` |
| `studio.png` | `features/studio.md` |
| `schema.png` | `features/schema.md` |
| `explore.png` | `features/explore.md` |
| `admin.png` | `features/admin.md` |
| `analytics-overview.png` | `features/analytics.md` |
| `analytics-dashboard.png` | `features/analytics.md` |
| `studio-chart.png`, `studio-plan.png`, `studio-copilot.png` | `features/studio.md`, `features/mcp.md` |
| `scheduled.png`, `data.png` | `features/scheduled.md`, `features/data.md` |
| `settings.png`, `settings-studio.png` | `features/studio.md` |
| `profile.png` | `features/auth.md` |
| `notifications.png` | `features/notifications.md` |

## Rules

- Never commit a screenshot of a real environment. Everything here comes
  from the generator against the synthetic `tusk_demo` database, so there
  are no customer names, IPs, row data or internal schema names to redact.
- Regenerate rather than edit: fix the scene in `scripts/docs_screenshots.py`
  and rerun. Hand-retouched images drift from the product.
- `admin.png` shows `pg_stat_activity`: stop every other local Tusk first,
  or its pools will list your databases in the shot.
