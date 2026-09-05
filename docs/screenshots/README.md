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

## Sanitization checklist

Before committing a screenshot:

- [ ] **Connection names** anonymized to generic placeholders (`my-postgres-prod`, `analytics-db`, `staging-db`, etc.). Do **not** ship internal/customer names.
- [ ] **Table names** are OK to keep if they're generic (`users`, `orders`, `geo_administrative_area`) but rename if they expose schema decisions you'd rather not telegraph (`acme_internal_billing` → `customers_billing`).
- [ ] **Row data** that's PII (names, emails, addresses, IDs) blurred or replaced with `Lorem ipsum`. macOS Preview's annotation tools work; for batch use `pip install pillow` + a one-shot script.
- [ ] **IP addresses** in tunnel error messages redacted (`54.210.176.211` → `<bastion-ip>` or blur).
- [ ] **Database names** in error strings redacted (`api_socio_db_pro` → `<your-db>`).

When in doubt, do a screenshot of a fresh `tusk studio` install with seed data (`tusk demo seed` — planned), not your real environment.

## Where the source images came from (internal note)

The reference set used to write the v0.4.x docs came from a real deployment at `10.0.0.188:7000`. They were intentionally **not** committed — only the descriptive prose in the feature pages references them. When time permits, capture clean replacements on the demo dataset.
