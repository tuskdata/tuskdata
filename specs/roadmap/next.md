# Next — 2026-05-19

Committed work for the next 1-3 minor releases after `now.md`. Pulled here from `later/` when we've decided it's the next thing.

## 0.6.x — Alerts & Actions in widgets + GitOps dashboards (~5-6 weeks)

### Alerts & Actions in widgets (2-3 weeks)
Embed a reverse-ETL surface directly inside every dashboard widget. No separate tool. Rule shape:

```
WHEN <widget value> <op> <threshold> [for <duration>]
DO {
  slack(channel='#alerts'),
  webhook(url='...', payload={...}),
  write_back(connection='prod-pg', sql='UPDATE alerts SET ...'),
  email(to='...'),
}
```

Reuses:
- Existing notification system (Slack/email/webhook delivery already works).
- Existing scheduled tasks / cron infrastructure.
- Stat widget already supports a single `value` — same value is what the rule evaluates.

Net new:
- `widget_alerts` table (per widget: rule, action config, last fired, cooldown).
- API: `POST /api/bi/widgets/{id}/alerts`, `GET ...`, `DELETE ...`.
- Editor UI: an "Alerts & Actions" tab in the widget config right panel.
- Background job: evaluates every alert rule on a 1-minute tick (or matches the dashboard's `refresh_interval_seconds` if set).

### GitOps dashboards (1-2 weeks)
Dashboards exportable/importable as YAML, applied via CLI:

```
$ tusk apply path/to/dashboard.yml
```

Reuses:
- Existing dashboard export endpoint (already returns JSON).
- Existing dashboard import endpoint.

Net new:
- YAML serializer + deserializer (we already have the JSON one).
- `tusk apply <files...>` CLI subcommand. Idempotent — same YAML applied twice should not duplicate.
- Schema validation of the YAML against a JSON schema (we publish the schema URL alongside).
- Docs: `docs/guides/dashboards-as-code.md`.

### 0.6.x exit criteria
- Both features shipped with feature spec + docs page.
- Test coverage ≥ 55%.
- A user can write a YAML dashboard, push it through git, install on a fresh Tusk with `tusk apply`.
- A user can set an alert on a stat widget that pings Slack when MRR drops > 5%.

## 0.7.x — Embedded SDK + license-key infrastructure (~5-6 weeks)

### Embedded Analytics SDK (2-3 weeks)
React-first (Vue follows). White-label dashboards inside customer SaaS:

```tsx
<TuskDashboard
  token="emb_tk_..."     // generated server-side via existing embed_tokens
  theme={{ brand: '#ff5722' }}
  rls={{ tenant_id: '42' }}
/>
```

Reuses:
- Existing embed_tokens table + RLS clauses (already works).
- Existing `/bi/embed/<token>` view (already renders an iframe-friendly dashboard).

Net new:
- `@tuskdata/embed-react` package on npm.
- `@tuskdata/embed-vue` package on npm.
- Brand theming via CSS variable override.
- Docs: full integration guide + sample apps.

### License-key infrastructure (1-2 weeks)
Plumbing only — no features behind a gate yet, but every premium feature in 0.8.x+ will hook here:

- `core/license.py` — reads license key from `~/.tusk/license` or `TUSK_LICENSE_KEY` env, validates signature (Ed25519 public key embedded), exposes a `has_feature("multi_tenant_compute")` API.
- Self-hosted free tier: no license needed, `has_feature(...)` returns False for paid features → they render as upsell teasers.
- Self-hosted with license: signature-validated, expires after 1 year, online-check optional (offline grace period).

### 0.7.x exit criteria
- Embedded SDK published to npm with a working sample app.
- License plumbing in place but no features gated yet (we'll gate them in 0.8.x when we have the first paid feature ready).
- Multi-tenant compute scoping started but not shipped.

## What pushes a feature from later/ to next.md

Three triggers:
1. We've validated the demand (real user asked for it, or it blocks an SMB user we're chasing).
2. We've finished the predecessor features it depends on.
3. We have a designed feature spec in `features/` ready to execute.

If a `later/` item doesn't have all three, it stays deferred.
