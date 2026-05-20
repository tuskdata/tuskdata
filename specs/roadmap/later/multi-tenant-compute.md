# Later: Multi-tenant compute isolation

## Why later

Needed only when we start selling Embedded SDK to SaaS customers running multiple of their own tenants through one Tusk install — until then it's premature.

Technical risk: cgroups, query budgets, kill-switches on runaway SQL. Real sysadmin work. 6-8 weeks of focused effort.

## Shape when we build it

Per Tusk workspace (tenant): a resource quota (CPU s/min, max rows scanned, max concurrent queries). Enforced via:

- DuckDB query cancellation if soft budget exceeded.
- Postgres `statement_timeout` per-session.
- cgroups v2 on Linux to cap CPU/memory of the dataframe-side worker.
- A "quota exceeded" UI state with a friendly explanation.

## When this becomes "Next"

- Embedded SDK has been shipped (0.7.x).
- We have at least one paying customer using it to embed dashboards for **their** tenants.
- That customer asks "what stops a noisy neighbor from hosing the rest" — which they will.
