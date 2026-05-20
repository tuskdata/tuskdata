# Later: Postgres auto-tuning advisor

## Why later

Cool, narrow buyer. Database admins love this; SMB engineering teams won't pay extra for it. Most SMBs are on managed Postgres (RDS, Supabase, Neon) and want the host to handle tuning.

We have the building blocks already (`pg_stat_statements`, EXPLAIN viewer in the Admin panel), but turning them into actionable recommendations is a non-trivial advisor engine — 4-6 weeks of focused work for moderate uptake.

## Shape when we build it

A new "Advisor" tab in Admin:
- Top 20 slowest queries by total time (from `pg_stat_statements`).
- For each: estimated cost reduction if a particular index is added (estimated by re-running with `enable_seqscan=off` and comparing).
- "Apply" button writes the index migration to a contract file and stages it for review.

## When this becomes "Next"

- 0.8.x or later — when the buyer pains around dashboards, alerts, embedding, contracts are all covered and we're looking for the next adjacent expansion.
- Or earlier, if a customer asks for this specifically.
