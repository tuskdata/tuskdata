# Schema Watch

![Schema page with the Schema watch panel (bottom-left).](../screenshots/schema.png){ .screenshot }

Schema Watch takes a snapshot of a PostgreSQL connection's catalog — tables,
columns with types and nullability, primary and foreign keys, indexes —
compares it with the previous snapshot, keeps the history, and raises a
`schema.changed` notification when something moved.

It is the foundation of Data Contracts: a contract is "the schema I
expect", and Schema Watch is what notices when reality drifts.

## Turn it on

**Scheduled → New job → Schema watch**, pick the connection, choose a cron
(daily at 06:00 by default). The first run takes a baseline and reports
nothing; every later run reports the diff.

To be told about changes, subscribe a channel to **Schema Changed** under
Notifications → Subscriptions. The message is a one-paragraph summary:

```
orders: + column status text; orders.total: NOT NULL added;
- table legacy_imports; customers: + index customers_email_idx
```

with the connection name in the title and the structured diff in the
context (webhooks get the full JSON).

## Check on demand

On the **Schema** page the *Schema watch* panel shows the last snapshot,
the recent changes for the selected connection and a **Check now** button.
The same is available to agents through the MCP tool `schema_changes`:

> "What changed in production since yesterday?"

and through the API:

```
POST /api/schema-watch/{connection_id}/run
GET  /api/schema-watch/{connection_id}/status
GET  /api/schema-watch/{connection_id}/changes?days=30
```

## What counts as a change

| Detected | Example |
|---|---|
| Table added / removed | `+ table invoices` |
| Column added / removed | `orders: - column legacy_code` |
| Column type changed | `orders.id: type integer → bigint` |
| Nullability changed | `orders.total: NOT NULL added` / `now nullable` |
| Primary key changed | `orders: primary key ['id'] → ['id', 'region']` |
| Foreign key added / removed | `orders: - FK customer_id → customers.id` |
| Index added / removed | `orders: + index orders_status_idx` |

Row counts, data and permissions are *not* part of the snapshot — that is
what makes a check cheap enough to run every hour on a large database.

## Storage and retention

Snapshots and changes live in `~/.tusk/tusk.db` (with the rest of Tusk's own
state). The last 30
snapshots per connection are kept; change records keep their own copy of
the diff, so history survives pruning.

## Limits

- PostgreSQL only (DuckDB/SQLite connections are skipped).
- `pg_catalog` and `information_schema` are excluded; every other schema is
  included, qualified as `schema.table` unless it is `public`.
- Manual runs are audited (`schema_watch.run`); scheduled runs follow the
  job's own notify settings on failure.
