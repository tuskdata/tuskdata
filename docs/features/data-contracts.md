# Data Contracts

A data contract is the schema your consumers rely on: these tables, these
columns, these types, these keys. In Tusk a contract is **frozen, not
written** — you take the schema as it is today and Tusk tells you when
reality breaks it.

Contracts sit on top of [Schema Watch](schema-watch.md): every snapshot is
evaluated against the connection's active contract.

## Freeze a contract

On the **Schema** page, in the *Schema watch* panel, click **Freeze**. Tusk
takes a fresh snapshot and stores every table's columns (type,
nullability), primary key and foreign keys as the contract. One connection
has one active contract; **Re-freeze** replaces it once you've accepted a
change.

From the API you can freeze a subset:

```
POST /api/contracts/{connection_id}/freeze
{"name": "reporting v2", "tables": ["orders", "customers", "sales.invoices"]}
```

## What breaks a contract

| Violation | Example summary |
|---|---|
| Expected table gone | `customers: table no longer exists` |
| Expected column gone | `orders.customer_id: column no longer exists` |
| Column type changed | `orders.id: type integer → bigint` |
| Nullability changed | `orders.total: NOT NULL added` / `NOT NULL dropped` |
| Primary key changed | `orders: primary key ['id'] → ['id', 'region']` |
| Expected foreign key gone | `orders.customer_id: FK customer_id → customers.id is gone` |

Additions never break a contract — a new column, table or index is
reported by Schema Watch as a change, nothing more.

## Being told

Subscribe a channel to **Contract Violated** (and, if you like, **Contract
Restored**) under Notifications → Subscriptions. A violation is reported
once; the same breakage on later runs stays quiet until it either changes
or is fixed, at which point you get the restore notice.

The Schema page panel shows the contract state (`holds` / `violated`) with
the open violation, and agents can ask through the MCP tool
`contract_status`:

> "Does production still match the contract? What broke?"

API:

```
GET    /api/contracts/{connection_id}              # active contract + open violation
GET    /api/contracts/{connection_id}/violations   # history
GET    /api/contracts/{connection_id}/export.yaml  # the contract as YAML
DELETE /api/contracts/{connection_id}              # release it
```

## YAML export

The export is a readable description of the contract, meant for review,
version control or sharing with the team that owns the database:

```yaml
contract: reporting v2
connection_id: 76d39ef4
frozen_at: "2026-09-05T17:10:00+00:00"
tables:
  orders:
    primary_key: [id]
    columns:
      - name: id
        type: integer
        nullable: false
      - name: total
        type: numeric
        nullable: true
    foreign_keys:
      - column: customer_id
        references: customers.id
```

It is an export, not the source of truth — the frozen snapshot is. Rules
that can't be inferred from the catalog (freshness, row-count bounds,
uniqueness) are the next layer.

## Notes

- PostgreSQL connections only.
- Freezing and releasing are audited (`contract.freeze`, `contract.release`).
- Contracts and violations live next to the snapshots in
  `~/.tusk/schema_watch.db`.
