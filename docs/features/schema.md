# Schema

An interactive ER diagram of the active database. Tables are draggable cards; foreign keys are drawn as curved lines between them.

![Schema view — interactive ER diagram (89 tables · 254 FKs).](../screenshots/schema.png){ .screenshot }

*Schema view — interactive ER diagram (89 tables · 254 FKs).*


## What you see

A full-page canvas with one card per table. The screenshot of a real deployment shows **89 tables and 254 foreign keys**, all connected. Each card lists:

- The table name (as the header).
- Every column, with its type next to it (`character varying`, `timestamp with time zone`, `numeric`, `boolean`, `uuid`, …).
- A green dot next to columns that are primary keys (`PK`), an orange dot for foreign keys (`FK`).

## Top bar

```
┌──────────────────────────────────────────────────────────────────┐
│ [conn] · [schema]   89 tables · 254 FKs   Auto-layout  Fit       │
└──────────────────────────────────────────────────────────────────┘
```

- **Connection + schema picker** on the left. Switching pulls a fresh introspection from `pg_catalog`.
- **Counters** show how big the model is — useful when you've just landed in an unfamiliar database.
- **Auto-layout** runs a force-directed pass that arranges the cards so connected tables sit nearby.
- **Fit** zooms to show every table on screen.

## Interactions

- **Drag** any table card to reposition it. The layout persists in browser localStorage per (connection, schema) pair — your custom arrangement survives reloads.
- **Wheel** zooms in/out.
- **Space + drag** pans the canvas.
- **Click a table header** opens a side panel with: index list, row count estimate, sample rows, and a "Query in Studio" button that opens a new Studio tab with `SELECT * FROM <table> LIMIT 100`.
- **Click a foreign-key line** highlights the column pair on both ends.

## Drift detection

The schema view is always live — there's no cached snapshot. When a column gets renamed or a table dropped remotely, the next visit shows the new shape. Any saved query that referenced the old column gets a yellow warning badge in Studio when you reopen it.

## When to use it

- **Onboarding to an unfamiliar database** — the layout shows you the table neighborhoods faster than any ERD tool.
- **Joining tables you barely know** — hover the foreign-key line to see exactly which column → which column.
- **Auditing model bloat** — when the counter says 89 tables and 254 FKs, it's a good moment to ask whether half of them belong in a separate database.

## What it isn't

- **A migration tool**. Schema doesn't write to the database. Use the Studio editor or a dedicated migration runner (Alembic, Flyway, dbmate) to ship changes.
- **A reverse-engineering doc generator**. The layout is for exploration, not for export to a Confluence page (though you *can* screenshot it for that purpose).

## Related

- [studio.md](studio.md) — the "Query in Studio" button targets this.
- [explore.md](explore.md) — for going deeper into a single table's data, not just its structure.
