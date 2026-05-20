# Later: Postgres CDC + live materialized views

## Why later

High differentiator (nobody self-hosted single-binary has this) but **highest operational risk** of any feature on the roadmap:

- Replication slots managed badly **tumble the customer's prod DB** (WAL fills the disk, replication backs up, every write stalls).
- Needs a "read-only sandbox" mode before any user can flip it on against a real connection.
- Failure modes are diverse: slot dropped, WAL retention exceeded, network partition, slow consumer. Each needs explicit handling.

Risk-adjusted, this is 6-8 weeks of focused work — too much for 0.5-0.7.x while we're also doing the engineering hygiene rebuild + 4 other features.

## Shape when we build it

Per tunneled or direct PG connection: opt-in CDC subscription. Creates a logical replication slot (`wal2json` or `pgoutput`), Tusk subscribes, decoded changes flow into:

- A "Live" badge on the dashboard auto-refreshes per change (push, not poll).
- An `events` table in Tusk-managed DuckDB that any dashboard widget can query.
- An optional webhook firehose for downstream consumers.

Operational safeguards:
- Slot creation requires admin acknowledgement of WAL impact.
- Disk-usage monitor; auto-detach slot if storage drops below threshold.
- "Pause" button visible per slot.

## When this becomes "Next"

- We have ≥3 SMB users actively asking for real-time dashboards.
- Test coverage is at 60%+ globally — we need the safety net before touching replication slots.
- Someone (likely a co-founder) is available to handle the on-call paging that follows.
