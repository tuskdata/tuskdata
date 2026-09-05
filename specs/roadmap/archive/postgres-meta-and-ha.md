# Later: Postgres meta backend + HA on K8s

## Why later

Three reasons this can't be the next thing we do:

1. **No paying customer asking for it yet.** Until someone is blocked on `replicas: 1`, building HA is over-engineering. The SMB focus says: ship features that close deals first, HA when the deal demands it.

2. **It's a 3-month focused refactor.** Externalizing every SQLite-backed subsystem (sessions, jobs, history, audit, plugin storage) to an optional Postgres backend is a large surface change. Doing it during dashboard / contracts / embed work would slow both sides.

3. **0.5.x process-resilience work covers the urgent failure mode.** Last night's SSH-tunnel freeze is solved by Granian worker timeout + K8s liveness probe — single-pod, no HA needed. HA solves a different problem (the whole pod going away). One at a time.

See [ADR 0001](../architecture/adrs/0001-single-process-by-default.md) for the full reasoning and the list of 9 things that break with `replicas: 2+` today.

## Shape when we build it

Opt-in via `TUSK_META_DSN=postgres://...`. When set, Tusk uses that Postgres for **its own metadata only** (sessions, jobs, history, audit, notification rules, plugin storage). The customer's data lives wherever it lives — this is just Tusk's bookkeeping. When unset, SQLite stays the default — zero-friction install for solo users continues to work.

### Phase 1: Externalize state to Postgres (~6-8 weeks)

- `tusk.core.storage` abstraction. Today there's an implicit "open SQLite at `~/.tusk/{name}.db`". Replace with a thin layer that returns either a SQLite or a Postgres-schema-scoped connection, picked by `TUSK_META_DSN`.
- Migrate one subsystem at a time, behind the same interface:
  - sessions
  - jobs registry
  - audit log
  - notification config + subscriptions
  - schedules
  - history (query history, AI conversation memory)
  - plugin storage (one schema per plugin, migrated by the plugin author via the new abstraction)
- Alembic for schema migrations. Single migration file per subsystem.
- Tests run against both backends in CI.

### Phase 2: Scheduler leader election (~1 week)

- `pg_try_advisory_lock($scheduler_lock_id)` at scheduler startup.
- Only the leader runs the APScheduler tick. Other pods short-circuit `scheduler.start()`.
- Heartbeat: leader writes a row every 30s; if the row goes stale (>120s), another pod picks up the lock.
- Failure mode: 30-90s of no scheduled jobs running between leader death and successor takeover. Acceptable.

### Phase 3: Jobs with claim semantics (~1-2 weeks)

```sql
UPDATE jobs SET status='running', claimed_by=$pod_id, started_at=now()
WHERE id = (
    SELECT id FROM jobs
    WHERE status='pending' AND (deadline IS NULL OR deadline > now())
    ORDER BY priority DESC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *
```

Atomic claim with `FOR UPDATE SKIP LOCKED`. Any pod can be a worker. The watchdog from 0.5.x (job max-duration) just gets a Postgres `UPDATE jobs SET status='failed_timeout' WHERE deadline < now() AND status='running'` instead of an in-process timer.

### Phase 4: Plugin storage migration (~2-3 weeks per plugin)

Per plugin: define schema, write migration that copies SQLite → Postgres for existing installs. tusk-bi first (biggest plugin), tusk-cluster second.

### Phase 5: Docs + reference K8s setup (~1 week)

- `docs/deployment/kubernetes-ha.md` with a working manifest: 3 replicas, Postgres dependency, leader-election story, probe config.
- Helm chart in a separate repo `tuskdata-charts`.

## Total effort

~3 months solo. Acceleratable to 6-8 weeks with a second dev.

## When this becomes "Next"

Any one of:

- A paying customer concretely asks for `replicas: 2+` and the deal is contingent on it.
- We launch a Cloud-Managed offering ourselves (we become the customer that wants HA).
- A new subsystem genuinely needs cross-pod coordination (e.g. CDC consumers sharing a logical replication slot across replicas).

Until then: ADR 0001 is the answer.

## Non-goals

- **We are NOT adding Redis as part of HA.** Postgres advisory locks cover leader election; Postgres `LISTEN/NOTIFY` covers cross-pod fanout if we ever need it.
- **We are NOT requiring Kubernetes.** A two-VM HA setup with a load balancer in front and a shared Postgres should work identically.
- **We are NOT making Postgres mandatory for new installs.** SQLite default stays — the zero-friction install story is sacred.
