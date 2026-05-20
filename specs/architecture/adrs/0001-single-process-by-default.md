# ADR 0001: Single-process by default; HA via Postgres meta later

- **Status**: Accepted
- **Date**: 2026-05-20
- **Owner**: jeasoft

## Context

Tusk runs as a single Granian process. Every subsystem — Litestar request handlers, APScheduler cron jobs, the daemon-thread background job runner in `tusk.core.jobs`, AI Copilot calls, SSH tunnel manager, plugin host — lives inside that one process. State is persisted to SQLite under `~/.tusk/` (sessions, jobs, history, audit, plugin DBs).

This question keeps coming up: should we add a broker (Redis / RabbitMQ), split workers into a separate container, add leader election? Right now the answer is "no" but the reasoning needs to be written down so we don't accidentally re-litigate it every 3 months.

## Decision

**Tusk ships as a single-process app for 0.x. HA support comes later, via an optional Postgres "meta" backend, not via brokers.**

Concretely:

1. **No Redis, Celery, Dramatiq, or any broker** is required to run Tusk. Adding one is allowed only when a feature genuinely needs cross-pod pub/sub or hot cache shared across replicas. Until then, in-process is faster and simpler.

2. **One container, one process, one log stream** is the deployment story for 0.x. Coolify shows one service. K8s users deploy one StatefulSet with one replica + a PVC. Docker Compose users get a single `tuskdata` container.

3. **HA arrives in 0.9.x-1.0.x** via an optional `TUSK_META_DSN=postgres://...` environment variable. When set, Tusk routes its own metadata (sessions, jobs, history, audit, plugin storage) to that Postgres instead of SQLite. Without it, SQLite stays the default for solo + SMB deployments.

4. **Leader election uses Postgres advisory locks**, not Zookeeper / etcd / Consul / Raft. The first pod to grab `pg_try_advisory_lock(...)` runs the scheduler; the rest pass. When the leader dies, another grabs it at the next attempt. Cheap, no new deps, good enough for SMB→mid-market HA.

## Why not Redis / a broker?

Three reasons:

1. **It doesn't solve a problem we have at SMB scale.** APScheduler with a SQLite job store + the daemon-thread job runner handles hundreds of jobs/day with no contention. The bottleneck for SMB is not job throughput, it's database query time and dashboard render time.

2. **It's operational tax.** Every customer who self-hosts now has to deploy Redis too. Backups, upgrades, monitoring, secrets — multiplied. The whole pitch is "single container, easy install". Adding Redis kills that.

3. **The cases that *do* want Redis (cross-pod pub/sub, distributed cache) are exactly the HA cases, and HA is gated on the bigger refactor anyway.** No point adding Redis incrementally before the refactor that uses it lands.

## Why not "always Postgres for everything from day one"?

That's the inverse trap: requiring Postgres at install time would lose the "I just want to try Tusk" zero-friction install. The SQLite default keeps single-pod simple. Postgres meta is opt-in for HA.

## What multi-replica breaks (so we don't forget)

If a user deploys Tusk today with `replicas: 2+` in K8s, the following break. They are blockers for HA, not edge cases:

1. **APScheduler runs in every replica.** Cron jobs (cleanup expired sessions, retry failed notifications, cleanup temp files, BI embed token cleanup) fire N× — backups double-execute, notifications double-send.

2. **SQLite isn't multi-writer-safe across pods.** Shared volume → lock contention + corruption risk. Separate volumes → split-brain (session created on pod A invisible on pod B).

3. **Auth sessions** read from local SQLite. Login on pod A, next request hits pod B, user is logged out.

4. **PG connection pools** are per-process. N pods → N× the connections to the customer's PG. Easy to blow past `max_connections`.

5. **SSH tunnel cache** is per-process. N pods → N tunnel handshakes per bastion.

6. **Schema cache** is per-process. Refresh × N pods.

7. **Plugin DBs** (`~/.tusk/plugins/<id>.db`) — same as (2).

8. **Jobs registry** has no claim semantics. Multiple workers polling would race to run the same job.

9. **Plugin asset extraction at boot** writes into the venv. Idempotent in practice, but technically a race.

The HA roadmap (`roadmap/later/postgres-meta-and-ha.md`, to be written) fixes (1)-(8). (9) becomes irrelevant once the venv is read-only / mounted-shared.

## Process resilience ≠ HA

A related-but-distinct concern is **single-pod resilience**: what happens when our one process hangs or runs out of headroom. That's where last night's SSH-tunnel freeze hurt — the pod didn't crash, it just stopped responding. K8s wouldn't have caught it without a properly-tuned liveness probe; Granian wouldn't have recycled the worker without `--worker-timeout`.

Resilience knobs we're committing to in 0.5.x (see `roadmap/now.md`):

- **Granian worker timeout + recycle.** If a worker doesn't return for N seconds, kill it. Recycle workers every M requests to bound any slow leaks.
- **Request-level timeout middleware.** Any handler that doesn't return inside the budget → 504.
- **Job max-duration watchdog.** Background jobs in `tusk.core.jobs` carry a deadline; the watchdog kills them past it and marks them failed.
- **Documented K8s probe config.** Recommended liveness + readiness in `docs/deployment/kubernetes.md` so users who do run K8s get auto-restart on hang.

None of these need HA infrastructure. All of them help even with `replicas: 1`.

## When this ADR gets revisited

- A paying customer concretely asks for `replicas: 2+` and is blocked.
- We start running a Cloud-Managed offering ourselves (then we are the customer that wants HA).
- A subsystem we add genuinely needs cross-pod pub/sub (e.g. real-time CDC consumers consuming the same Postgres replication slot across pods).

Until any of those: stay single-process, focus on resilience, ship features.

## Consequences

**Positive:**
- Deployment story stays "one container, run it" — friction-free for SMB.
- No broker = no new ops burden, no version-pin matrix.
- All state inspectable via `sqlite3 ~/.tusk/*.db` — debuggable on a laptop.
- Aligns with the [Background Jobs philosophy](https://brandur.org/) of "the database is your queue".

**Negative:**
- Real HA needs the 3-month Postgres-meta refactor before we can sell it. Customers who ask for it before 0.9.x get told "not yet, here's the resilient single-pod config".
- Long-running jobs occupy a worker thread; without the resilience work (P1 in 0.5.x) a stuck job degrades the whole UI.

**Neutral:**
- tusk-cluster is orthogonal — it distributes *query* execution across DataFusion workers, not the *app* across replicas. Both can exist together once HA lands.
