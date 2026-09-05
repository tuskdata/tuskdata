# Later: tusk-cluster — distributed query plugin upgrades

## Status quo

`tusk-cluster` is a plugin (currently ~2k LOC) that adds distributed-query support to Tusk via **DataFusion + Apache Arrow Flight**. The user pushes SQL/dataframe ops; the plugin schedules them across one or more worker nodes; results flow back over Flight. It exists today, ships in v0.2.4, and works for the basic case (one coordinator, N workers, parallel scans on Parquet / Arrow data).

The decision in the 2026-05-18 audit was to **keep it as a plugin** (not promote to core) and improve it later. This spec is the "later" — what improvements would look like.

## Why later (and not now)

Two reasons it's not the most urgent thing:

1. **Solves a different problem than HA / resilience / SMB features.** tusk-cluster scales **query throughput**, not app availability and not the buying decision for the typical SMB. The SMB customer asks "can I see my dashboard?", not "can you join my 50M row table in 2s?".

2. **Predicates on HA being done.** A distributed query coordinator that itself isn't HA is fragile — if the coordinator pod dies, all in-flight queries die. So tusk-cluster's robust mode is downstream of the Phase 1-3 HA work in [postgres-meta-and-ha.md](postgres-meta-and-ha.md).

So this is a 1.x feature. Worth thinking about now so the dashboard / contracts / embed work doesn't make architectural choices that would block it later.

## What "mejorar jevi" actually means

Five upgrade dimensions, ranked by impact. Pick at most 2-3 per release when we get there:

### 1. Worker discovery + autoscaling

Today: workers are configured statically in the cluster config (`workers: [host1:port, host2:port, ...]`). You declare them up front.

Future: workers register dynamically on startup via a control-plane endpoint (or a Kubernetes selector). Coordinator maintains a heartbeat list. Dead workers drop out automatically; new ones join without coordinator restart.

Why this matters: lets you scale workers in K8s with `kubectl scale`. Today you have to edit config + restart.

### 2. Adaptive query planning

Today: the planner picks a fixed split count based on file size or a constant. If a worker is slow, the whole query is bottlenecked.

Future: planner observes per-worker latency over time, weights split assignment, and reschedules stragglers to faster workers. Borrow from Presto / Trino's "speculative execution" idea: if one worker is significantly slower than the median for the same shard, run the work on a second worker and take whichever finishes first.

### 3. More backends beyond Parquet / Arrow

Today: tusk-cluster handles Parquet + Arrow Flight nicely. Iceberg, Delta, and direct Postgres pushdown are limited.

Future: first-class **Apache Iceberg** read/write (this is the big one — 2026 trend, all the enterprise lakehouse vendors are converging here). **Delta Lake** read support. **Postgres pushdown** (the coordinator translates the cluster-level plan into a per-worker `COPY (SELECT ...) TO STDOUT` and reassembles).

This is where tusk-cluster goes from "neat trick" to "I can replace dbt + a small cluster".

### 4. Coordinator HA via the Postgres meta backend

Today: single coordinator process. If it dies mid-query, the query dies.

Future: any pod can be coordinator; the active one holds the `pg_try_advisory_lock(cluster_coordinator_lock_id)`. Query state lives in Postgres (or a small embedded store with replication). New coordinator picks up where the old one left off for in-flight queries that hadn't yet committed results.

Predicated on the HA work landing first.

### 5. Embedded UI for cluster ops

Today: cluster monitoring lives in the existing `studio/routes/cluster.py` (524 LOC, 0% test coverage per the audit). It's basic — shows workers, queries, basic stats.

Future: per-query timeline view (gantt of which shard ran where, for how long, on which worker). Slow-stage detection. Per-worker resource graphs. A "rerun this query in single-node mode for debugging" button.

## What we explicitly don't try to build

- **Our own query engine.** DataFusion is well-funded and getting better fast. We compose with it, we don't replace it.
- **Multi-master coordinator.** Single active coordinator with failover via advisory lock is enough. CRDTs / consensus protocols (Raft, Paxos) are overkill for our scale.
- **Heterogeneous compute (GPU, FPGA, etc.).** Maybe in 5 years. Not now.

## Sequencing inside a future "tusk-cluster 0.3.x → 1.x" arc

| Plugin version | Theme | Trigger to start |
|---|---|---|
| 0.3.0 | Worker discovery + autoscale | First customer with >2 workers asking |
| 0.4.0 | Iceberg + Delta backends | First customer using a lakehouse |
| 0.5.0 | Coordinator HA (needs Tusk core HA) | After Tusk 0.9.x |
| 0.6.0 | Adaptive planning + stragglers | When we have telemetry to drive it |
| 0.7.0 | Embedded ops UI | When we have ≥3 customers actively using it |

## When tusk-cluster moves from "later" to "now"

Same triggers as elsewhere: a real customer asks, the feature unblocks a deal, or we have telemetry showing existing users hitting the wall. Not before.

## Relation to the rest of the roadmap

- **HA (Postgres meta)** is the prereq for coordinator HA inside tusk-cluster. Don't start cluster HA before that.
- **Semantic layer** lives outside the cluster; cluster just executes the SQL the semantic layer emits.
- **Data contracts** apply across cluster too — if a shard's schema drifts, the contract violation fires the same way.
- **Embedded SDK** doesn't care if the underlying query ran on 1 or 50 workers — totally orthogonal.
