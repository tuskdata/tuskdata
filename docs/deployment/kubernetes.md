# Deploying Tusk on Kubernetes

Tusk runs as **one process**: the web server, the scheduler, background
jobs and the metadata store (`~/.tusk/tusk.db`) live in the same pod. The
deployment is therefore a `StatefulSet` with **one replica** and a
persistent volume. That is a design decision, not a gap —
[ADR 0001](https://github.com/tuskdata/tuskdata/blob/main/specs/architecture/adrs/0001-single-process-by-default.md)
explains why and what breaks with `replicas: 2`. Scale vertically.

## Install

The manifest lives in the repository:
[`deploy/k8s/tusk.yaml`](https://github.com/tuskdata/tuskdata/blob/main/deploy/k8s/tusk.yaml)
— Namespace, Service, StatefulSet with a 10 Gi volume claim, probes, and an
optional Traefik Ingress (k3s default).

```bash
kubectl apply -f https://raw.githubusercontent.com/tuskdata/tuskdata/main/deploy/k8s/tusk.yaml
kubectl -n tuskdata rollout status statefulset/tusk
kubectl -n tuskdata port-forward svc/tusk 8000:80     # http://127.0.0.1:8000
```

The image is `ghcr.io/tuskdata/tuskdata:<version>`, published by the
release workflow on every tag (`:latest` follows the newest release). It
runs as uid 1000 with `HOME=/var/lib/tusk`, which is where the volume is
mounted; the entrypoint fixes ownership on first start.

## Configuration

Everything Tusk needs from the environment:

| Variable | Default | What it does |
|---|---|---|
| `TUSK_AUTH_MODE` | `single` | `multi` turns on logins, groups, API tokens and the audit log. Then run `kubectl -n tuskdata exec statefulset/tusk -- tusk auth init` once to create the admin user. |
| `TUSK_PG_BIN_PATH` | (image's `pg_dump`) | Directory with PostgreSQL client binaries, if you mount your own. |
| `TUSK_LOG_FORMAT` | `text` | `json` for Loki / CloudWatch / Datadog. |
| `TUSK_LOG_LEVEL` | `info` | |
| `TUSK_REQUEST_TIMEOUT` | `60` | Seconds per HTTP request before a 504. Raise for slow exports; long work belongs in scheduled jobs anyway. |
| `TUSK_PORT` | `8000` | |
| `TUSK_AI_NUM_CTX` | `16384` | Context window requested from Ollama. |
| `TUSK_CDN` | auto | `1` loads frontend libraries from CDNs (the published image has no vendored copies, so this is effectively on). |
| `TZ` | `UTC` | Cron schedules are interpreted in this zone. |

Database connections, saved queries and everything else are configured in
the UI and stored on the volume.

## The volume

`/var/lib/tusk/.tusk/` holds:

- `tusk.db` — users, sessions, tokens, audit, history, saved queries, AI
  memory, notifications, jobs and runs, schema snapshots, contracts, admin
  stats. One SQLite file; copy it with `sqlite3 tusk.db ".backup out.db"`
  while Tusk is running.
- `config.toml`, `connections.toml` — settings and connections.
- `plugins/` — per-plugin SQLite (`tusk_bi.db` for Analytics).
- `backups/`, `scheduled_results/`, `workspaces/`, `schema_layouts/`.

Snapshot the PVC (your storage class's snapshot feature) before upgrades
and on a daily cadence. `backups/` can grow: give it its own volume if you
keep many `pg_dump` files.

## Probes

The numbers in the manifest match the app's own timeouts:

| Probe | Setting | Why |
|---|---|---|
| `startupProbe` | 12 × 5 s | Cold start (plugin init, scheduler) is well under a minute. |
| `readinessProbe` | 3 × 10 s | Stops routing while a worker recycles (`--workers-lifetime 3600`). |
| `livenessProbe` | 3 × 30 s | `/api/health` silent for ~90 s means a hung event loop; the request timeout (60 s) would already have fired for a merely slow request. |

## Upgrade

```bash
kubectl -n tuskdata set image statefulset/tusk tusk=ghcr.io/tuskdata/tuskdata:0.4.41
kubectl -n tuskdata rollout status statefulset/tusk
```

Schema migrations run on boot. Jobs that were running when the old pod
stopped are marked `interrupted`, not left spinning.

## What is deliberately not here

- **`replicas: 2+`.** Two schedulers would run every job twice and each
  pod would have its own `tusk.db`. Not supported, not planned.
- **A Helm chart.** One manifest is easier to read and to patch with
  `kustomize`; a chart can come if someone asks.
