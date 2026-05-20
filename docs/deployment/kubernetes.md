# Deploying Tusk on Kubernetes

Tusk is designed to run as **a single process**. The deployment here is a `StatefulSet` with **one replica** and a persistent volume for `~/.tusk`. This works in production for SMB-scale loads.

> ⚠️ **Do not increase `replicas`.** Tusk's scheduler, sessions, and plugin storage are not yet multi-pod safe. See [ADR 0001](../../specs/architecture/adrs/0001-single-process-by-default.md) for the full reasoning and the list of things that break with `replicas: 2+`. HA support is on the roadmap for 0.9.x — until then, scale vertically (more CPU/RAM per pod), not horizontally.

## Minimal working manifest

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: tuskdata
---
apiVersion: v1
kind: Service
metadata:
  name: tusk
  namespace: tuskdata
spec:
  selector:
    app: tusk
  ports:
    - name: http
      port: 80
      targetPort: 8000
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tusk
  namespace: tuskdata
spec:
  serviceName: tusk
  replicas: 1                # see warning above — do not change
  selector:
    matchLabels:
      app: tusk
  volumeClaimTemplates:
    - metadata:
        name: tusk-home
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
  template:
    metadata:
      labels:
        app: tusk
    spec:
      containers:
        - name: tusk
          # Replace with whatever image you publish (or build from
          # the tuskdata-compose repo).
          image: ghcr.io/tuskdata/tuskdata:0.4.16
          ports:
            - containerPort: 8000
              name: http
          env:
            - name: TUSK_AUTH_MODE
              value: "multi"             # single-user is fine too; multi enables RBAC
            - name: TUSK_LOG_FORMAT
              value: "json"              # structured logs for kubectl logs / Loki / etc.
            - name: TUSK_LOG_LEVEL
              value: "info"
            - name: TUSK_REQUEST_TIMEOUT
              value: "60"                # seconds; default, override if your handlers are slow
            - name: TZ
              value: "UTC"
          volumeMounts:
            - name: tusk-home
              mountPath: /var/lib/tusk
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
          # Probes — see the section below for what these numbers mean.
          livenessProbe:
            httpGet:
              path: /api/health
              port: 8000
            periodSeconds: 30
            timeoutSeconds: 5
            failureThreshold: 3            # ~90s before kubelet restarts the pod
            initialDelaySeconds: 30
          readinessProbe:
            httpGet:
              path: /api/health
              port: 8000
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 3
            initialDelaySeconds: 10
          startupProbe:
            httpGet:
              path: /api/health
              port: 8000
            periodSeconds: 5
            failureThreshold: 12           # allow up to 60s for plugin init on cold start
```

## Why these probe settings

The numbers are tuned to match Tusk's internal resilience knobs:

| Knob | Value | Why |
|---|---|---|
| `RequestTimeoutMiddleware` default | 60s | Per-request budget. Anything longer would have already returned 504 to the caller. |
| Granian `--workers-lifetime` | 3600s | Periodic worker recycle. Bounds slow leaks. |
| `livenessProbe failureThreshold × periodSeconds` | 90s | If `/api/health` is unreachable for ~90s, the pod is wedged at a level the app-layer timeouts didn't catch (frozen event loop, deadlock). Kubelet restarts. |
| `startupProbe failureThreshold × periodSeconds` | 60s | Cold-start budget for plugin init + scheduler boot. Above this we assume something's truly broken. |

If you have heavy workloads (e.g. exports of millions of rows, long-running pg_dumps), increase `TUSK_REQUEST_TIMEOUT` and the liveness `failureThreshold` proportionally — but **also** offload those operations into background jobs (`tusk.core.jobs`) so the HTTP handler returns fast and the work runs out-of-band.

## What goes wrong if you ignore the warning

If you set `replicas: 2+`:

- **APScheduler runs in every replica.** Cron jobs (cleanup, notifications retry, BI embed token cleanup, job watchdog) fire N×. Backups would double-execute.
- **SQLite at `~/.tusk/*.db` is per-pod.** Each replica has its own jobs/sessions/audit DB. Sessions don't migrate across pods.
- **PG connection pools multiply.** Each pod opens its own pool to your Postgres. Easy to blow past `max_connections`.

If you genuinely need HA today, run a single pod with **a robust auto-restart story** (the probes above) and a fast PVC. Real HA support arrives in Tusk 0.9.x — see [`specs/roadmap/later/postgres-meta-and-ha.md`](../../specs/roadmap/later/postgres-meta-and-ha.md).

## Backing up the volume

Tusk's own state lives in the `tusk-home` PVC at `~/.tusk/`:
- `tusk.db` — main app DB (sessions, history, audit, jobs, notifications).
- `plugins/<id>.db` — per-plugin storage.
- `backups/` — pg_dump output files (potentially large; consider a separate PVC if you keep many).
- `downloads/` — fetched data files.

Snapshot the PVC (via your storage class's snapshot feature) before upgrades and on a daily cadence. The DB files use SQLite — `.backup` to a sibling path is the safe way to copy them while Tusk is running.

## Image source

There's no official Tusk image on a public registry yet. Build from the [`tuskdata-compose`](https://github.com/tuskdata/tuskdata-compose) repo:

```bash
git clone git@github.com:tuskdata/tuskdata-compose.git
cd tuskdata-compose
docker build -t my-registry/tuskdata:0.4.16 .
docker push my-registry/tuskdata:0.4.16
```

Replace `my-registry` with whatever registry your cluster pulls from.

## Upgrade path

1. Build the new wheel + push to `tuskdata-compose/wheels/` (see `tuskdata-compose/README.md`).
2. Rebuild + push the image.
3. `kubectl rollout restart statefulset/tusk -n tuskdata` — does a clean stop, kubelet picks up the new image on restart, comes back up with the PVC intact.

Migrations run automatically on boot (see `init_db` in each subsystem). The Job registry marks any `running` job from the previous process as `interrupted` so the UI doesn't show a perpetual spinner.
