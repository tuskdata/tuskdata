# Deploying TuskData with Coolify on a local VM

This walks through a full local test deploy: spin up a VM, install Coolify,
push TuskData from a Git repo, and have it reachable at a local hostname
with a TLS cert.

TuskData is shipped as a single Docker image (see `Dockerfile`) that bundles
the core app plus every first-party plugin wheel (`tusk-cluster`,
`tusk-security`, `tusk-ci`, `tusk-bi`). The only external runtime dep is
PostgreSQL; Coolify can provision one for us.

---

## 0. What you need

- A **local VM** (UTM / Multipass / Orbstack / VirtualBox) running a recent
  Ubuntu / Debian. **4 GB RAM minimum, 8 GB recommended.** Give it a
  routable IP on your LAN or set up host-only networking.
- Docker reachable over the network or a recent kernel the Coolify installer
  can bring up for you.
- A domain you control (or the ability to edit `/etc/hosts` on your laptop
  and point `tusk.local` at the VM).
- The TuskData repo pushed to a Git host (GitHub / GitLab / local Gitea).
  Plain `git init --bare` over SSH works fine too.

---

## 1. Provision the VM

Assuming Ubuntu 24.04 LTS:

```bash
# On the VM
sudo apt update && sudo apt install -y curl git ca-certificates
sudo timedatectl set-timezone UTC
```

Open the firewall for Coolify's UI (8000) and the app (80/443):

```bash
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 8000/tcp   # Coolify dashboard; close once you're done
sudo ufw enable
```

## 2. Install Coolify

The one-liner the project ships with:

```bash
curl -fsSL https://cdn.coollabs.io/coolify/install.sh | sudo bash
```

When it finishes, it prints the admin URL (`http://<vm-ip>:8000`) and a
randomly-generated password. Log in and set up a root admin.

> **Note on Coolify version.** These steps target Coolify v4 (the current
> line as of 2026). The UI paths change from time to time; if something
> labeled differently, the underlying concept — "project → resource →
> Docker image" — is the same.

## 3. Get the TuskData source onto the VM (Git)

In Coolify, create a new **Project** (I called mine `tusk`). Inside it,
**+ New Resource → Public / Private Repository**. Point it at the repo
containing this `Dockerfile` and pick the branch you want to track
(`main`).

For this session:

```
Build Pack          → Dockerfile
Dockerfile location → ./Dockerfile
Ports Exposed       → 8000
Ports Mappings      → 8000
```

## 4. Ship the plugin wheels into the build context

The image expects a `wheels/` directory next to the Dockerfile so it can
install the plugin wheels. Two options:

**Option A — commit the wheels.** Run `make wheels` on your workstation
before pushing; the artifacts end up under `wheels/*.whl`. Git-track them
(they're ~a few MB each). Fast, but the repo grows over time.

**Option B — build them on the VM.** Add a pre-build step in the Dockerfile
(or a Coolify "pre-deployment command") that clones the plugin repos and
runs `python -m build --wheel` inside each one before `docker build`.
More hygienic but needs all four plugin repos reachable from the VM.

For the first smoke test, Option A is fine.

## 5. Configure environment

Still in the TuskData resource, open **Environment Variables** and set:

| Name | Value | Notes |
|---|---|---|
| `HOME` | `/var/lib/tusk` | Where `~/.tusk/` (config, connections, keys) lives |
| `TUSK_LOG_LEVEL` | `info` | `debug` while bringing up; `info` after |
| `TUSK_LOG_FORMAT` | `json` | Coolify's log viewer prefers structured output |
| `TUSK_QUERY_TIMEOUT` | `300` | Seconds |
| `TUSK_CLUSTER_SECRET` | *(generate)* | `openssl rand -hex 32` — only if you run workers |
| `TUSK_CLUSTER_TLS` | `0` | Leave off until you have certs on workers |

## 6. Persistent volumes

Attach a **Volume Mount** for `/var/lib/tusk`. This holds:

- `connections.toml` — *encrypted* database credentials
- `.key` — Fernet key for those credentials (back this up off-host!)
- `workspaces/` — saved pipelines
- `history.db` — query history
- `users.db` — in multi-user mode, user accounts + sessions
- Plugin DBs (`tusk-cluster.db`, `tusk-security.db`, `tusk-bi.db`, `tusk-ci.db`)
- `stats_history.db` — admin panel sparkline points
- `.tusk/backups/` — if you save backups here (large!)

If you lose this volume, **you lose the Fernet key and cannot decrypt
existing connection passwords**. Back it up.

## 7. PostgreSQL sidecar (optional but recommended)

Add a second resource in the project — **+ New Resource → Database →
PostgreSQL**. Coolify provisions a managed PG container with its own
volume. Copy the connection string Coolify shows you and save it inside
Tusk via the Studio UI (`Connections → New → PostgreSQL`). The password
gets encrypted with the Fernet key before writing to disk.

Coolify exposes the DB only on its internal Docker network by default,
so it's not reachable from outside the VM unless you explicitly map a
port. Good.

## 8. Domain + HTTPS

Under **Settings → Domains**, add something like:

- `tusk.local` (and update `/etc/hosts` on your laptop to point at the VM
  IP), or
- a real hostname on a domain whose DNS you control — Coolify runs
  Traefik + Let's Encrypt and will request a cert automatically.

For a purely local test, hosts-file + a self-signed cert is fine. Coolify
has a toggle for "Generate self-signed certificate"; flip it on if you
don't want to deal with real DNS for now.

## 9. Deploy

Hit **Deploy**. Coolify:

1. Pulls the latest commit from your Git remote.
2. Runs `docker build` with the Dockerfile in this repo.
3. Starts the container, wires the volume, opens port 8000 behind Traefik.
4. Runs the healthcheck (`/api/health`) — the container must report
   `"status":"ok"` within `start-period` (20s by default) or Coolify marks
   the deploy failed.

Logs stream in real time under the resource's **Logs** tab.

## 10. Verify

```bash
# From the VM or your laptop (via the Coolify-provided URL)
curl -fsS https://tusk.local/api/health | jq
curl -fsS https://tusk.local/api/metrics | head -20
```

Expected:

- `/api/health` → `{"status":"ok", "deps":{...}}`
- `/api/metrics` → Prometheus text with `tusk_build_info`, etc.

Then open the Studio in a browser: `https://tusk.local/`. You should see
the tab bar with **Studio / Admin / Data / Analytics / CI / Security /
Cluster**. The plugin tabs show up only if the plugin wheels installed
successfully.

## 11. Enable multi-user mode (only if Tusk is reachable from outside
your laptop)

By default Tusk boots in **single-user mode**. The admin endpoints refuse
non-loopback requests automatically (the v0.3.0 guard), but for a real
deploy you want real auth:

```bash
docker compose exec tusk tusk auth enable         # sets auth_mode = multi
docker compose exec tusk tusk users create admin  # prompts for password
docker compose exec tusk tusk users make-admin admin
```

Under Coolify, use the resource's **Terminal** tab to run the same
commands.

---

## Troubleshooting

**Healthcheck fails with `scheduler: down`.** The APScheduler in-process
thread needs ~5s to start. Increase `start-period` in the Dockerfile or
wait — once scheduler is running, status goes from `degraded` → `ok`.

**`connections.toml` has `enc:v1:...` but passwords fail on reload.**
Fernet key (`.key`) is on a different volume than `connections.toml`.
Either mount them together or export the key with
`cat /var/lib/tusk/.tusk/.key` before redeploying.

**PostgreSQL connection errors in the Studio.** The Postgres sidecar and
the Tusk container share the Coolify-managed Docker network; use the
**internal** hostname Coolify shows (something like
`postgres-abc123.internal`), not `localhost`.

**Plugins don't show up as tabs.** The wheel install step probably
errored silently. `docker compose logs tusk | grep -i plugin` should
show what failed; the most common cause is a plugin wheel being missing
from `wheels/` at build time.

**Admin panel redirects me away.** Single-user mode only accepts admin
calls from `127.0.0.1` / `::1`. If you see 401s, either enable
multi-user auth (see above) or `curl` through an SSH tunnel:

```bash
ssh -L 8000:localhost:8000 your-vm
# now browse http://localhost:8000
```

---

## Upgrading

1. On your workstation, bump `pyproject.toml` version, rebuild wheels,
   commit, push.
2. In Coolify, hit **Redeploy**. It re-pulls, rebuilds the image, and
   does a rolling restart with the existing volume mounted — no data
   loss.
3. If the release includes a schema migration (e.g. tusk-security
   migration v4), it runs automatically on the next `on_startup`.
