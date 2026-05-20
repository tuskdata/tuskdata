# Bug: Network blip leaves stale PG pool + SSH tunnel forever

- **Reported**: 2026-04-22 (lived experience — Wi-Fi flap / VPN reconnect / remote DB reboot)
- **Versions affected**: 0.4.0–0.4.7.1
- **Version that fixes**: 0.4.7.2
- **Severity**: high (made Tusk unusable until container restart, which is impossible on remote deploys you can't `docker compose restart` from a phone)

## Symptom

When the network dropped between Tusk and a remote Postgres (Wi-Fi flap, VPN reconnect, EC2 reboot), every subsequent query failed with one of:

- `OperationalError: server closed the connection unexpectedly`
- `EOFError`
- `SSL SYSCALL error: EOF detected`
- `BrokenPipeError`

Tusk kept handing out stale `psycopg` pool connections + the stale asyncssh tunnel forever. The only workaround was restarting the container — which is useless if Tusk is deployed on a remote host (Coolify, EC2) you can't reach when your own laptop has Wi-Fi issues.

Plain SSH from the same machine to the bastion worked fine — only Tusk's cached handles were dead.

## Root cause

Two layers of state caching that didn't have an eviction path on transient failure:

1. **`psycopg` connection pool.** When a connection died mid-flight, psycopg surfaced the error but the dead connection stayed in the pool. The next checkout could grab the same dead handle (or another one that died at the same time).

2. **`asyncssh` tunnel session.** The tunnel registry in `core/ssh_tunnel.py` kept the `SSHClientConnection` cached by fingerprint forever. A dropped TCP connection didn't trigger a re-handshake — the next query used the same dead session.

Both caches were write-once on first use. Nothing in the read path checked liveness.

## Fix

Two paths, automatic + manual:

### Automatic
`engines/postgres.execute_query` now wraps its body in a retry that catches the transient-error set (`OperationalError`, server-closed, EOF, SSL syscall, broken pipe). On first failure it:

1. Calls a new `postgres._reset_connection(config)` helper that closes the pool entry **and** drops the tunnel cache entry.
2. Runs the query once more, silently.

This makes the **first** post-blip query succeed transparently from the user's POV. If the second attempt also fails, the error surfaces.

### Manual
For cases where auto-retry shouldn't fire (e.g. user wants to force a fresh handshake, or the failure pattern isn't in the transient set):

- New `POST /api/connections/{id}/reconnect` endpoint.
- A ♻ button on every connection row in the Studio sidebar.

Both share the same `_reset_connection(config)` helper, so there's one path that knows how to evict cached state.

## Lessons

1. **Any process-level cache that holds network state needs an eviction path.** Either liveness-check on read, or a "you should consider this stale" trigger. Apply this to every cache we add — auth sessions, tunnel sessions, plugin DB connections, AI provider clients.

2. **Transient error sets are well-known.** For psycopg specifically, the canonical list is documented (`OperationalError` subclasses, plus SSL syscall and broken-pipe). Codify it as a constant `_TRANSIENT_ERRORS` and reuse across engines.

3. **Surfaces that auto-retry must also offer a manual force-retry** for the user. Auto-retry is invisible; users want a button when they suspect something is wrong but the auto path hasn't been triggered.

4. **Remote-deploy scenarios are different.** Restart-the-container is not always available. Every "could be fixed by restart" bug must have a UI affordance to fix it without restart.

## Tests added

- Manual test path: mock psycopg to raise `OperationalError` once, assert the second call succeeds.
- TODO when CI has a real Postgres: kill the connection mid-query, assert recovery.
