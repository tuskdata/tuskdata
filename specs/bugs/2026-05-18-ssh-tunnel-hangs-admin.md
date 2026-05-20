# Bug: SSH tunnel hang freezes the Admin page for ~2 minutes

- **Reported**: 2026-05-17 (user: changed networks, bastion SG didn't have new IP, Admin page sat at "Loading…" on every panel)
- **Versions affected**: 0.4.8.3 (when tunneled backups landed) through 0.4.11
- **Version that fixes**: 0.4.12
- **Severity**: high (made Admin unusable any time the bastion was unreachable)

## Symptom

After changing networks (Wi-Fi switch / IP change), the bastion's Security Group no longer allowed inbound SSH from the user's new IP. Loading `/admin` resulted in:

- Every panel showing "Loading…" indefinitely (Active Processes, Lock Monitor, Table Maintenance, Extensions, Database Settings, Scheduled Tasks, Roles & Users).
- No error toast, no banner, no console error.
- Tusk's APScheduler kept running its periodic jobs — process was alive.
- Eventually the requests would time out after ~127s (Linux's TCP SYN retry budget).

Logs showed `asyncssh: Opening SSH connection to <bastion>, port 22` followed by silence.

## Root cause

Two interacting issues:

### 1. `asyncssh.connect` had no `connect_timeout`
`core/ssh_tunnel.py:_open_session` called `asyncssh.connect(**kwargs)` with no timeout. When the bastion's SG drops the SYN packet, the kernel retries with exponential backoff for ~127s on Linux's default `tcp_syn_retries=6`. The handshake just sat.

### 2. The global `_lock` was held for the entire session-open
`get_tunneled_dsn` held an `asyncio.Lock` while calling `_open_session`. That meant **every other request needing a tunnel** (any admin panel polling, any query on a tunneled connection) queued behind the hanging request.

The Admin page kicks off ~8 HTMX requests in parallel on load — every panel. The first one queued, the others piled up. With a single Granian worker (default), the queue starved every other request to the studio. The whole UI looked dead.

## Fix

Three layered changes in `core/ssh_tunnel.py`:

1. **`SSH_CONNECT_TIMEOUT_S = 10.0`** — passed to `asyncssh.connect(connect_timeout=...)`. asyncssh internally `asyncio.wait_for`s the underlying connect. After 10s we raise `SSHTunnelUnreachable`.

2. **Broken-session cache with TTL 30s.** When a fingerprint's connect fails, we record it in `_broken_sessions[fp] = (timestamp, msg)`. Subsequent `get_tunneled_dsn` calls for the same bastion check the cache **before** queuing on the lock and raise immediately. So a flood of admin polls costs one 10s probe, not eight.

3. **`test_ssh_connection` clears the cooldown on success** so the user clicking the Test button to verify a fixed SG immediately unblocks subsequent admin polls.

In parallel, the admin route handlers got `_admin_error(request, message)` so each panel renders a red banner with the cause ("ssh_tunnel: bastion 1.2.3.4 marked unreachable") on HTMX failures, instead of leaving the spinner running forever.

## Lessons

1. **Every network call needs an explicit timeout.** asyncssh, asyncpg, httpx — none of them set sane defaults out of the box. Codify a 10s default in a constants module and import it.

2. **Don't hold a global lock across a network call.** If a cache write needs serialization, do it with a per-key lock. The pattern `async with _lock: do_slow_io()` is almost always wrong. (Per-bastion locking deferred to a future change — the broken-session cache covers the common case.)

3. **An infinite "Loading…" is a UX bug regardless of the backend cause.** Every HTMX poll endpoint must convert backend errors into a render-able error state. Generic 500 → "server error" banner. SSH timeout → "bastion unreachable" banner. Never leave the spinner on.

4. **Tail latency on background polls degrades the foreground too** when worker count is low. Either run more workers or fail-fast aggressively. We chose fail-fast.

## Tests added

- `tests/test_ssh_tunnel.py::test_unreachable_bastion_fails_fast_then_caches` — asserts attempt 1 fails in ≤10s and attempt 2 in <100ms.
- Smoke test still pending: would need a fake bastion that blackholes SYN. **TODO** in CI once we have docker compose for tests.
