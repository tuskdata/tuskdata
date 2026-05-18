"""SSH tunnel manager for connections sitting behind a bastion.

When a `ConnectionConfig` has its `ssh_*` fields populated, the postgres
engine asks this module for a tunneled DSN. We open an asyncssh
connection to the bastion and a local port-forward to the actual
database, then hand the engine a localhost DSN pointing at the local
end of the forward.

**Sharing**: multiple Tusk connections that point at the same bastion
(same `ssh_host:port:user:key`) share **one** asyncssh session — only
the per-target *forwarded port* is opened separately. Before this,
each Tusk connection opened its own SSH session, so a workspace with
five DBs behind one bastion meant five SSH handshakes (~1.5s each)
on first hit. Now it's one handshake plus N cheap port forwards.

Authentication priority when both are set:
    1. ssh_private_key (PEM contents, optionally with passphrase = ssh_password)
    2. ssh_password    (interactive password)

`ssh_known_hosts` is the contents of an `~/.ssh/known_hosts` line for
the bastion; if omitted we use accept-new (Trust on First Use), which
is fine in dev but you should pin it for prod.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tusk.core.logging import get_logger

# Cap the SSH handshake. asyncssh has no default connect timeout, so an
# unreachable bastion (e.g. SG drops your IP) would hang on TCP SYN
# retries for ~127s on Linux. With the global lock held, every other
# tunneled request piles up behind it — that's how the admin page locks
# up with "Loading…" forever.
SSH_CONNECT_TIMEOUT_S = 10.0

# After a connect failure we remember it for this long and fail-fast on
# the next request, so a flood of admin-panel polls doesn't each pay a
# fresh 10s timeout.
SSH_BROKEN_TTL_S = 30.0

if TYPE_CHECKING:
    from tusk.core.connection import ConnectionConfig

log = get_logger("ssh_tunnel")

try:
    import asyncssh
    HAS_ASYNCSSH = True
except ImportError:
    asyncssh = None
    HAS_ASYNCSSH = False


@dataclass
class _Forward:
    """A single local→remote port forward riding on a shared SSH session."""
    listener: object         # asyncssh listener
    local_port: int
    target_host: str
    target_port: int
    refcount: int = 0        # how many connection_ids are using this forward


@dataclass
class _Session:
    """One asyncssh connection. Hosts multiple forwards keyed by target."""
    conn: object             # asyncssh.SSHClientConnection
    fingerprint: str         # SSH-side params signature (host/user/key)
    forwards: dict[str, _Forward] = field(default_factory=dict)
    # connection_id → forward key it's using; lets us decrement on close.
    consumers: dict[str, str] = field(default_factory=dict)


# Sessions keyed by SSH-side fingerprint (host:port:user:keyhash).
_sessions: dict[str, _Session] = {}
_lock = asyncio.Lock()

# fingerprint -> (failed_at_monotonic, last_error_message). Entries older
# than SSH_BROKEN_TTL_S are ignored and cleared lazily.
_broken_sessions: dict[str, tuple[float, str]] = {}


class SSHTunnelUnreachable(RuntimeError):
    """Raised when the bastion is known-unreachable (cached failure)
    or fails to handshake within SSH_CONNECT_TIMEOUT_S.

    Callers can catch this specifically to render a friendlier error
    than a generic asyncssh traceback. It is still a RuntimeError so
    existing `except Exception` paths keep working.
    """


def _broken_hit(fingerprint: str) -> str | None:
    """Return the cached error message if this bastion is in cooldown."""
    entry = _broken_sessions.get(fingerprint)
    if entry is None:
        return None
    failed_at, msg = entry
    if time.monotonic() - failed_at >= SSH_BROKEN_TTL_S:
        _broken_sessions.pop(fingerprint, None)
        return None
    return msg


def _mark_broken(fingerprint: str, message: str) -> None:
    _broken_sessions[fingerprint] = (time.monotonic(), message)


def _clear_broken(fingerprint: str) -> None:
    _broken_sessions.pop(fingerprint, None)


def _ssh_fingerprint(config: "ConnectionConfig") -> str:
    """Stable signature of *just the SSH session* parameters.

    Two Tusk connections that share host/port/user/key share one SSH
    session, even if they target different downstream DB hosts.
    """
    key_material = ""
    if config.ssh_private_key:
        # Hash the key so the fingerprint isn't huge — actual auth still
        # uses the full key. We only need stable equality here.
        key_material = "k:" + hashlib.sha256(
            config.ssh_private_key.encode("utf-8", errors="replace")
        ).hexdigest()[:16]
    elif config.ssh_password:
        key_material = "p:" + hashlib.sha256(
            config.ssh_password.encode("utf-8", errors="replace")
        ).hexdigest()[:16]
    return "|".join((
        config.ssh_host or "",
        str(config.ssh_port or 22),
        config.ssh_user or "",
        key_material,
    ))


def _forward_key(config: "ConnectionConfig") -> str:
    """Stable key for the (target_host, target_port) pair within an SSH session."""
    return f"{config.host or 'localhost'}:{config.port}"


async def get_tunneled_dsn(config: "ConnectionConfig") -> str:
    """Return a DSN ready to hand to psycopg.

    For non-tunneled connections this is just `config.dsn`. For tunneled
    ones we open (or reuse) the SSH session + local forward, and return
    a DSN pointing at the local end of the forward.
    """
    if not config.uses_ssh_tunnel:
        return config.dsn

    if not HAS_ASYNCSSH:
        raise RuntimeError(
            "ssh_tunnel: asyncssh is not installed. "
            "Install with `pip install tuskdata[postgres]` to enable SSH tunnels."
        )

    sess_fp = _ssh_fingerprint(config)
    fwd_key = _forward_key(config)

    # Fast path: bastion is in cooldown after a recent failure. Bail
    # before queuing on the lock so a flood of polls doesn't each wait
    # 10s for the same answer.
    cached_error = _broken_hit(sess_fp)
    if cached_error is not None:
        raise SSHTunnelUnreachable(
            f"ssh_tunnel: bastion {config.ssh_host} marked unreachable "
            f"({cached_error}); will retry in <{int(SSH_BROKEN_TTL_S)}s"
        )

    async with _lock:
        # 1. Get-or-create the SSH session for this bastion.
        session = _sessions.get(sess_fp)
        if session is None:
            # Re-check inside the lock: another coroutine may have just
            # finished a failing attempt while we waited.
            cached_error = _broken_hit(sess_fp)
            if cached_error is not None:
                raise SSHTunnelUnreachable(
                    f"ssh_tunnel: bastion {config.ssh_host} marked unreachable "
                    f"({cached_error})"
                )
            try:
                conn = await _open_session(config)
            except SSHTunnelUnreachable as e:
                _mark_broken(sess_fp, str(e))
                raise
            except Exception as e:
                _mark_broken(sess_fp, str(e) or type(e).__name__)
                raise SSHTunnelUnreachable(
                    f"ssh_tunnel: failed to reach {config.ssh_host}: {e}"
                ) from e
            _clear_broken(sess_fp)
            session = _Session(conn=conn, fingerprint=sess_fp)
            _sessions[sess_fp] = session
            log.info(
                "SSH session opened",
                ssh_host=config.ssh_host, ssh_user=config.ssh_user,
            )

        # 2. Get-or-create the forward for this (target_host, target_port).
        forward = session.forwards.get(fwd_key)
        if forward is None:
            forward = await _open_forward(session.conn, config)
            session.forwards[fwd_key] = forward
            log.info(
                "SSH forward opened",
                target=fwd_key, local_port=forward.local_port,
                shares_session=len(session.consumers) > 0,
            )

        # 3. Bookkeeping: register this connection as a consumer of the
        #    forward, so close_tunnel() can decrement and (eventually)
        #    GC the forward when nobody is using it.
        prev_key = session.consumers.get(config.id)
        if prev_key and prev_key != fwd_key:
            # Connection was using a different forward (config changed).
            # Decrement the old one; it'll be GC'd if nobody else uses it.
            old = session.forwards.get(prev_key)
            if old:
                old.refcount = max(0, old.refcount - 1)
                if old.refcount == 0:
                    await _close_forward(old)
                    session.forwards.pop(prev_key, None)
        if session.consumers.get(config.id) != fwd_key:
            forward.refcount += 1
            session.consumers[config.id] = fwd_key

        return config.local_dsn(forward.local_port)


async def _open_session(config: "ConnectionConfig"):
    """Open a fresh asyncssh connection (no forwards yet)."""
    kwargs: dict = {
        "host": config.ssh_host,
        "port": config.ssh_port,
        "username": config.ssh_user,
    }

    if config.ssh_known_hosts:
        kwargs["known_hosts"] = asyncssh.import_known_hosts(config.ssh_known_hosts)
    else:
        kwargs["known_hosts"] = None

    if config.ssh_private_key:
        try:
            key = asyncssh.import_private_key(
                config.ssh_private_key,
                passphrase=config.ssh_password if config.ssh_password else None,
            )
            kwargs["client_keys"] = [key]
        except Exception as e:
            raise RuntimeError(f"ssh_tunnel: invalid private key: {e}") from e
    elif config.ssh_password:
        kwargs["password"] = config.ssh_password
    else:
        raise RuntimeError(
            "ssh_tunnel: neither ssh_private_key nor ssh_password is set"
        )

    # connect_timeout caps the TCP+handshake phase. asyncssh itself has
    # no default here, so without it a dropped SYN (e.g. bastion SG
    # blocks your IP) hangs ~127s on Linux's tcp_syn_retries.
    kwargs["connect_timeout"] = SSH_CONNECT_TIMEOUT_S

    try:
        return await asyncssh.connect(**kwargs)
    except asyncio.TimeoutError as e:
        raise SSHTunnelUnreachable(
            f"ssh_tunnel: handshake to {config.ssh_host}:{config.ssh_port or 22} "
            f"timed out after {SSH_CONNECT_TIMEOUT_S:.0f}s"
        ) from e


async def _open_forward(conn, config: "ConnectionConfig") -> _Forward:
    """Open a local→remote forward on an existing SSH session."""
    listener = await conn.forward_local_port(
        listen_host="127.0.0.1",
        listen_port=0,
        dest_host=config.host or "localhost",
        dest_port=config.port,
    )
    return _Forward(
        listener=listener,
        local_port=listener.get_port(),
        target_host=config.host or "localhost",
        target_port=config.port,
    )


async def _close_forward(forward: _Forward) -> None:
    try:
        forward.listener.close()
    except Exception:
        pass


async def _close_session(session: _Session) -> None:
    for fwd in list(session.forwards.values()):
        await _close_forward(fwd)
    session.forwards.clear()
    try:
        session.conn.close()
        await session.conn.wait_closed()
    except Exception:
        pass


async def close_tunnel(connection_id: str) -> None:
    """Drop a connection's reference. GC the forward (and session) if idle."""
    async with _lock:
        for sess_fp, session in list(_sessions.items()):
            fwd_key = session.consumers.pop(connection_id, None)
            if not fwd_key:
                continue
            forward = session.forwards.get(fwd_key)
            if forward:
                forward.refcount = max(0, forward.refcount - 1)
                if forward.refcount == 0:
                    await _close_forward(forward)
                    session.forwards.pop(fwd_key, None)
            # If this session no longer hosts any forwards, tear it down.
            if not session.forwards:
                await _close_session(session)
                _sessions.pop(sess_fp, None)


async def close_all_tunnels() -> None:
    """Close every SSH session — call on app shutdown."""
    async with _lock:
        sessions = list(_sessions.values())
        _sessions.clear()
    for s in sessions:
        await _close_session(s)
    if sessions:
        log.info("Closed SSH sessions", count=len(sessions))


async def test_ssh_connection(config: "ConnectionConfig") -> tuple[bool, str]:
    """Quick probe: open the tunnel, close it. Used by the test-connection UI.

    Uses an explicit close-stack so a failure mid-cleanup (e.g.
    `wait_closed()` raising because the SSH child already died) doesn't
    leak the listener or the connection. The previous version had two
    layers of `try/except: pass` around close which masked exactly this
    leak — small surface in practice (the test UI is a manual click)
    but irritating when it happened in tests.
    """
    if not config.uses_ssh_tunnel:
        return True, "no SSH tunnel configured"
    if not HAS_ASYNCSSH:
        return False, "asyncssh is not installed"

    try:
        conn = await _open_session(config)
    except Exception as e:
        # User-initiated probe: surface the underlying error verbatim,
        # but don't pollute the bastion's broken-cooldown cache. The
        # admin polls do their own caching via get_tunneled_dsn.
        return False, f"SSH connect failed: {e}"

    forward = None
    try:
        try:
            forward = await _open_forward(conn, config)
        except Exception as e:
            return False, f"port forward failed: {e}"
        # A successful manual probe clears any stale broken marker so
        # the next admin poll doesn't keep failing for SSH_BROKEN_TTL_S.
        _clear_broken(_ssh_fingerprint(config))
        return True, "SSH + port forward OK"
    finally:
        # Best-effort close of every resource we opened, in reverse
        # order. Each step is independently guarded — failure of one
        # never skips the next.
        if forward is not None:
            try:
                await _close_forward(forward)
            except Exception as e:
                log.debug("test_ssh_connection: forward close failed", error=str(e))
        try:
            conn.close()
        except Exception as e:
            log.debug("test_ssh_connection: conn.close failed", error=str(e))
        try:
            await conn.wait_closed()
        except Exception as e:
            log.debug("test_ssh_connection: wait_closed failed", error=str(e))
