"""SSH tunnel manager for connections sitting behind a bastion.

When a `ConnectionConfig` has its `ssh_*` fields populated, the postgres
engine asks this module for a tunneled DSN. We open an asyncssh
connection to the bastion, set up a port-forward to the actual database,
and hand the engine a localhost DSN pointing at the forwarded port.

Tunnels are reused across queries — one per `ConnectionConfig.id` — and
torn down on app shutdown.

Authentication priority when both are set:
    1. ssh_private_key (PEM contents, optionally with passphrase = ssh_password)
    2. ssh_password    (interactive password)

`ssh_known_hosts` is the contents of an `~/.ssh/known_hosts` line for
the bastion; if omitted we use accept-new (Trust on First Use), which
is fine in dev but you should pin it for prod.
"""

from __future__ import annotations

import asyncio
import io
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tusk.core.logging import get_logger

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
class _Tunnel:
    conn: object               # asyncssh.SSHClientConnection
    listener: object           # asyncssh listener
    local_port: int
    fingerprint: str           # signature of the tunnel params, used to detect config drift


_tunnels: dict[str, _Tunnel] = {}
_lock = asyncio.Lock()


def _fingerprint(config: "ConnectionConfig") -> str:
    """Stable signature of the parameters that define the tunnel.

    If any of these change, the existing tunnel is stale and we rebuild.
    """
    return "|".join(str(x) for x in (
        config.ssh_host, config.ssh_port, config.ssh_user,
        config.host, config.port,
        bool(config.ssh_private_key), bool(config.ssh_password),
    ))


async def get_tunneled_dsn(config: "ConnectionConfig") -> str:
    """Return a DSN ready to hand to psycopg.

    For non-tunneled connections this is just `config.dsn`. For tunneled
    ones we open (or reuse) the SSH forward and return a DSN pointing at
    the local end of the forward.
    """
    if not config.uses_ssh_tunnel:
        return config.dsn

    if not HAS_ASYNCSSH:
        raise RuntimeError(
            "ssh_tunnel: asyncssh is not installed. "
            "Install with `pip install tuskdata[postgres]` to enable SSH tunnels."
        )

    fp = _fingerprint(config)

    async with _lock:
        existing = _tunnels.get(config.id)
        if existing and existing.fingerprint == fp:
            return config.local_dsn(existing.local_port)

        if existing:
            # Stale tunnel (config changed) — close before rebuilding
            await _close(existing)
            _tunnels.pop(config.id, None)

        tunnel = await _open(config, fp)
        _tunnels[config.id] = tunnel
        return config.local_dsn(tunnel.local_port)


async def _open(config: "ConnectionConfig", fingerprint: str) -> _Tunnel:
    """Open a fresh SSH connection + local port forward."""
    kwargs: dict = {
        "host": config.ssh_host,
        "port": config.ssh_port,
        "username": config.ssh_user,
    }

    if config.ssh_known_hosts:
        kwargs["known_hosts"] = asyncssh.import_known_hosts(config.ssh_known_hosts)
    else:
        # Accept-new: don't fail on first connect, but still verify subsequent ones.
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

    log.info(
        "Opening SSH tunnel",
        ssh_host=config.ssh_host,
        ssh_user=config.ssh_user,
        target=f"{config.host}:{config.port}",
    )

    conn = await asyncssh.connect(**kwargs)
    # Bind to any free local port; asyncssh will report which it picked.
    listener = await conn.forward_local_port(
        listen_host="127.0.0.1",
        listen_port=0,
        dest_host=config.host or "localhost",
        dest_port=config.port,
    )
    local_port = listener.get_port()
    log.info(
        "SSH tunnel established",
        connection_id=config.id,
        local_port=local_port,
    )
    return _Tunnel(conn=conn, listener=listener, local_port=local_port, fingerprint=fingerprint)


async def _close(tunnel: _Tunnel) -> None:
    try:
        tunnel.listener.close()
    except Exception:
        pass
    try:
        tunnel.conn.close()
        await tunnel.conn.wait_closed()
    except Exception:
        pass


async def close_tunnel(connection_id: str) -> None:
    """Force-close the tunnel for a connection (e.g. on connection delete)."""
    async with _lock:
        tunnel = _tunnels.pop(connection_id, None)
    if tunnel:
        await _close(tunnel)


async def close_all_tunnels() -> None:
    """Close every open tunnel — call on app shutdown."""
    async with _lock:
        snapshot = list(_tunnels.values())
        _tunnels.clear()
    for tunnel in snapshot:
        await _close(tunnel)
    if snapshot:
        log.info("Closed SSH tunnels", count=len(snapshot))


async def test_ssh_connection(config: "ConnectionConfig") -> tuple[bool, str]:
    """Quick probe: open the tunnel, close it. Used by the test-connection UI."""
    if not config.uses_ssh_tunnel:
        return True, "no SSH tunnel configured"
    if not HAS_ASYNCSSH:
        return False, "asyncssh is not installed"

    try:
        tunnel = await _open(config, _fingerprint(config))
    except Exception as e:
        return False, f"tunnel failed: {e}"

    await _close(tunnel)
    return True, f"tunnel OK (local port would be {tunnel.local_port})"
