"""URL safety helpers for outbound HTTP.

Outbound HTTP from server-side code is an SSRF surface: a user-supplied
URL pointed at `169.254.169.254` (cloud metadata), `127.0.0.1`,
`10.0.0.0/8`, etc. lets an attacker pivot through Tusk into the
internal network.

`validate_outbound_url` rejects:
- non-http/https schemes
- hosts that resolve to private, loopback, link-local, multicast,
  or reserved IP ranges
- redirects to those targets (caller is responsible for re-checking
  on each redirect hop)

Set `TUSK_ALLOW_PRIVATE_WEBHOOKS=1` to skip the check (useful in dev
with a localhost webhook listener — never set in production).
"""

from __future__ import annotations

import ipaddress
import os
import socket
from urllib.parse import urlparse

_ALLOWED_SCHEMES = {"http", "https"}


class UnsafeURL(ValueError):
    """The URL was rejected as unsafe for server-side fetch."""


def _resolve(host: str) -> list[ipaddress._BaseAddress]:
    """Resolve a host to its IPs. Returns [] if it can't be resolved."""
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return []
    out = []
    for info in infos:
        addr = info[4][0]
        try:
            out.append(ipaddress.ip_address(addr))
        except ValueError:
            continue
    return out


def validate_outbound_url(url: str) -> None:
    """Raise `UnsafeURL` if the URL would let an attacker hit internals.

    Override with `TUSK_ALLOW_PRIVATE_WEBHOOKS=1` (dev only).
    """
    if os.environ.get("TUSK_ALLOW_PRIVATE_WEBHOOKS") == "1":
        return

    if not url:
        raise UnsafeURL("empty URL")

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise UnsafeURL(f"scheme {scheme!r} not allowed (use http or https)")

    host = parsed.hostname
    if not host:
        raise UnsafeURL("URL has no host")

    # If the host is already a literal IP, just check it; don't resolve.
    try:
        addr = ipaddress.ip_address(host)
        ips = [addr]
    except ValueError:
        ips = _resolve(host)
        if not ips:
            raise UnsafeURL(f"host {host!r} did not resolve")

    for ip in ips:
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            raise UnsafeURL(
                f"host {host!r} resolves to {ip} which is in a private/loopback/reserved range"
            )
