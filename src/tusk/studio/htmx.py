"""HTMX helpers for Litestar.

Provides utilities for detecting HTMX requests and building
HX-Trigger response headers (e.g. for toast notifications).

Usage in a controller::

    from tusk.studio.htmx import is_htmx, htmx_toast, HtmxHeaders

    @post("/something")
    async def do_thing(self, request: Request) -> Response | dict:
        # ... do work ...
        if is_htmx(request):
            return Response(
                content="",
                status_code=200,
                headers=htmx_toast("Saved!", "success"),
            )
        return {"success": True}
"""

from __future__ import annotations

import json
from typing import Any

from litestar import Request


def is_htmx(request: Request) -> bool:
    """Check if request comes from HTMX (has HX-Request header)."""
    return request.headers.get("HX-Request") == "true"


def is_htmx_boosted(request: Request) -> bool:
    """Check if request is an HTMX boosted navigation."""
    return request.headers.get("HX-Boosted") == "true"


def htmx_trigger(name: str, detail: Any = None) -> dict[str, str]:
    """Create HX-Trigger response header dict.

    Args:
        name: Event name to trigger on the client.
        detail: Optional detail object sent with the event.

    Returns:
        Dict with the HX-Trigger header ready for a Response.
    """
    if detail is not None:
        return {"HX-Trigger": json.dumps({name: detail})}
    return {"HX-Trigger": name}


def htmx_toast(message: str, variant: str = "success") -> dict[str, str]:
    """Convenience: HX-Trigger header that fires a tuskToast on the client.

    Args:
        message: Toast message text.
        variant: One of 'success', 'error', 'warning', 'info'.

    Returns:
        Dict with HX-Trigger header for toast.
    """
    return htmx_trigger("tuskToast", {"message": message, "variant": variant})


def htmx_redirect(url: str) -> dict[str, str]:
    """HX-Redirect header — tells HTMX to do a client-side redirect.

    Args:
        url: Target URL.

    Returns:
        Dict with HX-Redirect header.
    """
    return {"HX-Redirect": url}


def htmx_refresh() -> dict[str, str]:
    """HX-Refresh header — tells HTMX to do a full page refresh."""
    return {"HX-Refresh": "true"}


def htmx_retarget(css_selector: str) -> dict[str, str]:
    """HX-Retarget header — override the target on the client side."""
    return {"HX-Retarget": css_selector}


def merge_htmx_headers(*header_dicts: dict[str, str]) -> dict[str, str]:
    """Merge multiple HTMX header dicts.

    If multiple HX-Trigger dicts are present, they are merged into
    a single JSON object.

    Args:
        *header_dicts: Header dicts to merge.

    Returns:
        Single merged dict.
    """
    merged: dict[str, str] = {}
    triggers: dict[str, Any] = {}

    for hd in header_dicts:
        for key, value in hd.items():
            if key == "HX-Trigger":
                # Try to parse as JSON (multi-event trigger)
                try:
                    obj = json.loads(value)
                    if isinstance(obj, dict):
                        triggers.update(obj)
                    else:
                        triggers[value] = None
                except (json.JSONDecodeError, TypeError):
                    triggers[value] = None
            else:
                merged[key] = value

    if triggers:
        # If all values are None, use simple comma-separated format
        if all(v is None for v in triggers.values()):
            merged["HX-Trigger"] = ", ".join(triggers.keys())
        else:
            merged["HX-Trigger"] = json.dumps(
                {k: v for k, v in triggers.items() if v is not None}
                | {k: "" for k, v in triggers.items() if v is None}
            )

    return merged
