"""Tusk Embed Client — standalone helper for Django/Flask apps.

This module has NO Tusk dependencies. Copy it into your project or
install tusk-bi and import directly:

    from tusk.bi.embed_client import TuskEmbed

Usage:
    tusk = TuskEmbed("http://localhost:8000", "your-embed-secret")
    url = tusk.get_dashboard_url(1, rls_clauses={"company_id": "42"})
    html = tusk.iframe_html(1, rls_clauses={"company_id": "42"}, height="600px")
"""

from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError


class TuskEmbed:
    """Client for generating Tusk BI embed URLs."""

    def __init__(self, base_url: str, embed_secret: str, timeout: int = 10):
        """Initialize the embed client.

        Args:
            base_url: Tusk Studio base URL (e.g. "http://localhost:8000")
            embed_secret: The TUSK_EMBED_SECRET or embed_secret from config
            timeout: HTTP request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.embed_secret = embed_secret
        self.timeout = timeout

    def get_dashboard_url(
        self,
        dashboard_id: int,
        rls_clauses: dict[str, str] | None = None,
        expires_in: int = 3600,
        app_id: str = "",
    ) -> str:
        """Request an embed token from Tusk and return the iframe URL.

        Args:
            dashboard_id: ID of the dashboard to embed
            rls_clauses: Row-level security filters {column: value}
            expires_in: Token lifetime in seconds (default 1 hour)
            app_id: Optional identifier for the calling app

        Returns:
            Full embed URL ready for iframe src

        Raises:
            RuntimeError: If the token request fails
        """
        payload = json.dumps({
            "dashboard_id": dashboard_id,
            "rls_clauses": rls_clauses or {},
            "expires_in_seconds": expires_in,
            "app_id": app_id,
        }).encode()

        req = Request(
            f"{self.base_url}/api/embed/token",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-Embed-Key": self.embed_secret,
            },
            method="POST",
        )

        try:
            with urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read())
                return data["embed_url"]
        except URLError as e:
            raise RuntimeError(f"Failed to get embed token: {e}") from e
        except (KeyError, json.JSONDecodeError) as e:
            raise RuntimeError(f"Invalid response from Tusk: {e}") from e

    def iframe_html(
        self,
        dashboard_id: int,
        rls_clauses: dict[str, str] | None = None,
        expires_in: int = 3600,
        app_id: str = "",
        height: str = "600px",
        width: str = "100%",
        css_class: str = "",
        allow: str = "fullscreen",
    ) -> str:
        """Return a complete <iframe> HTML tag for embedding a dashboard.

        Args:
            dashboard_id: ID of the dashboard to embed
            rls_clauses: Row-level security filters {column: value}
            expires_in: Token lifetime in seconds
            app_id: Optional identifier for the calling app
            height: iframe height CSS value
            width: iframe width CSS value
            css_class: Optional CSS class for the iframe
            allow: iframe allow attribute

        Returns:
            HTML string with <iframe> tag
        """
        url = self.get_dashboard_url(
            dashboard_id,
            rls_clauses=rls_clauses,
            expires_in=expires_in,
            app_id=app_id,
        )
        class_attr = f' class="{css_class}"' if css_class else ""
        return (
            f'<iframe src="{url}" '
            f'width="{width}" height="{height}" '
            f'frameborder="0" allow="{allow}"'
            f'{class_attr}></iframe>'
        )
