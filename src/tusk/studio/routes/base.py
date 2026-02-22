"""Base controller class for Tusk routes.

MiniJinja doesn't support template globals like Jinja2.
All controllers must use get_base_context() or inherit from TuskController
to get consistent context (features, plugin_tabs, etc.)

Example:
    from tusk.studio.routes.base import TuskController

    class MyController(TuskController):
        path = "/my-page"

        @get("/")
        async def my_page(self) -> Template:
            return self.render(
                "my_page.html",
                active_page="my-page",
                my_data=some_data,
            )
"""

import os

from litestar import Controller, Request
from litestar.response import Template, Response

import tusk
from tusk.core.deps import get_available_features
from tusk.plugins.registry import get_plugin_tabs
from tusk.studio.htmx import is_htmx


def _use_cdn() -> bool:
    """Check if CDN mode is enabled.

    Enabled when TUSK_CDN=1 env var is set, or when vendor files are not present.
    This avoids 404 errors in development when vendor.sh hasn't been run.
    """
    env_val = os.environ.get("TUSK_CDN", "").lower()
    if env_val in ("0", "false", "no"):
        return False
    if env_val in ("1", "true", "yes"):
        return True
    # Auto-detect: use CDN if vendor files don't exist
    vendor_dir = os.path.join(os.path.dirname(__file__), "..", "static", "vendor")
    return not os.path.isfile(os.path.join(vendor_dir, "alpine.min.js"))


def get_base_context(active_page: str = "", **extra) -> dict:
    """Build base context for templates.

    Args:
        active_page: Current page identifier for sidebar highlighting
        **extra: Additional context variables

    Returns:
        Context dict with features, plugin_tabs, version, and extras
    """
    return {
        "active_page": active_page,
        "features": get_available_features(),
        "plugin_tabs": get_plugin_tabs(),
        "version": tusk.__version__,
        "use_cdn": _use_cdn(),
        **extra,
    }


class TuskController(Controller):
    """Base controller providing common context for all templates.

    All page controllers should inherit from this class for
    consistent template context.

    Usage:
        class MyController(TuskController):
            path = "/my-page"

            @get("/")
            async def my_page(self) -> Template:
                return self.render(
                    "my_page.html",
                    active_page="my-page",
                    my_data=some_data,
                )
    """

    def get_context(self, active_page: str = "", **extra) -> dict:
        """Build context for templates.

        Args:
            active_page: Current page for sidebar highlighting
            **extra: Additional context variables

        Returns:
            Context dict with features, plugin_tabs, and extras
        """
        return get_base_context(active_page, **extra)

    def render(
        self,
        template_name: str,
        active_page: str = "",
        **context
    ) -> Template:
        """Render template with base context.

        Convenience method that builds context automatically.

        Args:
            template_name: Template file path
            active_page: Current page for sidebar
            **context: Template variables

        Returns:
            Litestar Template response
        """
        full_context = self.get_context(active_page, **context)
        return Template(template_name, context=full_context)

    def render_partial(
        self,
        template_name: str,
        **context
    ) -> Template:
        """Render a partial template (no base context needed).

        Use for HTMX fragment responses that don't extend base.html.

        Args:
            template_name: Partial template path (e.g. "partials/admin/stats.html")
            **context: Template variables

        Returns:
            Litestar Template response
        """
        return Template(template_name, context=context)

    def render_or_partial(
        self,
        request: Request,
        full_template: str,
        partial_template: str,
        active_page: str = "",
        **context
    ) -> Template:
        """Render full page or HTMX partial based on request type.

        Args:
            request: The Litestar request.
            full_template: Full page template (extends base.html).
            partial_template: Partial template for HTMX.
            active_page: Page identifier for nav highlighting.
            **context: Template variables.

        Returns:
            Template response (full or partial).
        """
        if is_htmx(request):
            return Template(partial_template, context=context)
        full_context = self.get_context(active_page, **context)
        return Template(full_template, context=full_context)
