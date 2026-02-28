"""Base class for Tusk plugins"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from litestar import Controller

import tusk


class TuskPlugin(ABC):
    """Abstract base class for Tusk plugins.

    Plugins are separate pip packages that extend Tusk functionality.
    They register via entry_points in pyproject.toml:

    [project.entry-points."tusk.plugins"]
    my_plugin = "my_package:MyPlugin"

    Example:
        class SecurityPlugin(TuskPlugin):
            @property
            def name(self) -> str:
                return "tusk-security"

            @property
            def version(self) -> str:
                return "0.1.0"

            def get_route_handlers(self) -> list:
                return [SecurityController, SecurityAPIController]
    """

    # ─────────────────────────────────────────────────────────────
    # Required metadata
    # ─────────────────────────────────────────────────────────────

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique plugin identifier (e.g., 'tusk-security')"""
        ...

    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version (semver)"""
        ...

    @property
    def description(self) -> str:
        """Short description for UI"""
        return ""

    # ─────────────────────────────────────────────────────────────
    # Tab configuration (for sidebar)
    # ─────────────────────────────────────────────────────────────

    @property
    def tab_id(self) -> str:
        """Unique tab identifier for URL routing"""
        return self.name.replace("tusk-", "")

    @property
    def tab_label(self) -> str:
        """Display label in sidebar"""
        return self.tab_id.title()

    @property
    def tab_icon(self) -> str:
        """Lucide icon name for sidebar (e.g., 'shield', 'server')"""
        return "puzzle"

    @property
    def tab_url(self) -> str:
        """Base URL path for plugin routes"""
        return f"/{self.tab_id}"

    # ─────────────────────────────────────────────────────────────
    # Compatibility
    # ─────────────────────────────────────────────────────────────

    @property
    def min_tusk_version(self) -> str:
        """Minimum required Tusk version"""
        return "0.2.0"

    @property
    def max_tusk_version(self) -> str | None:
        """Maximum supported Tusk version (None = no limit)"""
        return None

    def is_compatible(self) -> bool:
        """Check if plugin is compatible with current Tusk version"""
        from packaging.version import Version

        current = Version(tusk.__version__)
        min_ver = Version(self.min_tusk_version)

        if current < min_ver:
            return False

        if self.max_tusk_version:
            max_ver = Version(self.max_tusk_version)
            if current > max_ver:
                return False

        return True

    # ─────────────────────────────────────────────────────────────
    # Storage & Config
    # ─────────────────────────────────────────────────────────────

    @property
    def requires_storage(self) -> bool:
        """Whether this plugin needs SQLite storage"""
        return False

    @property
    def requires_config(self) -> bool:
        """Whether this plugin needs TOML config"""
        return False

    # ─────────────────────────────────────────────────────────────
    # Auth & Permissions
    # ─────────────────────────────────────────────────────────────

    @property
    def requires_auth(self) -> bool:
        """Whether routes require authentication"""
        return True

    @property
    def requires_admin(self) -> bool:
        """Whether routes require admin role"""
        return False

    def check_access(self, user: dict | None) -> bool:
        """Custom access check. Override for complex logic."""
        if not self.requires_auth:
            return True
        if user is None:
            return False
        if self.requires_admin:
            return user.get("is_admin", False)
        return True

    # ─────────────────────────────────────────────────────────────
    # Dataset Integration (for Data module)
    # ─────────────────────────────────────────────────────────────

    def get_datasets(self) -> list[dict]:
        """Datasets this plugin exposes to the Data module.

        Data can query these via DuckDB's sqlite_scanner.

        Returns:
            List of dataset descriptors:
            [
                {
                    "name": "security_scans",
                    "description": "Vulnerability scan results",
                    "table": "scan_results",  # SQLite table name
                }
            ]
        """
        return []

    # ─────────────────────────────────────────────────────────────
    # Routes & Templates
    # ─────────────────────────────────────────────────────────────

    @abstractmethod
    def get_route_handlers(self) -> list["Controller"]:
        """Return list of Litestar Controller classes.

        Example:
            return [SecurityController, SecurityAPIController]
        """
        ...

    def get_templates_path(self) -> Path | None:
        """Path to plugin's templates directory.

        Templates will be copied to:
        templates/plugins/{plugin_id}/
        """
        return None

    def get_static_path(self) -> Path | None:
        """Path to plugin's static files directory."""
        return None

    # ─────────────────────────────────────────────────────────────
    # CLI Commands
    # ─────────────────────────────────────────────────────────────

    def get_cli_commands(self) -> dict[str, callable]:
        """CLI commands this plugin provides.

        Returns:
            Dict mapping command name to handler function.
            Handler receives (args: list[str]) and returns exit code.

        Example:
            return {"security": self.handle_security_cli}
        """
        return {}

    # ─────────────────────────────────────────────────────────────
    # Notifications
    # ─────────────────────────────────────────────────────────────

    def get_notification_events(self) -> list[dict]:
        """Notification events this plugin provides.

        Returns:
            List of event descriptors:
            [
                {
                    "event_key": "ci.pipeline.failed",
                    "label": "Pipeline Failed",
                    "description": "A pipeline run has failed",
                }
            ]
        """
        return []

    # ─────────────────────────────────────────────────────────────
    # Lifecycle Hooks
    # ─────────────────────────────────────────────────────────────

    async def on_startup(self) -> None:
        """Called when Tusk starts. Initialize resources here."""
        pass

    async def on_shutdown(self) -> None:
        """Called when Tusk stops. Cleanup resources here."""
        pass
