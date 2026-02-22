"""Tests for the health check endpoint."""

import pytest


class TestHealthEndpoint:
    """Test /api/health returns correct data."""

    def test_health_function_returns_dict(self):
        """Test the health_check handler directly (no HTTP server needed)."""
        import asyncio
        from tusk.studio.routes.api import health_check

        # Litestar wraps the function; access the underlying fn
        fn = health_check.fn if hasattr(health_check, "fn") else health_check
        result = asyncio.run(fn())
        assert isinstance(result, dict)
        assert result["status"] == "ok"
        assert "version" in result

    def test_health_version_matches(self):
        """Verify health endpoint returns the actual package version."""
        import asyncio
        import tusk
        from tusk.studio.routes.api import health_check

        fn = health_check.fn if hasattr(health_check, "fn") else health_check
        result = asyncio.run(fn())
        assert result["version"] == tusk.__version__
