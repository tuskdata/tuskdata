"""Tests for plugin discovery and registry."""

from tusk.plugins.registry import (
    discover_plugins,
    get_all_plugins,
    get_plugin_tabs,
    get_plugin_route_handlers,
    reset_registry,
)


class TestPluginDiscovery:
    """Test plugin discovery via entry_points."""

    def test_discover_returns_dict(self):
        plugins = discover_plugins()
        assert isinstance(plugins, dict)

    def test_get_all_plugins_returns_list(self):
        discover_plugins()
        plugins = get_all_plugins()
        assert isinstance(plugins, list)

    def test_reset_registry(self):
        discover_plugins()
        reset_registry()
        # After reset, discover should re-run
        plugins = discover_plugins()
        assert isinstance(plugins, dict)


class TestPluginTabs:
    """Test plugin tab configuration."""

    def test_get_plugin_tabs_returns_list(self):
        discover_plugins()
        tabs = get_plugin_tabs()
        assert isinstance(tabs, list)

    def test_plugin_tabs_have_required_fields(self):
        discover_plugins()
        for tab in get_plugin_tabs():
            assert "id" in tab
            assert "label" in tab
            assert "icon" in tab
            assert "url" in tab


class TestPluginRouteHandlers:
    """Test plugin route handler collection."""

    def test_get_plugin_route_handlers_returns_list(self):
        discover_plugins()
        handlers = get_plugin_route_handlers()
        assert isinstance(handlers, list)
