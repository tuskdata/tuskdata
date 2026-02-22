"""Litestar application for Tusk Studio"""

import asyncio
from pathlib import Path
from litestar import Litestar
from litestar.static_files import StaticFilesConfig
from litestar.template import TemplateConfig
from litestar.config.compression import CompressionConfig

from litestar.contrib.minijinja import MiniJinjaTemplateEngine

import os

from tusk.studio.routes import (
    PageController,
    APIController,
    AdminController,
    SettingsController,
    FilesController,
    DuckDBController,
    DataController,
    AuthController,
    UsersController,
    GroupsController,
    AuthSetupController,
    ProfileController,
    AuditLogController,
    SchedulerController,
    DownloadsController,
    health_check,
)
from tusk.core.connection import load_connections_from_file
from tusk.core.logging import setup_logging, get_logger
from tusk.core.scheduler import get_scheduler
from tusk.plugins.registry import (
    discover_plugins,
    get_all_plugins,
    get_plugin_route_handlers,
)
from tusk.plugins.templates import (
    setup_plugin_templates,
    setup_plugin_statics,
    cleanup_plugin_statics,
)

# Paths
STUDIO_DIR = Path(__file__).parent
TEMPLATES_DIR = STUDIO_DIR / "templates"
STATIC_DIR = STUDIO_DIR / "static"


def get_route_handlers() -> list:
    """Collect all route handlers including plugins"""
    # Core handlers
    handlers = [
        PageController,
        APIController,
        AdminController,
        SettingsController,
        FilesController,
        DuckDBController,
        DataController,
        AuthController,
        UsersController,
        GroupsController,
        AuthSetupController,
        ProfileController,
        AuditLogController,
        SchedulerController,
        DownloadsController,
        health_check,
    ]

    # Add plugin handlers
    handlers.extend(get_plugin_route_handlers())

    return handlers


def on_startup() -> None:
    """Initialize logging, load connections, discover plugins, and start scheduler"""
    debug = os.environ.get("TUSK_DEBUG", "").lower() in ("1", "true", "yes")
    setup_logging(debug=debug)
    log = get_logger("studio")
    log.info("Starting Tusk Studio")

    # Load connections
    load_connections_from_file()
    log.info("Connections loaded")

    # Discover plugins
    plugins = discover_plugins()
    if plugins:
        log.info("Plugins discovered", count=len(plugins))

    # Call plugin startup hooks
    for plugin in get_all_plugins():
        try:
            # Check if there's already a running event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're in a running loop, schedule the coroutine
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, plugin.on_startup())
                    future.result(timeout=30)
            except RuntimeError:
                # No running loop, we can use asyncio.run directly
                asyncio.run(plugin.on_startup())
            log.info("Plugin started", plugin=plugin.name)
        except Exception as e:
            log.error("Plugin startup failed", plugin=plugin.name, error=str(e))

    # Setup plugin templates and statics
    setup_plugin_templates(TEMPLATES_DIR)
    setup_plugin_statics(STATIC_DIR)

    # Start the task scheduler
    scheduler = get_scheduler()
    scheduler.start()
    log.info("Scheduler started")

    # Register scheduled downloads
    try:
        from tusk.core.downloads import schedule_downloads
        schedule_downloads()
    except Exception as e:
        log.warning("Failed to register scheduled downloads", error=str(e))


def on_shutdown() -> None:
    """Cleanup on shutdown"""
    log = get_logger("studio")

    # Call plugin shutdown hooks
    for plugin in get_all_plugins():
        try:
            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, plugin.on_shutdown())
                    future.result(timeout=30)
            except RuntimeError:
                asyncio.run(plugin.on_shutdown())
            log.info("Plugin stopped", plugin=plugin.name)
        except Exception as e:
            log.error("Plugin shutdown failed", plugin=plugin.name, error=str(e))

    # Cleanup plugin files
    cleanup_plugin_statics(STATIC_DIR)

    log.info("Tusk Studio stopped")


# Discover plugins before creating app (needed for route handlers)
discover_plugins()

app = Litestar(
    route_handlers=get_route_handlers(),
    template_config=TemplateConfig(
        directory=TEMPLATES_DIR,
        engine=MiniJinjaTemplateEngine,
    ),
    static_files_config=[
        StaticFilesConfig(
            directories=[STATIC_DIR],
            path="/static",
        )
    ],
    compression_config=CompressionConfig(
        backend="zstd",
        minimum_size=500,  # Compress responses larger than 500 bytes
    ),
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    debug=os.environ.get("TUSK_DEBUG", "").lower() in ("1", "true", "yes"),
)
