"""Litestar application for Tusk Studio"""

from pathlib import Path
from litestar import Litestar
from litestar.static_files import StaticFilesConfig
from litestar.template import TemplateConfig
from litestar.config.compression import CompressionConfig

from litestar.contrib.minijinja import MiniJinjaTemplateEngine

from tusk.studio.routes import (
    PageController,
    APIController,
    AdminController,
    SettingsController,
    FilesController,
    DuckDBController,
    DataController,
    ClusterController,
    AuthController,
    UsersController,
    GroupsController,
    AuthSetupController,
    ProfileController,
    SchedulerController,
)
from tusk.core.connection import load_connections_from_file
from tusk.core.logging import setup_logging, get_logger
from tusk.core.scheduler import get_scheduler

# Paths
STUDIO_DIR = Path(__file__).parent
TEMPLATES_DIR = STUDIO_DIR / "templates"
STATIC_DIR = STUDIO_DIR / "static"


def on_startup() -> None:
    """Initialize logging, load connections, and start scheduler on startup"""
    setup_logging(debug=True)
    log = get_logger("studio")
    log.info("Starting Tusk Studio")
    load_connections_from_file()
    log.info("Connections loaded")
    # Start the task scheduler
    scheduler = get_scheduler()
    scheduler.start()
    log.info("Scheduler started")


app = Litestar(
    route_handlers=[
        PageController,
        APIController,
        AdminController,
        SettingsController,
        FilesController,
        DuckDBController,
        DataController,
        ClusterController,
        AuthController,
        UsersController,
        GroupsController,
        AuthSetupController,
        ProfileController,
        SchedulerController,
    ],
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
    debug=True,
)
