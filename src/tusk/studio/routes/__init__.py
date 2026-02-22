"""Studio routes"""

from tusk.studio.routes.base import TuskController, get_base_context
from tusk.studio.routes.pages import PageController
from tusk.studio.routes.api import APIController, health_check
from tusk.studio.routes.admin import AdminController
from tusk.studio.routes.settings import SettingsController
from tusk.studio.routes.files import FilesController, DuckDBController
from tusk.studio.routes.data import DataController
from tusk.studio.routes.auth import AuthController, UsersController, GroupsController, AuthSetupController, ProfileController, AuditLogController
from tusk.studio.routes.scheduler import SchedulerController
from tusk.studio.routes.downloads import DownloadsController

__all__ = [
    # Base
    "TuskController",
    "get_base_context",
    # Pages
    "PageController",
    "APIController",
    "health_check",
    "AdminController",
    "SettingsController",
    "FilesController",
    "DuckDBController",
    "DataController",
    "AuthController",
    "UsersController",
    "GroupsController",
    "AuthSetupController",
    "ProfileController",
    "AuditLogController",
    "SchedulerController",
    "DownloadsController",
]
