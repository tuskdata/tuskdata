"""Studio routes"""

from tusk.studio.routes.base import TuskController, get_base_context
from tusk.studio.routes.pages import PageController
from tusk.studio.routes.api import APIController, health_check, metrics
from tusk.studio.routes.admin import AdminController, HealthController
from tusk.studio.routes.settings import SettingsController
from tusk.studio.routes.files import FilesController, DuckDBController
from tusk.studio.routes.data import DataController, ExploreController
from tusk.studio.routes.auth import AuthController, UsersController, GroupsController, AuthSetupController, ProfileController, AuditLogController
from tusk.studio.routes.scheduler import SchedulerController
from tusk.studio.routes.downloads import DownloadsController
from tusk.studio.routes.notifications import NotificationPageController, NotificationAPIController
from tusk.studio.routes.ai import AICopilotController, AISettingsPageController
from tusk.studio.routes.jobs import JobsController

__all__ = [
    # Base
    "TuskController",
    "get_base_context",
    # Pages
    "PageController",
    "APIController",
    "health_check",
    "metrics",
    "AdminController",
    "HealthController",
    "SettingsController",
    "FilesController",
    "DuckDBController",
    "DataController",
    "ExploreController",
    "AuthController",
    "UsersController",
    "GroupsController",
    "AuthSetupController",
    "ProfileController",
    "AuditLogController",
    "SchedulerController",
    "DownloadsController",
    "NotificationPageController",
    "NotificationAPIController",
    "AICopilotController",
    "AISettingsPageController",
    "JobsController",
]
