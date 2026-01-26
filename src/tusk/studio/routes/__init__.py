"""Studio routes"""

from tusk.studio.routes.pages import PageController
from tusk.studio.routes.api import APIController
from tusk.studio.routes.admin import AdminController
from tusk.studio.routes.settings import SettingsController
from tusk.studio.routes.files import FilesController, DuckDBController
from tusk.studio.routes.data import DataController
from tusk.studio.routes.cluster import ClusterController
from tusk.studio.routes.auth import AuthController, UsersController, GroupsController, AuthSetupController, ProfileController
from tusk.studio.routes.scheduler import SchedulerController

__all__ = [
    "PageController",
    "APIController",
    "AdminController",
    "SettingsController",
    "FilesController",
    "DuckDBController",
    "DataController",
    "ClusterController",
    "AuthController",
    "UsersController",
    "GroupsController",
    "AuthSetupController",
    "ProfileController",
    "SchedulerController",
]
