"""BI plugin route controllers"""

from tusk.bi.routes.views import BIPageController
from tusk.bi.routes.api import BIAPIController
from tusk.bi.routes.embed import EmbedAPIController, EmbedPageController

__all__ = ["BIPageController", "BIAPIController", "EmbedAPIController", "EmbedPageController"]
