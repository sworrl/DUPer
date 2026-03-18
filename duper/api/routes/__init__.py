"""API routes for DUPer."""

from duper.api.routes.config import router as config_router
from duper.api.routes.duplicates import router as duplicates_router
from duper.api.routes.files import router as files_router
from duper.api.routes.media import router as media_router
from duper.api.routes.media import saves_router
from duper.api.routes.retroachievements import router as ra_router
from duper.api.routes.scan import router as scan_router
from duper.api.routes.screenscraper import router as ss_router
from duper.api.routes.system import router as system_router

__all__ = [
    "config_router",
    "duplicates_router",
    "files_router",
    "media_router",
    "ra_router",
    "saves_router",
    "scan_router",
    "ss_router",
    "system_router",
]
