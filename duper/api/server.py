"""FastAPI server for DUPer."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from duper import __codename__, __version__
from duper.api.routes import (
    config_router,
    duplicates_router,
    files_router,
    media_router,
    ra_router,
    saves_router,
    scan_router,
    ss_router,
    system_router,
)
from duper.api.routes.acquisition import router as acquisition_router
from duper.api.routes.devices import router as devices_router
from duper.api.routes.live import router as live_router
from duper.api.routes.games import router as games_router
from duper.api.routes.libraries import router as libraries_router
from duper.api.routes.queue import router as queue_router
from duper.core import DuperConfig, DuperDatabase, get_config, set_config


def create_app(config: DuperConfig | None = None) -> FastAPI:
    """Create and configure the FastAPI application."""

    # Set config if provided
    if config:
        set_config(config)
    else:
        config = get_config()

    # Ensure directories exist
    config.ensure_directories()

    # Initialize database
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    db.close()

    # Create FastAPI app
    app = FastAPI(
        title="DUPer API",
        description="Duplicate file finder and manager with remote access",
        version=__version__,
        docs_url="/api/docs" if config.server.web_ui_enabled else None,
        redoc_url="/api/redoc" if config.server.web_ui_enabled else None,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, restrict this
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(system_router)
    app.include_router(scan_router)
    app.include_router(files_router)
    app.include_router(duplicates_router)
    app.include_router(config_router)
    app.include_router(media_router)
    app.include_router(saves_router)
    app.include_router(ra_router)
    app.include_router(ss_router)
    # Library management routers
    app.include_router(libraries_router)
    app.include_router(games_router)
    app.include_router(queue_router)
    app.include_router(devices_router)
    app.include_router(acquisition_router)
    app.include_router(live_router)

    # Mount static files for web UI if enabled
    if config.server.web_ui_enabled:
        static_dir = Path(__file__).parent.parent / "web" / "static"
        if static_dir.exists():
            from starlette.middleware.base import BaseHTTPMiddleware
            from starlette.responses import Response

            class NoCacheStaticMiddleware(BaseHTTPMiddleware):
                async def dispatch(self, request, call_next):
                    response: Response = await call_next(request)
                    path = request.url.path
                    if path.endswith((".js", ".css", ".html")) or path == "/":
                        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                        response.headers["Pragma"] = "no-cache"
                        response.headers["Expires"] = "0"
                    return response

            app.add_middleware(NoCacheStaticMiddleware)
            app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    @app.on_event("startup")
    async def startup_event():
        """Run on application startup."""
        pass

    @app.on_event("shutdown")
    async def shutdown_event():
        """Run on application shutdown."""
        pass

    return app


def run_server(
    host: str | None = None,
    port: int | None = None,
    config: DuperConfig | None = None,
    reload: bool = False,
) -> None:
    """Run the FastAPI server."""
    import uvicorn

    if config is None:
        config = get_config()

    host = host or config.server.host
    port = port or config.server.port

    app = create_app(config)

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=reload,
    )


# For running with uvicorn directly: uvicorn duper.api.server:app
app = create_app()
