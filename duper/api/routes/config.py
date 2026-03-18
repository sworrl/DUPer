"""Configuration API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from duper.api.auth import require_auth
from duper.api.models import (
    ConfigResponse,
    ConfigUpdateRequest,
    RemoteAddRequest,
    RemoteHostResponse,
    RemoteListResponse,
)
from duper.core import get_config

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("", response_model=ConfigResponse)
async def get_configuration(
    _: None = Depends(require_auth),
) -> ConfigResponse:
    """Get current configuration."""
    config = get_config()

    return ConfigResponse(
        server={
            "port": config.server.port,
            "host": config.server.host,
            "web_ui_enabled": config.server.web_ui_enabled,
            "auth_enabled": config.server.auth_enabled,
            # Don't expose the actual API key
            "has_api_key": bool(config.server.api_key),
        },
        scanner={
            "ignore_fodder": config.scanner.ignore_fodder,
            "ignore_video": config.scanner.ignore_video,
            "ignore_music": config.scanner.ignore_music,
            "ignore_pictures": config.scanner.ignore_pictures,
            "retroarch_mode": config.scanner.retroarch_mode,
        },
        paths={
            "working_dir": config.paths.working_dir,
            "database": config.paths.database,
            "duplicates_dir": config.paths.duplicates_dir,
        },
        remotes={
            name: {
                "host": remote.host,
                "port": remote.port,
                "has_api_key": bool(remote.api_key),
            }
            for name, remote in config.remotes.items()
        },
    )


@router.put("", response_model=ConfigResponse)
async def update_configuration(
    request: ConfigUpdateRequest,
    _: None = Depends(require_auth),
) -> ConfigResponse:
    """Update configuration."""
    config = get_config()

    # Update server settings
    if request.server_port is not None:
        config.server.port = request.server_port
    if request.server_host is not None:
        config.server.host = request.server_host
    if request.web_ui_enabled is not None:
        config.server.web_ui_enabled = request.web_ui_enabled
    if request.auth_enabled is not None:
        config.server.auth_enabled = request.auth_enabled
    if request.api_key is not None:
        config.server.api_key = request.api_key

    # Update scanner settings
    if request.ignore_fodder is not None:
        config.scanner.ignore_fodder = request.ignore_fodder
    if request.ignore_video is not None:
        config.scanner.ignore_video = request.ignore_video
    if request.ignore_music is not None:
        config.scanner.ignore_music = request.ignore_music
    if request.ignore_pictures is not None:
        config.scanner.ignore_pictures = request.ignore_pictures
    if request.retroarch_mode is not None:
        config.scanner.retroarch_mode = request.retroarch_mode

    # Update path settings
    if request.working_dir is not None:
        config.paths.working_dir = request.working_dir
    if request.database is not None:
        config.paths.database = request.database
    if request.duplicates_dir is not None:
        config.paths.duplicates_dir = request.duplicates_dir

    # Save configuration
    config.save()

    # Return updated config
    return await get_configuration(_)


@router.get("/remotes", response_model=RemoteListResponse)
async def list_remotes(
    _: None = Depends(require_auth),
) -> RemoteListResponse:
    """List all configured remote hosts."""
    config = get_config()

    return RemoteListResponse(
        remotes=[
            RemoteHostResponse(
                name=name,
                host=remote.host,
                port=remote.port,
                has_api_key=bool(remote.api_key),
            )
            for name, remote in config.remotes.items()
        ]
    )


@router.post("/remotes", response_model=RemoteHostResponse)
async def add_remote(
    request: RemoteAddRequest,
    _: None = Depends(require_auth),
) -> RemoteHostResponse:
    """Add a remote host configuration."""
    config = get_config()

    if request.name in config.remotes:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Remote '{request.name}' already exists",
        )

    config.add_remote(
        name=request.name,
        host=request.host,
        port=request.port,
        api_key=request.api_key,
    )
    config.save()

    return RemoteHostResponse(
        name=request.name,
        host=request.host,
        port=request.port,
        has_api_key=bool(request.api_key),
    )


@router.delete("/remotes/{name}")
async def remove_remote(
    name: str,
    _: None = Depends(require_auth),
) -> dict:
    """Remove a remote host configuration."""
    config = get_config()

    if name not in config.remotes:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Remote '{name}' not found",
        )

    config.remove_remote(name)
    config.save()

    return {"status": "deleted", "name": name}
