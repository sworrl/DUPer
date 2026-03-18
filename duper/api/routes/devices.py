"""Device management and offline mode API routes."""

from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends

from duper.api.auth import optional_auth
from duper.core.devices import get_device_manager

router = APIRouter(prefix="/api/devices", tags=["devices"])


@router.get("")
async def list_devices(is_authed: bool = Depends(optional_auth)):
    """List all managed devices."""
    dm = get_device_manager()
    return {
        "devices": [asdict(d) for d in dm.list_devices()]
    }


@router.get("/{device_id}")
async def get_device(device_id: str, is_authed: bool = Depends(optional_auth)):
    """Get a specific device."""
    dm = get_device_manager()
    device = dm.get_device(device_id)
    if not device:
        return {"error": "Device not found"}
    return asdict(device)


@router.get("/{device_id}/offline-games")
async def get_offline_games(device_id: str, is_authed: bool = Depends(optional_auth)):
    """Get list of games available offline on a device."""
    dm = get_device_manager()
    device = dm.get_device(device_id)
    if not device:
        return {"error": "Device not found"}
    return {
        "device": device.name,
        "offline_games": device.offline_games,
        "count": len(device.offline_games),
    }


@router.post("/{device_id}/make-offline")
async def make_game_offline(
    device_id: str,
    system: str = "",
    rom_filename: str = "",
    is_authed: bool = Depends(optional_auth),
):
    """Make a game available offline on a device (copy from NAS to local)."""
    dm = get_device_manager()
    device = dm.get_device(device_id)
    if not device:
        return {"error": "Device not found"}
    if not system or not rom_filename:
        return {"error": "system and rom_filename required"}

    game = dm.resolve_game_files(system, rom_filename)
    success = dm.make_offline(device_id, game)

    return {
        "status": "ok" if success else "failed",
        "game": game.game_title,
        "files_copied": len(game.rom_files) + len(game.save_files) + len(game.media_files),
        "rom_files": game.rom_files,
        "save_files": game.save_files,
        "media_files": game.media_files,
    }


@router.post("/{device_id}/return-to-cloud")
async def return_game_to_cloud(
    device_id: str,
    system: str = "",
    rom_filename: str = "",
    is_authed: bool = Depends(optional_auth),
):
    """Return a game to cloud-only (delete local copy, restore symlink)."""
    dm = get_device_manager()
    device = dm.get_device(device_id)
    if not device:
        return {"error": "Device not found"}
    if not system or not rom_filename:
        return {"error": "system and rom_filename required"}

    game = dm.resolve_game_files(system, rom_filename)
    success = dm.return_to_cloud(device_id, game)

    return {
        "status": "ok" if success else "failed",
        "game": game.game_title,
    }


@router.post("/sync-configs")
async def sync_configs(
    source_device: str = "steamdeck",
    target_device: str = "glassite",
    is_authed: bool = Depends(optional_auth),
):
    """Sync emulator configs between devices."""
    dm = get_device_manager()
    return dm.sync_configs(source_device, target_device)
