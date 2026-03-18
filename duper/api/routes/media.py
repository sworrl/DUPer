"""Media and save-related API routes."""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from duper.api.auth import optional_auth, require_auth
from duper.core import DuperDatabase, get_config
from duper.core.media import MediaCorrelator

router = APIRouter(prefix="/api/media", tags=["media"])
saves_router = APIRouter(prefix="/api/saves", tags=["saves"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


class MediaFileResponse(BaseModel):
    path: str
    media_type: str
    category: str
    rom_name: str
    size_bytes: int


class OrphanedMediaResponse(BaseModel):
    rom_name: str
    rom_path: str
    media_files: list[MediaFileResponse]
    total_size_bytes: int
    file_count: int


class MediaScanResponse(BaseModel):
    orphaned: list[OrphanedMediaResponse]
    total_files: int
    total_size_bytes: int


class MediaCleanupRequest(BaseModel):
    directory: str | None = None
    move_to: str | None = None
    dry_run: bool = False
    cleanup_moved_roms: bool = False


class MediaCleanupResponse(BaseModel):
    removed_count: int
    removed_size_bytes: int
    errors: list[str]


class MediaScanRequest(BaseModel):
    directory: str


class MediaScanStatsResponse(BaseModel):
    roms_scanned: int
    media_found: int
    roms_with_media: int


class MediaStatsResponse(BaseModel):
    total_media_files: int
    roms_with_media: int
    by_category: dict[str, int]
    by_type: dict[str, int]
    total_size_bytes: int
    total_size_mb: float


class RomMediaResponse(BaseModel):
    media_id: int
    rom_filepath: str
    media_path: str
    media_type: str
    category: str
    filename: str
    size_bytes: int
    mime_type: str


@router.post("/scan", response_model=MediaScanStatsResponse)
async def scan_media_for_directory(
    request: MediaScanRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> MediaScanStatsResponse:
    """
    Scan and associate media files with ROMs in a directory.

    This finds media files (covers, screenshots, videos, etc.) and
    stores the associations in the database for fast lookup.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.scan_media_for_directory(request.directory)

    return MediaScanStatsResponse(
        roms_scanned=result["roms_scanned"],
        media_found=result["media_found"],
        roms_with_media=result["roms_with_media"],
    )


@router.get("/stats", response_model=MediaStatsResponse)
async def get_media_stats(
    _: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
) -> MediaStatsResponse:
    """Get statistics about stored media files."""
    stats = db.get_media_stats()
    return MediaStatsResponse(**stats)


@router.get("/for-rom")
async def get_media_for_rom(
    rom_path: str = Query(..., description="Path to the ROM file"),
    _: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
) -> list[RomMediaResponse]:
    """Get all media files associated with a specific ROM from the database."""
    media_files = db.get_media_for_rom(rom_path)
    return [
        RomMediaResponse(
            media_id=m.media_id,
            rom_filepath=m.rom_filepath,
            media_path=m.media_path,
            media_type=m.media_type,
            category=m.category,
            filename=m.filename,
            size_bytes=m.size_bytes,
            mime_type=m.mime_type,
        )
        for m in media_files
    ]


@router.get("/orphaned", response_model=MediaScanResponse)
async def find_orphaned_media(
    directory: str = Query(..., description="ROM directory to scan for orphaned media"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> MediaScanResponse:
    """
    Find orphaned media files - media without corresponding ROMs.

    Scans common media directories (videos, screenshots, boxart, etc.)
    and identifies files that don't have a matching ROM.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.find_orphaned_media(directory)

    return MediaScanResponse(
        orphaned=[
            OrphanedMediaResponse(
                rom_name=o.rom_name,
                rom_path=o.rom_path,
                media_files=[
                    MediaFileResponse(
                        path=m.path,
                        media_type=m.media_type,
                        category=m.category,
                        rom_name=m.rom_name,
                        size_bytes=m.size_bytes,
                    )
                    for m in o.media_files
                ],
                total_size_bytes=o.total_size_bytes,
                file_count=len(o.media_files),
            )
            for o in result.orphaned
        ],
        total_files=result.total_files,
        total_size_bytes=result.total_size_bytes,
    )


@router.get("/moved-roms", response_model=MediaScanResponse)
async def find_media_for_moved_roms(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> MediaScanResponse:
    """
    Find media files that belong to ROMs that have been moved as duplicates.

    Use this after processing duplicates to find leftover media files
    that should also be cleaned up.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.find_media_for_moved_roms()

    return MediaScanResponse(
        orphaned=[
            OrphanedMediaResponse(
                rom_name=o.rom_name,
                rom_path=o.rom_path,
                media_files=[
                    MediaFileResponse(
                        path=m.path,
                        media_type=m.media_type,
                        category=m.category,
                        rom_name=m.rom_name,
                        size_bytes=m.size_bytes,
                    )
                    for m in o.media_files
                ],
                total_size_bytes=o.total_size_bytes,
                file_count=len(o.media_files),
            )
            for o in result.orphaned
        ],
        total_files=result.total_files,
        total_size_bytes=result.total_size_bytes,
    )


@router.post("/cleanup", response_model=MediaCleanupResponse)
async def cleanup_media(
    request: MediaCleanupRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> MediaCleanupResponse:
    """
    Clean up orphaned media files.

    Can either:
    - Clean up orphaned media in a directory (set directory)
    - Clean up media for moved ROMs (set cleanup_moved_roms=true)

    Files can be deleted or moved to a specified location.
    Use dry_run=true to preview without making changes.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    if request.cleanup_moved_roms:
        result = correlator.cleanup_moved_rom_media(
            move_to=request.move_to,
            dry_run=request.dry_run,
        )
    elif request.directory:
        result = correlator.cleanup_orphaned_media(
            rom_directory=request.directory,
            move_to=request.move_to,
            dry_run=request.dry_run,
        )
    else:
        return MediaCleanupResponse(
            removed_count=0,
            removed_size_bytes=0,
            errors=["Must specify either directory or cleanup_moved_roms=true"],
        )

    return MediaCleanupResponse(
        removed_count=result.removed_count,
        removed_size_bytes=result.removed_size_bytes,
        errors=result.errors,
    )


@router.get("/for-rom", response_model=list[MediaFileResponse])
async def find_media_for_rom(
    rom_path: str = Query(..., description="Path to the ROM file"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> list[MediaFileResponse]:
    """
    Find all media files associated with a specific ROM.

    Useful for previewing what media will be affected when a ROM is removed.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    media_files = correlator.find_media_for_rom(rom_path)

    return [
        MediaFileResponse(
            path=m.path,
            media_type=m.media_type,
            category=m.category,
            rom_name=m.rom_name,
            size_bytes=m.size_bytes,
        )
        for m in media_files
    ]


class CoverArtResponse(BaseModel):
    """Cover art information for a ROM."""
    has_cover: bool
    cover_path: str | None = None
    cover_category: str | None = None
    all_media: list[MediaFileResponse] = []


# Cover art priority (higher = preferred)
COVER_PRIORITY = {
    "boxart": 100,
    "3dbox": 90,
    "screenshot": 80,
    "titlescreen": 70,
    "snap": 60,
    "wheel": 50,
    "fanart": 40,
    "marquee": 30,
    "video": 20,
    "other": 10,
}


def get_best_cover(media_files: list) -> tuple[str | None, str | None]:
    """Get the best cover art from a list of media files."""
    if not media_files:
        return None, None

    # Filter to images only for cover art
    image_media = [m for m in media_files if m.media_type == "image"]

    if not image_media:
        # Fall back to any media (like video thumbnail)
        image_media = media_files

    # Sort by priority
    sorted_media = sorted(
        image_media,
        key=lambda m: COVER_PRIORITY.get(m.category, 10),
        reverse=True,
    )

    if sorted_media:
        best = sorted_media[0]
        return best.path, best.category

    return None, None


@router.get("/cover/{rom_path:path}")
async def get_cover_art(
    rom_path: str,
    _: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
) -> CoverArtResponse:
    """
    Get the best cover art for a ROM.

    Priority: boxart > 3dbox > screenshot > titlescreen > snap > wheel > fanart
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    # Prepend / if not present (path parameter strips leading /)
    if not rom_path.startswith("/"):
        rom_path = "/" + rom_path

    media_files = correlator.find_media_for_rom(rom_path)

    cover_path, cover_category = get_best_cover(media_files)

    return CoverArtResponse(
        has_cover=cover_path is not None,
        cover_path=cover_path,
        cover_category=cover_category,
        all_media=[
            MediaFileResponse(
                path=m.path,
                media_type=m.media_type,
                category=m.category,
                rom_name=m.rom_name,
                size_bytes=m.size_bytes,
            )
            for m in media_files
        ],
    )


@router.get("/file/{file_path:path}")
async def serve_media_file(
    file_path: str,
    _: bool = Depends(optional_auth),
) -> FileResponse:
    """
    Serve a media file for display in the web UI.

    This endpoint serves image and video files from the filesystem.
    """
    # Prepend / if not present (path parameter strips leading /)
    if not file_path.startswith("/"):
        file_path = "/" + file_path

    path = Path(file_path)

    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")

    if not path.is_file():
        raise HTTPException(status_code=400, detail="Not a file")

    # Determine MIME type
    mime_type, _ = mimetypes.guess_type(str(path))
    if mime_type is None:
        mime_type = "application/octet-stream"

    # Only serve image and video files
    if not (mime_type.startswith("image/") or mime_type.startswith("video/")):
        raise HTTPException(status_code=400, detail="Not a media file")

    return FileResponse(
        path=str(path),
        media_type=mime_type,
        filename=path.name,
    )


@router.get("/cover-data/{rom_path:path}")
async def get_cover_art_data(
    rom_path: str,
    _: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """
    Get cover art as base64 encoded data (for embedding in responses).

    First checks the database for cached media associations, then falls back
    to scanning the filesystem if not found.

    Returns the image data directly as base64 for easy display.
    """
    config = get_config()

    # Prepend / if not present
    if not rom_path.startswith("/"):
        rom_path = "/" + rom_path

    cover_path = None
    cover_category = None

    # First, check database for cached media
    db_cover = db.get_best_cover_for_rom(rom_path)
    if db_cover:
        cover_path = db_cover.media_path
        cover_category = db_cover.category

    # Fall back to filesystem scan if not in database
    if not cover_path:
        correlator = MediaCorrelator(db=db, config=config)
        media_files = correlator.find_media_for_rom(rom_path)
        cover_path, cover_category = get_best_cover(media_files)

        # Store found media in database for next time
        if media_files:
            for media in media_files:
                mime_type_m, _ = mimetypes.guess_type(media.path)
                db.insert_media(
                    rom_filepath=rom_path,
                    media_path=media.path,
                    media_type=media.media_type,
                    category=media.category,
                    filename=Path(media.path).name,
                    size_bytes=media.size_bytes,
                    mime_type=mime_type_m or "",
                )

    if not cover_path:
        return {"has_cover": False, "data": None, "mime_type": None}

    path = Path(cover_path)
    if not path.exists() or not path.is_file():
        return {"has_cover": False, "data": None, "mime_type": None}

    mime_type, _ = mimetypes.guess_type(str(path))
    if not mime_type or not mime_type.startswith("image/"):
        return {"has_cover": False, "data": None, "mime_type": None}

    try:
        with open(path, "rb") as f:
            data = base64.b64encode(f.read()).decode("utf-8")
        return {
            "has_cover": True,
            "data": data,
            "mime_type": mime_type,
            "category": cover_category,
        }
    except OSError:
        return {"has_cover": False, "data": None, "mime_type": None}


@router.get("/game-image/{system}/{game_name}")
async def get_game_image(
    system: str,
    game_name: str,
    category: str = "covers",
    _: bool = Depends(optional_auth),
):
    """Get game image by system and game name. Searches NAS media directory.

    Falls back through: covers -> miximages -> screenshots -> titlescreens
    Returns the image file directly for <img src=""> usage.
    """
    import os
    import re

    media_base = Path("/var/mnt/retronas/media")
    # Also check local media dir
    local_base = Path.home() / "Emulation" / "tools" / "downloaded_media"

    # Strip leading ./ from gamelist paths
    game_name = game_name.lstrip("./")

    # Clean game name for filename matching - strip extension, region codes
    clean = re.sub(r'\.(zip|smd|nes|sfc|smc|chd|iso|xiso\.iso|rvz|z64|n64|gba|gbc|gb|nds|bin|cue|ccd|img|wbfs|gcm|nkit\.iso)$', '', game_name, flags=re.I)

    # Try each category in priority order
    categories = [category, "covers", "miximages", "screenshots", "titlescreens", "3dboxes"]
    seen = set()

    for cat in categories:
        if cat in seen:
            continue
        seen.add(cat)

        for base in [local_base, media_base]:
            cat_dir = base / system / cat
            if not cat_dir.is_dir():
                continue

            # Try exact match first
            for ext in [".png", ".jpg", ".jpeg", ".webp"]:
                candidate = cat_dir / f"{clean}{ext}"
                if candidate.is_file():
                    mime, _ = mimetypes.guess_type(str(candidate))
                    return FileResponse(str(candidate), media_type=mime or "image/png")

            # Try fuzzy match - find files containing the game name
            try:
                for f in cat_dir.iterdir():
                    if not f.is_file():
                        continue
                    if clean.lower() in f.stem.lower():
                        mime, _ = mimetypes.guess_type(str(f))
                        return FileResponse(str(f), media_type=mime or "image/png")
            except OSError:
                continue

    # No image found
    raise HTTPException(status_code=404, detail="No image found")


@router.get("/game-image-b64/{system}/{game_name}")
async def get_game_image_b64(
    system: str,
    game_name: str,
    category: str = "covers",
    _: bool = Depends(optional_auth),
):
    """Same as game-image but returns base64 for embedding."""
    import os
    import re

    media_base = Path("/var/mnt/retronas/media")
    local_base = Path.home() / "Emulation" / "tools" / "downloaded_media"

    game_name = game_name.lstrip("./")
    clean = re.sub(r'\.(zip|smd|nes|sfc|smc|chd|iso|xiso\.iso|rvz|z64|n64|gba|gbc|gb|nds|bin|cue|ccd|img|wbfs|gcm|nkit\.iso)$', '', game_name, flags=re.I)

    categories = [category, "covers", "miximages", "screenshots", "titlescreens"]
    seen = set()

    for cat in categories:
        if cat in seen:
            continue
        seen.add(cat)

        for base_dir in [local_base, media_base]:
            cat_dir = base_dir / system / cat
            if not cat_dir.is_dir():
                continue

            for ext in [".png", ".jpg", ".jpeg", ".webp"]:
                candidate = cat_dir / f"{clean}{ext}"
                if candidate.is_file():
                    mime, _ = mimetypes.guess_type(str(candidate))
                    data = base64.b64encode(candidate.read_bytes()).decode("utf-8")
                    return {"has_image": True, "data": data, "mime_type": mime, "category": cat}

            try:
                for f in cat_dir.iterdir():
                    if f.is_file() and clean.lower() in f.stem.lower():
                        mime, _ = mimetypes.guess_type(str(f))
                        data = base64.b64encode(f.read_bytes()).decode("utf-8")
                        return {"has_image": True, "data": data, "mime_type": mime, "category": cat}
            except OSError:
                continue

    return {"has_image": False, "data": None, "mime_type": None}


# =============================================================================
# SAVE GAME / SAVE STATE ROUTES
# IMPORTANT: Saves are NEVER deleted - only moved or preserved
# =============================================================================


class SaveFileResponse(BaseModel):
    path: str
    save_type: str  # "save" or "state"
    rom_name: str
    size_bytes: int
    extension: str


class OrphanedSavesResponse(BaseModel):
    rom_name: str
    rom_path: str
    save_files: list[SaveFileResponse]
    state_files: list[SaveFileResponse]
    total_size_bytes: int
    save_count: int
    state_count: int


class SaveScanResponse(BaseModel):
    orphaned: list[OrphanedSavesResponse]
    total_saves: int
    total_states: int
    total_files: int
    total_size_bytes: int


class SavePreserveRequest(BaseModel):
    directory: str | None = None
    move_to: str
    dry_run: bool = False
    preserve_moved_roms: bool = False


class SavePreserveResponse(BaseModel):
    moved_count: int
    moved_size_bytes: int
    errors: list[str]


class SaveRenameRequest(BaseModel):
    old_rom_name: str
    new_rom_name: str
    rom_directory: str
    dry_run: bool = False


class SaveRenameResponse(BaseModel):
    renamed_count: int
    errors: list[str]


@saves_router.get("/orphaned", response_model=SaveScanResponse)
async def find_orphaned_saves(
    directory: str = Query(..., description="ROM directory to scan for orphaned saves"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> SaveScanResponse:
    """
    Find orphaned save files - saves without corresponding ROMs.

    These saves belong to ROMs that may have been deleted or moved.
    Use the preserve endpoint to move them to a safe location.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.find_orphaned_saves(directory)

    return SaveScanResponse(
        orphaned=[
            OrphanedSavesResponse(
                rom_name=o.rom_name,
                rom_path=o.rom_path,
                save_files=[
                    SaveFileResponse(
                        path=s.path,
                        save_type=s.save_type,
                        rom_name=s.rom_name,
                        size_bytes=s.size_bytes,
                        extension=s.extension,
                    )
                    for s in o.save_files
                ],
                state_files=[
                    SaveFileResponse(
                        path=s.path,
                        save_type=s.save_type,
                        rom_name=s.rom_name,
                        size_bytes=s.size_bytes,
                        extension=s.extension,
                    )
                    for s in o.state_files
                ],
                total_size_bytes=o.total_size_bytes,
                save_count=len(o.save_files),
                state_count=len(o.state_files),
            )
            for o in result.orphaned
        ],
        total_saves=result.total_saves,
        total_states=result.total_states,
        total_files=result.total_saves + result.total_states,
        total_size_bytes=result.total_size_bytes,
    )


@saves_router.get("/moved-roms", response_model=SaveScanResponse)
async def find_saves_for_moved_roms(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> SaveScanResponse:
    """
    Find save files that belong to ROMs that have been moved as duplicates.

    Use this after processing duplicates to find leftover saves
    that should be preserved or moved.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.find_saves_for_moved_roms()

    return SaveScanResponse(
        orphaned=[
            OrphanedSavesResponse(
                rom_name=o.rom_name,
                rom_path=o.rom_path,
                save_files=[
                    SaveFileResponse(
                        path=s.path,
                        save_type=s.save_type,
                        rom_name=s.rom_name,
                        size_bytes=s.size_bytes,
                        extension=s.extension,
                    )
                    for s in o.save_files
                ],
                state_files=[
                    SaveFileResponse(
                        path=s.path,
                        save_type=s.save_type,
                        rom_name=s.rom_name,
                        size_bytes=s.size_bytes,
                        extension=s.extension,
                    )
                    for s in o.state_files
                ],
                total_size_bytes=o.total_size_bytes,
                save_count=len(o.save_files),
                state_count=len(o.state_files),
            )
            for o in result.orphaned
        ],
        total_saves=result.total_saves,
        total_states=result.total_states,
        total_files=result.total_saves + result.total_states,
        total_size_bytes=result.total_size_bytes,
    )


@saves_router.post("/preserve", response_model=SavePreserveResponse)
async def preserve_saves(
    request: SavePreserveRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> SavePreserveResponse:
    """
    Preserve orphaned save files by moving them to a safe location.

    IMPORTANT: This endpoint NEVER deletes saves. It only moves them.

    Can either:
    - Preserve orphaned saves in a directory (set directory)
    - Preserve saves for moved ROMs (set preserve_moved_roms=true)
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    if request.preserve_moved_roms:
        result = correlator.preserve_moved_rom_saves(
            move_to=request.move_to,
            dry_run=request.dry_run,
        )
    elif request.directory:
        result = correlator.preserve_orphaned_saves(
            rom_directory=request.directory,
            move_to=request.move_to,
            dry_run=request.dry_run,
        )
    else:
        return SavePreserveResponse(
            moved_count=0,
            moved_size_bytes=0,
            errors=["Must specify either directory or preserve_moved_roms=true"],
        )

    return SavePreserveResponse(
        moved_count=result.moved_count,
        moved_size_bytes=result.moved_size_bytes,
        errors=result.errors,
    )


@saves_router.post("/rename", response_model=SaveRenameResponse)
async def rename_saves(
    request: SaveRenameRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> SaveRenameResponse:
    """
    Rename save files to match a new ROM name.

    Useful when renaming a ROM file and wanting to keep saves associated.
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    result = correlator.rename_saves_for_rom(
        old_rom_name=request.old_rom_name,
        new_rom_name=request.new_rom_name,
        rom_directory=request.rom_directory,
        dry_run=request.dry_run,
    )

    return SaveRenameResponse(
        renamed_count=result.renamed_count,
        errors=result.errors,
    )


@saves_router.get("/for-rom", response_model=list[SaveFileResponse])
async def find_saves_for_rom(
    rom_path: str = Query(..., description="Path to the ROM file"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> list[SaveFileResponse]:
    """
    Find all save files associated with a specific ROM.

    Returns both save games (.srm, .sav, etc.) and save states (.state, etc.).
    """
    config = get_config()
    correlator = MediaCorrelator(db=db, config=config)

    save_files, state_files = correlator.find_saves_for_rom(rom_path)

    return [
        SaveFileResponse(
            path=s.path,
            save_type=s.save_type,
            rom_name=s.rom_name,
            size_bytes=s.size_bytes,
            extension=s.extension,
        )
        for s in save_files + state_files
    ]
