"""Duplicate-related API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from duper.api.auth import require_auth
from duper.api.models import (
    DuplicateGroupResponse,
    DuplicateSummaryResponse,
    MovedFilesSummaryResponse,
    ProcessDuplicatesRequest,
    ProcessResultResponse,
    RestoreFileRequest,
    RestoreResultResponse,
)
from duper.core import Deduper, DuperDatabase, get_config

router = APIRouter(prefix="/api/duplicates", tags=["duplicates"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


@router.get("", response_model=DuplicateSummaryResponse)
async def get_duplicates(
    directory: str = Query(..., description="Directory to get duplicates from"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> DuplicateSummaryResponse:
    """Get all duplicate groups in a directory."""
    config = get_config()
    deduper = Deduper(db=db, config=config)

    summary = deduper.get_duplicate_summary(directory)

    return DuplicateSummaryResponse(
        total_groups=summary["total_groups"],
        total_duplicate_files=summary["total_duplicate_files"],
        files_to_remove=summary["files_to_remove"],
        wasted_space_mb=summary["wasted_space_mb"],
        groups=[
            DuplicateGroupResponse(
                md5=g["md5"],
                rom_serial=g.get("rom_serial", ""),
                files=[
                    {
                        "filepath": f["filepath"],
                        "filename": f["filename"],
                        "size_mb": f["size_mb"],
                        "score": f["score"],
                        "rom_serial": f.get("rom_serial", ""),
                        "ra_supported": f.get("ra_supported", False),
                        "ra_game_id": f.get("ra_game_id", 0),
                        "ra_game_title": f.get("ra_game_title", ""),
                        "ra_checked_date": f.get("ra_checked_date"),
                    }
                    for f in g["files"]
                ],
                recommended_keep=g["recommended_keep"],
                file_count=g["file_count"],
            )
            for g in summary["groups"]
        ],
    )


@router.post("/process", response_model=ProcessResultResponse)
async def process_duplicates(
    request: ProcessDuplicatesRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> ProcessResultResponse:
    """
    Process duplicates by archiving or deleting them.

    The file with the highest score in each group is kept (unless overridden).

    Actions:
    - "archive": Move duplicates to archive location (default)
    - "delete": Permanently remove duplicates
    """
    config = get_config()
    deduper = Deduper(db=db, config=config)

    result = deduper.process_duplicates(
        directory=request.directory,
        action=request.action,
        archive_location=request.archive_location,
        dry_run=request.dry_run,
        group_hashes=request.group_hashes,
        keep_overrides=request.keep_overrides,
    )

    return ProcessResultResponse(
        action=result.action,
        processed_count=result.processed_count,
        archived_count=result.archived_count,
        deleted_count=result.deleted_count,
        space_freed_mb=result.space_freed_mb,
        media_processed_count=result.media_processed_count,
        media_space_freed_mb=result.media_space_freed_mb,
        errors=result.errors,
        processed_files=result.processed_files,
    )


@router.get("/moved", response_model=MovedFilesSummaryResponse)
async def get_moved_files(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> MovedFilesSummaryResponse:
    """Get all files that have been moved."""
    config = get_config()
    deduper = Deduper(db=db, config=config)

    summary = deduper.get_moved_files_summary()

    return MovedFilesSummaryResponse(
        total_moved=summary["total_moved"],
        total_size_mb=summary["total_size_mb"],
        files=[
            {
                "move_id": f["move_id"],
                "original_filepath": f["original_filepath"],
                "moved_to_path": f["moved_to_path"],
                "moved_time": f["moved_time"],
                "filename": f["filename"],
                "size_mb": f["size_mb"],
                "md5": f["md5"],
                "rom_serial": f["rom_serial"],
                "reason": f["reason"],
            }
            for f in summary["files"]
        ],
    )


@router.post("/restore", response_model=RestoreResultResponse)
async def restore_file(
    request: RestoreFileRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> RestoreResultResponse:
    """Restore a single moved file."""
    config = get_config()
    deduper = Deduper(db=db, config=config)

    result = deduper.restore_file(request.move_id)

    if result.restored_count == 0 and not result.errors:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No moved file with ID {request.move_id}",
        )

    return RestoreResultResponse(
        restored_count=result.restored_count,
        errors=result.errors,
    )


@router.post("/restore-all", response_model=RestoreResultResponse)
async def restore_all_files(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> RestoreResultResponse:
    """Restore all moved files."""
    config = get_config()
    deduper = Deduper(db=db, config=config)

    result = deduper.restore_all_files()

    return RestoreResultResponse(
        restored_count=result.restored_count,
        errors=result.errors,
    )


@router.get("/cross-platform")
async def get_cross_platform(
    directory: str = Query(..., description="Directory to check for cross-platform games"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> dict:
    """
    Get cross-platform groups -- same game found on multiple systems.

    These are NOT duplicates; they are flagged for informational purposes
    so the user can review multi-platform releases.
    """
    config = get_config()
    deduper = Deduper(db=db, config=config)

    groups = deduper.get_cross_platform_groups(directory)

    return {
        "total_groups": len(groups),
        "groups": [g.to_dict() for g in groups],
    }
