"""File-related API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from duper.api.auth import require_auth
from duper.api.models import FileListResponse, FileResponse
from duper.core import DuperDatabase, get_config

router = APIRouter(prefix="/api/files", tags=["files"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


@router.get("", response_model=FileListResponse)
async def list_files(
    directory: str = Query("", description="Directory to list files from"),
    duplicates_only: bool = Query(False, description="Only show potential duplicates"),
    limit: int = Query(100, ge=1, le=10000, description="Maximum number of files to return"),
    offset: int = Query(0, ge=0, description="Number of files to skip"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> FileListResponse:
    """List files in a directory or by system name."""
    if not directory:
        # Return all files
        files = db.get_files_in_directory("/")
    elif "/" not in directory:
        # Treat as system name — search in known ROM paths
        files = db.get_files_in_directory(f"/var/mnt/retronas/roms/{directory}")
        if not files:
            files = db.get_files_in_directory(directory)
    else:
        files = db.get_files_in_directory(directory)

    if duplicates_only:
        files = [f for f in files if f.is_potential_duplicate]

    total = len(files)
    files = files[offset : offset + limit]

    return FileListResponse(
        total=total,
        files=[
            FileResponse(
                filepath=f.filepath,
                filename=f.filename,
                md5=f.md5,
                size_mb=f.size_mb,
                extension=f.extension,
                is_duplicate=f.is_potential_duplicate,
                rom_serial=f.rom_serial,
                ra_supported=bool(f.ra_supported) if f.ra_supported is not None else None,
                ra_game_id=f.ra_game_id if f.ra_game_id and f.ra_game_id > 0 else None,
                ra_game_title=f.ra_game_title or None,
                ra_checked_date=f.ra_checked_date,
            )
            for f in files
        ],
    )


@router.get("/{filepath:path}", response_model=FileResponse)
async def get_file(
    filepath: str,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> FileResponse:
    """Get information about a specific file."""
    # Ensure filepath starts with /
    if not filepath.startswith("/"):
        filepath = "/" + filepath

    file = db.get_file(filepath)
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {filepath}",
        )

    return FileResponse(
        filepath=file.filepath,
        filename=file.filename,
        md5=file.md5,
        size_mb=file.size_mb,
        extension=file.extension,
        is_duplicate=file.is_potential_duplicate,
        rom_serial=file.rom_serial,
    )


@router.delete("/{filepath:path}")
async def delete_file_record(
    filepath: str,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> dict:
    """Delete a file record from the database (does not delete the actual file)."""
    # Ensure filepath starts with /
    if not filepath.startswith("/"):
        filepath = "/" + filepath

    file = db.get_file(filepath)
    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"File not found: {filepath}",
        )

    if db.delete_file(filepath):
        return {"status": "deleted", "filepath": filepath}
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete file record",
        )
