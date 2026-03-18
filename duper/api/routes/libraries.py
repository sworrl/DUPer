"""Library management API routes."""

from fastapi import APIRouter, Depends, HTTPException

from duper.api.auth import require_auth
from duper.api.models import (
    LibraryCreateRequest,
    LibraryListResponse,
    LibraryResponse,
    LibraryUpdateRequest,
)
from duper.core.config import get_config
from duper.core.database import DuperDatabase
from duper.core.library import Library

router = APIRouter(prefix="/api/libraries", tags=["libraries"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.initialize()
    return db


@router.get("", response_model=LibraryListResponse)
def list_libraries(
    device_type: str | None = None,
    _: None = Depends(require_auth),
) -> LibraryListResponse:
    """List all libraries, optionally filtered by device type."""
    db = get_db()

    if device_type == "local":
        libraries = db.get_local_libraries()
    elif device_type == "remote":
        libraries = db.get_remote_libraries()
    else:
        libraries = db.get_all_libraries()

    return LibraryListResponse(
        total=len(libraries),
        libraries=[
            LibraryResponse(
                library_id=lib.library_id,
                name=lib.name,
                root_path=lib.root_path,
                device_type=lib.device_type,
                remote_host_id=lib.remote_host_id,
                status=lib.status,
                last_scan_time=lib.last_scan_time,
                last_sync_time=lib.last_sync_time,
                total_games=lib.total_games,
                total_files=lib.total_files,
                total_size_mb=lib.total_size_mb,
                duplicate_count=lib.duplicate_count,
                created_time=lib.created_time,
                updated_time=lib.updated_time,
            )
            for lib in libraries
        ],
    )


@router.post("", response_model=LibraryResponse, status_code=201)
def create_library(
    request: LibraryCreateRequest,
    _: None = Depends(require_auth),
) -> LibraryResponse:
    """Create a new library."""
    db = get_db()

    # Check if name already exists
    existing = db.get_library_by_name(request.name)
    if existing:
        raise HTTPException(status_code=400, detail=f"Library '{request.name}' already exists")

    library = Library(
        name=request.name,
        root_path=request.root_path,
        device_type=request.device_type,
        remote_host_id=request.remote_host_id,
    )

    if not db.insert_library(library):
        raise HTTPException(status_code=500, detail="Failed to create library")

    return LibraryResponse(
        library_id=library.library_id,
        name=library.name,
        root_path=library.root_path,
        device_type=library.device_type,
        remote_host_id=library.remote_host_id,
        status=library.status,
        last_scan_time=library.last_scan_time,
        last_sync_time=library.last_sync_time,
        total_games=library.total_games,
        total_files=library.total_files,
        total_size_mb=library.total_size_mb,
        duplicate_count=library.duplicate_count,
        created_time=library.created_time,
        updated_time=library.updated_time,
    )


@router.get("/default", response_model=LibraryResponse)
def get_default_library(_: None = Depends(require_auth)) -> LibraryResponse:
    """Get or create the default 'Local Collection' library."""
    db = get_db()
    library = db.get_or_create_default_library()

    return LibraryResponse(
        library_id=library.library_id,
        name=library.name,
        root_path=library.root_path,
        device_type=library.device_type,
        remote_host_id=library.remote_host_id,
        status=library.status,
        last_scan_time=library.last_scan_time,
        last_sync_time=library.last_sync_time,
        total_games=library.total_games,
        total_files=library.total_files,
        total_size_mb=library.total_size_mb,
        duplicate_count=library.duplicate_count,
        created_time=library.created_time,
        updated_time=library.updated_time,
    )


@router.get("/{library_id}", response_model=LibraryResponse)
def get_library(
    library_id: str,
    _: None = Depends(require_auth),
) -> LibraryResponse:
    """Get a library by ID."""
    db = get_db()
    library = db.get_library(library_id)

    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    return LibraryResponse(
        library_id=library.library_id,
        name=library.name,
        root_path=library.root_path,
        device_type=library.device_type,
        remote_host_id=library.remote_host_id,
        status=library.status,
        last_scan_time=library.last_scan_time,
        last_sync_time=library.last_sync_time,
        total_games=library.total_games,
        total_files=library.total_files,
        total_size_mb=library.total_size_mb,
        duplicate_count=library.duplicate_count,
        created_time=library.created_time,
        updated_time=library.updated_time,
    )


@router.put("/{library_id}", response_model=LibraryResponse)
def update_library(
    library_id: str,
    request: LibraryUpdateRequest,
    _: None = Depends(require_auth),
) -> LibraryResponse:
    """Update a library."""
    db = get_db()
    library = db.get_library(library_id)

    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    # Update fields if provided
    if request.name is not None:
        # Check for name collision
        existing = db.get_library_by_name(request.name)
        if existing and existing.library_id != library_id:
            raise HTTPException(status_code=400, detail=f"Library '{request.name}' already exists")
        library.name = request.name

    if request.root_path is not None:
        library.root_path = request.root_path

    if request.status is not None:
        library.status = request.status

    if request.settings is not None:
        library.settings = request.settings

    if not db.update_library(library):
        raise HTTPException(status_code=500, detail="Failed to update library")

    return LibraryResponse(
        library_id=library.library_id,
        name=library.name,
        root_path=library.root_path,
        device_type=library.device_type,
        remote_host_id=library.remote_host_id,
        status=library.status,
        last_scan_time=library.last_scan_time,
        last_sync_time=library.last_sync_time,
        total_games=library.total_games,
        total_files=library.total_files,
        total_size_mb=library.total_size_mb,
        duplicate_count=library.duplicate_count,
        created_time=library.created_time,
        updated_time=library.updated_time,
    )


@router.delete("/{library_id}")
def delete_library(
    library_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Delete a library and all associated data."""
    db = get_db()
    library = db.get_library(library_id)

    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    # Don't allow deleting the default library
    if library.name == "Local Collection":
        raise HTTPException(status_code=400, detail="Cannot delete the default library")

    if not db.delete_library(library_id):
        raise HTTPException(status_code=500, detail="Failed to delete library")

    return {"status": "deleted", "library_id": library_id}


@router.get("/{library_id}/stats")
def get_library_stats(
    library_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Get detailed statistics for a library."""
    db = get_db()
    library = db.get_library(library_id)

    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    # Get game counts by system
    systems = db.get_games_by_system(library_id)

    return {
        "library_id": library_id,
        "name": library.name,
        "total_games": library.total_games,
        "total_files": library.total_files,
        "total_size_mb": library.total_size_mb,
        "duplicate_count": library.duplicate_count,
        "systems": systems,
        "last_scan_time": library.last_scan_time,
    }
