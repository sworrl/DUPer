"""Scan queue management API routes."""

from fastapi import APIRouter, Depends, HTTPException

from duper.api.auth import require_auth
from duper.api.models import (
    QueueAddRequest,
    QueueItemResponse,
    QueueReorderRequest,
    QueueResponse,
)
from duper.core.config import get_config
from duper.core.database import DuperDatabase
from duper.core.library import ScanQueueItem

router = APIRouter(prefix="/api/queue", tags=["queue"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.initialize()
    return db


def item_to_response(item: ScanQueueItem) -> QueueItemResponse:
    """Convert ScanQueueItem to response model."""
    return QueueItemResponse(
        queue_id=item.queue_id,
        library_id=item.library_id,
        directory=item.directory,
        status=item.status,
        priority=item.priority,
        position=item.position,
        queued_time=item.queued_time,
        started_time=item.started_time,
        completed_time=item.completed_time,
        total_files=item.total_files,
        processed_files=item.processed_files,
        current_file=item.current_file,
        percent_complete=item.percent_complete,
        files_processed=item.files_processed,
        errors=item.errors,
    )


@router.get("", response_model=QueueResponse)
def get_queue(
    include_completed: bool = False,
    _: None = Depends(require_auth),
) -> QueueResponse:
    """Get the scan queue."""
    db = get_db()
    items = db.get_scan_queue(include_completed=include_completed)

    running = None
    pending = []
    completed = []

    for item in items:
        response = item_to_response(item)
        if item.status == "running":
            running = response
        elif item.status == "pending":
            pending.append(response)
        else:
            completed.append(response)

    return QueueResponse(
        total=len(items),
        running=running,
        pending=pending,
        completed=completed,
    )


@router.post("", response_model=QueueItemResponse, status_code=201)
def add_to_queue(
    request: QueueAddRequest,
    _: None = Depends(require_auth),
) -> QueueItemResponse:
    """Add a directory to the scan queue."""
    db = get_db()

    # Verify library exists
    library = db.get_library(request.library_id)
    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    # Check if directory is already in queue (pending or running)
    existing_items = db.get_scan_queue()
    for item in existing_items:
        if item.directory == request.directory and item.status in ("pending", "running"):
            raise HTTPException(
                status_code=400,
                detail=f"Directory already in queue (status: {item.status})"
            )

    item = db.add_to_queue(
        library_id=request.library_id,
        directory=request.directory,
        priority=request.priority,
        full_scan=request.full_scan,
        scan_media=request.scan_media,
        scan_ra=request.scan_ra,
    )

    return item_to_response(item)


@router.get("/{queue_id}", response_model=QueueItemResponse)
def get_queue_item(
    queue_id: str,
    _: None = Depends(require_auth),
) -> QueueItemResponse:
    """Get a specific queue item."""
    db = get_db()
    item = db.get_queue_item(queue_id)

    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    return item_to_response(item)


@router.put("/{queue_id}/position", response_model=QueueItemResponse)
def reorder_queue_item(
    queue_id: str,
    request: QueueReorderRequest,
    _: None = Depends(require_auth),
) -> QueueItemResponse:
    """Move a queue item to a new position."""
    db = get_db()
    item = db.get_queue_item(queue_id)

    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    if item.status != "pending":
        raise HTTPException(status_code=400, detail="Can only reorder pending items")

    if not db.reorder_queue_item(queue_id, request.new_position):
        raise HTTPException(status_code=500, detail="Failed to reorder queue item")

    # Get updated item
    item = db.get_queue_item(queue_id)
    return item_to_response(item)


@router.delete("/{queue_id}")
def remove_from_queue(
    queue_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Remove an item from the queue."""
    db = get_db()
    item = db.get_queue_item(queue_id)

    if not item:
        raise HTTPException(status_code=404, detail="Queue item not found")

    if item.status == "running":
        raise HTTPException(status_code=400, detail="Cannot remove a running scan")

    if not db.delete_queue_item(queue_id):
        raise HTTPException(status_code=500, detail="Failed to remove queue item")

    return {"status": "removed", "queue_id": queue_id}


@router.post("/clear-completed")
def clear_completed(
    _: None = Depends(require_auth),
) -> dict:
    """Clear all completed and failed queue items."""
    db = get_db()
    count = db.clear_completed_queue_items()
    return {"status": "cleared", "count": count}


@router.get("/next", response_model=QueueItemResponse | None)
def get_next_item(
    _: None = Depends(require_auth),
) -> QueueItemResponse | None:
    """Get the next item to be processed."""
    db = get_db()
    item = db.get_next_queue_item()

    if not item:
        return None

    return item_to_response(item)


@router.get("/current", response_model=QueueItemResponse | None)
def get_current_item(
    _: None = Depends(require_auth),
) -> QueueItemResponse | None:
    """Get the currently running queue item."""
    db = get_db()
    item = db.get_running_queue_item()

    if not item:
        return None

    return item_to_response(item)
