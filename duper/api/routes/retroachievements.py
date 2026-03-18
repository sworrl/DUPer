"""RetroAchievements API routes."""

from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel

from duper.api.auth import require_auth
from duper.core import DuperDatabase, get_config
from duper.core.retroachievements import (
    RetroAchievementsClient,
    RAGameInfo,
    get_ra_client,
    set_ra_client,
)

router = APIRouter(prefix="/api/ra", tags=["retroachievements"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


class RAConfigResponse(BaseModel):
    """RA configuration status response."""
    enabled: bool
    username: str
    has_api_key: bool
    ra_score_bonus: int
    verify_on_scan: bool


class RAConfigUpdateRequest(BaseModel):
    """Request to update RA configuration."""
    enabled: bool | None = None
    username: str | None = None
    api_key: str | None = None
    ra_score_bonus: int | None = None
    verify_on_scan: bool | None = None


class RAVerifyResponse(BaseModel):
    """Response from hash verification."""
    md5: str
    ra_supported: bool
    game_id: int | None = None
    game_title: str | None = None
    console_name: str | None = None
    achievement_count: int | None = None


class RAVerifyBatchResponse(BaseModel):
    """Response from batch verification."""
    verified_count: int
    supported_count: int
    not_supported_count: int
    results: list[RAVerifyResponse]


class RAStatsResponse(BaseModel):
    """RA verification statistics."""
    ra_supported: int
    ra_not_supported: int
    ra_unverified: int
    ra_enabled: bool


@router.get("/config", response_model=RAConfigResponse)
async def get_ra_config(
    _: None = Depends(require_auth),
) -> RAConfigResponse:
    """Get RetroAchievements configuration status."""
    config = get_config()
    ra = config.retroachievements

    return RAConfigResponse(
        enabled=ra.enabled,
        username=ra.username,
        has_api_key=bool(ra.api_key),
        ra_score_bonus=ra.ra_score_bonus,
        verify_on_scan=ra.verify_on_scan,
    )


@router.put("/config", response_model=RAConfigResponse)
async def update_ra_config(
    request: RAConfigUpdateRequest,
    _: None = Depends(require_auth),
) -> RAConfigResponse:
    """Update RetroAchievements configuration."""
    config = get_config()
    ra = config.retroachievements

    if request.enabled is not None:
        ra.enabled = request.enabled
    if request.username is not None:
        ra.username = request.username
    if request.api_key is not None:
        ra.api_key = request.api_key
    if request.ra_score_bonus is not None:
        ra.ra_score_bonus = request.ra_score_bonus
    if request.verify_on_scan is not None:
        ra.verify_on_scan = request.verify_on_scan

    # Save config
    config.save()

    # Update the RA client if credentials changed
    if ra.enabled and ra.username and ra.api_key:
        set_ra_client(RetroAchievementsClient(ra.username, ra.api_key))
    else:
        set_ra_client(None)

    return RAConfigResponse(
        enabled=ra.enabled,
        username=ra.username,
        has_api_key=bool(ra.api_key),
        ra_score_bonus=ra.ra_score_bonus,
        verify_on_scan=ra.verify_on_scan,
    )


@router.get("/verify/{md5}", response_model=RAVerifyResponse)
async def verify_hash(
    md5: str,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> RAVerifyResponse:
    """Verify a single MD5 hash against RetroAchievements."""
    config = get_config()
    ra_config = config.retroachievements

    if not ra_config.enabled:
        raise HTTPException(status_code=400, detail="RetroAchievements integration is not enabled")

    if not ra_config.username or not ra_config.api_key:
        raise HTTPException(status_code=400, detail="RetroAchievements credentials not configured")

    # Get or create RA client
    client = get_ra_client(ra_config.username, ra_config.api_key)
    if not client:
        raise HTTPException(status_code=500, detail="Failed to create RA client")

    # Look up the hash
    game_info = client.get_game_by_hash(md5.lower())

    # Defensive check: ensure game_info is RAGameInfo, not a bool
    if game_info is not None and hasattr(game_info, 'game_id'):
        # Update database
        db.update_ra_status_by_md5(
            md5=md5.lower(),
            ra_supported=True,
            ra_game_id=game_info.game_id,
            ra_game_title=game_info.title,
        )

        return RAVerifyResponse(
            md5=md5.lower(),
            ra_supported=True,
            game_id=game_info.game_id,
            game_title=game_info.title,
            console_name=game_info.console_name,
            achievement_count=game_info.achievement_count,
        )
    else:
        # Mark as verified but not supported (ra_game_id = -1)
        db.update_ra_status_by_md5(
            md5=md5.lower(),
            ra_supported=False,
            ra_game_id=-1,  # -1 indicates checked but not supported
            ra_game_title="",
        )

        return RAVerifyResponse(
            md5=md5.lower(),
            ra_supported=False,
        )


@router.post("/verify-directory", response_model=RAVerifyBatchResponse)
async def verify_directory(
    directory: str = Query(..., description="Directory to verify hashes for"),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> RAVerifyBatchResponse:
    """Verify all unverified hashes in a directory against RetroAchievements."""
    config = get_config()
    ra_config = config.retroachievements

    if not ra_config.enabled:
        raise HTTPException(status_code=400, detail="RetroAchievements integration is not enabled")

    if not ra_config.username or not ra_config.api_key:
        raise HTTPException(status_code=400, detail="RetroAchievements credentials not configured")

    # Get unverified hashes
    unverified = db.get_unverified_hashes(directory)

    if not unverified:
        return RAVerifyBatchResponse(
            verified_count=0,
            supported_count=0,
            not_supported_count=0,
            results=[],
        )

    # Get or create RA client
    client = get_ra_client(ra_config.username, ra_config.api_key)
    if not client:
        raise HTTPException(status_code=500, detail="Failed to create RA client")

    # Verify hashes
    results = []
    supported_count = 0
    not_supported_count = 0

    for md5 in unverified:
        game_info = client.get_game_by_hash(md5)

        # Defensive check: ensure game_info is RAGameInfo, not a bool
        if game_info is not None and hasattr(game_info, 'game_id'):
            db.update_ra_status_by_md5(
                md5=md5,
                ra_supported=True,
                ra_game_id=game_info.game_id,
                ra_game_title=game_info.title,
            )
            supported_count += 1
            results.append(RAVerifyResponse(
                md5=md5,
                ra_supported=True,
                game_id=game_info.game_id,
                game_title=game_info.title,
                console_name=game_info.console_name,
                achievement_count=game_info.achievement_count,
            ))
        else:
            db.update_ra_status_by_md5(
                md5=md5,
                ra_supported=False,
                ra_game_id=-1,
                ra_game_title="",
            )
            not_supported_count += 1
            results.append(RAVerifyResponse(
                md5=md5,
                ra_supported=False,
            ))

    return RAVerifyBatchResponse(
        verified_count=len(results),
        supported_count=supported_count,
        not_supported_count=not_supported_count,
        results=results,
    )


_ra_verify_job = {"active": False, "verified": 0, "supported": 0, "total": 0}


@router.post("/verify-unchecked")
async def verify_unchecked(
    limit: int = 100,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Verify all unchecked files against RetroAchievements. Runs in background.

    Only processes files that have an MD5 hash and haven't been RA-checked yet
    (ra_checked_date IS NULL). Skips already-verified files automatically.
    """
    if _ra_verify_job["active"]:
        return _ra_verify_job

    config = get_config()
    ra_config = config.retroachievements

    if not ra_config.enabled or not ra_config.username or not ra_config.api_key:
        return {"error": "RetroAchievements not configured"}

    # Count unchecked files
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT COUNT(*) FROM files
            WHERE md5 IS NOT NULL AND md5 != ''
            AND ra_checked_date IS NULL
        """)
        total = cursor.fetchone()[0]

    if total == 0:
        return {"status": "all_verified", "total": 0}

    actual_limit = min(limit, total)
    _ra_verify_job.update({"active": True, "verified": 0, "supported": 0, "total": actual_limit})

    def _run_verify():
        from duper.core.database import DuperDatabase as DB
        from duper.core.retroachievements import get_ra_client as _get_client
        import time

        _db = DB(config.paths.database)
        _db.connect()
        _db.initialize()

        client = _get_client(ra_config.username, ra_config.api_key)
        if not client:
            _ra_verify_job["active"] = False
            return

        with _db.cursor() as cursor:
            cursor.execute("""
                SELECT md5 FROM files
                WHERE md5 IS NOT NULL AND md5 != ''
                AND ra_checked_date IS NULL
                LIMIT ?
            """, (actual_limit,))
            hashes = [row["md5"] for row in cursor.fetchall()]

        for md5 in hashes:
            try:
                game_info = client.get_game_by_hash(md5)
                if game_info is not None and hasattr(game_info, 'game_id'):
                    _db.update_ra_status_by_md5(md5=md5, ra_supported=True,
                        ra_game_id=game_info.game_id, ra_game_title=game_info.title)
                    _ra_verify_job["supported"] += 1
                else:
                    _db.update_ra_status_by_md5(md5=md5, ra_supported=False,
                        ra_game_id=-1, ra_game_title="")
                _ra_verify_job["verified"] += 1
                time.sleep(0.15)  # Rate limit
            except Exception:
                _ra_verify_job["verified"] += 1

        _ra_verify_job["active"] = False
        _db.close()

    background_tasks.add_task(_run_verify)
    return {"status": "started", "total": actual_limit}


@router.get("/verify-unchecked/status")
async def verify_unchecked_status():
    """Check status of the background RA verification job."""
    return _ra_verify_job


@router.get("/stats", response_model=RAStatsResponse)
async def get_ra_stats(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> RAStatsResponse:
    """Get RetroAchievements verification statistics."""
    config = get_config()
    stats = db.get_ra_stats()

    return RAStatsResponse(
        ra_supported=stats["ra_supported"],
        ra_not_supported=stats["ra_not_supported"],
        ra_unverified=stats["ra_unverified"],
        ra_enabled=config.retroachievements.enabled,
    )


@router.get("/game/{game_id}")
async def get_game_info(
    game_id: int,
    _: None = Depends(require_auth),
):
    """Get game information from RetroAchievements by game ID."""
    config = get_config()
    ra_config = config.retroachievements

    if not ra_config.enabled:
        raise HTTPException(status_code=400, detail="RetroAchievements integration is not enabled")

    if not ra_config.username or not ra_config.api_key:
        raise HTTPException(status_code=400, detail="RetroAchievements credentials not configured")

    client = get_ra_client(ra_config.username, ra_config.api_key)
    if not client:
        raise HTTPException(status_code=500, detail="Failed to create RA client")

    # Get valid hashes for this game
    hashes = client.get_game_hashes(game_id)

    return {
        "game_id": game_id,
        "valid_hashes": hashes,
        "hash_count": len(hashes),
    }
