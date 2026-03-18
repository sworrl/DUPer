"""ScreenScraper API routes for game metadata and media scraping."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

from duper.api.auth import require_auth
from duper.core import DuperDatabase, get_config
from duper.core.screenscraper import (
    ScreenScraperClient,
    SSGameInfo,
    get_ss_client,
    set_ss_client,
    get_system_id,
)

router = APIRouter(prefix="/api/ss", tags=["screenscraper"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


# ---------- Request/Response Models ----------


class SSConfigResponse(BaseModel):
    """ScreenScraper configuration status response."""

    enabled: bool
    username: str
    has_password: bool
    has_dev_credentials: bool
    preferred_region: str
    preferred_language: str
    download_box_art: bool
    download_screenshot: bool
    download_wheel: bool
    download_video: bool
    media_path: str


class SSConfigUpdateRequest(BaseModel):
    """Request to update ScreenScraper configuration."""

    enabled: bool | None = None
    username: str | None = None
    password: str | None = None
    dev_id: str | None = None
    dev_password: str | None = None
    preferred_region: str | None = None
    preferred_language: str | None = None
    download_box_art: bool | None = None
    download_screenshot: bool | None = None
    download_wheel: bool | None = None
    download_video: bool | None = None
    media_path: str | None = None


class SSConnectionTestResponse(BaseModel):
    """Response from connection test."""

    success: bool
    error: str | None = None
    username: str | None = None
    level: str | None = None
    requests_today: int | None = None
    requests_max: int | None = None
    threads: int | None = None


class SSGameInfoResponse(BaseModel):
    """Game information response."""

    game_id: int
    rom_id: int
    title: str
    region: str
    system_id: int
    system_name: str
    publisher: str
    developer: str
    genre: str
    players: str
    rating: float
    release_date: str
    description: str
    media: dict
    hash_match: str


class SSScrapeRequest(BaseModel):
    """Request to scrape a ROM file."""

    filepath: str
    md5: str | None = None
    system_hint: str | None = None


class SSScrapeResponse(BaseModel):
    """Response from scraping a ROM."""

    filepath: str
    md5: str
    found: bool
    game_info: SSGameInfoResponse | None = None
    error: str | None = None


class SSSystemResponse(BaseModel):
    """System information response."""

    id: int
    name: str
    company: str
    type: str


# ---------- Config Endpoints ----------


@router.get("/config", response_model=SSConfigResponse)
async def get_ss_config(
    _: None = Depends(require_auth),
) -> SSConfigResponse:
    """Get ScreenScraper configuration status."""
    config = get_config()
    ss = config.screenscraper

    return SSConfigResponse(
        enabled=ss.enabled,
        username=ss.username,
        has_password=bool(ss.password),
        has_dev_credentials=bool(ss.dev_id and ss.dev_password),
        preferred_region=ss.preferred_region,
        preferred_language=ss.preferred_language,
        download_box_art=ss.download_box_art,
        download_screenshot=ss.download_screenshot,
        download_wheel=ss.download_wheel,
        download_video=ss.download_video,
        media_path=ss.media_path,
    )


@router.put("/config", response_model=SSConfigResponse)
async def update_ss_config(
    request: SSConfigUpdateRequest,
    _: None = Depends(require_auth),
) -> SSConfigResponse:
    """Update ScreenScraper configuration."""
    config = get_config()
    ss = config.screenscraper

    if request.enabled is not None:
        ss.enabled = request.enabled
    if request.username is not None:
        ss.username = request.username
    if request.password is not None:
        ss.password = request.password
    if request.dev_id is not None:
        ss.dev_id = request.dev_id
    if request.dev_password is not None:
        ss.dev_password = request.dev_password
    if request.preferred_region is not None:
        ss.preferred_region = request.preferred_region
    if request.preferred_language is not None:
        ss.preferred_language = request.preferred_language
    if request.download_box_art is not None:
        ss.download_box_art = request.download_box_art
    if request.download_screenshot is not None:
        ss.download_screenshot = request.download_screenshot
    if request.download_wheel is not None:
        ss.download_wheel = request.download_wheel
    if request.download_video is not None:
        ss.download_video = request.download_video
    if request.media_path is not None:
        ss.media_path = request.media_path

    # Save config
    config.save()

    # Update the SS client if credentials changed
    if ss.enabled and ss.username and ss.password:
        set_ss_client(
            ScreenScraperClient(
                dev_id=ss.dev_id,
                dev_password=ss.dev_password,
                user_id=ss.username,
                user_password=ss.password,
            )
        )
    else:
        set_ss_client(None)

    return SSConfigResponse(
        enabled=ss.enabled,
        username=ss.username,
        has_password=bool(ss.password),
        has_dev_credentials=bool(ss.dev_id and ss.dev_password),
        preferred_region=ss.preferred_region,
        preferred_language=ss.preferred_language,
        download_box_art=ss.download_box_art,
        download_screenshot=ss.download_screenshot,
        download_wheel=ss.download_wheel,
        download_video=ss.download_video,
        media_path=ss.media_path,
    )


# ---------- Connection Test ----------


@router.get("/test", response_model=SSConnectionTestResponse)
async def test_connection(
    _: None = Depends(require_auth),
) -> SSConnectionTestResponse:
    """Test connection to ScreenScraper and validate credentials."""
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled:
        return SSConnectionTestResponse(
            success=False,
            error="ScreenScraper integration is not enabled",
        )

    if not ss.username or not ss.password:
        return SSConnectionTestResponse(
            success=False,
            error="ScreenScraper credentials not configured",
        )

    # Get or create client
    client = get_ss_client(
        username=ss.username,
        password=ss.password,
        dev_id=ss.dev_id,
        dev_password=ss.dev_password,
    )

    if not client:
        return SSConnectionTestResponse(
            success=False,
            error="Failed to create ScreenScraper client",
        )

    # Test the connection
    result = client.test_connection()

    return SSConnectionTestResponse(
        success=result.get("success", False),
        error=result.get("error"),
        username=result.get("username"),
        level=result.get("level"),
        requests_today=result.get("requests_today"),
        requests_max=result.get("requests_max"),
        threads=result.get("threads"),
    )


# ---------- Scraping Endpoints ----------


@router.post("/scrape", response_model=SSScrapeResponse)
async def scrape_rom(
    request: SSScrapeRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> SSScrapeResponse:
    """Scrape game info for a single ROM file."""
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled:
        raise HTTPException(status_code=400, detail="ScreenScraper integration is not enabled")

    if not ss.username or not ss.password:
        raise HTTPException(status_code=400, detail="ScreenScraper credentials not configured")

    # Get or create client
    client = get_ss_client(
        username=ss.username,
        password=ss.password,
        dev_id=ss.dev_id,
        dev_password=ss.dev_password,
    )

    if not client:
        raise HTTPException(status_code=500, detail="Failed to create ScreenScraper client")

    # Scrape the ROM
    result = client.scrape_rom(request.filepath, md5=request.md5 or "")

    game_info_response = None
    if result.found and result.game_info:
        gi = result.game_info
        game_info_response = SSGameInfoResponse(
            game_id=gi.game_id,
            rom_id=gi.rom_id,
            title=gi.title,
            region=gi.region,
            system_id=gi.system_id,
            system_name=gi.system_name,
            publisher=gi.publisher,
            developer=gi.developer,
            genre=gi.genre,
            players=gi.players,
            rating=gi.rating,
            release_date=gi.release_date,
            description=gi.description,
            media=gi.media.to_dict(),
            hash_match=gi.hash_match,
        )

    return SSScrapeResponse(
        filepath=result.filepath,
        md5=result.md5,
        found=result.found,
        game_info=game_info_response,
        error=result.error,
    )


@router.get("/lookup/{md5}", response_model=SSScrapeResponse)
async def lookup_by_hash(
    md5: str,
    _: None = Depends(require_auth),
) -> SSScrapeResponse:
    """Look up game info by MD5 hash."""
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled:
        raise HTTPException(status_code=400, detail="ScreenScraper integration is not enabled")

    if not ss.username or not ss.password:
        raise HTTPException(status_code=400, detail="ScreenScraper credentials not configured")

    client = get_ss_client(
        username=ss.username,
        password=ss.password,
        dev_id=ss.dev_id,
        dev_password=ss.dev_password,
    )

    if not client:
        raise HTTPException(status_code=500, detail="Failed to create ScreenScraper client")

    # Look up by hash
    game_info = client.get_game_by_hash(md5=md5.lower())

    if game_info:
        return SSScrapeResponse(
            filepath="",
            md5=md5.lower(),
            found=True,
            game_info=SSGameInfoResponse(
                game_id=game_info.game_id,
                rom_id=game_info.rom_id,
                title=game_info.title,
                region=game_info.region,
                system_id=game_info.system_id,
                system_name=game_info.system_name,
                publisher=game_info.publisher,
                developer=game_info.developer,
                genre=game_info.genre,
                players=game_info.players,
                rating=game_info.rating,
                release_date=game_info.release_date,
                description=game_info.description,
                media=game_info.media.to_dict(),
                hash_match=game_info.hash_match,
            ),
        )
    else:
        return SSScrapeResponse(
            filepath="",
            md5=md5.lower(),
            found=False,
        )


# ---------- Systems Endpoint ----------


@router.get("/systems", response_model=list[SSSystemResponse])
async def get_systems(
    _: None = Depends(require_auth),
) -> list[SSSystemResponse]:
    """Get list of all supported systems from ScreenScraper."""
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled:
        raise HTTPException(status_code=400, detail="ScreenScraper integration is not enabled")

    if not ss.username or not ss.password:
        raise HTTPException(status_code=400, detail="ScreenScraper credentials not configured")

    client = get_ss_client(
        username=ss.username,
        password=ss.password,
        dev_id=ss.dev_id,
        dev_password=ss.dev_password,
    )

    if not client:
        raise HTTPException(status_code=500, detail="Failed to create ScreenScraper client")

    systems = client.get_systems()

    return [
        SSSystemResponse(
            id=s["id"],
            name=s["name"],
            company=s["company"],
            type=s["type"],
        )
        for s in systems
    ]


@router.get("/system-id")
async def get_system_id_by_name(
    name: str = Query(..., description="System name to look up"),
    _: None = Depends(require_auth),
) -> dict:
    """Get ScreenScraper system ID from common system name."""
    system_id = get_system_id(name)
    return {
        "name": name,
        "system_id": system_id,
        "found": system_id > 0,
    }


# ---------- Batch Scrape for Missing Media ----------

_scrape_jobs: dict[str, dict] = {}


@router.post("/scrape-missing")
async def scrape_missing_media(
    directory: str = "",
    limit: int = 50,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> dict:
    """Scrape media for all ROMs in the DB that have no media.

    Finds ROMs without media entries, looks them up on ScreenScraper by MD5,
    and downloads box art + screenshots. Tied to discovery — only scrapes
    games already in the database (from scan or acquisition).
    """
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled or not ss.username or not ss.password:
        return {"error": "ScreenScraper not configured", "scraped": 0}

    roms = db.get_files_without_media(directory=directory, limit=limit)
    if not roms:
        return {"status": "nothing_to_scrape", "scraped": 0, "reason": "all ROMs have media"}

    files_to_scrape = [(r["filepath"], r["md5"]) for r in roms if r["md5"]]
    if not files_to_scrape:
        return {"status": "nothing_to_scrape", "scraped": 0, "reason": "no MD5 hashes available"}

    import uuid
    job_id = str(uuid.uuid4())

    # Determine media save path
    media_dir = ss.media_path
    if not media_dir and directory:
        from pathlib import Path as P
        parent = str(P(directory).parent)
        media_dir = str(P(parent) / "tools" / "downloaded_media")

    _scrape_jobs[job_id] = {
        "status": "running",
        "total": len(files_to_scrape),
        "completed": 0,
        "found": 0,
    }

    def _run_scrape():
        client = get_ss_client(
            username=ss.username,
            password=ss.password,
            dev_id=ss.dev_id,
            dev_password=ss.dev_password,
        )
        if not client:
            _scrape_jobs[job_id]["status"] = "error"
            return

        def _cb(result, completed, total):
            _scrape_jobs[job_id]["completed"] = completed
            if result and result.found:
                _scrape_jobs[job_id]["found"] = _scrape_jobs[job_id].get("found", 0) + 1

        try:
            client.batch_scrape(
                files=files_to_scrape,
                media_dir=media_dir,
                download_art=True,
                callback=_cb,
            )
            _scrape_jobs[job_id]["status"] = "completed"

            # Re-correlate media
            from duper.core import MediaCorrelator
            try:
                correlator = MediaCorrelator(db=db, config=config)
                if directory:
                    correlator.scan_media_for_directory(directory)
            except Exception:
                pass
        except Exception as e:
            _scrape_jobs[job_id]["status"] = f"error: {e}"

    background_tasks.add_task(_run_scrape)

    return {
        "job_id": job_id,
        "status": "started",
        "total": len(files_to_scrape),
        "skipped_no_md5": len(roms) - len(files_to_scrape),
    }


@router.get("/scrape-missing/{job_id}")
async def scrape_missing_status(
    job_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Check status of a batch scrape job."""
    if job_id not in _scrape_jobs:
        return {"error": "Job not found"}
    return _scrape_jobs[job_id]


@router.get("/missing-media-count")
async def missing_media_count(
    directory: str = "",
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> dict:
    """Get count of ROMs that have no media (candidates for scraping)."""
    roms = db.get_files_without_media(directory=directory)
    with_md5 = sum(1 for r in roms if r["md5"])
    return {
        "total_without_media": len(roms),
        "with_md5": with_md5,
        "without_md5": len(roms) - with_md5,
    }
