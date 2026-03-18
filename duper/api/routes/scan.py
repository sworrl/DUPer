"""Scan-related API routes."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel

from duper.api.auth import require_auth
from duper.api.models import (
    ScanProgressResponse,
    ScanRequest,
    ScanResultResponse,
)
from duper.core import (
    Deduper,
    DuperDatabase,
    MediaCorrelator,
    Scanner,
    ScanProgress,
    detect_systems,
    get_config,
)

router = APIRouter(prefix="/api/scan", tags=["scan"])


# Response models for suggestions
class RemovableDevice(BaseModel):
    """Information about a removable storage device."""
    name: str
    path: str
    label: str | None = None
    size_bytes: int = 0
    size_human: str = ""
    fstype: str = ""
    is_mounted: bool = False


class ScanHistoryItem(BaseModel):
    """A previously scanned directory."""
    path: str
    last_scanned: str | None = None
    file_count: int = 0
    duplicate_count: int = 0


class ScanSuggestionsResponse(BaseModel):
    """Response with scan suggestions."""
    history: list[ScanHistoryItem]
    removable_devices: list[RemovableDevice]
    common_paths: list[str]


def _get_removable_devices() -> list[RemovableDevice]:
    """Detect removable storage devices (USB, SD cards, etc.)."""
    devices = []

    try:
        # Use lsblk to find removable devices
        result = subprocess.run(
            ["lsblk", "-J", "-o", "NAME,SIZE,FSTYPE,MOUNTPOINT,LABEL,RM,TYPE,HOTPLUG"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            return devices

        import json
        data = json.loads(result.stdout)

        for device in data.get("blockdevices", []):
            # Check partitions
            for child in device.get("children", []):
                # Look for mounted partitions on removable/hotplug devices
                is_removable = device.get("rm") == "1" or device.get("hotplug") == "1"
                mountpoint = child.get("mountpoint")
                fstype = child.get("fstype") or ""

                # Skip if not a filesystem we care about
                if fstype not in ("ext4", "ext3", "vfat", "exfat", "ntfs", "btrfs"):
                    continue

                # Skip boot/system partitions
                if mountpoint and ("/boot" in mountpoint or mountpoint == "/"):
                    continue

                # Parse size (e.g., "477.5G" -> bytes)
                size_str = child.get("size", "0")
                size_bytes = 0
                try:
                    if size_str.endswith("G"):
                        size_bytes = int(float(size_str[:-1]) * 1024 * 1024 * 1024)
                    elif size_str.endswith("M"):
                        size_bytes = int(float(size_str[:-1]) * 1024 * 1024)
                    elif size_str.endswith("T"):
                        size_bytes = int(float(size_str[:-1]) * 1024 * 1024 * 1024 * 1024)
                except ValueError:
                    pass

                if is_removable or (mountpoint and "/media" in mountpoint) or (mountpoint and "/mnt" in mountpoint):
                    devices.append(RemovableDevice(
                        name=f"/dev/{child.get('name', '')}",
                        path=mountpoint or "",
                        label=child.get("label"),
                        size_bytes=size_bytes,
                        size_human=size_str,
                        fstype=fstype,
                        is_mounted=bool(mountpoint),
                    ))

    except Exception as e:
        print(f"Error detecting removable devices: {e}")

    return devices


def _get_common_paths() -> list[str]:
    """Get common ROM directory paths that exist on this system."""
    common = []
    home = Path.home()

    candidates = [
        home / "ROMs",
        home / "roms",
        home / "Games",
        home / "Emulation" / "roms",
        home / "RetroPie" / "roms",
        home / ".local" / "share" / "retroarch" / "roms",
        Path("/media"),
        Path("/mnt"),
    ]

    for path in candidates:
        if path.exists() and path.is_dir():
            common.append(str(path))

    return common


# Store for ongoing scan progress (in production, use Redis or similar)
_scan_progress: dict[str, ScanProgress] = {}
_scan_results: dict[str, dict[str, Any]] = {}


def _auto_scrape_new_roms(
    db: DuperDatabase,
    directory: str,
    scan_id: str,
) -> dict[str, int]:
    """Auto-scrape media for ROMs that have no media after scan + media correlation.

    Only runs if ScreenScraper is configured. Returns stats dict.
    """
    config = get_config()
    ss_config = config.screenscraper

    if not ss_config.enabled or not ss_config.username or not ss_config.password:
        return {"skipped": 0, "scraped": 0, "found": 0, "reason": "screenscraper_not_configured"}

    # Find ROMs without any media
    roms_without_media = db.get_files_without_media(directory=directory)
    if not roms_without_media:
        return {"skipped": 0, "scraped": 0, "found": 0, "reason": "all_have_media"}

    _scan_progress[scan_id] = ScanProgress(
        status="auto-scraping",
        current_step=f"Scraping media for {len(roms_without_media)} games without art...",
        current_phase="auto_scrape",
    )

    from duper.core.screenscraper import get_ss_client

    client = get_ss_client(
        username=ss_config.username,
        password=ss_config.password,
        dev_id=ss_config.dev_id,
        dev_password=ss_config.dev_password,
    )
    if not client or not client.is_configured():
        return {"skipped": 0, "scraped": 0, "found": 0, "reason": "client_not_configured"}

    # Build (filepath, md5) tuples for batch_scrape
    files_to_scrape = [
        (rom["filepath"], rom["md5"])
        for rom in roms_without_media
        if rom["md5"]  # Need MD5 for ScreenScraper lookup
    ]

    if not files_to_scrape:
        return {"skipped": 0, "scraped": 0, "found": 0, "reason": "no_md5_hashes"}

    # Determine media save path
    media_dir = ss_config.media_path
    if not media_dir:
        # Default: alongside the ROMs in downloaded_media
        parent = str(Path(directory).parent)
        media_dir = str(Path(parent) / "tools" / "downloaded_media")

    found_count = 0
    scraped_count = 0

    def _progress_callback(result, completed, total):
        nonlocal found_count, scraped_count
        scraped_count = completed
        if result and result.found:
            found_count += 1
        _scan_progress[scan_id] = ScanProgress(
            status="auto-scraping",
            current_step=f"Scraping: {completed}/{total} ({found_count} found)",
            current_phase="auto_scrape",
        )

    try:
        results = client.batch_scrape(
            files=files_to_scrape,
            media_dir=media_dir,
            download_art=True,
            callback=_progress_callback,
        )

        # Re-correlate media now that new art has been downloaded
        try:
            correlator = MediaCorrelator(db=db, config=config)
            correlator.scan_media_for_directory(directory)
        except Exception:
            pass

        return {
            "skipped": len(roms_without_media) - len(files_to_scrape),
            "scraped": scraped_count,
            "found": found_count,
            "reason": "completed",
        }
    except Exception as e:
        print(f"Auto-scrape error: {e}")
        return {
            "skipped": 0,
            "scraped": scraped_count,
            "found": found_count,
            "reason": f"error: {e}",
        }


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


def run_scan_task(
    directory: str,
    scan_id: str,
    full_scan: bool = False,
    directories: list[str] | None = None,
) -> None:
    """Background task to run a scan.

    Args:
        directory: Base directory for the scan
        scan_id: Unique scan identifier
        full_scan: Force full rescan even if previously scanned
        directories: Optional list of specific subdirectories to scan (from system detection)
    """
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()

    def progress_callback(progress: ScanProgress) -> None:
        _scan_progress[scan_id] = progress

    try:
        scanner = Scanner(db=db, config=config, progress_callback=progress_callback)

        # If specific directories provided (from system selection), scan each
        if directories and len(directories) > 0:
            # Scan each selected system directory
            total_files = 0
            total_errors = 0
            total_duration = 0
            last_result = None

            for idx, dir_path in enumerate(directories):
                # Update progress to show which system we're scanning
                _scan_progress[scan_id] = ScanProgress(
                    status=f"scanning system {idx + 1}/{len(directories)}",
                    current_step=f"Scanning {Path(dir_path).name}...",
                    current_phase="system_scan",
                )

                if full_scan:
                    result = scanner.scan(dir_path)
                else:
                    result = scanner.scan_or_update(dir_path)

                total_files += result.files_processed
                total_errors += result.errors
                total_duration += result.duration_seconds
                last_result = result

            # Mark duplicates for the base directory
            deduper = Deduper(db=db, config=config)
            deduper.mark_duplicates(directory)
            deduper.log_statistics(directory)

            # Scan for media files
            _scan_progress[scan_id] = ScanProgress(
                status="scanning media",
                current_step="Correlating media files with ROMs...",
                current_phase="media_scan",
            )

            media_stats = {"total_media": 0, "roms_with_media": 0}
            try:
                correlator = MediaCorrelator(db=db, config=config)
                media_stats = correlator.scan_media_for_directory(directory)
            except Exception as e:
                print(f"Media scan warning: {e}")

            # Auto-scrape new ROMs without media
            scrape_stats = _auto_scrape_new_roms(db, directory, scan_id)

            # Create combined result
            combined_result = {
                "directory": directory,
                "files_processed": total_files,
                "duration_seconds": total_duration,
                "errors": total_errors,
                "is_update": not full_scan,
                "systems_scanned": len(directories),
                "media_files_found": media_stats.get("total_media", 0),
                "roms_with_media": media_stats.get("roms_with_media", 0),
                "auto_scrape": scrape_stats,
            }

            _scan_results[scan_id] = {
                "status": "completed",
                "result": combined_result,
            }
        else:
            # Standard single directory scan
            if full_scan:
                result = scanner.scan(directory)
            else:
                result = scanner.scan_or_update(directory)

            # Mark duplicates and log statistics
            deduper = Deduper(db=db, config=config)
            deduper.mark_duplicates(directory)
            deduper.log_statistics(directory)

            # Scan for media files
            _scan_progress[scan_id] = ScanProgress(
                status="scanning media",
                current_step="Correlating media files with ROMs...",
                current_phase="media_scan",
            )

            media_stats = {"total_media": 0, "roms_with_media": 0}
            try:
                correlator = MediaCorrelator(db=db, config=config)
                media_stats = correlator.scan_media_for_directory(directory)
            except Exception as e:
                print(f"Media scan warning: {e}")

            # Auto-scrape new ROMs without media
            scrape_stats = _auto_scrape_new_roms(db, directory, scan_id)

            result_dict = result.to_dict()
            result_dict["media_files_found"] = media_stats.get("total_media", 0)
            result_dict["roms_with_media"] = media_stats.get("roms_with_media", 0)
            result_dict["auto_scrape"] = scrape_stats

            _scan_results[scan_id] = {
                "status": "completed",
                "result": result_dict,
            }

    except Exception as e:
        _scan_results[scan_id] = {
            "status": "error",
            "error": str(e),
        }
        if scan_id in _scan_progress:
            _scan_progress[scan_id].status = "error"

    finally:
        db.close()


@router.post("", response_model=dict)
async def start_scan(
    request: ScanRequest,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_auth),
) -> dict:
    """
    Start a scan on a directory.

    Returns a scan_id that can be used to check progress.
    If directories list is provided, only those specific subdirectories will be scanned.
    """
    import uuid

    scan_id = str(uuid.uuid4())

    # Initialize progress
    _scan_progress[scan_id] = ScanProgress(status="starting")
    _scan_results[scan_id] = {"status": "running"}

    # Start background task
    background_tasks.add_task(
        run_scan_task,
        request.directory,
        scan_id,
        request.full_scan,
        request.directories,  # Pass selected directories
    )

    return {
        "scan_id": scan_id,
        "status": "started",
        "directory": request.directory,
        "systems": len(request.directories) if request.directories else None,
    }


@router.get("/{scan_id}/status", response_model=ScanProgressResponse)
async def get_scan_status(
    scan_id: str,
    _: None = Depends(require_auth),
) -> ScanProgressResponse:
    """Get the status of a running or completed scan."""
    if scan_id not in _scan_progress:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scan {scan_id} not found",
        )

    progress = _scan_progress[scan_id]
    return ScanProgressResponse(
        total_files=progress.total_files,
        processed_files=progress.processed_files,
        current_file=progress.current_file,
        errors=progress.errors,
        elapsed_seconds=progress.elapsed_seconds,
        percent_complete=progress.percent_complete,
        status=progress.status,
        # Verbose fields
        current_step=progress.current_step,
        current_phase=progress.current_phase,
        files_hashed=progress.files_hashed,
        files_ra_checked=progress.files_ra_checked,
        files_ra_matched=progress.files_ra_matched,
        storage_type=progress.storage_type,
        thread_count=progress.thread_count,
        last_hash=progress.last_hash,
        last_serial=progress.last_serial,
        last_ra_result=progress.last_ra_result,
    )


@router.get("/{scan_id}/result")
async def get_scan_result(
    scan_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Get the result of a completed scan."""
    if scan_id not in _scan_results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Scan {scan_id} not found",
        )

    return _scan_results[scan_id]


@router.post("/sync", response_model=ScanResultResponse)
async def scan_sync(
    request: ScanRequest,
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> ScanResultResponse:
    """
    Run a scan synchronously and wait for completion.

    Use this for smaller directories or when you need immediate results.
    For larger directories, use the async POST /api/scan endpoint.
    """
    config = get_config()

    try:
        scanner = Scanner(db=db, config=config)

        if request.full_scan:
            result = scanner.scan(request.directory)
        else:
            result = scanner.scan_or_update(request.directory)

        # Mark duplicates and log statistics
        deduper = Deduper(db=db, config=config)
        deduper.mark_duplicates(request.directory)
        deduper.log_statistics(request.directory)

        # Scan for media files
        media_stats = {"total_media": 0, "roms_with_media": 0}
        try:
            correlator = MediaCorrelator(db=db, config=config)
            media_stats = correlator.scan_media_for_directory(request.directory)
        except Exception as e:
            print(f"Media scan warning: {e}")

        return ScanResultResponse(
            directory=result.directory,
            files_processed=result.files_processed,
            duration_seconds=result.duration_seconds,
            errors=result.errors,
            is_update=result.is_update,
            media_files_found=media_stats.get("total_media", 0),
            roms_with_media=media_stats.get("roms_with_media", 0),
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Scan failed: {e}",
        )


@router.get("/suggestions", response_model=ScanSuggestionsResponse)
async def get_scan_suggestions(
    _: None = Depends(require_auth),
    db: DuperDatabase = Depends(get_db),
) -> ScanSuggestionsResponse:
    """
    Get scan suggestions including:
    - Previously scanned directories (history)
    - Detected removable media (USB drives, SD cards)
    - Common ROM directory paths
    """
    # Get scan history from database
    history_items = []
    try:
        scanned_dirs = db.get_scanned_directories()
        for dir_info in scanned_dirs:
            history_items.append(ScanHistoryItem(
                path=dir_info["directory"],
                last_scanned=dir_info.get("last_scanned"),
                file_count=dir_info.get("file_count", 0),
                duplicate_count=dir_info.get("duplicate_count", 0),
            ))
    except Exception as e:
        print(f"Error getting scan history: {e}")

    # Get removable devices
    removable = _get_removable_devices()

    # Get common paths
    common = _get_common_paths()

    return ScanSuggestionsResponse(
        history=history_items,
        removable_devices=removable,
        common_paths=common,
    )


class DetectedSystem(BaseModel):
    """A detected game system in a ROM directory."""
    folder: str
    system: str
    path: str
    file_count: int
    size_mb: float


class DetectSystemsResponse(BaseModel):
    """Response with detected systems."""
    directory: str
    systems: list[DetectedSystem]
    total_systems: int
    total_files: int
    total_size_mb: float


@router.get("/detect-systems", response_model=DetectSystemsResponse)
async def detect_systems_in_directory(
    directory: str,
    _: None = Depends(require_auth),
) -> DetectSystemsResponse:
    """
    Detect game systems present in a ROM directory.

    Scans top-level folders and matches them against known system names.
    Returns a list of systems with file counts and sizes.
    """
    path = Path(directory)
    if not path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Directory not found: {directory}",
        )
    if not path.is_dir():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Path is not a directory: {directory}",
        )

    systems = detect_systems(directory)

    total_files = sum(s["file_count"] for s in systems)
    total_size = sum(s["size_mb"] for s in systems)

    return DetectSystemsResponse(
        directory=directory,
        systems=[
            DetectedSystem(
                folder=s["folder"],
                system=s["system"],
                path=s["path"],
                file_count=s["file_count"],
                size_mb=s["size_mb"],
            )
            for s in systems
        ],
        total_systems=len(systems),
        total_files=total_files,
        total_size_mb=round(total_size, 2),
    )
