"""Library, Game, and ScanQueue models for DUPer."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


def generate_uuid() -> str:
    """Generate a new UUID string."""
    return str(uuid.uuid4())


@dataclass
class Library:
    """
    Represents a game library - the core organizational entity.

    Libraries can be:
    - local: Files on the local filesystem
    - remote: Files on a remote DUPer agent (Steam Deck, etc.)
    - removable: Files on removable storage (SD card, USB drive)
    """

    # Identity
    library_id: str = field(default_factory=generate_uuid)
    name: str = ""  # User-friendly name (e.g., "Steam Deck ROMs", "Main Collection")

    # Location
    root_path: str = ""  # Base directory path
    device_type: str = "local"  # "local", "remote", "removable"

    # Remote configuration (for device_type="remote")
    remote_host_id: str | None = None  # References RemoteHost.name from config
    remote_library_id: str | None = None  # ID of library on remote host

    # State
    status: str = "active"  # "active", "offline", "scanning", "syncing", "error"
    last_scan_time: str | None = None
    last_sync_time: str | None = None  # For remote libraries

    # Statistics (cached)
    total_games: int = 0
    total_files: int = 0
    total_size_mb: float = 0.0
    duplicate_count: int = 0

    # Metadata
    created_time: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_time: str = field(default_factory=lambda: datetime.now().isoformat())

    # Settings per-library (stored as JSON)
    settings: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Library":
        """Create a Library from a database row."""
        import json
        keys = row.keys()
        settings = {}
        if "settings_json" in keys and row["settings_json"]:
            try:
                settings = json.loads(row["settings_json"])
            except json.JSONDecodeError:
                pass

        return cls(
            library_id=row["library_id"],
            name=row["name"],
            root_path=row["root_path"],
            device_type=row["device_type"] if "device_type" in keys else "local",
            remote_host_id=row["remote_host_id"] if "remote_host_id" in keys else None,
            remote_library_id=row["remote_library_id"] if "remote_library_id" in keys else None,
            status=row["status"] if "status" in keys else "active",
            last_scan_time=row["last_scan_time"] if "last_scan_time" in keys else None,
            last_sync_time=row["last_sync_time"] if "last_sync_time" in keys else None,
            total_games=row["total_games"] if "total_games" in keys else 0,
            total_files=row["total_files"] if "total_files" in keys else 0,
            total_size_mb=row["total_size_mb"] if "total_size_mb" in keys else 0.0,
            duplicate_count=row["duplicate_count"] if "duplicate_count" in keys else 0,
            created_time=row["created_time"] if "created_time" in keys else datetime.now().isoformat(),
            updated_time=row["updated_time"] if "updated_time" in keys else datetime.now().isoformat(),
            settings=settings,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "library_id": self.library_id,
            "name": self.name,
            "root_path": self.root_path,
            "device_type": self.device_type,
            "remote_host_id": self.remote_host_id,
            "remote_library_id": self.remote_library_id,
            "status": self.status,
            "last_scan_time": self.last_scan_time,
            "last_sync_time": self.last_sync_time,
            "total_games": self.total_games,
            "total_files": self.total_files,
            "total_size_mb": self.total_size_mb,
            "duplicate_count": self.duplicate_count,
            "created_time": self.created_time,
            "updated_time": self.updated_time,
            "settings": self.settings,
        }

    @property
    def is_remote(self) -> bool:
        """Check if this is a remote library."""
        return self.device_type == "remote"

    @property
    def is_local(self) -> bool:
        """Check if this is a local library."""
        return self.device_type == "local"


@dataclass
class Game:
    """
    Represents a single game, potentially with multiple ROM files and media.

    Games aggregate multiple ROM variants (different regions, formats) into
    a single entity with a "primary" ROM selection.
    """

    # Identity
    game_id: str = field(default_factory=generate_uuid)
    library_id: str = ""  # References Library

    # Core identification
    title: str = ""  # Canonical game title
    normalized_title: str = ""  # For matching (e.g., "super mario bros")
    system: str = ""  # Platform (e.g., "SNES", "PlayStation")

    # External IDs
    ra_game_id: int = 0  # RetroAchievements ID
    ss_game_id: int = 0  # ScreenScraper ID (future)

    # Best ROM selection
    primary_file_path: str = ""  # The "best" ROM for this game
    primary_rom_serial: str = ""  # DUP-XXXXXXXX serial of primary

    # Metadata
    region: str = ""  # USA, EUR, JPN, etc.
    release_year: int = 0
    genre: str = ""
    developer: str = ""
    publisher: str = ""
    description: str = ""

    # Statistics
    file_count: int = 1  # Number of ROM variants
    total_size_mb: float = 0.0
    has_media: bool = False
    ra_supported: bool = False

    # Timestamps
    created_time: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_time: str = field(default_factory=lambda: datetime.now().isoformat())

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Game":
        """Create a Game from a database row."""
        keys = row.keys()
        return cls(
            game_id=row["game_id"],
            library_id=row["library_id"],
            title=row["title"],
            normalized_title=row["normalized_title"] if "normalized_title" in keys else "",
            system=row["system"] if "system" in keys else "",
            ra_game_id=row["ra_game_id"] if "ra_game_id" in keys else 0,
            ss_game_id=row["ss_game_id"] if "ss_game_id" in keys else 0,
            primary_file_path=row["primary_file_path"] if "primary_file_path" in keys else "",
            primary_rom_serial=row["primary_rom_serial"] if "primary_rom_serial" in keys else "",
            region=row["region"] if "region" in keys else "",
            release_year=row["release_year"] if "release_year" in keys else 0,
            genre=row["genre"] if "genre" in keys else "",
            developer=row["developer"] if "developer" in keys else "",
            publisher=row["publisher"] if "publisher" in keys else "",
            description=row["description"] if "description" in keys else "",
            file_count=row["file_count"] if "file_count" in keys else 1,
            total_size_mb=row["total_size_mb"] if "total_size_mb" in keys else 0.0,
            has_media=bool(row["has_media"]) if "has_media" in keys else False,
            ra_supported=bool(row["ra_supported"]) if "ra_supported" in keys else False,
            created_time=row["created_time"] if "created_time" in keys else datetime.now().isoformat(),
            updated_time=row["updated_time"] if "updated_time" in keys else datetime.now().isoformat(),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "game_id": self.game_id,
            "library_id": self.library_id,
            "title": self.title,
            "normalized_title": self.normalized_title,
            "system": self.system,
            "ra_game_id": self.ra_game_id,
            "ss_game_id": self.ss_game_id,
            "primary_file_path": self.primary_file_path,
            "primary_rom_serial": self.primary_rom_serial,
            "region": self.region,
            "release_year": self.release_year,
            "genre": self.genre,
            "developer": self.developer,
            "publisher": self.publisher,
            "description": self.description,
            "file_count": self.file_count,
            "total_size_mb": self.total_size_mb,
            "has_media": self.has_media,
            "ra_supported": self.ra_supported,
            "created_time": self.created_time,
            "updated_time": self.updated_time,
        }


@dataclass
class ScanQueueItem:
    """
    Represents an item in the scan queue.

    The queue allows users to schedule multiple directories for scanning
    in a specific order.
    """

    # Identity
    queue_id: str = field(default_factory=generate_uuid)
    library_id: str = ""  # Which library this belongs to
    directory: str = ""  # Path to scan

    # Queue state
    status: str = "pending"  # "pending", "running", "completed", "failed", "cancelled"
    priority: int = 0  # Higher = more urgent
    position: int = 0  # Position in queue (auto-calculated)

    # Timing
    queued_time: str = field(default_factory=lambda: datetime.now().isoformat())
    started_time: str | None = None
    completed_time: str | None = None

    # Options
    full_scan: bool = False  # Force full rescan
    scan_media: bool = True  # Include media scanning
    scan_ra: bool = True  # Verify RetroAchievements

    # Progress (for running items)
    total_files: int = 0
    processed_files: int = 0
    current_file: str = ""

    # Result (for completed items)
    files_processed: int = 0
    errors: int = 0
    error_log: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ScanQueueItem":
        """Create a ScanQueueItem from a database row."""
        keys = row.keys()
        return cls(
            queue_id=row["queue_id"],
            library_id=row["library_id"],
            directory=row["directory"],
            status=row["status"] if "status" in keys else "pending",
            priority=row["priority"] if "priority" in keys else 0,
            position=row["position"] if "position" in keys else 0,
            queued_time=row["queued_time"] if "queued_time" in keys else datetime.now().isoformat(),
            started_time=row["started_time"] if "started_time" in keys else None,
            completed_time=row["completed_time"] if "completed_time" in keys else None,
            full_scan=bool(row["full_scan"]) if "full_scan" in keys else False,
            scan_media=bool(row["scan_media"]) if "scan_media" in keys else True,
            scan_ra=bool(row["scan_ra"]) if "scan_ra" in keys else True,
            total_files=row["total_files"] if "total_files" in keys else 0,
            processed_files=row["processed_files"] if "processed_files" in keys else 0,
            current_file=row["current_file"] if "current_file" in keys else "",
            files_processed=row["files_processed"] if "files_processed" in keys else 0,
            errors=row["errors"] if "errors" in keys else 0,
            error_log=row["error_log"] if "error_log" in keys else "",
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "queue_id": self.queue_id,
            "library_id": self.library_id,
            "directory": self.directory,
            "status": self.status,
            "priority": self.priority,
            "position": self.position,
            "queued_time": self.queued_time,
            "started_time": self.started_time,
            "completed_time": self.completed_time,
            "full_scan": self.full_scan,
            "scan_media": self.scan_media,
            "scan_ra": self.scan_ra,
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "current_file": self.current_file,
            "files_processed": self.files_processed,
            "errors": self.errors,
            "error_log": self.error_log,
            "percent_complete": self.percent_complete,
        }

    @property
    def percent_complete(self) -> float:
        """Calculate completion percentage."""
        if self.total_files == 0:
            return 0.0
        return (self.processed_files / self.total_files) * 100

    @property
    def is_pending(self) -> bool:
        """Check if item is pending."""
        return self.status == "pending"

    @property
    def is_running(self) -> bool:
        """Check if item is currently running."""
        return self.status == "running"

    @property
    def is_complete(self) -> bool:
        """Check if item has finished (completed or failed)."""
        return self.status in ("completed", "failed", "cancelled")
