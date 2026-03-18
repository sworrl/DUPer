"""Pydantic models for API requests and responses."""

from __future__ import annotations

from pydantic import BaseModel, Field


# === Request Models ===


class ScanRequest(BaseModel):
    """Request to start a scan."""

    directory: str = Field(..., description="Directory path to scan")
    full_scan: bool = Field(
        False, description="Force a full rescan even if previously scanned"
    )
    directories: list[str] | None = Field(
        None, description="Optional list of specific subdirectories to scan (from system detection)"
    )


class ProcessDuplicatesRequest(BaseModel):
    """Request to process duplicates."""

    directory: str = Field(..., description="Directory to process duplicates in")
    action: str = Field("archive", description="Action: 'archive' or 'delete'")
    archive_location: str | None = Field(
        None, description="Custom location to archive duplicates to (only for archive action)"
    )
    dry_run: bool = Field(False, description="If true, don't actually process files")
    # Optional: process specific groups only (by MD5 hash)
    group_hashes: list[str] | None = Field(
        None, description="Optional list of MD5 hashes to process (None = all)"
    )
    # Optional: override which file to keep per group
    keep_overrides: dict[str, str] | None = Field(
        None, description="Override which file to keep: {md5: filepath_to_keep}"
    )


class RestoreFileRequest(BaseModel):
    """Request to restore a moved file."""

    move_id: int = Field(..., description="ID of the moved file to restore")


class ConfigUpdateRequest(BaseModel):
    """Request to update configuration."""

    server_port: int | None = None
    server_host: str | None = None
    web_ui_enabled: bool | None = None
    auth_enabled: bool | None = None
    api_key: str | None = None
    ignore_fodder: bool | None = None
    ignore_video: bool | None = None
    ignore_music: bool | None = None
    ignore_pictures: bool | None = None
    retroarch_mode: bool | None = None
    working_dir: str | None = None
    database: str | None = None
    duplicates_dir: str | None = None


class RemoteAddRequest(BaseModel):
    """Request to add a remote host."""

    name: str = Field(..., description="Name for the remote host")
    host: str = Field(..., description="Host address")
    port: int = Field(8420, description="Port number")
    api_key: str = Field("", description="API key for authentication")


# === Response Models ===


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str
    codename: str


class ScanProgressResponse(BaseModel):
    """Scan progress response."""

    total_files: int
    processed_files: int
    current_file: str
    errors: int
    elapsed_seconds: float
    percent_complete: float
    status: str
    # Verbose progress fields
    current_step: str = ""
    current_phase: str = ""
    files_hashed: int = 0
    files_ra_checked: int = 0
    files_ra_matched: int = 0
    storage_type: str = ""
    thread_count: int = 0
    last_hash: str = ""
    last_serial: str = ""
    last_ra_result: str = ""


class ScanResultResponse(BaseModel):
    """Scan result response."""

    directory: str
    files_processed: int
    duration_seconds: int
    errors: int
    is_update: bool
    media_files_found: int = 0
    roms_with_media: int = 0


class FileResponse(BaseModel):
    """File record response."""

    filepath: str
    filename: str
    md5: str
    size_mb: float
    extension: str
    is_duplicate: bool
    rom_serial: str = ""
    ra_supported: bool | None = None
    ra_game_id: int | None = None
    ra_game_title: str | None = None
    ra_checked_date: str | None = None


class FileListResponse(BaseModel):
    """List of files response."""

    total: int
    files: list[FileResponse]


class DuplicateFileResponse(BaseModel):
    """Duplicate file in a group."""

    filepath: str
    filename: str
    size_mb: float
    score: float
    rom_serial: str = ""
    ra_supported: bool = False
    ra_game_id: int = 0
    ra_game_title: str = ""
    ra_checked_date: str | None = None


class DuplicateGroupResponse(BaseModel):
    """Duplicate group response."""

    md5: str
    rom_serial: str = ""
    files: list[DuplicateFileResponse]
    recommended_keep: str
    file_count: int


class DuplicateSummaryResponse(BaseModel):
    """Duplicate summary response."""

    total_groups: int
    total_duplicate_files: int
    files_to_remove: int
    wasted_space_mb: float
    groups: list[DuplicateGroupResponse]


class ProcessResultResponse(BaseModel):
    """Process duplicates result response."""

    action: str = "archive"
    processed_count: int = 0
    archived_count: int = 0
    deleted_count: int = 0
    space_freed_mb: float = 0.0
    media_processed_count: int = 0
    media_space_freed_mb: float = 0.0
    errors: list[str] = []
    processed_files: list[dict] = []  # [{filepath, action, destination?}]


class MovedFileResponse(BaseModel):
    """Moved file record response."""

    move_id: int
    original_filepath: str
    moved_to_path: str
    moved_time: str
    filename: str
    size_mb: float = 0.0
    md5: str = ""
    rom_serial: str = ""
    reason: str = ""


class MovedFilesSummaryResponse(BaseModel):
    """Moved files summary response."""

    total_moved: int
    total_size_mb: float = 0.0
    files: list[MovedFileResponse]


class RestoreResultResponse(BaseModel):
    """Restore result response."""

    restored_count: int
    errors: list[str]


class ConfigResponse(BaseModel):
    """Configuration response."""

    server: dict
    scanner: dict
    paths: dict
    remotes: dict


class SizeBreakdownItem(BaseModel):
    """Size breakdown by extension."""
    extension: str
    file_count: int
    total_size_mb: float
    avg_size_mb: float


class StatsResponse(BaseModel):
    """Statistics response."""

    total_files: int
    total_duplicates: int  # Name-based potential duplicates
    exact_duplicates: int = 0  # MD5-based exact duplicates
    total_moved: int
    total_size_mb: float
    database_path: str
    # Space statistics
    duplicate_space_mb: float = 0.0
    wasted_space_mb: float = 0.0
    duplicate_groups: int = 0
    space_saved_mb: float = 0.0
    # Size breakdown
    size_breakdown: list[SizeBreakdownItem] = []
    # Optional detailed info
    system_info: dict | None = None
    latest_metrics: dict | None = None
    latest_statistics: dict | None = None
    scan_history: list[dict] | None = None


class ErrorResponse(BaseModel):
    """Error response."""

    detail: str
    error_code: str | None = None


class RemoteHostResponse(BaseModel):
    """Remote host response."""

    name: str
    host: str
    port: int
    has_api_key: bool


class RemoteListResponse(BaseModel):
    """List of remote hosts response."""

    remotes: list[RemoteHostResponse]


# === Library Models ===


class LibraryCreateRequest(BaseModel):
    """Request to create a library."""

    name: str = Field(..., description="Library name")
    root_path: str = Field(..., description="Root directory path")
    device_type: str = Field("local", description="Device type: local, remote, removable")
    remote_host_id: str | None = Field(None, description="Remote host ID for remote libraries")


class LibraryUpdateRequest(BaseModel):
    """Request to update a library."""

    name: str | None = None
    root_path: str | None = None
    status: str | None = None
    settings: dict | None = None


class LibraryResponse(BaseModel):
    """Library response."""

    library_id: str
    name: str
    root_path: str
    device_type: str
    remote_host_id: str | None = None
    status: str
    last_scan_time: str | None = None
    last_sync_time: str | None = None
    total_games: int = 0
    total_files: int = 0
    total_size_mb: float = 0.0
    duplicate_count: int = 0
    created_time: str
    updated_time: str


class LibraryListResponse(BaseModel):
    """List of libraries response."""

    total: int
    libraries: list[LibraryResponse]


# === Game Models ===


class GameResponse(BaseModel):
    """Game response."""

    game_id: str
    library_id: str
    title: str
    normalized_title: str
    system: str
    ra_game_id: int = 0
    primary_file_path: str = ""
    primary_rom_serial: str = ""
    region: str = ""
    release_year: int = 0
    genre: str = ""
    developer: str = ""
    publisher: str = ""
    file_count: int = 1
    total_size_mb: float = 0.0
    has_media: bool = False
    ra_supported: bool = False
    cover_url: str | None = None  # URL to cover image if available


class GameListResponse(BaseModel):
    """List of games response."""

    total: int
    library_id: str
    games: list[GameResponse]
    systems: dict[str, int] = {}  # System breakdown


class GameDetailResponse(GameResponse):
    """Detailed game response with files and media."""

    files: list[FileResponse] = []
    media: list[dict] = []  # MediaRecord dicts


# === Scan Queue Models ===


class QueueAddRequest(BaseModel):
    """Request to add item to scan queue."""

    library_id: str = Field(..., description="Library ID to scan into")
    directory: str = Field(..., description="Directory to scan")
    priority: int = Field(0, description="Priority (higher = more urgent)")
    full_scan: bool = Field(False, description="Force full rescan")
    scan_media: bool = Field(True, description="Include media scanning")
    scan_ra: bool = Field(True, description="Verify RetroAchievements")


class QueueItemResponse(BaseModel):
    """Scan queue item response."""

    queue_id: str
    library_id: str
    directory: str
    status: str
    priority: int
    position: int
    queued_time: str
    started_time: str | None = None
    completed_time: str | None = None
    total_files: int = 0
    processed_files: int = 0
    current_file: str = ""
    percent_complete: float = 0.0
    files_processed: int = 0
    errors: int = 0


class QueueResponse(BaseModel):
    """Scan queue response."""

    total: int
    running: QueueItemResponse | None = None
    pending: list[QueueItemResponse] = []
    completed: list[QueueItemResponse] = []


class QueueReorderRequest(BaseModel):
    """Request to reorder a queue item."""

    new_position: int = Field(..., description="New position in queue")
