"""Database operations for DUPer using SQLite."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

from duper import __version__
from duper.core.library import Game, Library, ScanQueueItem


def generate_rom_serial(md5: str) -> str:
    """
    Generate a unique ROM serial number from an MD5 hash.

    The serial is deterministic - same MD5 always produces the same serial.
    Format: DUP-XXXXXXXX (8 uppercase hex chars from MD5)
    """
    if not md5:
        return ""
    return f"DUP-{md5[:8].upper()}"


@dataclass
class FileRecord:
    """Represents a file record in the database."""

    filepath: str
    filename: str
    md5: str
    simplified_filename: str
    size_mb: float
    created_time: str | None
    modified_time: str | None
    extension: str
    is_potential_duplicate: bool = False
    is_cross_platform: bool = False
    dupe_score: int = 0  # 0-100 confidence score for duplicate match
    dupe_reason: str = ""  # Why this was flagged (md5, filename, normalized)
    normalized_name: str = ""  # Normalized name for fuzzy matching
    ra_supported: bool = False
    ra_game_id: int = 0
    ra_game_title: str = ""
    ra_checked_date: str | None = None
    rom_serial: str = ""

    def __post_init__(self):
        """Generate rom_serial from MD5 if not provided."""
        if not self.rom_serial and self.md5:
            self.rom_serial = generate_rom_serial(self.md5)

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FileRecord":
        keys = row.keys()
        md5 = row["md5"]
        rom_serial = row["rom_serial"] if "rom_serial" in keys else ""
        # Auto-generate serial if not in database
        if not rom_serial and md5:
            rom_serial = generate_rom_serial(md5)
        return cls(
            filepath=row["filepath"],
            filename=row["filename"],
            md5=md5,
            simplified_filename=row["simplified_filename"],
            size_mb=row["size_mb"],
            created_time=row["created_time"],
            modified_time=row["modified_time"],
            extension=row["extension"],
            is_potential_duplicate=bool(row["is_potential_duplicate"]),
            is_cross_platform=bool(row["is_cross_platform"]) if "is_cross_platform" in keys else False,
            dupe_score=row["dupe_score"] if "dupe_score" in keys else 0,
            dupe_reason=row["dupe_reason"] if "dupe_reason" in keys else "",
            normalized_name=row["normalized_name"] if "normalized_name" in keys else "",
            ra_supported=bool(row["ra_supported"]) if "ra_supported" in keys else False,
            ra_game_id=row["ra_game_id"] if "ra_game_id" in keys else 0,
            ra_game_title=row["ra_game_title"] if "ra_game_title" in keys else "",
            ra_checked_date=row["ra_checked_date"] if "ra_checked_date" in keys else None,
            rom_serial=rom_serial,
        )


@dataclass
class MovedFile:
    """Represents a moved file record."""

    move_id: int
    original_filepath: str
    moved_to_path: str
    moved_time: str
    size_mb: float = 0.0
    filename: str = ""
    md5: str = ""
    rom_serial: str = ""
    reason: str = ""

    def __post_init__(self):
        """Generate rom_serial from MD5 if not provided."""
        if not self.rom_serial and self.md5:
            self.rom_serial = generate_rom_serial(self.md5)

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MovedFile":
        keys = row.keys()
        md5 = row["md5"] if "md5" in keys else ""
        rom_serial = row["rom_serial"] if "rom_serial" in keys else ""
        if not rom_serial and md5:
            rom_serial = generate_rom_serial(md5)
        return cls(
            move_id=row["move_id"],
            original_filepath=row["original_filepath"],
            moved_to_path=row["moved_to_path"],
            moved_time=row["moved_time"],
            size_mb=row["size_mb"] if "size_mb" in keys else 0.0,
            filename=row["filename"] if "filename" in keys else "",
            md5=md5,
            rom_serial=rom_serial,
            reason=row["reason"] if "reason" in keys else "",
        )


@dataclass
class MediaRecord:
    """Represents a media file record in the database."""

    media_id: int
    rom_filepath: str  # Foreign key to files.filepath
    media_path: str  # Absolute path to media file
    media_type: str  # image, video, document
    category: str  # boxart, screenshot, video, fanart, etc.
    filename: str
    size_bytes: int = 0
    mime_type: str = ""
    width: int = 0  # For images
    height: int = 0  # For images
    duration_seconds: float = 0.0  # For videos
    scanned_time: str = ""

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MediaRecord":
        keys = row.keys()
        return cls(
            media_id=row["media_id"],
            rom_filepath=row["rom_filepath"],
            media_path=row["media_path"],
            media_type=row["media_type"],
            category=row["category"],
            filename=row["filename"],
            size_bytes=row["size_bytes"] if "size_bytes" in keys else 0,
            mime_type=row["mime_type"] if "mime_type" in keys else "",
            width=row["width"] if "width" in keys else 0,
            height=row["height"] if "height" in keys else 0,
            duration_seconds=row["duration_seconds"] if "duration_seconds" in keys else 0.0,
            scanned_time=row["scanned_time"] if "scanned_time" in keys else "",
        )

    def to_dict(self) -> dict:
        return {
            "media_id": self.media_id,
            "rom_filepath": self.rom_filepath,
            "media_path": self.media_path,
            "media_type": self.media_type,
            "category": self.category,
            "filename": self.filename,
            "size_bytes": self.size_bytes,
            "mime_type": self.mime_type,
            "width": self.width,
            "height": self.height,
            "duration_seconds": self.duration_seconds,
            "scanned_time": self.scanned_time,
        }


@dataclass
class ScanMetrics:
    """Represents scan metrics."""

    start_time: str
    end_time: str
    scan_duration_seconds: int
    scan_duration_verbose: str
    errors_encountered: int
    error_log: str
    script_version: str
    scan_directory: str
    user: str
    database_path: str
    files_processed: int

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ScanMetrics":
        return cls(
            start_time=row["start_time"],
            end_time=row["end_time"],
            scan_duration_seconds=row["scan_duration_seconds"],
            scan_duration_verbose=row["scan_duration_verbose"],
            errors_encountered=row["errors_encountered"],
            error_log=row["error_log"],
            script_version=row["script_version"],
            scan_directory=row["scan_directory"],
            user=row["user"],
            database_path=row["database_path"],
            files_processed=row["files_processed"],
        )


@dataclass
class FileStatistics:
    """Represents file statistics for a scan."""

    scan_id: int
    scan_start_time: str
    total_files: int
    potential_duplicates: int
    duplicate_file_info: dict[str, list[str]]
    scan_directory: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FileStatistics":
        try:
            dup_info = json.loads(row["duplicate_file_info"]) if row["duplicate_file_info"] else {}
        except json.JSONDecodeError:
            dup_info = {}
        return cls(
            scan_id=row["scan_id"],
            scan_start_time=row["scan_start_time"],
            total_files=row["total_files"],
            potential_duplicates=row["potential_duplicates"],
            duplicate_file_info=dup_info,
            scan_directory=row["scan_directory"],
        )


class DuperDatabase:
    """Database manager for DUPer."""

    SCHEMA_VERSION = 3  # Bumped for library/game/queue support

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        """Connect to the database with optimized settings."""
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            # Performance: WAL mode for concurrent reads during scans
            self._conn.execute("PRAGMA journal_mode=WAL")
            # Performance: increase cache to 64MB
            self._conn.execute("PRAGMA cache_size=-65536")
            # Performance: normal sync (safe with WAL, much faster)
            self._conn.execute("PRAGMA synchronous=NORMAL")
            # Performance: store temp tables in memory
            self._conn.execute("PRAGMA temp_store=MEMORY")
            # Performance: 32KB mmap for better I/O
            self._conn.execute("PRAGMA mmap_size=268435456")
            # Integrity: enable foreign keys
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        """Context manager for database cursor."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    def initialize(self) -> None:
        """Initialize the database schema."""
        with self.cursor() as cursor:
            # Files table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    filepath TEXT PRIMARY KEY,
                    filename TEXT,
                    md5 TEXT,
                    simplified_filename TEXT,
                    size_mb REAL,
                    created_time TEXT,
                    modified_time TEXT,
                    extension TEXT,
                    is_potential_duplicate INTEGER DEFAULT 0,
                    ra_supported INTEGER DEFAULT 0,
                    ra_game_id INTEGER DEFAULT 0,
                    ra_game_title TEXT DEFAULT '',
                    ra_checked_date TEXT DEFAULT NULL,
                    rom_serial TEXT DEFAULT ''
                )
            """)

            # Migration: Add RA columns to existing files table
            cursor.execute("PRAGMA table_info(files)")
            file_columns = {row[1] for row in cursor.fetchall()}
            if "ra_supported" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN ra_supported INTEGER DEFAULT 0")
            if "ra_game_id" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN ra_game_id INTEGER DEFAULT 0")
            if "ra_game_title" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN ra_game_title TEXT DEFAULT ''")
            if "ra_checked_date" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN ra_checked_date TEXT DEFAULT NULL")
            if "rom_serial" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN rom_serial TEXT DEFAULT ''")
                # Generate rom_serial for existing records from their MD5
                cursor.execute("""
                    UPDATE files
                    SET rom_serial = 'DUP-' || UPPER(SUBSTR(md5, 1, 8))
                    WHERE md5 IS NOT NULL AND md5 != '' AND (rom_serial IS NULL OR rom_serial = '')
                """)
            if "is_cross_platform" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN is_cross_platform INTEGER DEFAULT 0")
            if "dupe_score" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN dupe_score INTEGER DEFAULT 0")
            if "dupe_reason" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN dupe_reason TEXT DEFAULT ''")
            if "normalized_name" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN normalized_name TEXT DEFAULT ''")

            # Metrics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    start_time TEXT PRIMARY KEY,
                    end_time TEXT,
                    scan_duration_seconds INTEGER,
                    scan_duration_verbose TEXT,
                    errors_encountered INTEGER,
                    error_log TEXT,
                    script_version TEXT,
                    scan_directory TEXT,
                    user TEXT,
                    database_path TEXT,
                    files_processed INTEGER
                )
            """)

            # File statistics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_statistics (
                    scan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_start_time TEXT,
                    total_files INTEGER,
                    potential_duplicates INTEGER,
                    duplicate_file_info TEXT,
                    scan_directory TEXT,
                    FOREIGN KEY (scan_start_time) REFERENCES metrics(start_time)
                )
            """)

            # Scan history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scan_history (
                    directory TEXT PRIMARY KEY,
                    last_scan_time TEXT
                )
            """)

            # Moved files table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS moved_files (
                    move_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_filepath TEXT UNIQUE,
                    moved_to_path TEXT,
                    moved_time TEXT,
                    size_mb REAL DEFAULT 0,
                    filename TEXT DEFAULT '',
                    md5 TEXT DEFAULT '',
                    rom_serial TEXT DEFAULT '',
                    reason TEXT DEFAULT ''
                )
            """)

            # Migration: Add new columns to existing moved_files table
            cursor.execute("PRAGMA table_info(moved_files)")
            columns = {row[1] for row in cursor.fetchall()}
            if "size_mb" not in columns:
                cursor.execute("ALTER TABLE moved_files ADD COLUMN size_mb REAL DEFAULT 0")
            if "filename" not in columns:
                cursor.execute("ALTER TABLE moved_files ADD COLUMN filename TEXT DEFAULT ''")
            if "md5" not in columns:
                cursor.execute("ALTER TABLE moved_files ADD COLUMN md5 TEXT DEFAULT ''")
            if "rom_serial" not in columns:
                cursor.execute("ALTER TABLE moved_files ADD COLUMN rom_serial TEXT DEFAULT ''")
                # Generate rom_serial for existing records from their MD5
            if "reason" not in columns:
                cursor.execute("ALTER TABLE moved_files ADD COLUMN reason TEXT DEFAULT ''")
                cursor.execute("""
                    UPDATE moved_files
                    SET rom_serial = 'DUP-' || UPPER(SUBSTR(md5, 1, 8))
                    WHERE md5 IS NOT NULL AND md5 != '' AND (rom_serial IS NULL OR rom_serial = '')
                """)

            # Media files table - stores media associated with ROMs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS media_files (
                    media_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rom_filepath TEXT,
                    media_path TEXT UNIQUE,
                    media_type TEXT,
                    category TEXT,
                    filename TEXT,
                    size_bytes INTEGER DEFAULT 0,
                    mime_type TEXT DEFAULT '',
                    width INTEGER DEFAULT 0,
                    height INTEGER DEFAULT 0,
                    duration_seconds REAL DEFAULT 0,
                    scanned_time TEXT,
                    FOREIGN KEY (rom_filepath) REFERENCES files(filepath)
                )
            """)

            # === Schema v3: Libraries, Games, Scan Queue ===

            # Libraries table - core organizational entity
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS libraries (
                    library_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    root_path TEXT NOT NULL,
                    device_type TEXT DEFAULT 'local',
                    remote_host_id TEXT,
                    remote_library_id TEXT,
                    status TEXT DEFAULT 'active',
                    last_scan_time TEXT,
                    last_sync_time TEXT,
                    total_games INTEGER DEFAULT 0,
                    total_files INTEGER DEFAULT 0,
                    total_size_mb REAL DEFAULT 0.0,
                    duplicate_count INTEGER DEFAULT 0,
                    created_time TEXT NOT NULL,
                    updated_time TEXT NOT NULL,
                    settings_json TEXT DEFAULT '{}'
                )
            """)

            # Games table - aggregates ROM files into game entities
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id TEXT PRIMARY KEY,
                    library_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    normalized_title TEXT NOT NULL,
                    system TEXT NOT NULL,
                    ra_game_id INTEGER DEFAULT 0,
                    ss_game_id INTEGER DEFAULT 0,
                    primary_file_path TEXT,
                    primary_rom_serial TEXT,
                    region TEXT DEFAULT '',
                    release_year INTEGER DEFAULT 0,
                    genre TEXT DEFAULT '',
                    developer TEXT DEFAULT '',
                    publisher TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    file_count INTEGER DEFAULT 1,
                    total_size_mb REAL DEFAULT 0.0,
                    has_media INTEGER DEFAULT 0,
                    ra_supported INTEGER DEFAULT 0,
                    created_time TEXT NOT NULL,
                    updated_time TEXT NOT NULL,
                    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
                )
            """)

            # Scan queue table - ordered scan jobs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scan_queue (
                    queue_id TEXT PRIMARY KEY,
                    library_id TEXT NOT NULL,
                    directory TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    priority INTEGER DEFAULT 0,
                    position INTEGER DEFAULT 0,
                    queued_time TEXT NOT NULL,
                    started_time TEXT,
                    completed_time TEXT,
                    full_scan INTEGER DEFAULT 0,
                    scan_media INTEGER DEFAULT 1,
                    scan_ra INTEGER DEFAULT 1,
                    total_files INTEGER DEFAULT 0,
                    processed_files INTEGER DEFAULT 0,
                    current_file TEXT DEFAULT '',
                    files_processed INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    error_log TEXT DEFAULT '',
                    FOREIGN KEY (library_id) REFERENCES libraries(library_id)
                )
            """)

            # Migration: Add library_id and game_id to files table
            cursor.execute("PRAGMA table_info(files)")
            file_columns = {row[1] for row in cursor.fetchall()}
            if "library_id" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN library_id TEXT")
            if "game_id" not in file_columns:
                cursor.execute("ALTER TABLE files ADD COLUMN game_id TEXT")

            # Migration: Add game_id to media_files table
            cursor.execute("PRAGMA table_info(media_files)")
            media_columns = {row[1] for row in cursor.fetchall()}
            if "game_id" not in media_columns:
                cursor.execute("ALTER TABLE media_files ADD COLUMN game_id TEXT")

            # Migration: Add library_id to scan_history table
            cursor.execute("PRAGMA table_info(scan_history)")
            scan_history_columns = {row[1] for row in cursor.fetchall()}
            if "library_id" not in scan_history_columns:
                cursor.execute("ALTER TABLE scan_history ADD COLUMN library_id TEXT")

            # Create indexes for better query performance
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_md5 ON files(md5)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_filename ON files(filename)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_duplicate ON files(is_potential_duplicate)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_rom_serial ON files(rom_serial)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_moved_rom_serial ON moved_files(rom_serial)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_rom ON media_files(rom_filepath)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_category ON media_files(category)"
            )

            # Indexes for new tables and columns
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_library ON files(library_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_game ON files(game_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_game ON media_files(game_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_library ON games(library_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_system ON games(system)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_normalized ON games(normalized_title)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_ra ON games(ra_game_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_queue_status ON scan_queue(status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_queue_position ON scan_queue(position)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_queue_library ON scan_queue(library_id)"
            )

            # Additional indexes for performance
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_extension ON files(extension)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_ra_supported ON files(ra_supported)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_normalized ON files(normalized_name)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_simplified ON files(simplified_filename)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_title ON games(title)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_games_ra_supported ON games(ra_supported)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_media_type ON media_files(media_type)"
            )
            # Composite indexes for common query patterns
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_dir_ext ON files(extension, size_mb)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_md5_dupe ON files(md5, is_potential_duplicate)"
            )

            # Device transfers table - tracks what files have been sent where
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS device_transfers (
                    transfer_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filepath TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    dest_host TEXT NOT NULL,
                    dest_path TEXT NOT NULL,
                    file_size INTEGER DEFAULT 0,
                    md5 TEXT DEFAULT '',
                    rom_serial TEXT DEFAULT '',
                    system TEXT DEFAULT '',
                    status TEXT DEFAULT 'transferred',
                    transferred_at TEXT NOT NULL,
                    verified_at TEXT,
                    UNIQUE(filepath, dest_host, dest_path)
                )
            """)

            # Indexes for device_transfers
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_dest ON device_transfers(dest_host)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_filepath ON device_transfers(filepath)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_system ON device_transfers(system)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_transfers_status ON device_transfers(status)"
            )

            # Full-text search for game titles
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS games_fts USING fts5(
                    title, normalized_title, system, genre, developer, publisher,
                    content='games', content_rowid='rowid'
                )
            """)

            # Acquisition jobs table — tracks collection download jobs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS acquisition_jobs (
                    job_id TEXT PRIMARY KEY,
                    collection_id TEXT NOT NULL,
                    sub_collection TEXT DEFAULT 'all',
                    system TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    total_files INTEGER DEFAULT 0,
                    completed_files INTEGER DEFAULT 0,
                    failed_files INTEGER DEFAULT 0,
                    skipped_files INTEGER DEFAULT 0,
                    total_bytes INTEGER DEFAULT 0,
                    started_at TEXT,
                    completed_at TEXT,
                    pid INTEGER DEFAULT 0
                )
            """)

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_acq_status ON acquisition_jobs(status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_acq_collection ON acquisition_jobs(collection_id)"
            )

            # Schema version tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            cursor.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('version', ?)",
                (str(self.SCHEMA_VERSION),),
            )
            cursor.execute(
                "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('updated', ?)",
                (datetime.now().isoformat(),),
            )

    # === File Operations ===

    def insert_file(self, file: FileRecord) -> bool:
        """Insert or update a file record."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO files
                    (filepath, filename, md5, simplified_filename, size_mb,
                     created_time, modified_time, extension, is_potential_duplicate,
                     ra_supported, ra_game_id, ra_game_title, ra_checked_date, rom_serial)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file.filepath,
                        file.filename,
                        file.md5,
                        file.simplified_filename,
                        file.size_mb,
                        file.created_time,
                        file.modified_time,
                        file.extension,
                        int(file.is_potential_duplicate),
                        int(file.ra_supported),
                        file.ra_game_id,
                        file.ra_game_title,
                        file.ra_checked_date,
                        file.rom_serial,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def get_file(self, filepath: str) -> FileRecord | None:
        """Get a file record by filepath."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM files WHERE filepath = ?", (filepath,))
            row = cursor.fetchone()
            return FileRecord.from_row(row) if row else None

    def get_file_by_serial(self, rom_serial: str) -> FileRecord | None:
        """Get a file record by its ROM serial number."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM files WHERE rom_serial = ?", (rom_serial,))
            row = cursor.fetchone()
            return FileRecord.from_row(row) if row else None

    def get_files_by_serial(self, rom_serial: str) -> list[FileRecord]:
        """Get all files with a given ROM serial (may include duplicates with same content)."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM files WHERE rom_serial = ?", (rom_serial,))
            return [FileRecord.from_row(row) for row in cursor.fetchall()]

    def get_files_in_directory(self, directory: str) -> list[FileRecord]:
        """Get all files in a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            return [FileRecord.from_row(row) for row in cursor.fetchall()]

    def get_all_filepaths_in_directory(self, directory: str) -> set[str]:
        """Get all filepaths in a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT filepath FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            return {row["filepath"] for row in cursor.fetchall()}

    def delete_file(self, filepath: str) -> bool:
        """Delete a file record and its associated media records."""
        try:
            with self.cursor() as cursor:
                # Remove associated media records to prevent orphaned DB entries
                cursor.execute("DELETE FROM media_files WHERE rom_filepath = ?", (filepath,))
                cursor.execute("DELETE FROM files WHERE filepath = ?", (filepath,))
            return True
        except sqlite3.Error:
            return False

    def update_ra_status(
        self,
        filepath: str,
        ra_supported: bool,
        ra_game_id: int = 0,
        ra_game_title: str = "",
    ) -> bool:
        """Update RetroAchievements status for a file."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE files
                    SET ra_supported = ?, ra_game_id = ?, ra_game_title = ?, ra_checked_date = ?
                    WHERE filepath = ?
                    """,
                    (
                        int(ra_supported),
                        ra_game_id,
                        ra_game_title,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        filepath,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def update_ra_status_by_md5(
        self,
        md5: str,
        ra_supported: bool,
        ra_game_id: int = 0,
        ra_game_title: str = "",
    ) -> int:
        """Update RetroAchievements status for all files with a given MD5. Returns count updated."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                UPDATE files
                SET ra_supported = ?, ra_game_id = ?, ra_game_title = ?, ra_checked_date = ?
                WHERE md5 = ?
                """,
                (
                    int(ra_supported),
                    ra_game_id,
                    ra_game_title,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    md5,
                ),
            )
            return cursor.rowcount

    def get_unverified_hashes(self, directory: str | None = None) -> list[str]:
        """Get unique MD5 hashes that haven't been verified against RA."""
        with self.cursor() as cursor:
            if directory:
                cursor.execute(
                    """
                    SELECT DISTINCT md5 FROM files
                    WHERE md5 != '' AND ra_supported = 0 AND ra_game_id = 0
                    AND filepath LIKE ?
                    """,
                    (directory.rstrip("/") + "/%",),
                )
            else:
                cursor.execute(
                    """
                    SELECT DISTINCT md5 FROM files
                    WHERE md5 != '' AND ra_supported = 0 AND ra_game_id = 0
                    """
                )
            return [row["md5"] for row in cursor.fetchall()]

    def get_ra_stats(self) -> dict[str, int]:
        """Get RetroAchievements verification statistics."""
        with self.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_supported = 1")
            supported = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_supported = 0 AND ra_game_id = -1")
            not_supported = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_supported = 0 AND ra_game_id = 0")
            unverified = cursor.fetchone()[0]

            return {
                "ra_supported": supported,
                "ra_not_supported": not_supported,
                "ra_unverified": unverified,
            }

    def get_scanned_directories(self) -> list[dict]:
        """Get list of previously scanned directories with stats."""
        with self.cursor() as cursor:
            # Get directories from scan_history with file counts
            cursor.execute("""
                SELECT
                    sh.directory,
                    sh.last_scan_time,
                    COUNT(f.filepath) as file_count,
                    COUNT(CASE WHEN f.is_potential_duplicate = 1 THEN 1 END) as duplicate_count
                FROM scan_history sh
                LEFT JOIN files f ON f.filepath LIKE sh.directory || '/%'
                GROUP BY sh.directory
                ORDER BY sh.last_scan_time DESC
            """)

            results = []
            for row in cursor.fetchall():
                results.append({
                    "directory": row[0],
                    "last_scanned": row[1],
                    "file_count": row[2],
                    "duplicate_count": row[3],
                })
            return results

    def delete_files_in_directory(self, directory: str) -> int:
        """Delete all file records in a directory. Returns count deleted."""
        with self.cursor() as cursor:
            cursor.execute(
                "DELETE FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            return cursor.rowcount

    def get_file_count_in_directory(self, directory: str) -> int:
        """Get count of files in directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            return cursor.fetchone()[0]

    def get_total_size_in_directory(self, directory: str) -> float:
        """Get total size of files in directory (MB)."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT SUM(size_mb) FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            result = cursor.fetchone()[0]
            return result if result is not None else 0.0

    # === Duplicate Operations ===

    def reset_duplicates_in_directory(self, directory: str) -> None:
        """Reset all duplicate and cross-platform flags in a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                """UPDATE files SET
                    is_potential_duplicate = 0,
                    is_cross_platform = 0,
                    dupe_score = 0,
                    dupe_reason = ''
                WHERE filepath LIKE ?""",
                (directory.rstrip("/") + "/%",),
            )

    def populate_normalized_names(self, directory: str) -> int:
        """Populate normalized_name for all files in directory using the normalize function."""
        from duper.utils.helpers import normalize_rom_name

        with self.cursor() as cursor:
            cursor.execute(
                "SELECT filepath, filename FROM files WHERE filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            rows = cursor.fetchall()

            updated = 0
            for row in rows:
                normalized = normalize_rom_name(row["filename"])
                cursor.execute(
                    "UPDATE files SET normalized_name = ? WHERE filepath = ?",
                    (normalized, row["filepath"]),
                )
                updated += 1

            return updated

    def get_system_directories(self, base_directory: str) -> list[str]:
        """Get list of unique system directories (immediate subdirectories with files)."""
        with self.cursor() as cursor:
            # Get distinct parent directories of files
            cursor.execute(
                """
                SELECT DISTINCT
                    CASE
                        WHEN filepath LIKE ? || '%/%/%' THEN
                            SUBSTR(filepath, 1, LENGTH(?) + INSTR(SUBSTR(filepath, LENGTH(?) + 1), '/'))
                        ELSE
                            SUBSTR(filepath, 1, LENGTH(filepath) - LENGTH(filename) - 1)
                    END as system_dir
                FROM files
                WHERE filepath LIKE ?
                """,
                (
                    base_directory.rstrip("/") + "/",
                    base_directory.rstrip("/") + "/",
                    base_directory.rstrip("/") + "/",
                    base_directory.rstrip("/") + "/%",
                ),
            )
            # Filter to only immediate subdirectories
            base_len = len(base_directory.rstrip("/")) + 1
            system_dirs = set()
            for row in cursor.fetchall():
                if row[0]:
                    # Extract just the first-level subdirectory
                    remaining = row[0][base_len:]
                    if "/" in remaining:
                        first_dir = remaining.split("/")[0]
                    else:
                        first_dir = remaining
                    if first_dir:
                        system_dirs.add(base_directory.rstrip("/") + "/" + first_dir)
            return sorted(system_dirs)

    def mark_duplicates_per_system(self, system_directory: str) -> int:
        """
        Mark duplicates within a single system directory with confidence scores.

        Scoring:
        - MD5 match: 100 (identical content - definite duplicate)
        - Same simplified_filename: 80 (same name, different extension like .z64/.v64)
        - Same normalized_name: 60 (same game, different region/version)

        Returns count of files marked.
        """
        with self.cursor() as cursor:
            pattern = system_directory.rstrip("/") + "/%"

            # Score 100: MD5 duplicates (identical content)
            cursor.execute(
                """
                UPDATE files
                SET is_potential_duplicate = 1,
                    dupe_score = CASE WHEN dupe_score < 100 THEN 100 ELSE dupe_score END,
                    dupe_reason = CASE WHEN dupe_score < 100 THEN 'md5' ELSE dupe_reason END
                WHERE filepath IN (
                    SELECT T1.filepath
                    FROM files T1, files T2
                    WHERE T1.md5 = T2.md5
                      AND T1.filepath != T2.filepath
                      AND T1.md5 != ''
                      AND T1.filepath LIKE ?
                      AND T2.filepath LIKE ?
                )
                """,
                (pattern, pattern),
            )

            # Score 80: Same simplified_filename (format variants like .z64/.v64)
            cursor.execute(
                """
                UPDATE files
                SET is_potential_duplicate = 1,
                    dupe_score = CASE WHEN dupe_score < 80 THEN 80 ELSE dupe_score END,
                    dupe_reason = CASE WHEN dupe_score < 80 THEN 'filename' ELSE dupe_reason END
                WHERE filepath IN (
                    SELECT T1.filepath
                    FROM files T1, files T2
                    WHERE T1.simplified_filename = T2.simplified_filename
                      AND T1.filepath != T2.filepath
                      AND T1.simplified_filename != ''
                      AND T1.filepath LIKE ?
                      AND T2.filepath LIKE ?
                )
                AND (dupe_score IS NULL OR dupe_score < 80)
                """,
                (pattern, pattern),
            )

            # Score 60: Same normalized_name (region/version variants)
            cursor.execute(
                """
                UPDATE files
                SET is_potential_duplicate = 1,
                    dupe_score = CASE WHEN dupe_score < 60 THEN 60 ELSE dupe_score END,
                    dupe_reason = CASE WHEN dupe_score < 60 THEN 'normalized' ELSE dupe_reason END
                WHERE filepath IN (
                    SELECT T1.filepath
                    FROM files T1, files T2
                    WHERE T1.normalized_name = T2.normalized_name
                      AND T1.filepath != T2.filepath
                      AND T1.normalized_name != ''
                      AND T1.filepath LIKE ?
                      AND T2.filepath LIKE ?
                )
                AND (dupe_score IS NULL OR dupe_score < 60)
                """,
                (pattern, pattern),
            )

            # Return count of marked files
            cursor.execute(
                "SELECT COUNT(*) FROM files WHERE is_potential_duplicate = 1 AND filepath LIKE ?",
                (pattern,),
            )
            return cursor.fetchone()[0]

    def mark_cross_platform_files(self, base_directory: str) -> int:
        """
        Mark files that exist on multiple platforms (same normalized name, different system directories).

        These are NOT duplicates - they're intentional multi-platform releases.
        Returns count of files marked.
        """
        with self.cursor() as cursor:
            pattern = base_directory.rstrip("/") + "/%"
            base_len = len(base_directory.rstrip("/")) + 1

            # Get all files with their system directories
            cursor.execute(
                "SELECT filepath, normalized_name FROM files WHERE filepath LIKE ? AND normalized_name != ''",
                (pattern,),
            )
            files = cursor.fetchall()

            # Group by normalized_name and extract system directory
            name_to_systems: dict[str, set[str]] = {}
            for filepath, norm_name in files:
                # Extract system directory (first subdirectory after base)
                remaining = filepath[base_len:]
                if "/" in remaining:
                    system = remaining.split("/")[0]
                else:
                    continue
                if norm_name not in name_to_systems:
                    name_to_systems[norm_name] = set()
                name_to_systems[norm_name].add(system)

            # Find names that appear in multiple systems
            cross_platform_names = {name for name, systems in name_to_systems.items() if len(systems) > 1}

            if not cross_platform_names:
                return 0

            # Mark all files with these names as cross-platform
            marked = 0
            for name in cross_platform_names:
                cursor.execute(
                    "UPDATE files SET is_cross_platform = 1 WHERE normalized_name = ? AND filepath LIKE ?",
                    (name, pattern),
                )
                marked += cursor.rowcount

            return marked

    def mark_filename_duplicates(self, directory: str, exclude_filename: str = "") -> None:
        """Mark files with duplicate filenames (legacy method, use mark_duplicates_per_system)."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                UPDATE files
                SET is_potential_duplicate = 1
                WHERE filepath IN (
                    SELECT T1.filepath
                    FROM files T1, files T2
                    WHERE T1.filename = T2.filename
                      AND T1.filepath != T2.filepath
                      AND T1.filename != ?
                      AND T1.filepath LIKE ?
                      AND T2.filepath LIKE ?
                )
                """,
                (exclude_filename, directory.rstrip("/") + "/%", directory.rstrip("/") + "/%"),
            )

    def mark_md5_duplicates(self, directory: str) -> None:
        """Mark files with duplicate MD5 hashes (legacy method, use mark_duplicates_per_system)."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                UPDATE files
                SET is_potential_duplicate = 1
                WHERE filepath IN (
                    SELECT T1.filepath
                    FROM files T1, files T2
                    WHERE T1.md5 = T2.md5
                      AND T1.filepath != T2.filepath
                      AND T1.md5 != ''
                      AND T1.filepath LIKE ?
                      AND T2.filepath LIKE ?
                )
                """,
                (directory.rstrip("/") + "/%", directory.rstrip("/") + "/%"),
            )

    def get_duplicate_md5_hashes(self, directory: str) -> list[str]:
        """Get list of MD5 hashes that have duplicates."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT md5 FROM files
                WHERE is_potential_duplicate = 1 AND md5 != '' AND filepath LIKE ?
                GROUP BY md5 HAVING COUNT(*) > 1
                """,
                (directory.rstrip("/") + "/%",),
            )
            return [row["md5"] for row in cursor.fetchall()]

    def get_files_by_md5(self, md5: str, directory: str) -> list[FileRecord]:
        """Get all files with a specific MD5 hash in a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM files WHERE md5 = ? AND filepath LIKE ?",
                (md5, directory.rstrip("/") + "/%"),
            )
            return [FileRecord.from_row(row) for row in cursor.fetchall()]

    def get_name_based_duplicate_groups(self, directory: str) -> dict[str, list[str]]:
        """Get groups of files with the same normalized name (potential duplicates by name).

        Returns a dict mapping normalized_name -> list of filepaths.
        Only includes groups with 2+ files that are marked as potential duplicates.
        """
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT normalized_name, GROUP_CONCAT(filepath, '||') as filepaths
                FROM files
                WHERE is_potential_duplicate = 1
                  AND normalized_name != ''
                  AND filepath LIKE ?
                GROUP BY normalized_name
                HAVING COUNT(*) > 1
                """,
                (directory.rstrip("/") + "/%",),
            )
            result = {}
            for row in cursor.fetchall():
                name = row["normalized_name"]
                paths = row["filepaths"].split("||")
                result[name] = paths
            return result

    def get_cross_platform_groups(self, directory: str) -> dict[str, list[str]]:
        """Get groups of files with the same normalized name across different system directories.

        Returns a dict mapping normalized_name -> list of filepaths.
        Only includes groups that span 2+ system directories.
        """
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT normalized_name, GROUP_CONCAT(filepath, '||') as filepaths
                FROM files
                WHERE is_cross_platform = 1
                  AND normalized_name != ''
                  AND filepath LIKE ?
                GROUP BY normalized_name
                HAVING COUNT(*) > 1
                """,
                (directory.rstrip("/") + "/%",),
            )
            result = {}
            for row in cursor.fetchall():
                name = row["normalized_name"]
                paths = row["filepaths"].split("||")
                result[name] = paths
            return result

    def get_potential_duplicates_count(self, directory: str) -> int:
        """Get count of potential duplicates in directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM files WHERE is_potential_duplicate = 1 AND filepath LIKE ?",
                (directory.rstrip("/") + "/%",),
            )
            return cursor.fetchone()[0]

    def get_duplicate_groups(self, directory: str) -> dict[str, list[str]]:
        """Get dictionary mapping MD5 hashes to lists of duplicate filepaths."""
        result: dict[str, list[str]] = {}
        for md5 in self.get_duplicate_md5_hashes(directory):
            with self.cursor() as cursor:
                cursor.execute(
                    "SELECT filepath FROM files WHERE md5 = ? AND filepath LIKE ?",
                    (md5, directory.rstrip("/") + "/%"),
                )
                result[md5] = [row["filepath"] for row in cursor.fetchall()]
        return result

    # === Moved Files Operations ===

    def record_moved_file(
        self,
        original_path: str,
        moved_to_path: str,
        size_mb: float = 0.0,
        filename: str = "",
        md5: str = "",
        rom_serial: str = "",
        reason: str = "",
    ) -> bool:
        """Record a file that was moved with its metadata for tracking space savings."""
        # Auto-generate rom_serial if not provided
        if not rom_serial and md5:
            rom_serial = generate_rom_serial(md5)
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO moved_files (original_filepath, moved_to_path, moved_time, size_mb, filename, md5, rom_serial, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        original_path,
                        moved_to_path,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        size_mb,
                        filename,
                        md5,
                        rom_serial,
                        reason,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def get_moved_files(self) -> list[MovedFile]:
        """Get all moved file records."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM moved_files ORDER BY move_id")
            return [MovedFile.from_row(row) for row in cursor.fetchall()]

    def get_moved_file(self, move_id: int) -> MovedFile | None:
        """Get a specific moved file record."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM moved_files WHERE move_id = ?", (move_id,))
            row = cursor.fetchone()
            return MovedFile.from_row(row) if row else None

    def delete_moved_file_record(self, move_id: int) -> bool:
        """Delete a moved file record (after restoration)."""
        try:
            with self.cursor() as cursor:
                cursor.execute("DELETE FROM moved_files WHERE move_id = ?", (move_id,))
            return True
        except sqlite3.Error:
            return False

    def get_moved_files_count(self) -> int:
        """Get count of moved files."""
        with self.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM moved_files")
            return cursor.fetchone()[0]

    def get_moved_filepaths(self) -> set[str]:
        """Get set of original filepaths that were moved."""
        with self.cursor() as cursor:
            cursor.execute("SELECT original_filepath FROM moved_files")
            return {row["original_filepath"] for row in cursor.fetchall()}

    # === Scan History Operations ===

    def has_scanned_directory(self, directory: str) -> bool:
        """Check if a directory has been scanned before."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM scan_history WHERE directory = ?",
                (directory,),
            )
            return cursor.fetchone()[0] > 0

    def update_scan_history(self, directory: str) -> None:
        """Update the scan history for a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "INSERT OR REPLACE INTO scan_history (directory, last_scan_time) VALUES (?, ?)",
                (directory, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )

    def get_scan_history(self) -> list[tuple[str, str]]:
        """Get all scan history records."""
        with self.cursor() as cursor:
            cursor.execute("SELECT directory, last_scan_time FROM scan_history")
            return [(row["directory"], row["last_scan_time"]) for row in cursor.fetchall()]

    def get_last_scan_time(self, directory: str) -> str | None:
        """Get the last scan time for a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT last_scan_time FROM scan_history WHERE directory = ?",
                (directory,),
            )
            row = cursor.fetchone()
            return row["last_scan_time"] if row else None

    # === Metrics Operations ===

    def log_metrics(
        self,
        start_time: float,
        end_time: float,
        scan_duration: int,
        errors: int,
        error_log: str,
        scan_directory: str,
        user: str,
        files_processed: int,
    ) -> None:
        """Log scan metrics."""
        start_time_str = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        end_time_str = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")

        hours = scan_duration // 3600
        minutes = (scan_duration % 3600) // 60
        seconds = scan_duration % 60
        duration_verbose = f"{hours} hours {minutes} minutes {seconds} seconds"

        with self.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO metrics
                (start_time, end_time, scan_duration_seconds, scan_duration_verbose,
                 errors_encountered, error_log, script_version, scan_directory,
                 user, database_path, files_processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    start_time_str,
                    end_time_str,
                    scan_duration,
                    duration_verbose,
                    errors,
                    error_log,
                    __version__,
                    scan_directory,
                    user,
                    str(self.db_path),
                    files_processed,
                ),
            )

    def get_latest_metrics(self) -> ScanMetrics | None:
        """Get the most recent scan metrics."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM metrics ORDER BY start_time DESC LIMIT 1")
            row = cursor.fetchone()
            return ScanMetrics.from_row(row) if row else None

    # === Statistics Operations ===

    def log_statistics(
        self,
        total_files: int,
        potential_duplicates: int,
        duplicate_info: dict[str, list[str]],
        scan_directory: str,
    ) -> None:
        """Log file statistics."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO file_statistics
                (scan_start_time, total_files, potential_duplicates, duplicate_file_info, scan_directory)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    total_files,
                    potential_duplicates,
                    json.dumps(duplicate_info),
                    scan_directory,
                ),
            )

    def get_latest_statistics(self) -> FileStatistics | None:
        """Get the most recent file statistics."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM file_statistics ORDER BY scan_id DESC LIMIT 1")
            row = cursor.fetchone()
            return FileStatistics.from_row(row) if row else None

    # === Utility Methods ===

    def delete_database(self) -> bool:
        """Delete the database file."""
        self.close()
        try:
            self.db_path.unlink()
            return True
        except OSError:
            return False

    def vacuum(self) -> None:
        """Vacuum the database to reclaim space."""
        conn = self.connect()
        conn.execute("VACUUM")

    def get_duplicate_space_stats(self) -> dict[str, Any]:
        """Get space statistics for duplicates.

        Returns:
            Dict with:
            - duplicate_space_mb: Total space used by duplicate files
            - wasted_space_mb: Space that would be saved by removing duplicates (keeps 1 per group)
            - duplicate_groups: Number of duplicate groups
            - duplicate_files: Total number of duplicate files
        """
        with self.cursor() as cursor:
            # Get total space used by all duplicates
            cursor.execute("""
                SELECT COALESCE(SUM(size_mb), 0) FROM files WHERE is_potential_duplicate = 1
            """)
            duplicate_space_mb = cursor.fetchone()[0]

            # Get MD5 hashes that have duplicates and calculate wasted space
            # Wasted space = total duplicate size - (size of one file per group)
            cursor.execute("""
                SELECT md5, COUNT(*) as cnt, MIN(size_mb) as single_size, SUM(size_mb) as total_size
                FROM files
                WHERE is_potential_duplicate = 1 AND md5 != ''
                GROUP BY md5
                HAVING COUNT(*) > 1
            """)
            rows = cursor.fetchall()

            duplicate_groups = len(rows)
            duplicate_files = sum(row["cnt"] for row in rows)
            wasted_space_mb = sum(row["total_size"] - row["single_size"] for row in rows)

        return {
            "duplicate_space_mb": duplicate_space_mb,
            "wasted_space_mb": wasted_space_mb,
            "duplicate_groups": duplicate_groups,
            "duplicate_files": duplicate_files,
        }

    def get_moved_files_size(self) -> float:
        """Get total size of files that have been moved (space saved).

        Uses the size_mb stored directly in moved_files table for accurate tracking.
        """
        with self.cursor() as cursor:
            # Get total size directly from moved_files table
            cursor.execute("SELECT COALESCE(SUM(size_mb), 0) FROM moved_files")
            result = cursor.fetchone()[0]
            return result or 0.0

    def get_size_breakdown(self) -> list[dict[str, Any]]:
        """Get size breakdown by file extension."""
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT
                    extension,
                    COUNT(*) as file_count,
                    SUM(size_mb) as total_size_mb,
                    AVG(size_mb) as avg_size_mb
                FROM files
                WHERE extension != ''
                GROUP BY extension
                ORDER BY total_size_mb DESC
                LIMIT 20
            """)
            rows = cursor.fetchall()

        return [
            {
                "extension": row[0],
                "file_count": row[1],
                "total_size_mb": round(row[2] or 0, 2),
                "avg_size_mb": round(row[3] or 0, 2),
            }
            for row in rows
        ]

    def get_size_by_directory(self) -> list[dict[str, Any]]:
        """Get size breakdown by top-level directory."""
        with self.cursor() as cursor:
            # Get size by directory (first path component after base)
            cursor.execute("""
                SELECT
                    CASE
                        WHEN INSTR(SUBSTR(filepath, 1), '/') > 0 THEN
                            SUBSTR(filepath, 1, INSTR(filepath, '/', 2) - 1)
                        ELSE filepath
                    END as directory,
                    COUNT(*) as file_count,
                    SUM(size_mb) as total_size_mb
                FROM files
                GROUP BY directory
                ORDER BY total_size_mb DESC
                LIMIT 15
            """)
            rows = cursor.fetchall()

        return [
            {
                "directory": row[0],
                "file_count": row[1],
                "total_size_mb": round(row[2] or 0, 2),
            }
            for row in rows
        ]

    def get_stats(self) -> dict[str, Any]:
        """Get overall database statistics."""
        with self.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM files")
            total_files = cursor.fetchone()[0]

            # Potential duplicates (by name similarity)
            cursor.execute("SELECT COUNT(*) FROM files WHERE is_potential_duplicate = 1")
            total_duplicates = cursor.fetchone()[0]

            # Exact MD5 duplicates (identical content)
            cursor.execute("""
                SELECT COUNT(*) FROM files WHERE md5 IN (
                    SELECT md5 FROM files WHERE md5 != ''
                    GROUP BY md5 HAVING COUNT(*) > 1
                )
            """)
            exact_duplicates = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM moved_files")
            total_moved = cursor.fetchone()[0]

            cursor.execute("SELECT SUM(size_mb) FROM files")
            total_size = cursor.fetchone()[0] or 0

        # Get duplicate space stats
        dup_stats = self.get_duplicate_space_stats()

        # Get space saved from moved files
        space_saved_mb = self.get_moved_files_size()

        # Get size breakdown
        size_breakdown = self.get_size_breakdown()

        return {
            "total_files": total_files,
            "total_duplicates": total_duplicates,  # Name-based potential duplicates
            "exact_duplicates": exact_duplicates,  # MD5-based exact duplicates
            "total_moved": total_moved,
            "total_size_mb": total_size,
            "database_path": str(self.db_path),
            # Space statistics
            "duplicate_space_mb": dup_stats["duplicate_space_mb"],
            "wasted_space_mb": dup_stats["wasted_space_mb"],
            "duplicate_groups": dup_stats["duplicate_groups"],
            "space_saved_mb": space_saved_mb,
            # Size breakdown
            "size_breakdown": size_breakdown,
        }

    # === Media Operations ===

    def insert_media(
        self,
        rom_filepath: str,
        media_path: str,
        media_type: str,
        category: str,
        filename: str,
        size_bytes: int = 0,
        mime_type: str = "",
        width: int = 0,
        height: int = 0,
        duration_seconds: float = 0.0,
    ) -> int | None:
        """Insert or update a media file record. Returns the media_id."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO media_files
                    (rom_filepath, media_path, media_type, category, filename,
                     size_bytes, mime_type, width, height, duration_seconds, scanned_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rom_filepath,
                        media_path,
                        media_type,
                        category,
                        filename,
                        size_bytes,
                        mime_type,
                        width,
                        height,
                        duration_seconds,
                        datetime.now().isoformat(),
                    ),
                )
                return cursor.lastrowid
        except sqlite3.Error:
            return None

    def get_media_for_rom(self, rom_filepath: str) -> list[MediaRecord]:
        """Get all media files associated with a ROM."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM media_files
                WHERE rom_filepath = ?
                ORDER BY category
                """,
                (rom_filepath,),
            )
            rows = cursor.fetchall()
        return [MediaRecord.from_row(row) for row in rows]

    def get_media_by_category(self, rom_filepath: str, category: str) -> MediaRecord | None:
        """Get a specific media file by category for a ROM."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM media_files
                WHERE rom_filepath = ? AND category = ?
                LIMIT 1
                """,
                (rom_filepath, category),
            )
            row = cursor.fetchone()
        return MediaRecord.from_row(row) if row else None

    def get_best_cover_for_rom(self, rom_filepath: str) -> MediaRecord | None:
        """Get the best cover art for a ROM, checking preferred categories in order."""
        # Preferred order for cover art
        preferred_categories = ["covers", "boxart", "box", "3dboxes", "miximages", "screenshots", "titlescreens"]

        with self.cursor() as cursor:
            for category in preferred_categories:
                cursor.execute(
                    """
                    SELECT * FROM media_files
                    WHERE rom_filepath = ? AND category = ?
                    LIMIT 1
                    """,
                    (rom_filepath, category),
                )
                row = cursor.fetchone()
                if row:
                    return MediaRecord.from_row(row)

            # Fall back to any image media
            cursor.execute(
                """
                SELECT * FROM media_files
                WHERE rom_filepath = ? AND media_type = 'image'
                LIMIT 1
                """,
                (rom_filepath,),
            )
            row = cursor.fetchone()
            return MediaRecord.from_row(row) if row else None

    def delete_media_for_rom(self, rom_filepath: str) -> int:
        """Delete all media records for a ROM. Returns count deleted."""
        with self.cursor() as cursor:
            cursor.execute(
                "DELETE FROM media_files WHERE rom_filepath = ?",
                (rom_filepath,),
            )
            return cursor.rowcount

    def get_media_stats(self) -> dict[str, Any]:
        """Get media statistics."""
        with self.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM media_files")
            total_media = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT rom_filepath) FROM media_files")
            roms_with_media = cursor.fetchone()[0]

            cursor.execute("""
                SELECT category, COUNT(*) as count
                FROM media_files
                GROUP BY category
                ORDER BY count DESC
            """)
            by_category = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("""
                SELECT media_type, COUNT(*) as count
                FROM media_files
                GROUP BY media_type
            """)
            by_type = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM media_files")
            total_size_bytes = cursor.fetchone()[0]

        return {
            "total_media_files": total_media,
            "roms_with_media": roms_with_media,
            "by_category": by_category,
            "by_type": by_type,
            "total_size_bytes": total_size_bytes,
            "total_size_mb": round(total_size_bytes / (1024 * 1024), 2) if total_size_bytes else 0,
        }

    def clear_media_for_directory(self, directory: str) -> int:
        """Clear all media records for ROMs in a directory."""
        with self.cursor() as cursor:
            cursor.execute(
                "DELETE FROM media_files WHERE rom_filepath LIKE ?",
                (f"{directory}%",),
            )
            return cursor.rowcount

    # === Library Operations ===

    def insert_library(self, library: Library) -> bool:
        """Insert or update a library record."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO libraries
                    (library_id, name, root_path, device_type, remote_host_id,
                     remote_library_id, status, last_scan_time, last_sync_time,
                     total_games, total_files, total_size_mb, duplicate_count,
                     created_time, updated_time, settings_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        library.library_id,
                        library.name,
                        library.root_path,
                        library.device_type,
                        library.remote_host_id,
                        library.remote_library_id,
                        library.status,
                        library.last_scan_time,
                        library.last_sync_time,
                        library.total_games,
                        library.total_files,
                        library.total_size_mb,
                        library.duplicate_count,
                        library.created_time,
                        library.updated_time,
                        json.dumps(library.settings),
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def get_library(self, library_id: str) -> Library | None:
        """Get a library by ID."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM libraries WHERE library_id = ?", (library_id,))
            row = cursor.fetchone()
            return Library.from_row(row) if row else None

    def get_library_by_name(self, name: str) -> Library | None:
        """Get a library by name."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM libraries WHERE name = ?", (name,))
            row = cursor.fetchone()
            return Library.from_row(row) if row else None

    def get_all_libraries(self) -> list[Library]:
        """Get all libraries."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM libraries ORDER BY name")
            return [Library.from_row(row) for row in cursor.fetchall()]

    def get_local_libraries(self) -> list[Library]:
        """Get all local libraries."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM libraries WHERE device_type = 'local' ORDER BY name"
            )
            return [Library.from_row(row) for row in cursor.fetchall()]

    def get_remote_libraries(self) -> list[Library]:
        """Get all remote libraries."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM libraries WHERE device_type = 'remote' ORDER BY name"
            )
            return [Library.from_row(row) for row in cursor.fetchall()]

    def update_library(self, library: Library) -> bool:
        """Update a library record."""
        library.updated_time = datetime.now().isoformat()
        return self.insert_library(library)

    def update_library_stats(
        self,
        library_id: str,
        total_games: int | None = None,
        total_files: int | None = None,
        total_size_mb: float | None = None,
        duplicate_count: int | None = None,
    ) -> bool:
        """Update library statistics."""
        try:
            with self.cursor() as cursor:
                updates = ["updated_time = ?"]
                values: list = [datetime.now().isoformat()]

                if total_games is not None:
                    updates.append("total_games = ?")
                    values.append(total_games)
                if total_files is not None:
                    updates.append("total_files = ?")
                    values.append(total_files)
                if total_size_mb is not None:
                    updates.append("total_size_mb = ?")
                    values.append(total_size_mb)
                if duplicate_count is not None:
                    updates.append("duplicate_count = ?")
                    values.append(duplicate_count)

                values.append(library_id)
                cursor.execute(
                    f"UPDATE libraries SET {', '.join(updates)} WHERE library_id = ?",
                    values,
                )
            return True
        except sqlite3.Error:
            return False

    def update_library_status(self, library_id: str, status: str) -> bool:
        """Update library status."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    "UPDATE libraries SET status = ?, updated_time = ? WHERE library_id = ?",
                    (status, datetime.now().isoformat(), library_id),
                )
            return True
        except sqlite3.Error:
            return False

    def update_library_scan_time(self, library_id: str) -> bool:
        """Update library last scan time to now."""
        try:
            with self.cursor() as cursor:
                now = datetime.now().isoformat()
                cursor.execute(
                    "UPDATE libraries SET last_scan_time = ?, updated_time = ? WHERE library_id = ?",
                    (now, now, library_id),
                )
            return True
        except sqlite3.Error:
            return False

    def delete_library(self, library_id: str) -> bool:
        """Delete a library and all its associated data."""
        try:
            with self.cursor() as cursor:
                # Delete associated games
                cursor.execute("DELETE FROM games WHERE library_id = ?", (library_id,))
                # Clear library_id from files (don't delete files)
                cursor.execute(
                    "UPDATE files SET library_id = NULL, game_id = NULL WHERE library_id = ?",
                    (library_id,),
                )
                # Delete scan queue items
                cursor.execute("DELETE FROM scan_queue WHERE library_id = ?", (library_id,))
                # Delete the library
                cursor.execute("DELETE FROM libraries WHERE library_id = ?", (library_id,))
            return True
        except sqlite3.Error:
            return False

    def get_or_create_default_library(self) -> Library:
        """Get or create the default 'Local Collection' library."""
        existing = self.get_library_by_name("Local Collection")
        if existing:
            return existing

        from duper.core.library import generate_uuid

        library = Library(
            library_id=generate_uuid(),
            name="Local Collection",
            root_path="",
            device_type="local",
            status="active",
        )
        self.insert_library(library)
        return library

    # === Game Operations ===

    def insert_game(self, game: Game) -> bool:
        """Insert or update a game record."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO games
                    (game_id, library_id, title, normalized_title, system,
                     ra_game_id, ss_game_id, primary_file_path, primary_rom_serial,
                     region, release_year, genre, developer, publisher, description,
                     file_count, total_size_mb, has_media, ra_supported,
                     created_time, updated_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        game.game_id,
                        game.library_id,
                        game.title,
                        game.normalized_title,
                        game.system,
                        game.ra_game_id,
                        game.ss_game_id,
                        game.primary_file_path,
                        game.primary_rom_serial,
                        game.region,
                        game.release_year,
                        game.genre,
                        game.developer,
                        game.publisher,
                        game.description,
                        game.file_count,
                        game.total_size_mb,
                        int(game.has_media),
                        int(game.ra_supported),
                        game.created_time,
                        game.updated_time,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def get_game(self, game_id: str) -> Game | None:
        """Get a game by ID."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM games WHERE game_id = ?", (game_id,))
            row = cursor.fetchone()
            return Game.from_row(row) if row else None

    def get_games_in_library(
        self,
        library_id: str,
        system: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Game]:
        """Get games in a library, optionally filtered by system."""
        with self.cursor() as cursor:
            query = "SELECT * FROM games WHERE library_id = ?"
            params: list = [library_id]

            if system:
                query += " AND system = ?"
                params.append(system)

            query += " ORDER BY title"

            if limit:
                query += " LIMIT ? OFFSET ?"
                params.extend([limit, offset])

            cursor.execute(query, params)
            return [Game.from_row(row) for row in cursor.fetchall()]

    def get_game_count_in_library(self, library_id: str) -> int:
        """Get count of games in a library."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM games WHERE library_id = ?", (library_id,)
            )
            return cursor.fetchone()[0]

    def get_games_by_system(self, library_id: str) -> dict[str, int]:
        """Get game counts grouped by system for a library."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT system, COUNT(*) as count
                FROM games
                WHERE library_id = ?
                GROUP BY system
                ORDER BY count DESC
                """,
                (library_id,),
            )
            return {row[0]: row[1] for row in cursor.fetchall()}

    def get_game_by_normalized_title(
        self, library_id: str, normalized_title: str, system: str
    ) -> Game | None:
        """Get a game by normalized title and system within a library."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM games
                WHERE library_id = ? AND normalized_title = ? AND system = ?
                """,
                (library_id, normalized_title, system),
            )
            row = cursor.fetchone()
            return Game.from_row(row) if row else None

    def update_game(self, game: Game) -> bool:
        """Update a game record."""
        game.updated_time = datetime.now().isoformat()
        return self.insert_game(game)

    def delete_game(self, game_id: str) -> bool:
        """Delete a game."""
        try:
            with self.cursor() as cursor:
                # Clear game_id from files
                cursor.execute("UPDATE files SET game_id = NULL WHERE game_id = ?", (game_id,))
                # Clear game_id from media
                cursor.execute("UPDATE media_files SET game_id = NULL WHERE game_id = ?", (game_id,))
                # Delete the game
                cursor.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
            return True
        except sqlite3.Error:
            return False

    def delete_games_in_library(self, library_id: str) -> int:
        """Delete all games in a library. Returns count deleted."""
        with self.cursor() as cursor:
            # Clear game_id from files in this library
            cursor.execute(
                "UPDATE files SET game_id = NULL WHERE library_id = ?", (library_id,)
            )
            # Clear game_id from media for files in this library
            cursor.execute(
                """
                UPDATE media_files SET game_id = NULL
                WHERE rom_filepath IN (SELECT filepath FROM files WHERE library_id = ?)
                """,
                (library_id,),
            )
            # Delete games
            cursor.execute("DELETE FROM games WHERE library_id = ?", (library_id,))
            return cursor.rowcount

    # === Scan Queue Operations ===

    def insert_queue_item(self, item: ScanQueueItem) -> bool:
        """Insert or update a scan queue item."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO scan_queue
                    (queue_id, library_id, directory, status, priority, position,
                     queued_time, started_time, completed_time, full_scan, scan_media,
                     scan_ra, total_files, processed_files, current_file,
                     files_processed, errors, error_log)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item.queue_id,
                        item.library_id,
                        item.directory,
                        item.status,
                        item.priority,
                        item.position,
                        item.queued_time,
                        item.started_time,
                        item.completed_time,
                        int(item.full_scan),
                        int(item.scan_media),
                        int(item.scan_ra),
                        item.total_files,
                        item.processed_files,
                        item.current_file,
                        item.files_processed,
                        item.errors,
                        item.error_log,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def get_queue_item(self, queue_id: str) -> ScanQueueItem | None:
        """Get a queue item by ID."""
        with self.cursor() as cursor:
            cursor.execute("SELECT * FROM scan_queue WHERE queue_id = ?", (queue_id,))
            row = cursor.fetchone()
            return ScanQueueItem.from_row(row) if row else None

    def get_scan_queue(self, include_completed: bool = False) -> list[ScanQueueItem]:
        """Get all scan queue items in order."""
        with self.cursor() as cursor:
            if include_completed:
                cursor.execute(
                    "SELECT * FROM scan_queue ORDER BY priority DESC, position ASC"
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM scan_queue
                    WHERE status IN ('pending', 'running')
                    ORDER BY priority DESC, position ASC
                    """
                )
            return [ScanQueueItem.from_row(row) for row in cursor.fetchall()]

    def get_pending_queue_items(self) -> list[ScanQueueItem]:
        """Get pending queue items in order."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM scan_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, position ASC
                """
            )
            return [ScanQueueItem.from_row(row) for row in cursor.fetchall()]

    def get_running_queue_item(self) -> ScanQueueItem | None:
        """Get the currently running queue item, if any."""
        with self.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM scan_queue WHERE status = 'running' LIMIT 1"
            )
            row = cursor.fetchone()
            return ScanQueueItem.from_row(row) if row else None

    def get_next_queue_item(self) -> ScanQueueItem | None:
        """Get the next pending item in the queue."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM scan_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, position ASC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            return ScanQueueItem.from_row(row) if row else None

    def add_to_queue(
        self,
        library_id: str,
        directory: str,
        priority: int = 0,
        full_scan: bool = False,
        scan_media: bool = True,
        scan_ra: bool = True,
    ) -> ScanQueueItem:
        """Add a directory to the scan queue."""
        from duper.core.library import generate_uuid

        # Get current max position
        with self.cursor() as cursor:
            cursor.execute("SELECT MAX(position) FROM scan_queue")
            max_pos = cursor.fetchone()[0] or 0

        item = ScanQueueItem(
            queue_id=generate_uuid(),
            library_id=library_id,
            directory=directory,
            status="pending",
            priority=priority,
            position=max_pos + 1,
            full_scan=full_scan,
            scan_media=scan_media,
            scan_ra=scan_ra,
        )
        self.insert_queue_item(item)
        return item

    def update_queue_item(self, item: ScanQueueItem) -> bool:
        """Update a queue item."""
        return self.insert_queue_item(item)

    def update_queue_item_status(
        self,
        queue_id: str,
        status: str,
        started_time: str | None = None,
        completed_time: str | None = None,
    ) -> bool:
        """Update queue item status."""
        try:
            with self.cursor() as cursor:
                if started_time:
                    cursor.execute(
                        "UPDATE scan_queue SET status = ?, started_time = ? WHERE queue_id = ?",
                        (status, started_time, queue_id),
                    )
                elif completed_time:
                    cursor.execute(
                        "UPDATE scan_queue SET status = ?, completed_time = ? WHERE queue_id = ?",
                        (status, completed_time, queue_id),
                    )
                else:
                    cursor.execute(
                        "UPDATE scan_queue SET status = ? WHERE queue_id = ?",
                        (status, queue_id),
                    )
            return True
        except sqlite3.Error:
            return False

    def update_queue_item_progress(
        self,
        queue_id: str,
        total_files: int,
        processed_files: int,
        current_file: str,
    ) -> bool:
        """Update queue item progress during scan."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE scan_queue
                    SET total_files = ?, processed_files = ?, current_file = ?
                    WHERE queue_id = ?
                    """,
                    (total_files, processed_files, current_file, queue_id),
                )
            return True
        except sqlite3.Error:
            return False

    def complete_queue_item(
        self,
        queue_id: str,
        files_processed: int,
        errors: int,
        error_log: str = "",
    ) -> bool:
        """Mark a queue item as completed."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE scan_queue
                    SET status = 'completed', completed_time = ?,
                        files_processed = ?, errors = ?, error_log = ?
                    WHERE queue_id = ?
                    """,
                    (
                        datetime.now().isoformat(),
                        files_processed,
                        errors,
                        error_log,
                        queue_id,
                    ),
                )
            return True
        except sqlite3.Error:
            return False

    def fail_queue_item(self, queue_id: str, error_log: str) -> bool:
        """Mark a queue item as failed."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE scan_queue
                    SET status = 'failed', completed_time = ?, error_log = ?
                    WHERE queue_id = ?
                    """,
                    (datetime.now().isoformat(), error_log, queue_id),
                )
            return True
        except sqlite3.Error:
            return False

    def delete_queue_item(self, queue_id: str) -> bool:
        """Delete a queue item."""
        try:
            with self.cursor() as cursor:
                cursor.execute("DELETE FROM scan_queue WHERE queue_id = ?", (queue_id,))
            return True
        except sqlite3.Error:
            return False

    def clear_completed_queue_items(self) -> int:
        """Clear completed and failed queue items. Returns count deleted."""
        with self.cursor() as cursor:
            cursor.execute(
                "DELETE FROM scan_queue WHERE status IN ('completed', 'failed', 'cancelled')"
            )
            return cursor.rowcount

    def reorder_queue_item(self, queue_id: str, new_position: int) -> bool:
        """Move a queue item to a new position."""
        try:
            item = self.get_queue_item(queue_id)
            if not item or item.status != "pending":
                return False

            with self.cursor() as cursor:
                old_position = item.position

                if new_position > old_position:
                    # Moving down - shift items up
                    cursor.execute(
                        """
                        UPDATE scan_queue
                        SET position = position - 1
                        WHERE position > ? AND position <= ? AND status = 'pending'
                        """,
                        (old_position, new_position),
                    )
                else:
                    # Moving up - shift items down
                    cursor.execute(
                        """
                        UPDATE scan_queue
                        SET position = position + 1
                        WHERE position >= ? AND position < ? AND status = 'pending'
                        """,
                        (new_position, old_position),
                    )

                cursor.execute(
                    "UPDATE scan_queue SET position = ? WHERE queue_id = ?",
                    (new_position, queue_id),
                )
            return True
        except sqlite3.Error:
            return False

    def reset_interrupted_queue_items(self) -> int:
        """Reset any 'running' items to 'pending' (for restart recovery). Returns count reset."""
        with self.cursor() as cursor:
            cursor.execute(
                "UPDATE scan_queue SET status = 'pending', started_time = NULL WHERE status = 'running'"
            )
            return cursor.rowcount

    # === Batch Operations ===

    def insert_files_batch(self, files: list[FileRecord]) -> int:
        """Insert multiple file records in a single transaction. Returns count inserted."""
        if not files:
            return 0
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.executemany(
                """
                INSERT OR REPLACE INTO files
                (filepath, filename, md5, simplified_filename, size_mb,
                 created_time, modified_time, extension, is_potential_duplicate,
                 ra_supported, ra_game_id, ra_game_title, ra_checked_date, rom_serial)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        f.filepath, f.filename, f.md5, f.simplified_filename, f.size_mb,
                        f.created_time, f.modified_time, f.extension,
                        int(f.is_potential_duplicate), int(f.ra_supported),
                        f.ra_game_id, f.ra_game_title, f.ra_checked_date, f.rom_serial,
                    )
                    for f in files
                ],
            )
            conn.commit()
            return len(files)
        except sqlite3.Error:
            conn.rollback()
            return 0
        finally:
            cursor.close()

    def update_ra_status_batch(
        self, updates: list[tuple[str, bool, int, str]]
    ) -> int:
        """Batch update RA status. Each tuple: (md5, ra_supported, ra_game_id, ra_game_title).
        Returns count updated."""
        if not updates:
            return 0
        conn = self.connect()
        cursor = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            total = 0
            for md5, supported, game_id, game_title in updates:
                cursor.execute(
                    """
                    UPDATE files
                    SET ra_supported = ?, ra_game_id = ?, ra_game_title = ?, ra_checked_date = ?
                    WHERE md5 = ?
                    """,
                    (int(supported), game_id, game_title, now, md5),
                )
                total += cursor.rowcount
            conn.commit()
            return total
        except sqlite3.Error:
            conn.rollback()
            return 0
        finally:
            cursor.close()

    # === Search Operations ===

    def search_games(self, query: str, library_id: str | None = None, limit: int = 50) -> list[Game]:
        """Full-text search for games by title, system, genre, etc."""
        with self.cursor() as cursor:
            try:
                if library_id:
                    cursor.execute(
                        """
                        SELECT g.* FROM games g
                        JOIN games_fts fts ON g.rowid = fts.rowid
                        WHERE games_fts MATCH ? AND g.library_id = ?
                        ORDER BY rank
                        LIMIT ?
                        """,
                        (query, library_id, limit),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT g.* FROM games g
                        JOIN games_fts fts ON g.rowid = fts.rowid
                        WHERE games_fts MATCH ?
                        ORDER BY rank
                        LIMIT ?
                        """,
                        (query, limit),
                    )
                return [Game.from_row(row) for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                # FTS table might not be populated yet, fall back to LIKE
                return self._search_games_like(query, library_id, limit)

    def _search_games_like(self, query: str, library_id: str | None, limit: int) -> list[Game]:
        """Fallback game search using LIKE."""
        with self.cursor() as cursor:
            pattern = f"%{query}%"
            if library_id:
                cursor.execute(
                    "SELECT * FROM games WHERE library_id = ? AND title LIKE ? ORDER BY title LIMIT ?",
                    (library_id, pattern, limit),
                )
            else:
                cursor.execute(
                    "SELECT * FROM games WHERE title LIKE ? ORDER BY title LIMIT ?",
                    (pattern, limit),
                )
            return [Game.from_row(row) for row in cursor.fetchall()]

    def rebuild_fts_index(self) -> None:
        """Rebuild the full-text search index from the games table."""
        with self.cursor() as cursor:
            cursor.execute("DELETE FROM games_fts")
            cursor.execute("""
                INSERT INTO games_fts(rowid, title, normalized_title, system, genre, developer, publisher)
                SELECT rowid, title, normalized_title, system, genre, developer, publisher FROM games
            """)

    # === Statistics & Analytics ===

    def get_collection_stats(self) -> dict[str, Any]:
        """Get comprehensive collection statistics in a single query batch."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            stats: dict[str, Any] = {}

            cursor.execute("SELECT COUNT(*), COALESCE(SUM(size_mb), 0) FROM files")
            row = cursor.fetchone()
            stats["total_files"] = row[0]
            stats["total_size_mb"] = row[1]

            cursor.execute("SELECT COUNT(DISTINCT extension) FROM files WHERE extension != ''")
            stats["total_formats"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE is_potential_duplicate = 1")
            stats["total_duplicates"] = cursor.fetchone()[0]

            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT md5 FROM files WHERE md5 != '' AND is_potential_duplicate = 1
                    GROUP BY md5 HAVING COUNT(*) > 1
                )
            """)
            stats["duplicate_groups"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM moved_files")
            stats["total_moved"] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(SUM(size_mb), 0) FROM moved_files")
            stats["space_saved_mb"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_supported = 1")
            stats["ra_supported"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_game_id = -1")
            stats["ra_not_supported"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM files WHERE ra_supported = 0 AND ra_game_id = 0")
            stats["ra_unverified"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games")
            stats["total_games"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM libraries")
            stats["total_libraries"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT system) FROM games WHERE system != ''")
            stats["total_systems"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM media_files")
            stats["total_media"] = cursor.fetchone()[0]

            # Per-system breakdown
            cursor.execute("""
                SELECT extension, COUNT(*) as file_count, COALESCE(SUM(size_mb), 0) as total_size
                FROM files WHERE extension != ''
                GROUP BY extension
                ORDER BY total_size DESC
            """)
            stats["size_breakdown"] = [
                {"extension": r[0], "file_count": r[1], "total_size_mb": r[2]}
                for r in cursor.fetchall()
            ]

            return stats
        finally:
            cursor.close()

    def get_system_summary(self, base_directory: str, top_games: int = 5) -> list[dict]:
        """Get per-system summary stats with top games for a directory."""
        with self.cursor() as cursor:
            pattern = base_directory.rstrip("/") + "/%"
            base_len = len(base_directory.rstrip("/")) + 1
            cursor.execute(
                """
                SELECT filepath, filename, size_mb, ra_supported, ra_game_title, extension
                FROM files WHERE filepath LIKE ?
                """,
                (pattern,),
            )
            systems: dict[str, dict] = {}
            system_files: dict[str, list] = {}
            for row in cursor.fetchall():
                remaining = row["filepath"][base_len:]
                system = remaining.split("/")[0] if "/" in remaining else "unsorted"
                if system not in systems:
                    systems[system] = {
                        "system": system, "file_count": 0, "total_size_mb": 0.0,
                        "ra_supported": 0, "ra_total": 0, "formats": set(),
                    }
                    system_files[system] = []
                systems[system]["file_count"] += 1
                systems[system]["total_size_mb"] += row["size_mb"] or 0
                systems[system]["ra_total"] += 1
                if row["ra_supported"]:
                    systems[system]["ra_supported"] += 1
                if row["extension"]:
                    systems[system]["formats"].add(row["extension"])
                # Collect file info for top games
                title = row["ra_game_title"] or row["filename"].rsplit(".", 1)[0]
                system_files[system].append({
                    "title": title,
                    "size_mb": row["size_mb"] or 0,
                    "ra_supported": bool(row["ra_supported"]),
                })

            result = []
            for s in sorted(systems.values(), key=lambda x: -x["total_size_mb"]):
                s["formats"] = sorted(s["formats"])
                # Top games: prioritize RA-supported, then largest
                files = system_files.get(s["system"], [])
                files.sort(key=lambda f: (-int(f["ra_supported"]), -f["size_mb"]))
                s["top_games"] = [f["title"] for f in files[:top_games]]
                result.append(s)
            return result

    # === Transfer Tracking ===

    def record_transfer(
        self,
        filepath: str,
        filename: str,
        dest_host: str,
        dest_path: str,
        file_size: int = 0,
        md5: str = "",
        rom_serial: str = "",
        system: str = "",
    ) -> bool:
        """Record a file transfer to a destination device."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO device_transfers
                    (filepath, filename, dest_host, dest_path, file_size,
                     md5, rom_serial, system, status, transferred_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'transferred', ?)
                    """,
                    (filepath, filename, dest_host, dest_path, file_size,
                     md5, rom_serial, system, datetime.now().isoformat()),
                )
            return True
        except sqlite3.Error:
            return False

    def is_file_transferred(
        self, filepath: str, dest_host: str, file_size: int = 0
    ) -> bool:
        """Check if a file has already been transferred to a destination.

        Matches on filepath + dest_host. If file_size is provided, also
        checks that the recorded size matches (detects changed files).
        """
        with self.cursor() as cursor:
            if file_size > 0:
                cursor.execute(
                    """
                    SELECT 1 FROM device_transfers
                    WHERE filepath = ? AND dest_host = ? AND file_size = ?
                    AND status = 'transferred'
                    """,
                    (filepath, dest_host, file_size),
                )
            else:
                cursor.execute(
                    """
                    SELECT 1 FROM device_transfers
                    WHERE filepath = ? AND dest_host = ?
                    AND status = 'transferred'
                    """,
                    (filepath, dest_host),
                )
            return cursor.fetchone() is not None

    def get_transferred_files(
        self, dest_host: str, system: str = ""
    ) -> list[dict[str, Any]]:
        """Get all files transferred to a destination, optionally filtered by system."""
        with self.cursor() as cursor:
            if system:
                cursor.execute(
                    """
                    SELECT filepath, filename, file_size, md5, system, transferred_at
                    FROM device_transfers
                    WHERE dest_host = ? AND system = ? AND status = 'transferred'
                    """,
                    (dest_host, system),
                )
            else:
                cursor.execute(
                    """
                    SELECT filepath, filename, file_size, md5, system, transferred_at
                    FROM device_transfers
                    WHERE dest_host = ? AND status = 'transferred'
                    """,
                    (dest_host,),
                )
            return [
                {
                    "filepath": row["filepath"],
                    "filename": row["filename"],
                    "file_size": row["file_size"],
                    "md5": row["md5"],
                    "system": row["system"],
                    "transferred_at": row["transferred_at"],
                }
                for row in cursor.fetchall()
            ]

    def get_transfer_stats(self, dest_host: str) -> dict[str, Any]:
        """Get transfer statistics for a destination."""
        with self.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) as total_files,
                       COALESCE(SUM(file_size), 0) as total_bytes,
                       COUNT(DISTINCT system) as total_systems,
                       MIN(transferred_at) as first_transfer,
                       MAX(transferred_at) as last_transfer
                FROM device_transfers
                WHERE dest_host = ? AND status = 'transferred'
                """,
                (dest_host,),
            )
            row = cursor.fetchone()
            return {
                "total_files": row["total_files"],
                "total_bytes": row["total_bytes"],
                "total_systems": row["total_systems"],
                "first_transfer": row["first_transfer"],
                "last_transfer": row["last_transfer"],
            }

    def build_transfer_manifest(
        self, source_dir: str, dest_host: str
    ) -> dict[str, list[dict[str, Any]]]:
        """Build a manifest of files that need transferring.

        Compares local files in source_dir against transfer records.
        Returns dict with 'to_transfer' (new/changed) and 'skipped' (already there).
        """
        import os

        to_transfer: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        roms_dir = os.path.join(source_dir, "roms")
        if not os.path.isdir(roms_dir):
            return {"to_transfer": to_transfer, "skipped": skipped}

        for sys_name in sorted(os.listdir(roms_dir)):
            sys_dir = os.path.join(roms_dir, sys_name)
            if not os.path.isdir(sys_dir):
                continue
            for fname in os.listdir(sys_dir):
                fpath = os.path.join(sys_dir, fname)
                if not os.path.isfile(fpath):
                    continue
                fsize = os.path.getsize(fpath)
                entry = {
                    "filepath": fpath,
                    "filename": fname,
                    "system": sys_name,
                    "file_size": fsize,
                }
                if self.is_file_transferred(fpath, dest_host, fsize):
                    skipped.append(entry)
                else:
                    to_transfer.append(entry)

        return {"to_transfer": to_transfer, "skipped": skipped}

    # === Media Transfer Tracking ===

    def get_files_without_media(
        self, directory: str = "", limit: int = 0
    ) -> list[dict[str, Any]]:
        """Get ROM files that have no associated media in the media_files table.

        These are candidates for scraping. Filters out non-ROM extensions
        (metadata.txt, systeminfo.txt, etc).
        """
        with self.cursor() as cursor:
            sql = """
                SELECT f.filepath, f.filename, f.md5, f.rom_serial, f.size_mb,
                       f.extension, f.ra_game_id
                FROM files f
                LEFT JOIN media_files m ON f.filepath = m.rom_filepath
                WHERE m.media_id IS NULL
                  AND f.extension NOT IN ('.txt', '.xml', '.json', '.cfg', '.ini', '.log', '.md')
                  AND f.filename NOT IN ('metadata.txt', 'systeminfo.txt')
            """
            params: list[Any] = []
            if directory:
                sql += " AND f.filepath LIKE ?"
                params.append(f"{directory}%")
            sql += " ORDER BY f.filepath"
            if limit > 0:
                sql += " LIMIT ?"
                params.append(limit)
            cursor.execute(sql, params)
            return [
                {
                    "filepath": row["filepath"],
                    "filename": row["filename"],
                    "md5": row["md5"],
                    "rom_serial": row["rom_serial"],
                    "size_mb": row["size_mb"],
                    "extension": row["extension"],
                    "ra_game_id": row["ra_game_id"],
                }
                for row in cursor.fetchall()
            ]

    def record_media_transfer(
        self,
        media_path: str,
        filename: str,
        dest_host: str,
        dest_path: str,
        file_size: int = 0,
        system: str = "",
        category: str = "",
    ) -> bool:
        """Record a media file transfer to a destination device."""
        try:
            with self.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO device_transfers
                    (filepath, filename, dest_host, dest_path, file_size,
                     md5, rom_serial, system, status, transferred_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'transferred', ?)
                    """,
                    (media_path, filename, dest_host, dest_path, file_size,
                     "", category, system, datetime.now().isoformat()),
                )
            return True
        except sqlite3.Error:
            return False

    # === Maintenance ===

    def vacuum(self) -> None:
        """Reclaim disk space and optimize database."""
        conn = self.connect()
        conn.execute("VACUUM")

    def analyze(self) -> None:
        """Update query planner statistics."""
        conn = self.connect()
        conn.execute("ANALYZE")

    def optimize(self) -> None:
        """Run full optimization: analyze + vacuum + FTS optimize."""
        self.analyze()
        self.vacuum()
        try:
            conn = self.connect()
            conn.execute("INSERT INTO games_fts(games_fts) VALUES('optimize')")
            conn.commit()
        except sqlite3.OperationalError:
            pass

    def integrity_check(self) -> dict[str, Any]:
        """Run integrity checks and report orphaned records."""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            result: dict[str, Any] = {}

            # SQLite integrity check
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            result["sqlite_integrity"] = integrity

            # Orphaned media (media pointing to non-existent files)
            cursor.execute("""
                SELECT COUNT(*) FROM media_files
                WHERE rom_filepath NOT IN (SELECT filepath FROM files)
                AND rom_filepath != ''
            """)
            result["orphaned_media"] = cursor.fetchone()[0]

            # Files with empty MD5 (scan incomplete)
            cursor.execute("SELECT COUNT(*) FROM files WHERE md5 IS NULL OR md5 = ''")
            result["files_no_md5"] = cursor.fetchone()[0]

            # Games with no files
            cursor.execute("""
                SELECT COUNT(*) FROM games
                WHERE game_id NOT IN (SELECT DISTINCT game_id FROM files WHERE game_id IS NOT NULL)
            """)
            result["games_no_files"] = cursor.fetchone()[0]

            # DB file size
            try:
                result["db_size_mb"] = self.db_path.stat().st_size / (1024 * 1024)
            except OSError:
                result["db_size_mb"] = 0

            # Table row counts
            for table in ["files", "games", "libraries", "media_files", "moved_files", "scan_queue"]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    result[f"count_{table}"] = cursor.fetchone()[0]
                except sqlite3.OperationalError:
                    result[f"count_{table}"] = 0

            return result
        finally:
            cursor.close()

    def cleanup_orphans(self) -> dict[str, int]:
        """Remove orphaned records. Returns counts of cleaned records."""
        cleaned = {}
        with self.cursor() as cursor:
            # Remove media for non-existent files
            cursor.execute("""
                DELETE FROM media_files
                WHERE rom_filepath NOT IN (SELECT filepath FROM files)
                AND rom_filepath != ''
            """)
            cleaned["orphaned_media"] = cursor.rowcount

            # Remove games with no files
            cursor.execute("""
                DELETE FROM games
                WHERE game_id NOT IN (
                    SELECT DISTINCT game_id FROM files WHERE game_id IS NOT NULL
                )
            """)
            cleaned["orphaned_games"] = cursor.rowcount

        return cleaned
