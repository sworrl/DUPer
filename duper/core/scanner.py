"""File scanner for DUPer."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from duper.core.config import DuperConfig, get_config
from duper.core.database import DuperDatabase, FileRecord, generate_rom_serial
from duper.core.retroachievements import get_ra_client
from duper.utils.helpers import (
    calculate_md5,
    get_file_create_time,
    get_file_mod_time,
    get_file_size_mb,
    get_username,
    should_ignore_file,
)


def _sanitize_path(path: str) -> str:
    """Sanitize a path string to remove invalid UTF-8 surrogates."""
    return path.encode("utf-8", errors="replace").decode("utf-8")


# Directories that should be skipped during scanning (won't contain ROMs)
SKIP_DIRECTORIES: frozenset[str] = frozenset({
    # Version control
    ".git", ".svn", ".hg", ".bzr",
    # Development/cache
    "__pycache__", "node_modules", ".venv", "venv", ".env",
    ".cache", ".npm", ".yarn", ".pip", "pip-cache",
    # System
    ".Trash", ".trash", "lost+found", "$RECYCLE.BIN",
    "System Volume Information", ".Spotlight-V100",
    # RetroArch media/cache directories (don't contain ROMs)
    "thumbnails", "Thumbnails", ".thumbnails",
    "boxarts", "Boxarts", "Named_Boxarts",
    "snaps", "Snaps", "Named_Snaps",
    "titles", "Titles", "Named_Titles",
    "screenshots", "Screenshots",
    "shaders", "Shaders", "shaders_glsl", "shaders_slang",
    "overlays", "Overlays",
    "cheats", "Cheats",
    "playlists", "Playlists",
    "database", "Database",
    "assets", "Assets",
    "autoconfig", "Autoconfig",
    "config", "configs", "Config", "Configs",
    "logs", "Logs",
    "saves", "Saves", "states", "States",  # Save files, not ROMs
    "downloaded_images", "Downloaded_Images",
    # Common non-ROM directories in game collections
    "bios", "BIOS", "Bios",
    "firmware", "Firmware", "FIRMWARE",
    "manuals", "Manuals", "docs", "Docs",
    "artwork", "Artwork", "art", "Art",
    "media", "Media",
    "videos", "Videos", "video", "Video",
    "music", "Music", "audio", "Audio",
    "images", "Images", "img", "Img",
    "backgrounds", "Backgrounds",
    "bezels", "Bezels",
    # macOS
    ".DS_Store", "__MACOSX",
    # Windows
    "desktop.ini", "Thumbs.db",
})

# File extensions that indicate a directory is NOT a ROM directory
NON_ROM_INDICATOR_EXTENSIONS: frozenset[str] = frozenset({
    ".mp3", ".flac", ".wav", ".ogg", ".m4a",  # Audio
    ".mp4", ".mkv", ".avi", ".mov", ".wmv",   # Video
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",  # Images
    ".pdf", ".doc", ".docx", ".txt", ".md",   # Documents
})


# Common system names and their aliases
SYSTEM_ALIASES: dict[str, str] = {
    # Nintendo
    "nes": "NES", "famicom": "NES", "fc": "NES",
    "snes": "SNES", "superfamicom": "SNES", "sfc": "SNES",
    "n64": "N64", "nintendo64": "N64",
    "gc": "GameCube", "gamecube": "GameCube", "ngc": "GameCube",
    "wii": "Wii",
    "wiiu": "Wii U",
    "switch": "Switch", "ns": "Switch",
    "gb": "Game Boy", "gameboy": "Game Boy",
    "gbc": "Game Boy Color", "gameboycolor": "Game Boy Color",
    "gba": "GBA", "gameboyadvance": "GBA",
    "nds": "NDS", "ds": "NDS", "nintendods": "NDS",
    "3ds": "3DS", "nintendo3ds": "3DS",
    "virtualboy": "Virtual Boy", "vb": "Virtual Boy",
    # Sega
    "genesis": "Genesis", "megadrive": "Genesis", "md": "Genesis",
    "sms": "Master System", "mastersystem": "Master System",
    "gamegear": "Game Gear", "gg": "Game Gear",
    "sega32x": "32X", "32x": "32X",
    "segacd": "Sega CD", "megacd": "Sega CD",
    "saturn": "Saturn", "segasaturn": "Saturn",
    "dreamcast": "Dreamcast", "dc": "Dreamcast",
    # Sony
    "psx": "PlayStation", "ps1": "PlayStation", "playstation": "PlayStation",
    "ps2": "PlayStation 2", "playstation2": "PlayStation 2",
    "ps3": "PlayStation 3", "playstation3": "PlayStation 3",
    "psp": "PSP", "playstationportable": "PSP",
    "psvita": "PS Vita", "vita": "PS Vita",
    # Atari
    "atari2600": "Atari 2600", "2600": "Atari 2600",
    "atari5200": "Atari 5200", "5200": "Atari 5200",
    "atari7800": "Atari 7800", "7800": "Atari 7800",
    "atarilynx": "Atari Lynx", "lynx": "Atari Lynx",
    "atarijaguar": "Atari Jaguar", "jaguar": "Atari Jaguar",
    # NEC
    "pce": "PC Engine", "pcengine": "PC Engine", "tg16": "PC Engine", "turbografx16": "PC Engine",
    "pcecd": "PC Engine CD", "pcenginecd": "PC Engine CD", "tg16cd": "PC Engine CD",
    "pcfx": "PC-FX",
    # SNK
    "neogeo": "Neo Geo", "ng": "Neo Geo", "aes": "Neo Geo",
    "neogeocd": "Neo Geo CD", "ngcd": "Neo Geo CD",
    "ngp": "Neo Geo Pocket", "neogeopocket": "Neo Geo Pocket",
    "ngpc": "Neo Geo Pocket Color", "neogeopocketcolor": "Neo Geo Pocket Color",
    # Other
    "mame": "MAME", "arcade": "Arcade", "fbneo": "FBNeo",
    "dos": "DOS", "pc": "PC",
    "msx": "MSX", "msx2": "MSX2",
    "coleco": "ColecoVision", "colecovision": "ColecoVision",
    "intellivision": "Intellivision",
    "vectrex": "Vectrex",
    "wonderswan": "WonderSwan", "ws": "WonderSwan",
    "wonderswancolor": "WonderSwan Color", "wsc": "WonderSwan Color",
    "3do": "3DO",
    "cdi": "CD-i", "philipscdi": "CD-i",
    "amiga": "Amiga",
    "c64": "Commodore 64", "commodore64": "Commodore 64",
    "zxspectrum": "ZX Spectrum", "spectrum": "ZX Spectrum",
    "scummvm": "ScummVM",
    "ports": "Ports",
}


def detect_systems(directory: str | Path) -> list[dict]:
    """
    Detect game systems present in a ROM directory.

    Scans top-level folders and matches them against known system names.
    Returns a list of dicts with system info including file counts.
    """
    directory = Path(directory)
    if not directory.is_dir():
        return []

    systems = []
    seen_systems = set()

    for entry in os.scandir(directory):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        if entry.name in SKIP_DIRECTORIES:
            continue

        folder_name = entry.name.lower().replace("-", "").replace("_", "").replace(" ", "")
        system_name = SYSTEM_ALIASES.get(folder_name, entry.name)

        # Count files in this folder
        file_count = 0
        total_size = 0
        try:
            for root, dirs, files in os.walk(entry.path):
                _filter_dirs_inplace(dirs)
                for f in files:
                    file_path = Path(root) / f
                    if file_path.is_file() and not f.startswith("."):
                        file_count += 1
                        try:
                            total_size += file_path.stat().st_size
                        except OSError:
                            pass
        except OSError:
            pass

        if file_count > 0 and system_name not in seen_systems:
            seen_systems.add(system_name)
            systems.append({
                "folder": entry.name,
                "system": system_name,
                "path": entry.path,
                "file_count": file_count,
                "size_mb": round(total_size / (1024 * 1024), 2),
            })

    # Sort by system name
    systems.sort(key=lambda x: x["system"])
    return systems


def _should_skip_directory(dirname: str) -> bool:
    """Check if a directory should be skipped during scanning."""
    return dirname in SKIP_DIRECTORIES or dirname.startswith(".")


def _filter_dirs_inplace(dirs: list[str]) -> None:
    """Filter out directories that should be skipped (modifies list in-place for os.walk)."""
    dirs[:] = [d for d in dirs if not _should_skip_directory(d)]


class StorageType:
    """Storage device type classification."""
    SSD = "ssd"
    HDD = "hdd"
    USB = "usb"
    NETWORK = "network"
    UNKNOWN = "unknown"


def _detect_storage_type(path: str) -> str:
    """
    Detect the storage type for a given path.

    Returns one of: StorageType.SSD, StorageType.HDD, StorageType.USB,
                    StorageType.NETWORK, StorageType.UNKNOWN

    - SSDs can handle parallel I/O well (8-16 threads)
    - HDDs are sequential, should use 1-2 threads
    - USB/SD cards vary, use conservative threading (2-4 threads)
    - Network mounts should be cautious (1-2 threads)
    """
    try:
        # Get the mount point for the path
        path = os.path.realpath(path)

        # Check if it's a network mount
        result = subprocess.run(
            ["df", "-T", path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 2:
                    fstype = parts[1].lower()
                    if fstype in ("nfs", "nfs4", "cifs", "smb", "smbfs", "sshfs", "fuse.sshfs"):
                        return StorageType.NETWORK

        # Get the device name for the path
        result = subprocess.run(
            ["df", path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return StorageType.UNKNOWN

        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            return StorageType.UNKNOWN

        device_path = lines[1].split()[0]

        # Handle LVM and device mapper
        if device_path.startswith("/dev/mapper/") or device_path.startswith("/dev/dm-"):
            # For LVM, assume SSD as modern systems commonly use them
            return StorageType.SSD

        # Extract the base device name (e.g., /dev/sda1 -> sda)
        if device_path.startswith("/dev/nvme"):
            return StorageType.SSD  # NVMe is always SSD

        if device_path.startswith("/dev/sd") or device_path.startswith("/dev/hd"):
            base_device = device_path.replace("/dev/", "")
            # Remove partition number
            base_device = "".join(c for c in base_device if not c.isdigit())

            # Check if it's rotational (HDD) or not (SSD)
            rotational_path = f"/sys/block/{base_device}/queue/rotational"
            if os.path.exists(rotational_path):
                with open(rotational_path, "r") as f:
                    is_rotational = f.read().strip() == "1"
                    if is_rotational:
                        return StorageType.HDD
                    else:
                        return StorageType.SSD

            # Check if it's removable (USB/SD card)
            removable_path = f"/sys/block/{base_device}/removable"
            if os.path.exists(removable_path):
                with open(removable_path, "r") as f:
                    is_removable = f.read().strip() == "1"
                    if is_removable:
                        return StorageType.USB

        # Check for mmcblk (SD card)
        if device_path.startswith("/dev/mmcblk"):
            return StorageType.USB

        return StorageType.UNKNOWN

    except Exception:
        return StorageType.UNKNOWN


def _get_optimal_threads(storage_type: str) -> int:
    """Get optimal number of threads for the storage type."""
    if storage_type == StorageType.SSD:
        return min(os.cpu_count() or 4, 12)  # SSDs can handle parallel I/O
    elif storage_type == StorageType.HDD:
        return 2  # HDDs are sequential, limit parallelism
    elif storage_type == StorageType.USB:
        return 4  # USB varies, be conservative
    elif storage_type == StorageType.NETWORK:
        return 2  # Network mounts need to be careful
    else:
        return 4  # Default to conservative


@dataclass
class ScanProgress:
    """Progress information for a scan."""

    total_files: int = 0
    processed_files: int = 0
    current_file: str = ""
    errors: int = 0
    started_at: float = 0.0
    status: str = "idle"  # idle, scanning, updating, completed, error
    # Verbose progress details
    current_step: str = ""  # e.g., "Hashing file...", "Checking RA...", "Assigning serial..."
    current_phase: str = ""  # e.g., "collecting", "processing", "verifying"
    files_hashed: int = 0
    files_ra_checked: int = 0
    files_ra_matched: int = 0
    storage_type: str = ""
    thread_count: int = 0
    last_hash: str = ""
    last_serial: str = ""
    last_ra_result: str = ""

    @property
    def elapsed_seconds(self) -> float:
        if self.started_at == 0:
            return 0
        return time.time() - self.started_at

    @property
    def percent_complete(self) -> float:
        if self.total_files == 0:
            return 0
        return (self.processed_files / self.total_files) * 100

    def to_dict(self) -> dict:
        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "current_file": _sanitize_path(self.current_file),
            "errors": self.errors,
            "elapsed_seconds": self.elapsed_seconds,
            "percent_complete": self.percent_complete,
            "status": self.status,
            # Verbose fields
            "current_step": self.current_step,
            "current_phase": self.current_phase,
            "files_hashed": self.files_hashed,
            "files_ra_checked": self.files_ra_checked,
            "files_ra_matched": self.files_ra_matched,
            "storage_type": self.storage_type,
            "thread_count": self.thread_count,
            "last_hash": self.last_hash,
            "last_serial": self.last_serial,
            "last_ra_result": self.last_ra_result,
        }


@dataclass
class ScanResult:
    """Result of a scan operation."""

    directory: str
    files_processed: int
    duration_seconds: int
    errors: int
    error_log: str
    is_update: bool = False

    def to_dict(self) -> dict:
        return {
            "directory": self.directory,
            "files_processed": self.files_processed,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
            "error_log": self.error_log,
            "is_update": self.is_update,
        }


class Scanner:
    """File scanner for finding and cataloging files."""

    def __init__(
        self,
        db: DuperDatabase,
        config: DuperConfig | None = None,
        progress_callback: Callable[[ScanProgress], None] | None = None,
    ):
        self.db = db
        self.config = config or get_config()
        self.progress_callback = progress_callback
        self.progress = ScanProgress()
        self._script_name = "duper"  # Exclude ourselves from scanning
        self._storage_type = StorageType.UNKNOWN
        self._num_threads = 4
        self._lock = None  # Will be set during threaded processing

    def _update_progress(self) -> None:
        """Call progress callback if set."""
        if self.progress_callback:
            self.progress_callback(self.progress)

    def _process_file(self, filepath: Path, lock: threading.Lock | None = None) -> bool:
        """
        Process a single file and add it to the database.

        Returns True if successful, False otherwise.
        """
        def update_step(step: str, **kwargs):
            """Update progress step safely."""
            if lock:
                with lock:
                    self.progress.current_step = step
                    for key, value in kwargs.items():
                        setattr(self.progress, key, value)
                    self._update_progress()
            else:
                self.progress.current_step = step
                for key, value in kwargs.items():
                    setattr(self.progress, key, value)
                self._update_progress()

        # Don't scan ourselves
        if filepath.name.lower() in ("duper.py", "duper", "__main__.py"):
            return False

        # Check if file should be ignored based on extension
        if should_ignore_file(
            filepath,
            ignore_fodder=self.config.scanner.ignore_fodder,
            ignore_video=self.config.scanner.ignore_video,
            ignore_music=self.config.scanner.ignore_music,
            ignore_pictures=self.config.scanner.ignore_pictures,
        ):
            return False

        # Extract file information
        filename = filepath.name
        simplified_filename = filepath.stem
        extension = filepath.suffix.lstrip(".").lower()

        # Step 1: Calculate MD5 hash
        update_step(f"Hashing: {filename}")
        md5 = calculate_md5(filepath)
        rom_serial = generate_rom_serial(md5)

        if lock:
            with lock:
                self.progress.files_hashed += 1
                self.progress.last_hash = md5
                self.progress.last_serial = rom_serial
        else:
            self.progress.files_hashed += 1
            self.progress.last_hash = md5
            self.progress.last_serial = rom_serial

        # Step 2: Assign ROM serial
        update_step(f"Assigned serial: {rom_serial}")

        # Step 3: Get file metadata
        update_step(f"Reading metadata: {filename}")
        size_mb = get_file_size_mb(filepath)
        create_time = get_file_create_time(filepath)
        mod_time = get_file_mod_time(filepath)

        # Step 4: Check RetroAchievements (if enabled)
        ra_supported = False
        ra_game_id = 0
        ra_game_title = ""
        ra_checked_date = None

        if self.config.retroachievements.enabled and self.config.retroachievements.verify_on_scan:
            update_step(f"Checking RA hash: {md5[:8]}...")
            ra_client = get_ra_client()
            if ra_client:
                try:
                    # get_game_by_hash returns RAGameInfo or None
                    game_info = ra_client.get_game_by_hash(md5)
                    # Ensure game_info is RAGameInfo, not a bool or other type
                    if game_info is not None and hasattr(game_info, 'game_id'):
                        ra_supported = True
                        ra_game_id = game_info.game_id
                        ra_game_title = game_info.title
                    else:
                        ra_supported = False
                    ra_checked_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    if lock:
                        with lock:
                            self.progress.files_ra_checked += 1
                            if ra_supported:
                                self.progress.files_ra_matched += 1
                                self.progress.last_ra_result = f"FOUND: {ra_game_title}"
                            else:
                                self.progress.last_ra_result = "Not in RA database"
                    else:
                        self.progress.files_ra_checked += 1
                        if ra_supported:
                            self.progress.files_ra_matched += 1
                            self.progress.last_ra_result = f"FOUND: {ra_game_title}"
                        else:
                            self.progress.last_ra_result = "Not in RA database"

                    update_step(f"RA: {self.progress.last_ra_result}")
                except Exception as e:
                    if lock:
                        with lock:
                            self.progress.last_ra_result = f"Error: {str(e)[:30]}"
                    else:
                        self.progress.last_ra_result = f"Error: {str(e)[:30]}"

        # Step 5: Create record and insert into database
        update_step(f"Saving to database: {filename}")
        record = FileRecord(
            filepath=str(filepath),
            filename=filename,
            md5=md5,
            simplified_filename=simplified_filename,
            size_mb=size_mb,
            created_time=create_time,
            modified_time=mod_time,
            extension=extension,
            is_potential_duplicate=False,
            rom_serial=rom_serial,
            ra_supported=ra_supported,
            ra_game_id=ra_game_id,
            ra_game_title=ra_game_title,
            ra_checked_date=ra_checked_date,
        )

        return self.db.insert_file(record)

    def _count_files(self, directory: Path, retroarch_mode: bool) -> int:
        """Count files that will be scanned."""
        count = 0

        if retroarch_mode:
            for root, dirs, files in os.walk(directory):
                # Filter out directories we should skip (modifies dirs in-place)
                _filter_dirs_inplace(dirs)

                root_path = Path(root)
                valid_files = [
                    f for f in files
                    if (root_path / f).is_file() and f.lower() not in ("duper.py", "duper")
                ]
                # Only count subdirs with more than 3 files, or top level
                if len(valid_files) > 3 or root_path == directory:
                    count += len(valid_files)
        else:
            for entry in os.scandir(directory):
                if entry.is_file() and entry.name.lower() not in ("duper.py", "duper"):
                    count += 1

        return count

    def _process_file_threaded(
        self,
        filepath: Path,
        processed_counter: list,
        error_counter: list,
        error_log: list,
        lock: threading.Lock,
    ) -> bool:
        """Process a file in a thread-safe manner."""
        try:
            with lock:
                self.progress.current_file = str(filepath)
                self.progress.current_phase = "processing"

            result = self._process_file(filepath, lock=lock)

            with lock:
                if result:
                    processed_counter[0] += 1
                self.progress.processed_files += 1
                self._update_progress()

            return result
        except Exception as e:
            with lock:
                error_counter[0] += 1
                error_log.append(f"{datetime.now()} - Error processing '{filepath}': {e}")
                self.progress.errors = error_counter[0]
                self.progress.processed_files += 1
                self.progress.current_step = f"ERROR: {str(e)[:50]}"
                self._update_progress()
            return False

    def _should_process_file(self, filepath: Path) -> bool:
        """Check if a file should be processed (not ignored)."""
        if filepath.name.lower() in ("duper.py", "duper", "__main__.py"):
            return False
        return not should_ignore_file(
            filepath,
            ignore_fodder=self.config.scanner.ignore_fodder,
            ignore_video=self.config.scanner.ignore_video,
            ignore_music=self.config.scanner.ignore_music,
            ignore_pictures=self.config.scanner.ignore_pictures,
        )

    def _collect_files(self, directory: Path, retroarch_mode: bool) -> list[Path]:
        """Collect all files that will be scanned (excluding ignored files)."""
        files_to_scan: list[Path] = []

        if retroarch_mode:
            for root, dirs, files in os.walk(directory):
                _filter_dirs_inplace(dirs)
                root_path = Path(root)

                # Get valid files (not ignored)
                valid_files = []
                for f in files:
                    file_path = root_path / f
                    if file_path.is_file() and self._should_process_file(file_path):
                        valid_files.append(file_path)

                # Only include subdirs with more than 3 valid files, or top level
                if len(valid_files) > 3 or root_path == directory:
                    files_to_scan.extend(valid_files)
        else:
            for entry in os.scandir(directory):
                if entry.is_file():
                    file_path = Path(entry.path)
                    if self._should_process_file(file_path):
                        files_to_scan.append(file_path)

        return files_to_scan

    def scan(self, directory: str | Path) -> ScanResult:
        """
        Perform a full scan of a directory.

        This will clear existing records for the directory and rescan everything.
        Uses parallel processing optimized for the storage type (SSD/HDD/USB).
        """
        directory = Path(directory).resolve()
        if not directory.is_dir():
            raise ValueError(f"Directory does not exist: {directory}")

        # Detect storage type and optimize threading
        self._storage_type = _detect_storage_type(str(directory))
        self._num_threads = _get_optimal_threads(self._storage_type)

        # Initialize progress with verbose fields
        self.progress = ScanProgress(
            started_at=time.time(),
            status="initializing",
            current_phase="init",
            current_step="Detecting storage type...",
            storage_type=self._storage_type,
            thread_count=self._num_threads,
        )
        self._update_progress()

        # Initialize RA client if enabled
        ra_enabled = self.config.retroachievements.enabled and self.config.retroachievements.verify_on_scan
        if ra_enabled:
            self.progress.current_step = "Initializing RetroAchievements client..."
            self._update_progress()
            ra_client = get_ra_client(
                username=self.config.retroachievements.username,
                api_key=self.config.retroachievements.api_key,
            )
            if ra_client:
                self.progress.current_step = "RA client ready - will verify hashes"
            else:
                self.progress.current_step = "RA client not available - skipping verification"
            self._update_progress()

        start_time = time.time()
        retroarch_mode = self.config.scanner.retroarch_mode

        # Collect all files first
        self.progress.status = "collecting"
        self.progress.current_phase = "collecting"
        self.progress.current_step = "Scanning directories for files..."
        self._update_progress()
        files_to_scan = self._collect_files(directory, retroarch_mode)

        self.progress.total_files = len(files_to_scan)
        self.progress.current_step = f"Found {len(files_to_scan)} files to process"
        self.progress.status = f"scanning ({self._storage_type}, {self._num_threads} threads)"
        self._update_progress()

        # Thread-safe counters
        processed_counter = [0]  # Using list to allow mutation in threads
        error_counter = [0]
        error_log: list[str] = []
        lock = threading.Lock()

        # Process files in parallel
        if self._num_threads > 1 and len(files_to_scan) > 10:
            with ThreadPoolExecutor(max_workers=self._num_threads) as executor:
                futures = [
                    executor.submit(
                        self._process_file_threaded,
                        filepath,
                        processed_counter,
                        error_counter,
                        error_log,
                        lock,
                    )
                    for filepath in files_to_scan
                ]
                # Wait for all to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        pass  # Errors already handled in _process_file_threaded
        else:
            # Sequential processing for small file counts or single-threaded mode
            for filepath in files_to_scan:
                self._process_file_threaded(
                    filepath,
                    processed_counter,
                    error_counter,
                    error_log,
                    lock,
                )

        end_time = time.time()
        duration = int(end_time - start_time)
        error_log_str = "\n".join(error_log)

        # Log metrics
        self.db.log_metrics(
            start_time=start_time,
            end_time=end_time,
            scan_duration=duration,
            errors=error_counter[0],
            error_log=error_log_str,
            scan_directory=str(directory),
            user=get_username(),
            files_processed=processed_counter[0],
        )

        # Update scan history
        self.db.update_scan_history(str(directory))

        self.progress.status = "completed"
        self._update_progress()

        return ScanResult(
            directory=str(directory),
            files_processed=processed_counter[0],
            duration_seconds=duration,
            errors=error_counter[0],
            error_log=error_log_str,
            is_update=False,
        )

    def update(self, directory: str | Path) -> ScanResult:
        """
        Update an existing scan with changes (new files, deleted files).

        This is faster than a full rescan as it only processes changes.
        Uses parallel processing optimized for the storage type.
        """
        directory = Path(directory).resolve()
        if not directory.is_dir():
            raise ValueError(f"Directory does not exist: {directory}")

        # Detect storage type and optimize threading
        self._storage_type = _detect_storage_type(str(directory))
        self._num_threads = _get_optimal_threads(self._storage_type)

        self.progress = ScanProgress(started_at=time.time(), status="updating")
        self._update_progress()

        start_time = time.time()
        retroarch_mode = self.config.scanner.retroarch_mode

        # Collect current files on disk
        self.progress.status = "collecting"
        self._update_progress()
        files_to_scan = self._collect_files(directory, retroarch_mode)
        current_files = {str(f) for f in files_to_scan}

        # Get files in database
        db_files = self.db.get_all_filepaths_in_directory(str(directory))

        # Find files to add and remove
        files_to_add = [Path(f) for f in (current_files - db_files)]
        files_to_remove = list(db_files - current_files)

        self.progress.total_files = len(files_to_add) + len(files_to_remove)
        self.progress.status = f"updating ({self._storage_type}, {self._num_threads} threads)"
        self._update_progress()

        # Thread-safe counters
        processed_counter = [0]
        error_counter = [0]
        error_log: list[str] = []
        lock = threading.Lock()

        # Add new files in parallel
        if self._num_threads > 1 and len(files_to_add) > 10:
            with ThreadPoolExecutor(max_workers=self._num_threads) as executor:
                futures = [
                    executor.submit(
                        self._process_file_threaded,
                        filepath,
                        processed_counter,
                        error_counter,
                        error_log,
                        lock,
                    )
                    for filepath in files_to_add
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        pass
        else:
            for filepath in files_to_add:
                self._process_file_threaded(
                    filepath,
                    processed_counter,
                    error_counter,
                    error_log,
                    lock,
                )

        # Remove deleted files (sequential - DB operations)
        for filepath in files_to_remove:
            with lock:
                self.progress.current_file = filepath
                self.db.delete_file(filepath)
                self.progress.processed_files += 1
                self._update_progress()

        end_time = time.time()
        duration = int(end_time - start_time)

        # Update scan history
        self.db.update_scan_history(str(directory))

        self.progress.status = "completed"
        self._update_progress()

        error_log_str = "\n".join(error_log)
        return ScanResult(
            directory=str(directory),
            files_processed=processed_counter[0],
            duration_seconds=duration,
            errors=error_counter[0],
            error_log=error_log_str,
            is_update=True,
        )

    def scan_or_update(self, directory: str | Path) -> ScanResult:
        """
        Scan a directory, using update if previously scanned.

        This is the recommended method for most use cases.
        """
        directory = Path(directory).resolve()

        if self.db.has_scanned_directory(str(directory)):
            return self.update(directory)
        else:
            return self.scan(directory)

    def get_progress(self) -> ScanProgress:
        """Get current scan progress."""
        return self.progress
