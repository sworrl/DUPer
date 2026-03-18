"""Utility helper functions for DUPer."""

from __future__ import annotations

import hashlib
import os
import platform
import re
import secrets
import shutil
from datetime import datetime
from pathlib import Path


def format_size(size_bytes: int | float) -> str:
    """Format bytes into human-readable size string."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.2f} {units[i]}"


def calculate_md5(filepath: str | Path) -> str:
    """Calculate MD5 hash of a file using chunked reading for efficiency."""
    filepath = Path(filepath)
    if not filepath.is_file():
        return ""
    try:
        with open(filepath, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)
            return file_hash.hexdigest()
    except OSError:
        return ""


def normalize_rom_name(filename: str) -> str:
    """
    Normalize a ROM filename by stripping metadata for duplicate detection.

    Removes:
    - File extension
    - Region codes: (U), (USA), (E), (Europe), (J), (Japan), (JU), (UE), etc.
    - Version info: (v1.0), (V1.1), (Rev A), (Rev 1), (Beta), (Proto), etc.
    - Verification flags: [!], [a], [b], [b1], [h], [o], [p], [t], [f], etc.
    - Disc numbers: (Disc 1), (Disk 2), (CD1), etc.
    - Language tags: (En), (Fr), (De), (Es), (It), (En,Fr,De), etc.
    - Common suffixes: The, A, An at start
    - Extra whitespace and special chars

    Example:
        "Super Mario 64 (U) (V1.1) [!].z64" -> "super mario 64"
        "Legend of Zelda, The (USA) (Rev A).n64" -> "legend of zelda"
    """
    # Remove extension
    name = Path(filename).stem

    # Convert to lowercase for consistent matching
    name = name.lower()

    # Remove verification/dump flags: [!], [a], [b1], [h], [o], [p], [t], [f], [T+Eng], etc.
    name = re.sub(r'\[[^\]]*\]', '', name)

    # Remove region codes in parentheses
    region_pattern = r'\((u|usa|e|eur|europe|j|jp|jpn|japan|w|world|k|korea|' \
                     r'ue|uj|ju|je|eu|jue|uej|' \
                     r'a|aus|australia|b|brazil|c|china|f|france|g|germany|' \
                     r'i|italy|nl|netherlands|r|russia|s|spain|sw|sweden|' \
                     r'uk|hk|tw|as|asia|pal|ntsc|ntsc-u|ntsc-j|secam)\)'
    name = re.sub(region_pattern, '', name, flags=re.IGNORECASE)

    # Remove version info: (v1.0), (V1.1), (Rev A), (Rev 1), (Beta), (Proto), (Sample), etc.
    name = re.sub(r'\(v?\d+\.?\d*[a-z]?\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\(rev\s*[a-z0-9]+\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\((beta|proto|prototype|sample|demo|promo|preview|alt|alternate)\s*\d*\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\((unl|pirate|hack|translation|patch)\)', '', name, flags=re.IGNORECASE)

    # Remove disc numbers: (Disc 1), (Disk 2), (CD1), (DVD1), etc.
    name = re.sub(r'\((disc|disk|cd|dvd)\s*\d+\)', '', name, flags=re.IGNORECASE)

    # Remove language tags: (En), (Fr), (En,Fr,De), etc.
    name = re.sub(r'\((?:en|fr|de|es|it|pt|ja|ko|zh|nl|sv|no|da|fi|ru|pl|cs|hu)(?:,(?:en|fr|de|es|it|pt|ja|ko|zh|nl|sv|no|da|fi|ru|pl|cs|hu))*\)', '', name, flags=re.IGNORECASE)

    # Remove any remaining parenthetical content that looks like metadata
    name = re.sub(r'\(\d{4}\)', '', name)  # Year like (1998)
    name = re.sub(r'\([^)]{1,3}\)', '', name)  # Short codes like (M5), (S)

    # Remove "The", "A", "An" from the beginning (for sorting consistency)
    name = re.sub(r'^(the|a|an)\s+', '', name)

    # Remove ", The" from the end
    name = re.sub(r',\s*(the|a|an)$', '', name)

    # Clean up special characters and extra whitespace
    name = re.sub(r'[_\-]+', ' ', name)  # Replace underscores/hyphens with spaces
    name = re.sub(r'\s+', ' ', name)  # Collapse multiple spaces
    name = name.strip()

    return name


def get_file_size_mb(filepath: str | Path) -> float:
    """Get file size in megabytes."""
    filepath = Path(filepath)
    if not filepath.is_file():
        return 0.0
    try:
        return filepath.stat().st_size / (1024 * 1024)
    except OSError:
        return 0.0


def get_file_size_bytes(filepath: str | Path) -> int:
    """Get file size in bytes."""
    filepath = Path(filepath)
    if not filepath.is_file():
        return 0
    try:
        return filepath.stat().st_size
    except OSError:
        return 0


def get_file_create_time(filepath: str | Path) -> str | None:
    """Get file creation time as formatted string."""
    filepath = Path(filepath)
    if not filepath.is_file():
        return None
    try:
        timestamp = filepath.stat().st_ctime
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        return None


def get_file_mod_time(filepath: str | Path) -> str | None:
    """Get file modification time as formatted string."""
    filepath = Path(filepath)
    if not filepath.is_file():
        return None
    try:
        timestamp = filepath.stat().st_mtime
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    except OSError:
        return None


def detect_steamos() -> bool:
    """Detect if running on SteamOS (SteamDeck)."""
    try:
        os_release = Path("/etc/os-release")
        if os_release.exists():
            content = os_release.read_text().lower()
            return "steam" in content
    except OSError:
        pass
    return False


def get_system_info() -> dict:
    """Get system information for diagnostics."""
    return {
        "platform": platform.system(),
        "platform_release": platform.release(),
        "platform_version": platform.version(),
        "architecture": platform.machine(),
        "hostname": platform.node(),
        "python_version": platform.python_version(),
        "is_steamos": detect_steamos(),
    }


def get_directory_stats(directory: str | Path) -> tuple[int, int, int]:
    """
    Get directory statistics.

    Returns:
        tuple: (total_files, total_size_bytes, free_space_bytes)
    """
    directory = Path(directory)
    total_files = 0
    total_size = 0

    try:
        if not directory.exists():
            return 0, 0, 0

        for entry in os.scandir(directory):
            if entry.is_file():
                total_files += 1
                try:
                    total_size += entry.stat().st_size
                except OSError:
                    pass

        free_space = shutil.disk_usage(directory).free
        return total_files, total_size, free_space
    except (FileNotFoundError, OSError):
        return 0, 0, 0


def get_username() -> str:
    """Get current username safely."""
    try:
        return os.getlogin()
    except OSError:
        # Fallback for systems without proper login session
        import getpass
        try:
            return getpass.getuser()
        except Exception:
            return "unknown"


def generate_api_key() -> str:
    """Generate a secure random API key."""
    return secrets.token_urlsafe(32)


def ensure_directory(path: str | Path) -> Path:
    """Ensure a directory exists, creating it if necessary."""
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# File extension sets for filtering
FODDER_EXTENSIONS = frozenset({
    ".txt", ".ini", ".lua", ".input", ".sh", ".bat", ".nfo", ".exe", ".html"
})
VIDEO_EXTENSIONS = frozenset({
    ".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm"
})
MUSIC_EXTENSIONS = frozenset({
    ".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a"
})
PICTURE_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"
})


def should_ignore_file(
    filepath: str | Path,
    ignore_fodder: bool = True,
    ignore_video: bool = True,
    ignore_music: bool = True,
    ignore_pictures: bool = True,
) -> bool:
    """Check if a file should be ignored based on its extension."""
    filepath = Path(filepath)
    ext = filepath.suffix.lower()

    if ignore_fodder and ext in FODDER_EXTENSIONS:
        return True
    if ignore_video and ext in VIDEO_EXTENSIONS:
        return True
    if ignore_music and ext in MUSIC_EXTENSIONS:
        return True
    if ignore_pictures and ext in PICTURE_EXTENSIONS:
        return True

    return False
