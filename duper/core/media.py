"""Media and save file correlation for DUPer.

Handles finding and cleaning up associated media files and save data when ROMs are moved/deleted.
Supports common EmulationStation, RetroArch, and Skraper directory structures.

IMPORTANT: Save games and save states are NEVER deleted by default - only moved/preserved.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from duper.core.config import DuperConfig, get_config
from duper.core.database import DuperDatabase


# Common media directory patterns relative to ROM directories
MEDIA_SUBDIR_PATTERNS = [
    # EmulationStation/RetroPie style
    "media/videos",
    "media/screenshots",
    "media/boxart",
    "media/wheel",
    "media/marquee",
    "media/fanart",
    "media/titlescreen",
    "media/snap",
    "media/manual",
    "media/3dbox",
    "media/physical_media",
    # Flat media folder
    "videos",
    "screenshots",
    "images",
    "boxart",
    "snaps",
    "titles",
    "manuals",
    # Skraper style
    "downloaded_media",
    "scraped_media",
]

# EmulationStation downloaded_media paths (relative to ES config or home)
ES_MEDIA_PATHS = [
    "~/.emulationstation/downloaded_media",
    "~/ES-DE/downloaded_media",
    "~/.config/emulationstation/downloaded_media",
    "/storage/.config/emulationstation/downloaded_media",  # LibreELEC
    "~/.var/app/org.es_de.Frontend/config/ES-DE/downloaded_media",  # Flatpak
]

# Skraper/ES-DE style media subdirectories within downloaded_media/{system}/
MEDIA_CATEGORY_SUBDIRS = [
    "covers",      # Front box art (preferred)
    "boxart",      # Alternative name for covers
    "3dboxes",     # 3D rendered box art
    "backcovers",  # Back cover art
    "screenshots", # In-game screenshots
    "titlescreens", # Title screen images
    "fanart",      # Fan-created artwork
    "marquees",    # Arcade marquee graphics
    "wheels",      # Wheel/logo graphics
    "miximages",   # Composite/mixed images
    "physicalmedia", # Physical media photos
    "videos",      # Preview videos
    "manuals",     # PDF manuals
]

# Additional media paths relative to emulation root (tools/downloaded_media style)
# These are checked when the ROM directory is under an Emulation folder
EMULATION_MEDIA_PATHS = [
    "tools/downloaded_media",
    "storage/downloaded_media",
    "downloaded_media",
    "media",
]

# Common media file extensions
MEDIA_EXTENSIONS = {
    "video": {".mp4", ".mkv", ".avi", ".webm", ".mov"},
    "image": {".png", ".jpg", ".jpeg", ".webp", ".gif"},
    "document": {".pdf", ".txt", ".html"},
}

# Save directory patterns relative to ROM directories
SAVE_SUBDIR_PATTERNS = [
    # Common local patterns
    "saves",
    "states",
    "savestates",
    "save",
    "state",
    "battery",  # Some emulators use this
    # RetroArch style (in ROM directory)
    ".retroarch/saves",
    ".retroarch/states",
]

# RetroArch save paths (system-wide)
RETROARCH_SAVE_PATHS = [
    "~/.config/retroarch/saves",
    "~/.config/retroarch/states",
    "~/.var/app/org.libretro.RetroArch/config/retroarch/saves",  # Flatpak
    "~/.var/app/org.libretro.RetroArch/config/retroarch/states",  # Flatpak
    "/storage/.config/retroarch/saves",  # LibreELEC
    "/storage/.config/retroarch/states",  # LibreELEC
    "~/Library/Application Support/RetroArch/saves",  # macOS
    "~/Library/Application Support/RetroArch/states",  # macOS
]

# EmulationStation/ES-DE save paths
ES_SAVE_PATHS = [
    "~/.emulationstation/saves",
    "~/ES-DE/saves",
    "~/.config/emulationstation/saves",
]

# Save game file extensions (battery saves, SRAM, etc.)
SAVE_EXTENSIONS = {
    # Battery/SRAM saves
    ".srm",  # SNES, GBA, etc.
    ".sav",  # Generic save
    ".dsv",  # DS saves
    ".sav0", ".sav1", ".sav2",  # Numbered saves
    ".eep",  # EEPROM saves
    ".fla",  # Flash saves
    ".mpk",  # N64 memory pak
    ".nv",   # NVRAM
    ".rtc",  # Real-time clock data
    # Memory card formats
    ".mcr",  # PS1 memory card
    ".mcd",  # PS1 memory card
    ".gme",  # PS1 memory card (DexDrive)
    ".vmp",  # PS Vita memory card
    ".ps2",  # PS2 memory card
    ".psu",  # PS2 save (EMS format)
    ".cbs",  # PS2 save (CodeBreaker)
    ".max",  # PS2 save (Action Replay Max)
    ".xps",  # PS2 save (X-Port)
    # Other formats
    ".dat",  # Generic data (context-dependent)
    ".bin",  # Binary save (context-dependent)
}

# Save state file extensions
STATE_EXTENSIONS = {
    # Generic save states
    ".state",
    ".savestate",
    ".ss",
    ".sst",
    # Numbered states (RetroArch style)
    ".state0", ".state1", ".state2", ".state3", ".state4",
    ".state5", ".state6", ".state7", ".state8", ".state9",
    ".state10", ".state11", ".state12", ".state13", ".state14",
    ".state15", ".state16", ".state17", ".state18", ".state19",
    # Auto-save states
    ".state.auto",
    ".auto",
    # Emulator-specific
    ".fs",    # FCEUX (NES)
    ".fcs",   # FCEUX (NES)
    ".sgm",   # Gens (Genesis)
    ".gqs",   # Gens Quick Save
    ".svs",   # Snes9x
    ".zs1", ".zs2", ".zs3", ".zs4", ".zs5",  # ZSNES
    ".zst",   # ZSNES
    ".frz",   # Freeze state
    ".sna",   # Spectrum snapshot
    ".z80",   # Spectrum snapshot
    ".cht",   # Cheat files (preserve these too)
}

# Combined all save-related extensions
ALL_SAVE_EXTENSIONS = SAVE_EXTENSIONS | STATE_EXTENSIONS


@dataclass
class MediaFile:
    """Represents an associated media file."""

    path: str
    media_type: str  # video, image, document
    category: str  # boxart, screenshot, video, manual, etc.
    rom_name: str  # The ROM name this is associated with
    size_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "media_type": self.media_type,
            "category": self.category,
            "rom_name": self.rom_name,
            "size_bytes": self.size_bytes,
        }


@dataclass
class OrphanedMedia:
    """Collection of orphaned media files for a ROM."""

    rom_name: str
    rom_path: str
    media_files: list[MediaFile] = field(default_factory=list)
    total_size_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            "rom_name": self.rom_name,
            "rom_path": self.rom_path,
            "media_files": [m.to_dict() for m in self.media_files],
            "total_size_bytes": self.total_size_bytes,
            "file_count": len(self.media_files),
        }


@dataclass
class MediaScanResult:
    """Result of scanning for orphaned media."""

    orphaned: list[OrphanedMedia]
    total_files: int
    total_size_bytes: int

    def to_dict(self) -> dict:
        return {
            "orphaned": [o.to_dict() for o in self.orphaned],
            "total_files": self.total_files,
            "total_size_bytes": self.total_size_bytes,
        }


@dataclass
class MediaCleanupResult:
    """Result of cleaning up media files."""

    removed_count: int
    removed_size_bytes: int
    errors: list[str]

    def to_dict(self) -> dict:
        return {
            "removed_count": self.removed_count,
            "removed_size_bytes": self.removed_size_bytes,
            "errors": self.errors,
        }


@dataclass
class SaveFile:
    """Represents a save game or save state file."""

    path: str
    save_type: str  # "save" (battery/SRAM) or "state" (save state)
    rom_name: str  # The ROM name this is associated with
    size_bytes: int = 0
    extension: str = ""

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "save_type": self.save_type,
            "rom_name": self.rom_name,
            "size_bytes": self.size_bytes,
            "extension": self.extension,
        }


@dataclass
class OrphanedSaves:
    """Collection of orphaned save files for a ROM."""

    rom_name: str
    rom_path: str  # Original ROM path if known, empty if truly orphaned
    save_files: list[SaveFile] = field(default_factory=list)
    state_files: list[SaveFile] = field(default_factory=list)
    total_size_bytes: int = 0

    def to_dict(self) -> dict:
        return {
            "rom_name": self.rom_name,
            "rom_path": self.rom_path,
            "save_files": [s.to_dict() for s in self.save_files],
            "state_files": [s.to_dict() for s in self.state_files],
            "total_size_bytes": self.total_size_bytes,
            "save_count": len(self.save_files),
            "state_count": len(self.state_files),
            "total_count": len(self.save_files) + len(self.state_files),
        }

    @property
    def all_files(self) -> list[SaveFile]:
        """Get all save and state files combined."""
        return self.save_files + self.state_files


@dataclass
class SaveScanResult:
    """Result of scanning for orphaned saves."""

    orphaned: list[OrphanedSaves]
    total_saves: int
    total_states: int
    total_size_bytes: int

    def to_dict(self) -> dict:
        return {
            "orphaned": [o.to_dict() for o in self.orphaned],
            "total_saves": self.total_saves,
            "total_states": self.total_states,
            "total_files": self.total_saves + self.total_states,
            "total_size_bytes": self.total_size_bytes,
        }


@dataclass
class SaveManageResult:
    """Result of managing (moving/renaming) save files.

    Note: Saves are NEVER deleted, only moved or renamed.
    """

    moved_count: int
    moved_size_bytes: int
    renamed_count: int
    errors: list[str]

    def to_dict(self) -> dict:
        return {
            "moved_count": self.moved_count,
            "moved_size_bytes": self.moved_size_bytes,
            "renamed_count": self.renamed_count,
            "errors": self.errors,
        }


def normalize_rom_name(filename: str) -> str:
    """
    Normalize a ROM filename to match against media files.

    Handles common naming variations:
    - Removes file extension
    - Removes region tags like (USA), [!], etc.
    - Normalizes whitespace
    """
    # Remove extension
    name = Path(filename).stem

    # Common patterns to potentially match with or without
    # We'll return the base name for matching
    return name


def extract_base_name(filename: str) -> str:
    """
    Extract the base name for fuzzy matching.

    Removes common suffixes like (USA), [!], (Rev 1), etc.
    """
    name = Path(filename).stem

    # Remove common ROM tags
    patterns = [
        r"\s*\([^)]*\)\s*",  # (USA), (Europe), etc.
        r"\s*\[[^\]]*\]\s*",  # [!], [T+Eng], etc.
        r"\s*\{[^}]*\}\s*",  # {C}, etc.
    ]

    base = name
    for pattern in patterns:
        base = re.sub(pattern, " ", base)

    # Clean up whitespace
    base = " ".join(base.split())

    return base.strip() or name  # Fall back to original if we stripped everything


def get_media_type(extension: str) -> str:
    """Get the media type for a file extension."""
    ext = extension.lower()
    for media_type, extensions in MEDIA_EXTENSIONS.items():
        if ext in extensions:
            return media_type
    return "other"


def get_save_type(extension: str) -> str | None:
    """Get the save type for a file extension.

    Returns:
        "save" for battery/SRAM saves
        "state" for save states
        None if not a save file
    """
    ext = extension.lower()

    # Handle multi-part extensions like .state.auto
    if ext in STATE_EXTENSIONS:
        return "state"
    if ext in SAVE_EXTENSIONS:
        return "save"

    # Check for numbered states (.state0, .state1, etc.)
    if ext.startswith(".state") and ext[6:].isdigit():
        return "state"

    return None


def is_save_file(path: Path) -> bool:
    """Check if a file is a save game or save state."""
    ext = path.suffix.lower()

    # Check standard extensions
    if ext in ALL_SAVE_EXTENSIONS:
        return True

    # Check for numbered states
    if ext.startswith(".state") and len(ext) > 6:
        return True

    # Check full filename for .state.auto pattern
    if ".state" in path.name.lower():
        return True

    return False


def guess_media_category(path: Path) -> str:
    """Guess the media category from the path."""
    path_lower = str(path).lower()

    categories = {
        "video": ["video", "preview", "snap"],
        "boxart": ["boxart", "box", "cover", "front"],
        "screenshot": ["screenshot", "snap", "screen"],
        "titlescreen": ["title", "titlescreen"],
        "wheel": ["wheel", "logo"],
        "marquee": ["marquee", "banner"],
        "fanart": ["fanart", "background"],
        "manual": ["manual", "pdf"],
        "3dbox": ["3dbox", "3d-box"],
    }

    for category, keywords in categories.items():
        for keyword in keywords:
            if keyword in path_lower:
                return category

    return "other"


class MediaCorrelator:
    """Correlates ROMs with their associated media files."""

    def __init__(
        self,
        db: DuperDatabase,
        config: DuperConfig | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ):
        self.db = db
        self.config = config or get_config()
        self.progress_callback = progress_callback
        self._media_dirs_cache: dict[str, list[Path]] = {}

    def _log(self, message: str) -> None:
        if self.progress_callback:
            self.progress_callback(message)

    def _find_emulation_root(self, rom_directory: str) -> Path | None:
        """Find the Emulation root directory from a ROM path."""
        path = Path(rom_directory)
        # Look for common emulation root indicators
        for parent in [path] + list(path.parents):
            if parent.name.lower() in ("emulation", "roms", "games"):
                return parent.parent if parent.name.lower() == "roms" else parent
            # Check for tools/downloaded_media sibling
            if (parent / "tools" / "downloaded_media").exists():
                return parent
            if (parent / "downloaded_media").exists():
                return parent
        return None

    def find_media_directories(self, rom_directory: str) -> list[Path]:
        """Find all media directories associated with a ROM directory."""
        if rom_directory in self._media_dirs_cache:
            return self._media_dirs_cache[rom_directory]

        rom_path = Path(rom_directory)
        media_dirs: list[Path] = []

        # Get the system name (e.g., "nes", "snes") from the ROM directory
        system_name = rom_path.name

        # Check subdirectories of the ROM directory
        for pattern in MEDIA_SUBDIR_PATTERNS:
            media_path = rom_path / pattern
            if media_path.exists() and media_path.is_dir():
                media_dirs.append(media_path)

        # Check parent directory (for cases where media is sibling to ROM folder)
        parent = rom_path.parent
        for pattern in MEDIA_SUBDIR_PATTERNS:
            # Check for system-specific media folder
            # e.g., /roms/snes -> /media/snes
            media_path = parent / pattern / system_name
            if media_path.exists() and media_path.is_dir():
                media_dirs.append(media_path)

        # Check EmulationStation paths
        for es_path_str in ES_MEDIA_PATHS:
            es_path = Path(es_path_str).expanduser()
            system_media = es_path / system_name
            if system_media.exists() and system_media.is_dir():
                media_dirs.append(system_media)

        # Check Emulation root paths (tools/downloaded_media style)
        # This handles structures like: /media/sdcard/Emulation/tools/downloaded_media/nes/
        emulation_root = self._find_emulation_root(rom_directory)
        if emulation_root:
            for emu_media_path in EMULATION_MEDIA_PATHS:
                media_base = emulation_root / emu_media_path / system_name
                if media_base.exists() and media_base.is_dir():
                    media_dirs.append(media_base)

        self._media_dirs_cache[rom_directory] = media_dirs
        return media_dirs

    def find_media_for_rom(self, rom_path: str) -> list[MediaFile]:
        """Find all media files associated with a specific ROM."""
        rom = Path(rom_path)
        rom_name = normalize_rom_name(rom.name)
        rom_base = extract_base_name(rom.name)
        rom_dir = rom.parent

        media_files: list[MediaFile] = []
        media_dirs = self.find_media_directories(str(rom_dir))

        for media_dir in media_dirs:
            if not media_dir.exists():
                continue

            # Search both the directory and its category subdirectories
            search_dirs = [media_dir]

            # Add category subdirectories (covers/, 3dboxes/, screenshots/, etc.)
            for cat_subdir in MEDIA_CATEGORY_SUBDIRS:
                cat_path = media_dir / cat_subdir
                if cat_path.exists() and cat_path.is_dir():
                    search_dirs.append(cat_path)

            for search_dir in search_dirs:
                # Look for files matching the ROM name
                try:
                    for media_file in search_dir.iterdir():
                        if not media_file.is_file():
                            continue

                        media_name = normalize_rom_name(media_file.name)
                        media_base = extract_base_name(media_file.name)

                        # Match by exact name or base name
                        if media_name == rom_name or media_base == rom_base:
                            media_type = get_media_type(media_file.suffix)
                            # Use the subdirectory name as category if available
                            category = search_dir.name if search_dir != media_dir else guess_media_category(media_dir)

                            try:
                                size = media_file.stat().st_size
                            except OSError:
                                size = 0

                            media_files.append(MediaFile(
                                path=str(media_file),
                                media_type=media_type,
                                category=category,
                                rom_name=rom_name,
                                size_bytes=size,
                            ))
                except PermissionError:
                    continue

        return media_files

    def scan_and_store_media_for_rom(self, rom_filepath: str) -> int:
        """
        Find media for a single ROM and store in database.

        Returns the number of media files found and stored.
        """
        import mimetypes

        media_files = self.find_media_for_rom(rom_filepath)

        if not media_files:
            return 0

        count = 0
        for media in media_files:
            # Get mime type
            mime_type, _ = mimetypes.guess_type(media.path)

            # Insert into database
            result = self.db.insert_media(
                rom_filepath=rom_filepath,
                media_path=media.path,
                media_type=media.media_type,
                category=media.category,
                filename=Path(media.path).name,
                size_bytes=media.size_bytes,
                mime_type=mime_type or "",
            )
            if result:
                count += 1

        return count

    def scan_media_for_directory(self, rom_directory: str) -> dict:
        """
        Scan and store media for all ROMs in a directory.

        This correlates media files with ROMs and stores the associations
        in the database.

        Returns statistics about the scan.
        """
        import mimetypes

        self._log(f"Scanning media for ROMs in {rom_directory}")

        # Clear existing media for this directory
        cleared = self.db.clear_media_for_directory(rom_directory)
        if cleared > 0:
            self._log(f"Cleared {cleared} existing media records")

        # Get all ROM files from the database for this directory
        rom_files = self.db.get_files_in_directory(rom_directory)

        if not rom_files:
            self._log("No ROM files found in database")
            return {
                "roms_scanned": 0,
                "media_found": 0,
                "roms_with_media": 0,
            }

        total_media = 0
        roms_with_media = 0

        for i, rom in enumerate(rom_files):
            if i > 0 and i % 100 == 0:
                self._log(f"Processed {i}/{len(rom_files)} ROMs...")

            media_files = self.find_media_for_rom(rom.filepath)

            if media_files:
                roms_with_media += 1

                for media in media_files:
                    # Get mime type
                    mime_type, _ = mimetypes.guess_type(media.path)

                    # Insert into database
                    self.db.insert_media(
                        rom_filepath=rom.filepath,
                        media_path=media.path,
                        media_type=media.media_type,
                        category=media.category,
                        filename=Path(media.path).name,
                        size_bytes=media.size_bytes,
                        mime_type=mime_type or "",
                    )
                    total_media += 1

        self._log(f"Media scan complete: {total_media} files for {roms_with_media} ROMs")

        return {
            "roms_scanned": len(rom_files),
            "media_found": total_media,
            "roms_with_media": roms_with_media,
        }

    def find_orphaned_media(self, rom_directory: str) -> MediaScanResult:
        """
        Find orphaned media files - media without corresponding ROMs.

        This scans all media directories and checks if each media file
        has a corresponding ROM still present.
        """
        self._log(f"Scanning for orphaned media in {rom_directory}")

        rom_path = Path(rom_directory)
        media_dirs = self.find_media_directories(rom_directory)

        # Get all ROM names currently in the directory
        existing_roms: set[str] = set()
        existing_bases: set[str] = set()

        for rom_file in rom_path.rglob("*"):
            if rom_file.is_file():
                name = normalize_rom_name(rom_file.name)
                base = extract_base_name(rom_file.name)
                existing_roms.add(name)
                existing_bases.add(base)

        # Also check database for ROMs in this directory
        db_files = self.db.get_files_in_directory(rom_directory)
        for f in db_files:
            name = normalize_rom_name(f.filename)
            base = extract_base_name(f.filename)
            existing_roms.add(name)
            existing_bases.add(base)

        # Scan media directories for orphans
        orphaned_by_rom: dict[str, OrphanedMedia] = {}
        total_files = 0
        total_size = 0

        for media_dir in media_dirs:
            if not media_dir.exists():
                continue

            self._log(f"Checking {media_dir}")

            for media_file in media_dir.iterdir():
                if not media_file.is_file():
                    continue

                media_name = normalize_rom_name(media_file.name)
                media_base = extract_base_name(media_file.name)

                # Check if this media has a corresponding ROM
                has_rom = (
                    media_name in existing_roms or
                    media_base in existing_bases
                )

                if not has_rom:
                    # This is orphaned media
                    try:
                        size = media_file.stat().st_size
                    except OSError:
                        size = 0

                    media = MediaFile(
                        path=str(media_file),
                        media_type=get_media_type(media_file.suffix),
                        category=guess_media_category(media_dir),
                        rom_name=media_name,
                        size_bytes=size,
                    )

                    if media_name not in orphaned_by_rom:
                        orphaned_by_rom[media_name] = OrphanedMedia(
                            rom_name=media_name,
                            rom_path="",  # ROM doesn't exist
                        )

                    orphaned_by_rom[media_name].media_files.append(media)
                    orphaned_by_rom[media_name].total_size_bytes += size
                    total_files += 1
                    total_size += size

        return MediaScanResult(
            orphaned=list(orphaned_by_rom.values()),
            total_files=total_files,
            total_size_bytes=total_size,
        )

    def find_media_for_moved_roms(self) -> MediaScanResult:
        """
        Find media files that correspond to ROMs that have been moved.

        This looks at the moved_files table and finds any media that
        was left behind.
        """
        self._log("Finding media for moved ROMs")

        moved_files = self.db.get_moved_files()
        orphaned_list: list[OrphanedMedia] = []
        total_files = 0
        total_size = 0

        for moved in moved_files:
            rom_path = moved.original_filepath
            media_files = self.find_media_for_rom(rom_path)

            if media_files:
                orphan = OrphanedMedia(
                    rom_name=normalize_rom_name(Path(rom_path).name),
                    rom_path=rom_path,
                )
                orphan.media_files = media_files
                orphan.total_size_bytes = sum(m.size_bytes for m in media_files)
                orphaned_list.append(orphan)

                total_files += len(media_files)
                total_size += orphan.total_size_bytes

        return MediaScanResult(
            orphaned=orphaned_list,
            total_files=total_files,
            total_size_bytes=total_size,
        )

    def cleanup_media(
        self,
        media_files: list[MediaFile],
        move_to: str | None = None,
        dry_run: bool = False,
    ) -> MediaCleanupResult:
        """
        Clean up (remove or move) media files.

        Args:
            media_files: List of media files to clean up
            move_to: If set, move files here instead of deleting
            dry_run: If True, don't actually delete/move files
        """
        import shutil

        removed_count = 0
        removed_size = 0
        errors: list[str] = []

        if move_to and not dry_run:
            Path(move_to).mkdir(parents=True, exist_ok=True)

        for media in media_files:
            media_path = Path(media.path)

            if not media_path.exists():
                continue

            try:
                if dry_run:
                    self._log(f"Would {'move' if move_to else 'remove'}: {media.path}")
                elif move_to:
                    dest = Path(move_to) / media_path.name
                    # Handle name conflicts
                    if dest.exists():
                        base = media_path.stem
                        ext = media_path.suffix
                        i = 1
                        while dest.exists():
                            dest = Path(move_to) / f"{base}_{i}{ext}"
                            i += 1
                    shutil.move(str(media_path), str(dest))
                    self._log(f"Moved: {media.path} -> {dest}")
                else:
                    media_path.unlink()
                    self._log(f"Removed: {media.path}")

                removed_count += 1
                removed_size += media.size_bytes

            except OSError as e:
                errors.append(f"Failed to process {media.path}: {e}")

        return MediaCleanupResult(
            removed_count=removed_count,
            removed_size_bytes=removed_size,
            errors=errors,
        )

    def cleanup_orphaned_media(
        self,
        rom_directory: str,
        move_to: str | None = None,
        dry_run: bool = False,
    ) -> MediaCleanupResult:
        """
        Find and clean up all orphaned media in a directory.

        Convenience method that combines find_orphaned_media and cleanup_media.
        """
        result = self.find_orphaned_media(rom_directory)

        all_media: list[MediaFile] = []
        for orphan in result.orphaned:
            all_media.extend(orphan.media_files)

        return self.cleanup_media(all_media, move_to=move_to, dry_run=dry_run)

    def cleanup_moved_rom_media(
        self,
        move_to: str | None = None,
        dry_run: bool = False,
    ) -> MediaCleanupResult:
        """
        Find and clean up media for all moved ROMs.

        Convenience method for cleaning up after duplicate processing.
        """
        result = self.find_media_for_moved_roms()

        all_media: list[MediaFile] = []
        for orphan in result.orphaned:
            all_media.extend(orphan.media_files)

        return self.cleanup_media(all_media, move_to=move_to, dry_run=dry_run)

    # =========================================================================
    # SAVE GAME / SAVE STATE METHODS
    # IMPORTANT: Saves are NEVER deleted - only moved or renamed
    # =========================================================================

    def find_save_directories(self, rom_directory: str) -> list[Path]:
        """Find all save directories associated with a ROM directory."""
        rom_path = Path(rom_directory)
        save_dirs: list[Path] = []

        # Check subdirectories of the ROM directory
        for pattern in SAVE_SUBDIR_PATTERNS:
            save_path = rom_path / pattern
            if save_path.exists() and save_path.is_dir():
                save_dirs.append(save_path)

        # Check parent directory for sibling save folders
        parent = rom_path.parent
        for pattern in SAVE_SUBDIR_PATTERNS:
            save_path = parent / pattern / rom_path.name
            if save_path.exists() and save_path.is_dir():
                save_dirs.append(save_path)

        # Check RetroArch system-wide paths
        system_name = rom_path.name
        for ra_path_str in RETROARCH_SAVE_PATHS:
            ra_path = Path(ra_path_str).expanduser()
            # Check both direct and system-specific subdirs
            if ra_path.exists() and ra_path.is_dir():
                save_dirs.append(ra_path)
                system_save = ra_path / system_name
                if system_save.exists() and system_save.is_dir():
                    save_dirs.append(system_save)

        # Check ES save paths
        for es_path_str in ES_SAVE_PATHS:
            es_path = Path(es_path_str).expanduser()
            system_save = es_path / system_name
            if system_save.exists() and system_save.is_dir():
                save_dirs.append(system_save)

        # Also check the ROM directory itself for saves next to ROMs
        if rom_path.exists() and rom_path.is_dir():
            save_dirs.append(rom_path)

        return list(set(save_dirs))  # Deduplicate

    def find_saves_for_rom(self, rom_path: str) -> tuple[list[SaveFile], list[SaveFile]]:
        """
        Find all save games and save states associated with a specific ROM.

        Returns:
            Tuple of (save_files, state_files)
        """
        rom = Path(rom_path)
        rom_name = normalize_rom_name(rom.name)
        rom_base = extract_base_name(rom.name)
        rom_dir = rom.parent

        save_files: list[SaveFile] = []
        state_files: list[SaveFile] = []

        save_dirs = self.find_save_directories(str(rom_dir))

        for save_dir in save_dirs:
            if not save_dir.exists():
                continue

            for save_file in save_dir.iterdir():
                if not save_file.is_file():
                    continue

                # Check if this is a save file by extension
                if not is_save_file(save_file):
                    continue

                save_name = normalize_rom_name(save_file.name)
                save_base = extract_base_name(save_file.name)

                # Match by exact name or base name
                if save_name == rom_name or save_base == rom_base:
                    try:
                        size = save_file.stat().st_size
                    except OSError:
                        size = 0

                    save_type = get_save_type(save_file.suffix) or "save"
                    sf = SaveFile(
                        path=str(save_file),
                        save_type=save_type,
                        rom_name=rom_name,
                        size_bytes=size,
                        extension=save_file.suffix,
                    )

                    if save_type == "state":
                        state_files.append(sf)
                    else:
                        save_files.append(sf)

        return save_files, state_files

    def find_orphaned_saves(self, rom_directory: str) -> SaveScanResult:
        """
        Find orphaned save files - saves without corresponding ROMs.

        This scans all save directories and checks if each save file
        has a corresponding ROM still present.
        """
        self._log(f"Scanning for orphaned saves in {rom_directory}")

        rom_path = Path(rom_directory)
        save_dirs = self.find_save_directories(rom_directory)

        # Get all ROM names currently in the directory
        existing_roms: set[str] = set()
        existing_bases: set[str] = set()

        for rom_file in rom_path.rglob("*"):
            if rom_file.is_file() and not is_save_file(rom_file):
                name = normalize_rom_name(rom_file.name)
                base = extract_base_name(rom_file.name)
                existing_roms.add(name)
                existing_bases.add(base)

        # Also check database for ROMs in this directory
        db_files = self.db.get_files_in_directory(rom_directory)
        for f in db_files:
            name = normalize_rom_name(f.filename)
            base = extract_base_name(f.filename)
            existing_roms.add(name)
            existing_bases.add(base)

        # Scan save directories for orphans
        orphaned_by_rom: dict[str, OrphanedSaves] = {}
        total_saves = 0
        total_states = 0
        total_size = 0

        for save_dir in save_dirs:
            if not save_dir.exists():
                continue

            self._log(f"Checking {save_dir}")

            for save_file in save_dir.iterdir():
                if not save_file.is_file():
                    continue

                if not is_save_file(save_file):
                    continue

                save_name = normalize_rom_name(save_file.name)
                save_base = extract_base_name(save_file.name)

                # Check if this save has a corresponding ROM
                has_rom = (
                    save_name in existing_roms or
                    save_base in existing_bases
                )

                if not has_rom:
                    try:
                        size = save_file.stat().st_size
                    except OSError:
                        size = 0

                    save_type = get_save_type(save_file.suffix) or "save"
                    sf = SaveFile(
                        path=str(save_file),
                        save_type=save_type,
                        rom_name=save_name,
                        size_bytes=size,
                        extension=save_file.suffix,
                    )

                    if save_name not in orphaned_by_rom:
                        orphaned_by_rom[save_name] = OrphanedSaves(
                            rom_name=save_name,
                            rom_path="",
                        )

                    if save_type == "state":
                        orphaned_by_rom[save_name].state_files.append(sf)
                        total_states += 1
                    else:
                        orphaned_by_rom[save_name].save_files.append(sf)
                        total_saves += 1

                    orphaned_by_rom[save_name].total_size_bytes += size
                    total_size += size

        return SaveScanResult(
            orphaned=list(orphaned_by_rom.values()),
            total_saves=total_saves,
            total_states=total_states,
            total_size_bytes=total_size,
        )

    def find_saves_for_moved_roms(self) -> SaveScanResult:
        """
        Find save files that correspond to ROMs that have been moved.

        This looks at the moved_files table and finds any saves that
        were left behind.
        """
        self._log("Finding saves for moved ROMs")

        moved_files = self.db.get_moved_files()
        orphaned_list: list[OrphanedSaves] = []
        total_saves = 0
        total_states = 0
        total_size = 0

        for moved in moved_files:
            rom_path = moved.original_filepath
            save_files, state_files = self.find_saves_for_rom(rom_path)

            if save_files or state_files:
                orphan = OrphanedSaves(
                    rom_name=normalize_rom_name(Path(rom_path).name),
                    rom_path=rom_path,
                    save_files=save_files,
                    state_files=state_files,
                )
                orphan.total_size_bytes = sum(s.size_bytes for s in save_files + state_files)
                orphaned_list.append(orphan)

                total_saves += len(save_files)
                total_states += len(state_files)
                total_size += orphan.total_size_bytes

        return SaveScanResult(
            orphaned=orphaned_list,
            total_saves=total_saves,
            total_states=total_states,
            total_size_bytes=total_size,
        )

    def move_saves(
        self,
        save_files: list[SaveFile],
        move_to: str,
        dry_run: bool = False,
        preserve_structure: bool = True,
    ) -> SaveManageResult:
        """
        Move save files to a new location.

        IMPORTANT: This method ONLY moves saves, it NEVER deletes them.

        Args:
            save_files: List of save files to move
            move_to: Destination directory
            dry_run: If True, don't actually move files
            preserve_structure: If True, preserve directory structure in destination
        """
        moved_count = 0
        moved_size = 0
        errors: list[str] = []

        if not dry_run:
            Path(move_to).mkdir(parents=True, exist_ok=True)

        for save in save_files:
            save_path = Path(save.path)

            if not save_path.exists():
                continue

            try:
                if preserve_structure:
                    # Preserve some directory context (e.g., saves/snes/game.srm)
                    parent_name = save_path.parent.name
                    dest_dir = Path(move_to) / parent_name
                    if not dry_run:
                        dest_dir.mkdir(parents=True, exist_ok=True)
                    dest = dest_dir / save_path.name
                else:
                    dest = Path(move_to) / save_path.name

                # Handle name conflicts
                if dest.exists() and not dry_run:
                    base = save_path.stem
                    ext = save_path.suffix
                    i = 1
                    while dest.exists():
                        if preserve_structure:
                            dest = dest_dir / f"{base}_{i}{ext}"
                        else:
                            dest = Path(move_to) / f"{base}_{i}{ext}"
                        i += 1

                if dry_run:
                    self._log(f"Would move: {save.path} -> {dest}")
                else:
                    shutil.move(str(save_path), str(dest))
                    self._log(f"Moved save: {save.path} -> {dest}")

                moved_count += 1
                moved_size += save.size_bytes

            except OSError as e:
                errors.append(f"Failed to move {save.path}: {e}")

        return SaveManageResult(
            moved_count=moved_count,
            moved_size_bytes=moved_size,
            renamed_count=0,
            errors=errors,
        )

    def rename_saves_for_rom(
        self,
        old_rom_name: str,
        new_rom_name: str,
        rom_directory: str,
        dry_run: bool = False,
    ) -> SaveManageResult:
        """
        Rename save files to match a new ROM name.

        Useful when renaming a ROM file and wanting to keep the saves associated.

        IMPORTANT: This method ONLY renames saves, it NEVER deletes them.

        Args:
            old_rom_name: Original ROM filename (without extension)
            new_rom_name: New ROM filename (without extension)
            rom_directory: Directory containing the ROM
            dry_run: If True, don't actually rename files
        """
        renamed_count = 0
        errors: list[str] = []

        # Create a fake path to find saves
        fake_rom_path = str(Path(rom_directory) / f"{old_rom_name}.rom")
        save_files, state_files = self.find_saves_for_rom(fake_rom_path)

        all_saves = save_files + state_files

        for save in all_saves:
            save_path = Path(save.path)

            if not save_path.exists():
                continue

            # Build new filename by replacing the ROM name portion
            old_stem = normalize_rom_name(save_path.name)
            new_name = save_path.name.replace(old_stem, new_rom_name, 1)

            # If the name didn't change (e.g., fuzzy match didn't work), try direct replace
            if new_name == save_path.name:
                ext = save_path.suffix
                new_name = new_rom_name + ext

            new_path = save_path.parent / new_name

            try:
                if dry_run:
                    self._log(f"Would rename: {save_path} -> {new_path}")
                else:
                    # Don't overwrite existing files
                    if new_path.exists():
                        errors.append(f"Cannot rename {save_path}: {new_path} already exists")
                        continue

                    save_path.rename(new_path)
                    self._log(f"Renamed save: {save_path} -> {new_path}")

                renamed_count += 1

            except OSError as e:
                errors.append(f"Failed to rename {save_path}: {e}")

        return SaveManageResult(
            moved_count=0,
            moved_size_bytes=0,
            renamed_count=renamed_count,
            errors=errors,
        )

    def preserve_orphaned_saves(
        self,
        rom_directory: str,
        move_to: str,
        dry_run: bool = False,
    ) -> SaveManageResult:
        """
        Find and move all orphaned saves in a directory to a safe location.

        IMPORTANT: This method moves saves to preserve them, it NEVER deletes them.

        Args:
            rom_directory: Directory to scan for orphaned saves
            move_to: Directory to move orphaned saves to
            dry_run: If True, don't actually move files
        """
        result = self.find_orphaned_saves(rom_directory)

        all_saves: list[SaveFile] = []
        for orphan in result.orphaned:
            all_saves.extend(orphan.all_files)

        return self.move_saves(all_saves, move_to=move_to, dry_run=dry_run)

    def preserve_moved_rom_saves(
        self,
        move_to: str,
        dry_run: bool = False,
    ) -> SaveManageResult:
        """
        Find and move saves for all moved ROMs to a safe location.

        IMPORTANT: This method moves saves to preserve them, it NEVER deletes them.

        Args:
            move_to: Directory to move saves to
            dry_run: If True, don't actually move files
        """
        result = self.find_saves_for_moved_roms()

        all_saves: list[SaveFile] = []
        for orphan in result.orphaned:
            all_saves.extend(orphan.all_files)

        return self.move_saves(all_saves, move_to=move_to, dry_run=dry_run)
