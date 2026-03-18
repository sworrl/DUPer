"""Duplicate detection and processing for DUPer."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from duper.core.config import DuperConfig, get_config
from duper.core.database import DuperDatabase, FileRecord
from duper.core.media import MediaCorrelator


@dataclass
class DuplicateGroup:
    """A group of files that are duplicates of each other."""

    md5: str
    files: list[FileRecord]
    recommended_keep: str = ""
    scores: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict:
        # Get rom_serial from first file (all files in group have same MD5, so same serial)
        rom_serial = self.files[0].rom_serial if self.files else ""
        return {
            "md5": self.md5,
            "rom_serial": rom_serial,
            "files": [
                {
                    "filepath": f.filepath,
                    "filename": f.filename,
                    "size_mb": f.size_mb,
                    "score": self.scores.get(f.filepath, 0),
                    "rom_serial": f.rom_serial,
                    "ra_supported": f.ra_supported,
                    "ra_game_id": f.ra_game_id,
                    "ra_game_title": f.ra_game_title,
                    "ra_checked_date": f.ra_checked_date,
                }
                for f in self.files
            ],
            "recommended_keep": self.recommended_keep,
            "file_count": len(self.files),
        }


@dataclass
class ProcessResult:
    """Result of processing duplicates."""

    action: str = "archive"
    processed_count: int = 0
    archived_count: int = 0
    deleted_count: int = 0
    space_freed_mb: float = 0.0
    media_processed_count: int = 0
    media_space_freed_mb: float = 0.0
    errors: list[str] = field(default_factory=list)
    processed_files: list[dict] = field(default_factory=list)  # [{filepath, action, destination?}]

    def to_dict(self) -> dict:
        return {
            "action": self.action,
            "processed_count": self.processed_count,
            "archived_count": self.archived_count,
            "deleted_count": self.deleted_count,
            "space_freed_mb": self.space_freed_mb,
            "media_processed_count": self.media_processed_count,
            "media_space_freed_mb": self.media_space_freed_mb,
            "errors": self.errors,
            "processed_files": self.processed_files,
        }


@dataclass
class RestoreResult:
    """Result of restoring files."""

    restored_count: int
    errors: list[str]

    def to_dict(self) -> dict:
        return {
            "restored_count": self.restored_count,
            "errors": self.errors,
        }


class Deduper:
    """Duplicate detection and processing engine."""

    def __init__(
        self,
        db: DuperDatabase,
        config: DuperConfig | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ):
        self.db = db
        self.config = config or get_config()
        self.progress_callback = progress_callback
        self.media_correlator = MediaCorrelator(db=db, config=self.config, progress_callback=progress_callback)

    def _log(self, message: str) -> None:
        """Log a message via callback if set."""
        if self.progress_callback:
            self.progress_callback(message)

    def mark_duplicates(self, directory: str) -> int:
        """
        Mark all duplicate files in a directory with confidence scoring.

        Duplicates are detected PER-SYSTEM (within each subdirectory), not across
        the entire directory. This prevents cross-platform releases from being
        marked as duplicates.

        Scoring:
        - 100: MD5 match (identical content - definite duplicate)
        - 80: Same simplified_filename (same name, different format like .z64/.v64)
        - 60: Same normalized_name (same game, different region/version)

        Also marks cross-platform files (same game on multiple systems) with the
        is_cross_platform flag for reference.

        Returns the count of potential duplicates found.
        """
        # Reset all duplicate and cross-platform flags first
        self.db.reset_duplicates_in_directory(directory)

        # Populate normalized names for fuzzy matching
        self.db.populate_normalized_names(directory)

        # Get all system directories (immediate subdirectories with files)
        system_dirs = self.db.get_system_directories(directory)

        total_marked = 0
        for system_dir in system_dirs:
            # Mark duplicates within each system directory
            marked = self.db.mark_duplicates_per_system(system_dir)
            total_marked += marked

        # Mark cross-platform files (same game on multiple systems)
        # These are NOT duplicates, just flagged for reference
        self.db.mark_cross_platform_files(directory)

        return total_marked

    def analyze_duplicates(self, directory: str, include_name_based: bool = True) -> list[DuplicateGroup]:
        """
        Analyze duplicates in a directory and return grouped results.

        Args:
            directory: Directory to analyze
            include_name_based: If True, include name-based duplicates (same game name).
                               If False, only return exact MD5 duplicates.

        Returns groups of files that are either:
        - Exact content duplicates (same MD5 hash)
        - Name-based duplicates (same normalized game name, different versions/regions)
        """
        groups: list[DuplicateGroup] = []
        seen_files: set[str] = set()  # Track files already in a group

        # First, get exact MD5 duplicates (highest priority)
        md5_hashes = self.db.get_duplicate_md5_hashes(directory)

        for md5 in md5_hashes:
            files = self.db.get_files_by_md5(md5, directory)
            if len(files) < 2:
                continue

            group = DuplicateGroup(md5=md5, files=files)
            group.scores = self._calculate_scores(files)
            group.recommended_keep = max(group.scores, key=group.scores.get)
            groups.append(group)

            # Track these files as already grouped
            for f in files:
                seen_files.add(f.filepath)

        # Then, get name-based duplicates if requested
        if include_name_based:
            name_groups = self.db.get_name_based_duplicate_groups(directory)

            for normalized_name, filepaths in name_groups.items():
                # Filter out files already in MD5 groups
                remaining = [fp for fp in filepaths if fp not in seen_files]
                if len(remaining) < 2:
                    continue

                # Get full file records
                files = [self.db.get_file(fp) for fp in remaining]
                files = [f for f in files if f is not None]
                if len(files) < 2:
                    continue

                # Create group with normalized_name as identifier
                group = DuplicateGroup(md5=f"name:{normalized_name}", files=files)
                group.scores = self._calculate_scores(files)
                group.recommended_keep = max(group.scores, key=group.scores.get)
                groups.append(group)

        return groups

    def get_cross_platform_groups(self, directory: str) -> list[DuplicateGroup]:
        """
        Get cross-platform groups -- same game across different system directories.

        These are NOT treated as duplicates by default (they are intentional
        multi-platform releases), but are returned for informational purposes
        so the user can decide whether to consolidate.

        Returns groups of files that share the same normalized name but live
        in different system subdirectories.
        """
        cross_groups = self.db.get_cross_platform_groups(directory)
        groups: list[DuplicateGroup] = []

        for normalized_name, filepaths in cross_groups.items():
            files = [self.db.get_file(fp) for fp in filepaths]
            files = [f for f in files if f is not None]
            if len(files) < 2:
                continue

            group = DuplicateGroup(md5=f"cross:{normalized_name}", files=files)
            group.scores = self._calculate_scores(files)
            group.recommended_keep = max(group.scores, key=group.scores.get)
            groups.append(group)

        return groups

    def _calculate_scores(self, files: list[FileRecord]) -> dict[str, float]:
        """
        Calculate keeping scores for a list of duplicate files.

        Higher score = more likely to be the "best" file to keep.

        Scoring factors (in order of importance):
        - RetroAchievements supported (+1000) - CRITICAL: RA-compatible ROMs are always preferred
        - Spaces in filename (+5) - indicates human-readable name
        - Longer filename (+0.1 per char) - more descriptive
        - Very short name penalty (-2) - less descriptive
        - Shortest name bonus (+1) - often the "clean" version
        - Alphabetically first (+1) - deterministic tiebreaker
        - Smallest size (+0.5) - may be more optimized
        """
        scores: dict[str, float] = {}

        if not files:
            return scores

        # Get RA score bonus from config
        ra_bonus = self.config.retroachievements.ra_score_bonus

        # Pre-calculate comparison values
        min_size = min(f.size_mb for f in files)
        shortest_name_len = min(len(f.simplified_filename) for f in files)
        first_alphabetically = min(f.simplified_filename for f in files)

        for file in files:
            score = 0.0
            filename = file.filename
            simplified = file.simplified_filename

            # CRITICAL: RetroAchievements supported ROMs get a MASSIVE bonus
            # This ensures RA-compatible ROMs are ALWAYS preferred
            if file.ra_supported:
                score += ra_bonus
                self._log(f"  RA SUPPORTED: {filename} (+{ra_bonus})")

            # Prioritize filenames with spaces (more human-readable)
            if " " in filename:
                score += 5

            # Prioritize longer filenames (more descriptive)
            score += len(simplified) * 0.1

            # Penalize very short names (less descriptive), but not the shortest
            # itself which is likely the "clean" version
            if len(simplified) != shortest_name_len and len(simplified) < shortest_name_len + 5:
                score -= 2

            # Bonus for exact shortest name (clean version)
            if len(simplified) == shortest_name_len:
                score += 1

            # Bonus for alphabetically first (deterministic)
            if simplified == first_alphabetically:
                score += 1

            # Slight bonus for smallest size
            if file.size_mb == min_size and min_size > 0:
                score += 0.5

            scores[file.filepath] = score

        return scores

    def process_duplicates(
        self,
        directory: str,
        action: str = "archive",
        archive_location: str | None = None,
        dry_run: bool = False,
        group_hashes: list[str] | None = None,
        keep_overrides: dict[str, str] | None = None,
    ) -> ProcessResult:
        """
        Process duplicates by archiving or deleting them.

        The file with the highest score in each group is kept (unless overridden);
        all others are archived or deleted based on the action.

        Args:
            directory: The directory to process duplicates in
            action: "archive" to move files, "delete" to permanently remove
            archive_location: Where to archive duplicates (uses config default if None)
            dry_run: If True, don't actually process files
            group_hashes: Optional list of MD5 hashes to process (None = all)
            keep_overrides: Optional dict of {md5: filepath} to override which file to keep

        Returns:
            ProcessResult with details of the operation
        """
        if archive_location is None:
            archive_location = self.config.paths.duplicates_dir

        archive_path = Path(archive_location)
        source_path = Path(directory)

        # Create archive location if needed (only for archive action)
        if action == "archive" and not dry_run:
            archive_path.mkdir(parents=True, exist_ok=True)
            scan_dir_name = source_path.name
            archive_subdir = archive_path / scan_dir_name
            archive_subdir.mkdir(parents=True, exist_ok=True)
        else:
            archive_subdir = archive_path / source_path.name

        retroarch_mode = self.config.scanner.retroarch_mode

        # Get all duplicate groups
        groups = self.analyze_duplicates(directory)

        # Filter to specific groups if requested
        if group_hashes:
            groups = [g for g in groups if g.md5 in group_hashes]

        result = ProcessResult(action=action)

        for group in groups:
            if len(group.files) < 2:
                continue

            # Determine which file to keep (check for override)
            if keep_overrides and group.md5 in keep_overrides:
                file_to_keep = keep_overrides[group.md5]
            else:
                file_to_keep = group.recommended_keep

            # Determine the duplicate reason from the group identifier
            if group.md5.startswith("name:"):
                dupe_reason = "name_duplicate"
            else:
                dupe_reason = "md5_duplicate"

            for file in group.files:
                if file.filepath == file_to_keep:
                    continue

                filepath = Path(file.filepath)
                filename = file.filename

                try:
                    # Find associated media files BEFORE moving/deleting the ROM
                    associated_media = self.media_correlator.find_media_for_rom(file.filepath)

                    if action == "delete":
                        # Delete the ROM file
                        if not dry_run:
                            filepath.unlink()
                            self.db.delete_file(str(filepath))

                        result.deleted_count += 1
                        result.processed_files.append({
                            "filepath": str(filepath),
                            "action": "deleted",
                            "size_mb": file.size_mb,
                        })
                        self._log(f"{'Would delete' if dry_run else 'Deleted'}: {filepath}")

                        # Also delete associated media files
                        for media in associated_media:
                            media_path = Path(media.path)
                            if not media_path.exists():
                                continue
                            try:
                                media_size_mb = media.size_bytes / (1024 * 1024)
                                if not dry_run:
                                    media_path.unlink()
                                result.media_processed_count += 1
                                result.media_space_freed_mb += media_size_mb
                                self._log(f"{'Would delete' if dry_run else 'Deleted'} media: {media_path}")
                            except OSError as e:
                                result.errors.append(f"Error deleting media '{media_path}': {e}")

                    else:  # archive
                        # Determine destination directory
                        destination_dir = archive_subdir

                        if retroarch_mode:
                            # Preserve subdirectory structure
                            try:
                                relative_path = filepath.relative_to(source_path)
                                if relative_path.parent != Path("."):
                                    destination_dir = archive_subdir / relative_path.parent
                                    if not dry_run:
                                        destination_dir.mkdir(parents=True, exist_ok=True)
                            except ValueError:
                                pass  # File not under source_path, use default

                        destination_path = destination_dir / filename

                        # Handle filename conflicts
                        if destination_path.exists() and not dry_run:
                            base = filepath.stem
                            ext = filepath.suffix
                            index = 1
                            while (destination_dir / f"{base}_{index}{ext}").exists():
                                index += 1
                            destination_path = destination_dir / f"{base}_{index}{ext}"

                        if not dry_run:
                            shutil.move(str(filepath), str(destination_path))

                            # Record the move with file metadata for space tracking
                            self.db.record_moved_file(
                                original_path=str(filepath),
                                moved_to_path=str(destination_path),
                                size_mb=file.size_mb,
                                filename=file.filename,
                                md5=file.md5,
                                reason=dupe_reason,
                            )

                            # Remove from files table (also removes media DB records)
                            self.db.delete_file(str(filepath))

                        result.archived_count += 1
                        result.processed_files.append({
                            "filepath": str(filepath),
                            "action": "archived",
                            "destination": str(destination_path),
                            "size_mb": file.size_mb,
                        })
                        self._log(f"{'Would archive' if dry_run else 'Archived'}: {filepath} -> {destination_path}")

                        # Also archive associated media files alongside the ROM
                        for media in associated_media:
                            media_path = Path(media.path)
                            if not media_path.exists():
                                continue
                            try:
                                media_size_mb = media.size_bytes / (1024 * 1024)
                                # Put media in a media/ subdirectory within the archive
                                media_dest_dir = destination_dir / "media"
                                if not dry_run:
                                    media_dest_dir.mkdir(parents=True, exist_ok=True)
                                media_dest = media_dest_dir / media_path.name
                                # Handle name conflicts
                                if media_dest.exists() and not dry_run:
                                    base = media_path.stem
                                    ext = media_path.suffix
                                    idx = 1
                                    while (media_dest_dir / f"{base}_{idx}{ext}").exists():
                                        idx += 1
                                    media_dest = media_dest_dir / f"{base}_{idx}{ext}"
                                if not dry_run:
                                    shutil.move(str(media_path), str(media_dest))
                                    self.db.record_moved_file(
                                        original_path=str(media_path),
                                        moved_to_path=str(media_dest),
                                        size_mb=media_size_mb,
                                        filename=media_path.name,
                                        reason="media_for_duplicate",
                                    )
                                result.media_processed_count += 1
                                result.media_space_freed_mb += media_size_mb
                                self._log(f"{'Would archive' if dry_run else 'Archived'} media: {media_path} -> {media_dest}")
                            except OSError as e:
                                result.errors.append(f"Error archiving media '{media_path}': {e}")

                    result.processed_count += 1
                    result.space_freed_mb += file.size_mb

                except OSError as e:
                    error_msg = f"Error processing '{filepath}': {e}"
                    result.errors.append(error_msg)
                    self._log(error_msg)

        return result

    def log_statistics(self, directory: str) -> None:
        """Log duplicate statistics to the database."""
        total_files = self.db.get_file_count_in_directory(directory)
        potential_duplicates = self.db.get_potential_duplicates_count(directory)
        duplicate_info = self.db.get_duplicate_groups(directory)

        self.db.log_statistics(
            total_files=total_files,
            potential_duplicates=potential_duplicates,
            duplicate_info=duplicate_info,
            scan_directory=directory,
        )

    def restore_file(self, move_id: int) -> RestoreResult:
        """
        Restore a single moved file.

        Args:
            move_id: The ID of the moved file record

        Returns:
            RestoreResult with details of the operation
        """
        moved_file = self.db.get_moved_file(move_id)
        if not moved_file:
            return RestoreResult(restored_count=0, errors=[f"No moved file with ID {move_id}"])

        errors: list[str] = []
        restored_count = 0

        try:
            # Ensure original directory exists
            original_dir = Path(moved_file.original_filepath).parent
            original_dir.mkdir(parents=True, exist_ok=True)

            # Move file back
            shutil.move(moved_file.moved_to_path, moved_file.original_filepath)

            # Remove record
            self.db.delete_moved_file_record(move_id)

            restored_count = 1
            self._log(f"Restored: {moved_file.moved_to_path} -> {moved_file.original_filepath}")

        except OSError as e:
            error_msg = f"Error restoring '{moved_file.original_filepath}': {e}"
            errors.append(error_msg)
            self._log(error_msg)

        return RestoreResult(restored_count=restored_count, errors=errors)

    def restore_all_files(self) -> RestoreResult:
        """
        Restore all moved files.

        Returns:
            RestoreResult with details of the operation
        """
        moved_files = self.db.get_moved_files()

        if not moved_files:
            return RestoreResult(restored_count=0, errors=[])

        errors: list[str] = []
        restored_count = 0

        for moved_file in moved_files:
            try:
                # Ensure original directory exists
                original_dir = Path(moved_file.original_filepath).parent
                original_dir.mkdir(parents=True, exist_ok=True)

                # Move file back
                shutil.move(moved_file.moved_to_path, moved_file.original_filepath)

                # Remove record
                self.db.delete_moved_file_record(moved_file.move_id)

                restored_count += 1
                self._log(f"Restored: {moved_file.moved_to_path} -> {moved_file.original_filepath}")

            except OSError as e:
                error_msg = f"Error restoring '{moved_file.original_filepath}': {e}"
                errors.append(error_msg)
                self._log(error_msg)

        return RestoreResult(restored_count=restored_count, errors=errors)

    def get_duplicate_summary(self, directory: str) -> dict:
        """Get a summary of duplicates in a directory."""
        groups = self.analyze_duplicates(directory)

        total_duplicate_files = sum(len(g.files) for g in groups)
        total_groups = len(groups)
        total_wasted_space = sum(
            sum(f.size_mb for f in g.files if f.filepath != g.recommended_keep)
            for g in groups
        )

        return {
            "total_groups": total_groups,
            "total_duplicate_files": total_duplicate_files,
            "files_to_remove": total_duplicate_files - total_groups,
            "wasted_space_mb": total_wasted_space,
            "groups": [g.to_dict() for g in groups],
        }

    def get_moved_files_summary(self) -> dict:
        """Get a summary of all moved files."""
        moved_files = self.db.get_moved_files()

        total_size_mb = sum(mf.size_mb for mf in moved_files)

        return {
            "total_moved": len(moved_files),
            "total_size_mb": total_size_mb,
            "files": [
                {
                    "move_id": mf.move_id,
                    "original_filepath": mf.original_filepath,
                    "moved_to_path": mf.moved_to_path,
                    "moved_time": mf.moved_time,
                    "filename": mf.filename or Path(mf.original_filepath).name,
                    "size_mb": mf.size_mb,
                    "md5": mf.md5,
                    "rom_serial": mf.rom_serial,
                    "reason": mf.reason,
                }
                for mf in moved_files
            ],
        }
