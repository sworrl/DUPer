"""Core modules for DUPer."""

from duper.core.config import (
    DuperConfig,
    PathsConfig,
    RemoteHost,
    RetroAchievementsConfig,
    ScannerConfig,
    ScreenScraperConfig,
    ServerConfig,
    get_config,
    get_default_config_dir,
    get_default_data_dir,
    reset_config,
    set_config,
)
from duper.core.database import (
    DuperDatabase,
    FileRecord,
    FileStatistics,
    MediaRecord,
    MovedFile,
    ScanMetrics,
    generate_rom_serial,
)
from duper.core.deduper import (
    Deduper,
    DuplicateGroup,
    ProcessResult,
    RestoreResult,
)
from duper.core.scanner import Scanner, ScanProgress, ScanResult, detect_systems, SYSTEM_ALIASES
from duper.core.media import (
    MediaCorrelator,
    MediaFile,
    MediaScanResult,
    MediaCleanupResult,
    OrphanedMedia,
    SaveFile,
    SaveScanResult,
    SaveManageResult,
    OrphanedSaves,
)
from duper.core.retroachievements import (
    RetroAchievementsClient,
    RAGameInfo,
    RAVerificationResult,
    get_ra_client,
    set_ra_client,
)
from duper.core.screenscraper import (
    ScreenScraperClient,
    SSGameInfo,
    SSGameMedia,
    SSScrapeResult,
    get_ss_client,
    set_ss_client,
    reset_ss_client,
    get_system_id,
    SYSTEM_IDS,
)

__all__ = [
    # Config
    "DuperConfig",
    "ServerConfig",
    "ScannerConfig",
    "PathsConfig",
    "RemoteHost",
    "RetroAchievementsConfig",
    "get_config",
    "set_config",
    "reset_config",
    "get_default_config_dir",
    "get_default_data_dir",
    # Database
    "DuperDatabase",
    "FileRecord",
    "MediaRecord",
    "MovedFile",
    "ScanMetrics",
    "FileStatistics",
    "generate_rom_serial",
    # Scanner
    "Scanner",
    "ScanProgress",
    "ScanResult",
    "detect_systems",
    "SYSTEM_ALIASES",
    # Deduper
    "Deduper",
    "DuplicateGroup",
    "ProcessResult",
    "RestoreResult",
    # Media
    "MediaCorrelator",
    "MediaFile",
    "MediaScanResult",
    "MediaCleanupResult",
    "OrphanedMedia",
    # Saves
    "SaveFile",
    "SaveScanResult",
    "SaveManageResult",
    "OrphanedSaves",
    # RetroAchievements
    "RetroAchievementsClient",
    "RAGameInfo",
    "RAVerificationResult",
    "get_ra_client",
    "set_ra_client",
    # ScreenScraper
    "ScreenScraperClient",
    "ScreenScraperConfig",
    "SSGameInfo",
    "SSGameMedia",
    "SSScrapeResult",
    "get_ss_client",
    "set_ss_client",
    "reset_ss_client",
    "get_system_id",
    "SYSTEM_IDS",
]
