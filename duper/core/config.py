"""Configuration management for DUPer using TOML."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import toml

from duper.utils.helpers import generate_api_key


def get_default_config_dir() -> Path:
    """Get the default configuration directory based on platform."""
    if os.name == "nt":  # Windows
        base = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    else:  # Linux/Mac
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return base / "duper"


def get_default_data_dir() -> Path:
    """Get the default data directory based on platform."""
    if os.name == "nt":  # Windows
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    else:  # Linux/Mac
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / "duper"


@dataclass
class ServerConfig:
    """Server configuration settings."""

    port: int = 8420
    host: str = "0.0.0.0"
    web_ui_enabled: bool = True
    auth_enabled: bool = False
    api_key: str = ""

    def __post_init__(self):
        if not self.api_key:
            self.api_key = generate_api_key()


@dataclass
class ScannerConfig:
    """Scanner configuration settings."""

    ignore_fodder: bool = True
    ignore_video: bool = True
    ignore_music: bool = True
    ignore_pictures: bool = True
    retroarch_mode: bool = True


@dataclass
class PathsConfig:
    """Path configuration settings."""

    working_dir: str = ""
    database: str = ""
    duplicates_dir: str = ""

    def __post_init__(self):
        data_dir = get_default_data_dir()
        if not self.working_dir:
            self.working_dir = str(data_dir)
        if not self.database:
            self.database = str(data_dir / "duper.db")
        if not self.duplicates_dir:
            self.duplicates_dir = str(data_dir / "duplicates")


@dataclass
class RetroAchievementsConfig:
    """RetroAchievements integration settings."""

    enabled: bool = False
    username: str = ""
    api_key: str = ""
    # Score bonus for RA-supported ROMs (very high to prioritize them)
    ra_score_bonus: int = 1000
    # Whether to automatically verify hashes during scan
    verify_on_scan: bool = True


@dataclass
class ScreenScraperConfig:
    """ScreenScraper integration settings for game metadata and media."""

    enabled: bool = False
    username: str = ""
    password: str = ""
    # Developer credentials (optional, for higher rate limits)
    dev_id: str = ""
    dev_password: str = ""
    # Preferred media region (us, eu, jp, wor)
    preferred_region: str = "us"
    # Preferred language (en, fr, de, es, pt, it, ja)
    preferred_language: str = "en"
    # Media download settings
    download_box_art: bool = True
    download_screenshot: bool = True
    download_wheel: bool = True
    download_video: bool = False
    # Media save path (relative to ROM directory or absolute)
    media_path: str = ""


@dataclass
class RemoteHost:
    """Configuration for a remote DUPer host."""

    host: str
    port: int = 8420
    api_key: str = ""
    name: str = ""


@dataclass
class DeviceConfig:
    """Configuration for a managed device (PC, Steam Deck, etc.)."""

    device_id: str = ""
    name: str = ""
    device_type: str = "pc"  # pc, steamdeck, handheld
    host: str = ""  # IP or hostname
    network: str = ""  # e.g., "192.168.13.0/24"
    emulation_path: str = ""  # e.g., /home/deck/Emulation or ~/Emulation
    storage_type: str = "network"  # network (symlinked to NAS) or local
    nas_mount_path: str = ""  # SMB mount point on this device
    nas_share: str = ""  # e.g., //10.99.11.8/retronas
    offline_games: list = field(default_factory=list)  # game IDs available offline
    last_sync_time: str = ""
    last_seen: str = ""


@dataclass
class DuperConfig:
    """Main configuration container for DUPer."""

    server: ServerConfig = field(default_factory=ServerConfig)
    scanner: ScannerConfig = field(default_factory=ScannerConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    retroachievements: RetroAchievementsConfig = field(default_factory=RetroAchievementsConfig)
    screenscraper: ScreenScraperConfig = field(default_factory=ScreenScraperConfig)
    remotes: dict[str, RemoteHost] = field(default_factory=dict)

    # Runtime state (not saved to config)
    config_file: str = ""
    last_scan_directory: str = ""

    @classmethod
    def load(cls, config_path: str | Path | None = None) -> "DuperConfig":
        """Load configuration from TOML file."""
        if config_path is None:
            config_path = get_default_config_dir() / "config.toml"
        else:
            config_path = Path(config_path)

        config = cls()
        config.config_file = str(config_path)

        if config_path.exists():
            try:
                data = toml.load(config_path)
                config._load_from_dict(data)
            except Exception:
                # If config is corrupted, use defaults
                pass

        return config

    def _load_from_dict(self, data: dict[str, Any]) -> None:
        """Load configuration from dictionary."""
        if "server" in data:
            server_data = data["server"]
            self.server = ServerConfig(
                port=server_data.get("port", self.server.port),
                host=server_data.get("host", self.server.host),
                web_ui_enabled=server_data.get("web_ui_enabled", self.server.web_ui_enabled),
                auth_enabled=server_data.get("auth_enabled", self.server.auth_enabled),
                api_key=server_data.get("api_key", self.server.api_key),
            )

        if "scanner" in data:
            scanner_data = data["scanner"]
            self.scanner = ScannerConfig(
                ignore_fodder=scanner_data.get("ignore_fodder", self.scanner.ignore_fodder),
                ignore_video=scanner_data.get("ignore_video", self.scanner.ignore_video),
                ignore_music=scanner_data.get("ignore_music", self.scanner.ignore_music),
                ignore_pictures=scanner_data.get("ignore_pictures", self.scanner.ignore_pictures),
                retroarch_mode=scanner_data.get("retroarch_mode", self.scanner.retroarch_mode),
            )

        if "paths" in data:
            paths_data = data["paths"]
            self.paths = PathsConfig(
                working_dir=paths_data.get("working_dir", self.paths.working_dir),
                database=paths_data.get("database", self.paths.database),
                duplicates_dir=paths_data.get("duplicates_dir", self.paths.duplicates_dir),
            )

        if "retroachievements" in data:
            ra_data = data["retroachievements"]
            self.retroachievements = RetroAchievementsConfig(
                enabled=ra_data.get("enabled", self.retroachievements.enabled),
                username=ra_data.get("username", self.retroachievements.username),
                api_key=ra_data.get("api_key", self.retroachievements.api_key),
                ra_score_bonus=ra_data.get("ra_score_bonus", self.retroachievements.ra_score_bonus),
                verify_on_scan=ra_data.get("verify_on_scan", self.retroachievements.verify_on_scan),
            )

        if "screenscraper" in data:
            ss_data = data["screenscraper"]
            self.screenscraper = ScreenScraperConfig(
                enabled=ss_data.get("enabled", self.screenscraper.enabled),
                username=ss_data.get("username", self.screenscraper.username),
                password=ss_data.get("password", self.screenscraper.password),
                dev_id=ss_data.get("dev_id", self.screenscraper.dev_id),
                dev_password=ss_data.get("dev_password", self.screenscraper.dev_password),
                preferred_region=ss_data.get("preferred_region", self.screenscraper.preferred_region),
                preferred_language=ss_data.get("preferred_language", self.screenscraper.preferred_language),
                download_box_art=ss_data.get("download_box_art", self.screenscraper.download_box_art),
                download_screenshot=ss_data.get("download_screenshot", self.screenscraper.download_screenshot),
                download_wheel=ss_data.get("download_wheel", self.screenscraper.download_wheel),
                download_video=ss_data.get("download_video", self.screenscraper.download_video),
                media_path=ss_data.get("media_path", self.screenscraper.media_path),
            )

        if "remotes" in data:
            for name, remote_data in data["remotes"].items():
                self.remotes[name] = RemoteHost(
                    host=remote_data.get("host", ""),
                    port=remote_data.get("port", 8420),
                    api_key=remote_data.get("api_key", ""),
                    name=name,
                )

        if "last_scan_directory" in data:
            self.last_scan_directory = data["last_scan_directory"]

    def save(self, config_path: str | Path | None = None) -> None:
        """Save configuration to TOML file."""
        if config_path is None:
            config_path = Path(self.config_file) if self.config_file else (
                get_default_config_dir() / "config.toml"
            )
        else:
            config_path = Path(config_path)

        # Ensure directory exists
        config_path.parent.mkdir(parents=True, exist_ok=True)

        data = self._to_dict()
        with open(config_path, "w") as f:
            toml.dump(data, f)

        self.config_file = str(config_path)

    def _to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        data: dict[str, Any] = {
            "server": {
                "port": self.server.port,
                "host": self.server.host,
                "web_ui_enabled": self.server.web_ui_enabled,
                "auth_enabled": self.server.auth_enabled,
                "api_key": self.server.api_key,
            },
            "scanner": {
                "ignore_fodder": self.scanner.ignore_fodder,
                "ignore_video": self.scanner.ignore_video,
                "ignore_music": self.scanner.ignore_music,
                "ignore_pictures": self.scanner.ignore_pictures,
                "retroarch_mode": self.scanner.retroarch_mode,
            },
            "paths": {
                "working_dir": self.paths.working_dir,
                "database": self.paths.database,
                "duplicates_dir": self.paths.duplicates_dir,
            },
            "retroachievements": {
                "enabled": self.retroachievements.enabled,
                "username": self.retroachievements.username,
                "api_key": self.retroachievements.api_key,
                "ra_score_bonus": self.retroachievements.ra_score_bonus,
                "verify_on_scan": self.retroachievements.verify_on_scan,
            },
            "screenscraper": {
                "enabled": self.screenscraper.enabled,
                "username": self.screenscraper.username,
                "password": self.screenscraper.password,
                "dev_id": self.screenscraper.dev_id,
                "dev_password": self.screenscraper.dev_password,
                "preferred_region": self.screenscraper.preferred_region,
                "preferred_language": self.screenscraper.preferred_language,
                "download_box_art": self.screenscraper.download_box_art,
                "download_screenshot": self.screenscraper.download_screenshot,
                "download_wheel": self.screenscraper.download_wheel,
                "download_video": self.screenscraper.download_video,
                "media_path": self.screenscraper.media_path,
            },
        }

        if self.remotes:
            data["remotes"] = {}
            for name, remote in self.remotes.items():
                data["remotes"][name] = {
                    "host": remote.host,
                    "port": remote.port,
                    "api_key": remote.api_key,
                }

        if self.last_scan_directory:
            data["last_scan_directory"] = self.last_scan_directory

        return data

    def add_remote(self, name: str, host: str, port: int = 8420, api_key: str = "") -> None:
        """Add a remote host configuration."""
        self.remotes[name] = RemoteHost(host=host, port=port, api_key=api_key, name=name)

    def remove_remote(self, name: str) -> bool:
        """Remove a remote host configuration."""
        if name in self.remotes:
            del self.remotes[name]
            return True
        return False

    def get_remote(self, name: str) -> RemoteHost | None:
        """Get a remote host configuration by name."""
        return self.remotes.get(name)

    def ensure_directories(self) -> None:
        """Ensure all configured directories exist."""
        Path(self.paths.working_dir).mkdir(parents=True, exist_ok=True)
        Path(self.paths.database).parent.mkdir(parents=True, exist_ok=True)
        Path(self.paths.duplicates_dir).mkdir(parents=True, exist_ok=True)


# Global config instance (can be overridden)
_config: DuperConfig | None = None


def get_config() -> DuperConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = DuperConfig.load()
    return _config


def set_config(config: DuperConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config


def reset_config() -> None:
    """Reset the global configuration instance."""
    global _config
    _config = None
