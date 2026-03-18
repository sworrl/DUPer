# Changelog

All notable changes to DUPer are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [2.3.5] - 2026-03-18 "RetroNAS"

### Added

#### Core
- **Cross-platform duplicate detection** -- Finds duplicates across different system folders
- **Smart transfer system** -- DB-backed skip logic ensures only new/changed files are transferred
- **Media transfer** -- Smart sync for box art, screenshots, and other media assets
- **ES-DE gamelist generation** -- Builds `gamelist.xml` from the database for `ParseGamelistOnly` mode
- **Custom game collections** -- Create themed collections (franchise, decade, genre, custom)
- **RetroNAS integration** -- Single NAS source of truth for all game data

#### Acquisition
- **Archive.org downloader** -- Automated downloading with 4x parallel connections
- **Sub-collections** -- Download Greatest Hits, Top 50, regional variants, or full sets
- **Acquisition watchdog** -- Systemd service for continuous background downloading

#### Integrations
- **ScreenScraper integration** -- Auto-scrape box art, screenshots, metadata for new games with tier-aware rate limiting
- **RetroAchievements per-game progress** -- View achievement completion, unlocked counts, badge images per game
- **RetroAchievements profile display** -- Dashboard shows RA username, avatar, points, rank
- **XP/Level system** -- Gamified progress tracking based on RA activity
- **Live game capture** -- Capture frames in real-time via RetroArch network command interface

#### Multi-Device
- **Bazzite PC + Steam Deck support** -- Manage collections across desktop and handheld
- **Config sync** -- Export/import RetroArch and ES-DE configurations between devices
- **Device-aware transfers** -- Push ROMs, media, and gamelists to specific devices
- **Deck export/import scripts** -- One-command config migration for Steam Deck

#### Web UI ("Pixel Forge" theme)
- **Dashboard** -- Now Playing, RA profile, collection stats, XP/level system
- **Games page** -- Cover art grid with lazy-loaded images, RA badges, per-game cheevo tracking
- **Acquisition page** -- Animated progress bars, speed gauges, live download feed
- **Game detail modals** -- Full metadata, RA achievement gallery, media carousel
- **Visual effects** -- Mouse-tracking card tilt/glow, ambient animations
- **Responsive layout** -- 4K desktop down to mobile

#### TUI
- **Textual-based terminal dashboard** via `duper-tui` command
- **6 tabs** -- Dashboard, Games, Acquisition, Operations, Collections, Log
- **Live polling** with sparkline graphs, progress bars, animated spinners
- **SSH-friendly** for headless server management

#### API
- **50+ REST endpoints** covering all functionality
- **Game detail with RA progress** endpoint
- **Collection management** endpoints (CRUD)
- **Live capture control** endpoints
- **Media serving** by system/game name
- **ScreenScraper scrape control** endpoints
- **Device management** endpoints

#### Scripts
- `transfer-worker.sh` -- Parallel ROM transfer from RetroNAS with DB tracking
- `media-worker.sh` -- Smart media sync with skip logic
- `acquisition-worker.sh` -- 4x parallel archive.org downloader
- `acquisition-watchdog.sh` -- Continuous acquisition daemon
- `build-game-index.sh` -- ES-DE gamelist generation + device push
- `build-rom-index.sh` -- ROM index for fast lookups
- `sync-media-cache.sh` -- Local media caching for stutter-free menus
- `auto-sync.sh` -- Full auto-sync pipeline
- `xbox-iso-convert.sh` -- Redump to XISO conversion
- `live-capture.sh` -- RetroArch game frame capture
- `deck-export-configs.sh` -- Steam Deck config export
- `deck-import-configs.sh` -- Steam Deck config import

#### Infrastructure
- **Systemd watchdog service** -- `duper-watchdog.service` for acquisition daemon
- **Deploy script** -- Updated to install TUI dependencies, watchdog service, and all new scripts
- **Docker support** -- Dockerfile and docker-compose.yml

### Changed
- Expanded dependency list: added `textual` for TUI, `aiohttp` for async HTTP operations
- Deploy script now creates both `duper.service` and `duper-watchdog.service`
- Deploy script installs all shell scripts to `~/.local/bin/`
- Service template updated with watchdog companion service
- README completely rewritten for v2.3.5 scope

### Supported Systems
- PS1, PS2, Xbox, Xbox 360, Dreamcast, Saturn, GameCube, N64, SNES, NES
- Game Boy, Game Boy Color, Game Boy Advance, Mega Drive/Genesis, Neo Geo, PSP
- 150+ additional systems via RetroArch cores

---

## [1.0.0] - Initial Release

### Added
- MD5 duplicate detection
- RetroAchievements hash verification
- Intelligent scoring system (RA bonus +1000)
- Web dashboard for duplicate management
- REST API for programmatic control
- Media correlation (boxart, screenshots)
- Save game preservation
- Archive/delete options for duplicates
- Space analysis and tracking
- Batch processing
- File restoration
- CLI interface with Typer
- Systemd service template
- PyInstaller build script
