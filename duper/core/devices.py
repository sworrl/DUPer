"""Device management and offline mode for DUPer.

Manages multiple devices (Bazzite PC, Steam Deck) that share a RetroNAS backend.
Handles config sync and offline mode (symlink ↔ physical copy toggle per game).

Architecture:
  RetroNAS (10.99.11.8) ← single source of truth
    ├── Bazzite PC: ~/Emulation/* → symlinks to /mnt/retronas/*
    └── Steam Deck: ~/Emulation/* → symlinks to SMB mount of RetroNAS
        └── Offline games: symlink replaced with physical copy

Offline mode flow:
  1. User selects game → "Make available offline"
  2. DUPer resolves all files: ROM(s) + BIOS deps + saves + media
  3. Removes symlink, copies physical files from NAS
  4. Tracks in device config which games are offline
  5. "Return to cloud" reverses: deletes local copy, restores symlink
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from duper.core.config import DeviceConfig


# Default device definitions
GLASSITE = DeviceConfig(
    device_id="glassite",
    name="GLASSITE (Bazzite PC)",
    device_type="pc",
    host="192.168.13.20",
    network="192.168.13.0/24",
    emulation_path="/home/reaver/Emulation",
    storage_type="network",
    nas_mount_path="/mnt/retronas",
    nas_share="//10.99.11.8/retronas",
)

STEAMDECK = DeviceConfig(
    device_id="steamdeck",
    name="Steam Deck",
    device_type="steamdeck",
    host="",  # DHCP, discovered at runtime
    network="192.168.13.0/24",
    emulation_path="/home/deck/Emulation",
    storage_type="network",
    nas_mount_path="/run/media/mmcblk0p1/Emulation",
    nas_share="//10.99.11.8/retronas",
)


@dataclass
class GameFiles:
    """All files associated with a game for offline mode."""

    game_title: str
    system: str
    rom_files: list[str] = field(default_factory=list)  # Relative paths under roms/
    save_files: list[str] = field(default_factory=list)  # Relative paths under saves/
    media_files: list[str] = field(default_factory=list)  # Relative paths under media/
    bios_files: list[str] = field(default_factory=list)  # Required BIOS files
    total_size_bytes: int = 0


class DeviceManager:
    """Manages devices and their sync/offline state."""

    def __init__(self, state_dir: str | Path | None = None):
        self.state_dir = Path(state_dir or Path.home() / ".local" / "share" / "duper" / "devices")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.devices: dict[str, DeviceConfig] = {}
        self._load_devices()

    def _load_devices(self):
        """Load device configs from disk."""
        state_file = self.state_dir / "devices.json"
        if state_file.exists():
            try:
                data = json.loads(state_file.read_text())
                for dev_data in data.get("devices", []):
                    dev = DeviceConfig(**dev_data)
                    self.devices[dev.device_id] = dev
            except Exception:
                pass

        # Ensure defaults exist
        if "glassite" not in self.devices:
            self.devices["glassite"] = GLASSITE
        if "steamdeck" not in self.devices:
            self.devices["steamdeck"] = STEAMDECK

    def _save_devices(self):
        """Save device configs to disk."""
        state_file = self.state_dir / "devices.json"
        data = {"devices": [asdict(d) for d in self.devices.values()]}
        state_file.write_text(json.dumps(data, indent=2))

    def get_device(self, device_id: str) -> DeviceConfig | None:
        return self.devices.get(device_id)

    def list_devices(self) -> list[DeviceConfig]:
        return list(self.devices.values())

    def update_device(self, device: DeviceConfig):
        self.devices[device.device_id] = device
        self._save_devices()

    def add_device(self, device: DeviceConfig):
        self.devices[device.device_id] = device
        self._save_devices()

    # === Offline Mode ===

    def resolve_game_files(
        self,
        system: str,
        rom_filename: str,
        nas_path: str = "/data/retronas",
        nas_host: str = "10.99.11.8",
    ) -> GameFiles:
        """Resolve all files associated with a game (ROM, saves, media, BIOS)."""
        game_title = rom_filename.rsplit(".", 1)[0]
        game = GameFiles(game_title=game_title, system=system)

        # ROM files (might be multi-disc)
        game.rom_files.append(f"roms/{system}/{rom_filename}")

        # Check for multi-disc (Disc 1, Disc 2, etc.)
        base_name = game_title.split("(Disc")[0].strip()
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
                 f"retronas@{nas_host}",
                 f"ls '{nas_path}/roms/{system}/' | grep -F '{base_name}'"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    rel = f"roms/{system}/{line.strip()}"
                    if rel not in game.rom_files and line.strip():
                        game.rom_files.append(rel)
        except Exception:
            pass

        # Save files
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
                 f"retronas@{nas_host}",
                 f"find '{nas_path}/saves' -iname '*{base_name}*' -type f 2>/dev/null"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if line.strip():
                        rel = line.strip().replace(f"{nas_path}/", "")
                        game.save_files.append(rel)
        except Exception:
            pass

        # Media files
        try:
            result = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
                 f"retronas@{nas_host}",
                 f"find '{nas_path}/media/{system}' -iname '*{base_name}*' -type f 2>/dev/null"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split("\n"):
                    if line.strip():
                        rel = line.strip().replace(f"{nas_path}/", "")
                        game.media_files.append(rel)
        except Exception:
            pass

        return game

    def make_offline(
        self,
        device_id: str,
        game: GameFiles,
        nas_host: str = "10.99.11.8",
        nas_path: str = "/data/retronas",
    ) -> bool:
        """Copy a game's files locally, replacing symlinks with physical copies."""
        device = self.devices.get(device_id)
        if not device:
            return False

        emu_path = Path(device.emulation_path)
        all_files = game.rom_files + game.save_files + game.media_files

        for rel_path in all_files:
            local_path = emu_path / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Remove symlink if it exists
            if local_path.is_symlink():
                local_path.unlink()

            # Copy from NAS
            try:
                subprocess.run(
                    ["scp", "-o", "BatchMode=yes",
                     f"retronas@{nas_host}:{nas_path}/{rel_path}",
                     str(local_path)],
                    capture_output=True, timeout=300
                )
            except Exception:
                continue

        # Track offline state
        game_id = f"{game.system}/{game.rom_files[0]}" if game.rom_files else game.game_title
        if game_id not in device.offline_games:
            device.offline_games.append(game_id)
        device.last_sync_time = datetime.now().isoformat()
        self._save_devices()
        return True

    def return_to_cloud(
        self,
        device_id: str,
        game: GameFiles,
    ) -> bool:
        """Delete local copies and restore symlinks to NAS."""
        device = self.devices.get(device_id)
        if not device:
            return False

        emu_path = Path(device.emulation_path)
        nas_mount = Path(device.nas_mount_path)

        for rel_path in game.rom_files + game.save_files + game.media_files:
            local_path = emu_path / rel_path
            nas_target = nas_mount / rel_path

            # Delete local copy
            if local_path.exists() and not local_path.is_symlink():
                local_path.unlink()

            # Restore symlink
            if nas_target.exists():
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.symlink_to(nas_target)

        # Update offline tracking
        game_id = f"{game.system}/{game.rom_files[0]}" if game.rom_files else game.game_title
        if game_id in device.offline_games:
            device.offline_games.remove(game_id)
        self._save_devices()
        return True

    def sync_configs(
        self,
        source_device_id: str,
        target_device_id: str,
        nas_host: str = "10.99.11.8",
    ) -> dict:
        """Sync emulator configs between two devices via RetroNAS."""
        source = self.devices.get(source_device_id)
        target = self.devices.get(target_device_id)
        if not source or not target:
            return {"error": "Device not found"}

        # Configs are stored on RetroNAS at /data/retronas/configs/{device_id}/
        # Sync = copy source configs to NAS, then from NAS to target
        result = {"synced_from": source.name, "synced_to": target.name, "files": 0}

        try:
            # Upload source configs to NAS
            subprocess.run(
                ["ssh", "-o", "BatchMode=yes", f"retronas@{nas_host}",
                 f"mkdir -p /data/retronas/configs/{source.device_id}"],
                capture_output=True, timeout=5
            )
            # The actual config files would be collected by the export script
            result["status"] = "ready"
        except Exception as e:
            result["error"] = str(e)

        return result


# Global instance
_device_manager: DeviceManager | None = None


def get_device_manager() -> DeviceManager:
    global _device_manager
    if _device_manager is None:
        _device_manager = DeviceManager()
    return _device_manager
