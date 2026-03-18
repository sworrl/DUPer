"""RetroAchievements API client for ROM verification.

This module provides integration with the RetroAchievements.org API to:
- Verify ROM hashes against the RA database
- Get game information for supported ROMs
- Prioritize RA-supported ROMs in duplicate detection

API Documentation: https://api-docs.retroachievements.org/
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

# RA API base URLs
RA_API_BASE = "https://retroachievements.org/API/"
RA_CONNECT_URL = "https://retroachievements.org/dorequest.php"

# Cache TTL in seconds (24 hours)
CACHE_TTL = 86400


@dataclass
class RAGameInfo:
    """Information about a game from RetroAchievements."""

    game_id: int
    title: str
    console_id: int
    console_name: str
    image_icon: str = ""
    image_title: str = ""
    image_ingame: str = ""
    image_boxart: str = ""
    publisher: str = ""
    developer: str = ""
    genre: str = ""
    release_date: str = ""
    achievement_count: int = 0
    points_total: int = 0
    players_total: int = 0
    hash_match: str = ""  # The hash that matched

    @classmethod
    def from_api_response(cls, data: dict, hash_match: str = "") -> "RAGameInfo":
        """Create from RA API response."""
        return cls(
            game_id=data.get("ID", data.get("GameID", 0)),
            title=data.get("Title", data.get("GameTitle", "")),
            console_id=data.get("ConsoleID", 0),
            console_name=data.get("ConsoleName", ""),
            image_icon=data.get("ImageIcon", ""),
            image_title=data.get("ImageTitle", ""),
            image_ingame=data.get("ImageIngame", ""),
            image_boxart=data.get("ImageBoxArt", ""),
            publisher=data.get("Publisher", ""),
            developer=data.get("Developer", ""),
            genre=data.get("Genre", ""),
            release_date=data.get("Released", ""),
            achievement_count=data.get("NumAchievements", data.get("NumDistinctPlayers", 0)),
            points_total=data.get("Points", data.get("TotalPoints", 0)),
            hash_match=hash_match,
        )

    def to_dict(self) -> dict:
        return {
            "game_id": self.game_id,
            "title": self.title,
            "console_id": self.console_id,
            "console_name": self.console_name,
            "image_icon": self.image_icon,
            "image_boxart": self.image_boxart,
            "publisher": self.publisher,
            "developer": self.developer,
            "genre": self.genre,
            "release_date": self.release_date,
            "achievement_count": self.achievement_count,
            "points_total": self.points_total,
            "hash_match": self.hash_match,
        }


@dataclass
class RAVerificationResult:
    """Result of verifying a ROM against RetroAchievements."""

    filepath: str
    md5: str
    ra_supported: bool
    game_info: RAGameInfo | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "filepath": self.filepath,
            "md5": self.md5,
            "ra_supported": self.ra_supported,
            "game_info": self.game_info.to_dict() if self.game_info else None,
            "error": self.error,
        }


@dataclass
class RAHashCache:
    """Cache for RA hash lookups."""

    cache: dict[str, tuple[float, RAGameInfo | None]] = field(default_factory=dict)

    def get(self, md5: str) -> RAGameInfo | None | bool:
        """Get cached result. Returns False if not in cache."""
        if md5 in self.cache:
            timestamp, result = self.cache[md5]
            if time.time() - timestamp < CACHE_TTL:
                return result
            del self.cache[md5]
        return False

    def set(self, md5: str, result: RAGameInfo | None) -> None:
        """Cache a result."""
        self.cache[md5] = (time.time(), result)

    def clear(self) -> None:
        """Clear the cache."""
        self.cache.clear()


class RetroAchievementsClient:
    """Client for RetroAchievements API.

    Requires RA credentials (username and API key) from:
    https://retroachievements.org/controlpanel.php
    """

    def __init__(self, username: str, api_key: str):
        """Initialize the RA client.

        Args:
            username: RetroAchievements username
            api_key: Web API key from RA control panel
        """
        self.username = username
        self.api_key = api_key
        self.cache = RAHashCache()
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def _make_request(self, endpoint: str, params: dict | None = None) -> dict | list | None:
        """Make an authenticated request to the RA API."""
        if not self.username or not self.api_key:
            return None

        url = f"{RA_API_BASE}{endpoint}"
        request_params = {
            "z": self.username,
            "y": self.api_key,
        }
        if params:
            request_params.update(params)

        try:
            response = self.client.get(url, params=request_params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            print(f"RA API error: {e}")
            return None
        except Exception as e:
            print(f"RA API unexpected error: {e}")
            return None

    def get_game_by_hash(self, md5: str) -> RAGameInfo | None:
        """Look up a game by its ROM hash (MD5).

        Args:
            md5: MD5 hash of the ROM file

        Returns:
            RAGameInfo if the hash is in the RA database, None otherwise
        """
        # Check cache first
        cached = self.cache.get(md5)
        if cached is not False:
            # Defensive check: ensure we return RAGameInfo or None, never a bool
            if isinstance(cached, RAGameInfo):
                return cached
            return None

        # Step 1: Resolve hash to game ID using Connect API (no auth required)
        try:
            response = self.client.get(
                RA_CONNECT_URL,
                params={"r": "gameid", "m": md5.lower()},
                headers={"User-Agent": "DUPer/0.4.0"}
            )
            response.raise_for_status()
            data = response.json()
            # Ensure data is a dict before accessing attributes
            if not isinstance(data, dict):
                self.cache.set(md5, None)
                return None
            game_id = data.get("GameID", 0)
        except Exception as e:
            print(f"RA hash lookup error: {e}")
            self.cache.set(md5, None)
            return None

        if not game_id or game_id == 0:
            self.cache.set(md5, None)
            return None

        # Step 2: Get full game info using the Web API
        game_data = self._make_request("API_GetGame.php", {"i": str(game_id)})

        if not game_data or not isinstance(game_data, dict):
            # At minimum, we know it's RA-supported even without full info
            game_info = RAGameInfo(
                game_id=game_id,
                title=f"Game #{game_id}",
                console_id=0,
                console_name="",
                hash_match=md5,
            )
            self.cache.set(md5, game_info)
            return game_info

        # API_GetGame doesn't include ID, so add it from our lookup
        game_data["ID"] = game_id
        game_info = RAGameInfo.from_api_response(game_data, hash_match=md5)
        self.cache.set(md5, game_info)
        return game_info

    def get_game_hashes(self, game_id: int) -> list[str]:
        """Get all valid hashes for a game.

        Args:
            game_id: RA game ID

        Returns:
            List of MD5 hashes that are valid for this game
        """
        data = self._make_request("API_GetGameHashes.php", {"i": str(game_id)})

        if not data or not isinstance(data, dict):
            return []

        results = data.get("Results", [])
        return [r.get("MD5", "") for r in results if r.get("MD5")]

    def verify_hash(self, md5: str) -> bool:
        """Check if a hash is supported by RetroAchievements.

        Args:
            md5: MD5 hash to verify

        Returns:
            True if the hash is in the RA database
        """
        return self.get_game_by_hash(md5) is not None

    def verify_rom_file(self, filepath: str | Path) -> RAVerificationResult:
        """Verify a ROM file against RetroAchievements.

        Calculates the MD5 hash and checks against RA database.

        Args:
            filepath: Path to the ROM file

        Returns:
            RAVerificationResult with verification status and game info
        """
        filepath = Path(filepath)

        if not filepath.exists():
            return RAVerificationResult(
                filepath=str(filepath),
                md5="",
                ra_supported=False,
                error="File not found",
            )

        # Calculate MD5
        try:
            md5 = hashlib.md5(filepath.read_bytes()).hexdigest()
        except OSError as e:
            return RAVerificationResult(
                filepath=str(filepath),
                md5="",
                ra_supported=False,
                error=f"Error reading file: {e}",
            )

        # Check RA database
        game_info = self.get_game_by_hash(md5)

        return RAVerificationResult(
            filepath=str(filepath),
            md5=md5,
            ra_supported=game_info is not None,
            game_info=game_info,
        )

    def verify_hashes_batch(self, md5_list: list[str]) -> dict[str, RAGameInfo | None]:
        """Verify multiple hashes in batch.

        Args:
            md5_list: List of MD5 hashes to verify

        Returns:
            Dict mapping MD5 -> RAGameInfo (or None if not supported)
        """
        results = {}
        for md5 in md5_list:
            results[md5] = self.get_game_by_hash(md5)
            # Small delay to avoid rate limiting
            time.sleep(0.1)
        return results

    def get_console_ids(self) -> dict[int, str]:
        """Get mapping of console IDs to names.

        Returns:
            Dict mapping console ID -> console name
        """
        data = self._make_request("API_GetConsoleIDs.php")

        if not data or not isinstance(data, list):
            return {}

        return {item.get("ID", 0): item.get("Name", "") for item in data}


# Singleton instance for reuse
_ra_client: RetroAchievementsClient | None = None


def get_ra_client(username: str = "", api_key: str = "") -> RetroAchievementsClient | None:
    """Get or create the RA client singleton.

    Args:
        username: RA username (required on first call)
        api_key: RA API key (required on first call)

    Returns:
        RetroAchievementsClient instance or None if not configured
    """
    global _ra_client

    if _ra_client is None and username and api_key:
        _ra_client = RetroAchievementsClient(username, api_key)
    elif username and api_key and _ra_client:
        # Update credentials if provided
        _ra_client.username = username
        _ra_client.api_key = api_key

    return _ra_client


def set_ra_client(client: RetroAchievementsClient | None) -> None:
    """Set the RA client singleton."""
    global _ra_client
    _ra_client = client
