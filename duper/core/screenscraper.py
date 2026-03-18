"""ScreenScraper API client for game metadata and media scraping.

This module provides integration with the ScreenScraper.fr API to:
- Look up game information by ROM hash (MD5, SHA1, CRC)
- Download box art, screenshots, videos, and other media
- Get system/console information

API Documentation: https://www.screenscraper.fr/webapi2.php
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx

# ScreenScraper API base URL
SS_API_BASE = "https://www.screenscraper.fr/api2/"

# Software identification (required for API)
SS_SOFT_NAME = "DUPer"
SS_SOFT_VERSION = "0.4.0"

# Default rate limit (used before tier detection)
DEFAULT_RATE_LIMIT_DELAY = 1.0

# Tier-based rate limiting (delay in seconds between requests)
# ScreenScraper tiers: higher level = faster allowed scraping
TIER_RATE_LIMITS = {
    0: 1.0,     # Free / unregistered
    1: 0.8,     # Registered
    2: 0.5,     # Member
    3: 0.3,     # Active contributor
    4: 0.2,     # Paid supporter
    5: 0.1,     # High-tier supporter
}

# How often to refresh tier info (seconds)
TIER_CACHE_TTL = 3600

# Cache TTL in seconds (24 hours)
CACHE_TTL = 86400

# Media types available from ScreenScraper
MEDIA_TYPES = {
    "box-2D": "2D Box Art",
    "box-3D": "3D Box Art",
    "box-texture": "Box Texture",
    "support-2D": "Media/Cart",
    "screenmarquee": "Marquee",
    "ss": "Screenshot",
    "sstitle": "Title Screen",
    "fanart": "Fan Art",
    "video": "Video",
    "video-normalized": "Normalized Video",
    "manuel": "Manual",
    "maps": "Maps",
    "wheel": "Wheel Logo",
    "wheel-hd": "HD Wheel Logo",
    "wheel-carbon": "Carbon Wheel",
    "wheel-steel": "Steel Wheel",
}

# Supported regions (priority order for media)
REGIONS = ["us", "eu", "jp", "wor", "ss", "uk", "fr", "de"]

# Supported languages
LANGUAGES = ["en", "fr", "de", "es", "pt", "it", "ja"]


@dataclass
class SSGameMedia:
    """Media URLs for a game from ScreenScraper."""

    box_2d: str = ""
    box_3d: str = ""
    screenshot: str = ""
    title_screen: str = ""
    wheel: str = ""
    fanart: str = ""
    video: str = ""
    manual: str = ""

    def to_dict(self) -> dict:
        return {
            "box_2d": self.box_2d,
            "box_3d": self.box_3d,
            "screenshot": self.screenshot,
            "title_screen": self.title_screen,
            "wheel": self.wheel,
            "fanart": self.fanart,
            "video": self.video,
            "manual": self.manual,
        }


@dataclass
class SSGameInfo:
    """Information about a game from ScreenScraper."""

    game_id: int
    rom_id: int = 0
    title: str = ""
    region: str = ""
    system_id: int = 0
    system_name: str = ""
    publisher: str = ""
    developer: str = ""
    genre: str = ""
    players: str = ""
    rating: float = 0.0
    release_date: str = ""
    description: str = ""
    media: SSGameMedia = field(default_factory=SSGameMedia)
    hash_match: str = ""

    @classmethod
    def from_api_response(cls, data: dict, hash_match: str = "") -> "SSGameInfo":
        """Create from ScreenScraper API response."""
        jeu = data.get("jeu", data)

        # Get localized name (prefer English, then first available)
        names = jeu.get("noms", [])
        title = ""
        for name in names:
            if name.get("region") == "us" or name.get("region") == "wor":
                title = name.get("text", "")
                break
        if not title and names:
            title = names[0].get("text", "")

        # Get system info
        system = jeu.get("systeme", {})

        # Get publisher/developer
        publisher = ""
        developer = ""
        editeur = jeu.get("editeur", {})
        if editeur:
            publisher = editeur.get("text", "")
        developpeur = jeu.get("developpeur", {})
        if developpeur:
            developer = developpeur.get("text", "")

        # Get genre (first one)
        genres = jeu.get("genres", [])
        genre = ""
        if genres:
            genre_names = genres[0].get("noms", [])
            for gn in genre_names:
                if gn.get("langue") == "en":
                    genre = gn.get("text", "")
                    break
            if not genre and genre_names:
                genre = genre_names[0].get("text", "")

        # Get description (prefer English)
        synopsis = jeu.get("synopsis", [])
        description = ""
        for syn in synopsis:
            if syn.get("langue") == "en":
                description = syn.get("text", "")
                break
        if not description and synopsis:
            description = synopsis[0].get("text", "")

        # Get release date
        dates = jeu.get("dates", [])
        release_date = ""
        for date in dates:
            if date.get("region") in ["us", "wor", "eu"]:
                release_date = date.get("text", "")
                break
        if not release_date and dates:
            release_date = dates[0].get("text", "")

        # Get media URLs
        medias = jeu.get("medias", [])
        media = cls._extract_media(medias)

        # Get rating
        note = jeu.get("note", {})
        rating = 0.0
        if note:
            try:
                rating = float(note.get("text", 0)) / 20.0 * 5  # Convert to 5-star scale
            except (ValueError, TypeError):
                pass

        return cls(
            game_id=int(jeu.get("id", 0)),
            rom_id=int(jeu.get("romid", 0)),
            title=title,
            region=jeu.get("region", ""),
            system_id=int(system.get("id", 0)),
            system_name=system.get("text", ""),
            publisher=publisher,
            developer=developer,
            genre=genre,
            players=jeu.get("joueurs", {}).get("text", ""),
            rating=rating,
            release_date=release_date,
            description=description[:500] if description else "",  # Truncate long descriptions
            media=media,
            hash_match=hash_match,
        )

    @staticmethod
    def _extract_media(medias: list) -> SSGameMedia:
        """Extract media URLs from the medias list."""
        media = SSGameMedia()

        # Priority: us > eu > wor > ss > first available
        def get_media_url(media_type: str) -> str:
            for region in REGIONS:
                for m in medias:
                    if m.get("type") == media_type and m.get("region") == region:
                        return m.get("url", "")
            # Fall back to any region
            for m in medias:
                if m.get("type") == media_type:
                    return m.get("url", "")
            return ""

        media.box_2d = get_media_url("box-2D")
        media.box_3d = get_media_url("box-3D")
        media.screenshot = get_media_url("ss")
        media.title_screen = get_media_url("sstitle")
        media.wheel = get_media_url("wheel") or get_media_url("wheel-hd")
        media.fanart = get_media_url("fanart")
        media.video = get_media_url("video") or get_media_url("video-normalized")
        media.manual = get_media_url("manuel")

        return media

    def to_dict(self) -> dict:
        return {
            "game_id": self.game_id,
            "rom_id": self.rom_id,
            "title": self.title,
            "region": self.region,
            "system_id": self.system_id,
            "system_name": self.system_name,
            "publisher": self.publisher,
            "developer": self.developer,
            "genre": self.genre,
            "players": self.players,
            "rating": self.rating,
            "release_date": self.release_date,
            "description": self.description,
            "media": self.media.to_dict(),
            "hash_match": self.hash_match,
        }


@dataclass
class SSScrapeResult:
    """Result of scraping a ROM via ScreenScraper."""

    filepath: str
    md5: str
    found: bool
    game_info: SSGameInfo | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "filepath": self.filepath,
            "md5": self.md5,
            "found": self.found,
            "game_info": self.game_info.to_dict() if self.game_info else None,
            "error": self.error,
        }


class ScreenScraperClient:
    """Client for the ScreenScraper API."""

    def __init__(
        self,
        dev_id: str = "",
        dev_password: str = "",
        user_id: str = "",
        user_password: str = "",
    ):
        """
        Initialize the ScreenScraper client.

        Args:
            dev_id: Developer ID (contact ScreenScraper for credentials)
            dev_password: Developer password
            user_id: User's ScreenScraper username
            user_password: User's ScreenScraper password
        """
        self.dev_id = dev_id
        self.dev_password = dev_password
        self.user_id = user_id
        self.user_password = user_password
        self._last_request_time = 0.0
        self._request_count = 0
        self.client = httpx.Client(timeout=30.0)

        # Tier-aware rate limiting
        self._tier_level: int = 0
        self._tier_detected: bool = False
        self._tier_detect_time: float = 0.0
        self._rate_limit_delay: float = DEFAULT_RATE_LIMIT_DELAY
        self._max_threads: int = 1
        self._requests_today: int = 0
        self._requests_max: int = 0

        # Simple in-memory cache
        self._cache: dict[str, tuple[float, Any]] = {}

    def is_configured(self) -> bool:
        """Check if ScreenScraper credentials are configured."""
        return bool(self.user_id and self.user_password)

    def _get_base_params(self) -> dict:
        """Get base parameters for all API requests."""
        params = {
            "output": "json",
            "softname": SS_SOFT_NAME,
        }

        # Add developer credentials if available
        if self.dev_id and self.dev_password:
            params["devid"] = self.dev_id
            params["devpassword"] = self.dev_password

        # Add user credentials
        if self.user_id and self.user_password:
            params["ssid"] = self.user_id
            params["sspassword"] = self.user_password

        return params

    def _detect_tier(self) -> None:
        """Detect user's API tier and set rate limits accordingly."""
        if self._tier_detected and (time.time() - self._tier_detect_time) < TIER_CACHE_TTL:
            return

        if not self.is_configured():
            return

        try:
            # Direct request to avoid recursion through _make_request
            url = f"{SS_API_BASE}ssuserInfos.php"
            params = {**self._get_base_params()}
            response = self.client.get(url, params=params, timeout=10.0)

            if response.status_code == 200:
                data = response.json()
                if data and "response" in data:
                    user_info = data["response"].get("ssuser", {})
                    try:
                        self._tier_level = int(user_info.get("niveau", 0))
                    except (ValueError, TypeError):
                        self._tier_level = 0
                    self._max_threads = int(user_info.get("maxthreads", 1) or 1)
                    self._requests_today = int(user_info.get("requeststoday", 0) or 0)
                    self._requests_max = int(user_info.get("maxrequestsperday", 0) or 0)

                    # Set rate limit based on tier (use highest matching tier)
                    for tier_level in sorted(TIER_RATE_LIMITS.keys(), reverse=True):
                        if self._tier_level >= tier_level:
                            self._rate_limit_delay = TIER_RATE_LIMITS[tier_level]
                            break

                    self._tier_detected = True
                    self._tier_detect_time = time.time()
                    print(
                        f"ScreenScraper: Tier {self._tier_level} detected — "
                        f"{self._rate_limit_delay}s delay, {self._max_threads} threads, "
                        f"{self._requests_today}/{self._requests_max} requests today"
                    )
        except Exception:
            # Fall back to conservative defaults
            self._rate_limit_delay = DEFAULT_RATE_LIMIT_DELAY

    def _rate_limit(self) -> None:
        """Apply tier-aware rate limiting between requests."""
        if not self._tier_detected:
            self._detect_tier()

        elapsed = time.time() - self._last_request_time
        if elapsed < self._rate_limit_delay:
            time.sleep(self._rate_limit_delay - elapsed)
        self._last_request_time = time.time()
        self._request_count += 1

    def _make_request(self, endpoint: str, params: dict) -> dict | None:
        """Make a request to the ScreenScraper API."""
        self._rate_limit()

        url = f"{SS_API_BASE}{endpoint}"
        all_params = {**self._get_base_params(), **params}

        try:
            response = self.client.get(url, params=all_params)

            if response.status_code == 200:
                data = response.json()
                # Check for API errors in response
                if "error" in data:
                    return None
                return data
            elif response.status_code == 430:
                # Too many requests - quota exceeded
                print("ScreenScraper: Daily quota exceeded")
                return None
            elif response.status_code == 431:
                # Game not found
                return None
            else:
                print(f"ScreenScraper API error: {response.status_code}")
                return None

        except httpx.TimeoutException:
            print("ScreenScraper: Request timeout")
            return None
        except Exception as e:
            print(f"ScreenScraper error: {e}")
            return None

    def get_game_by_hash(
        self,
        md5: str = "",
        sha1: str = "",
        crc: str = "",
        rom_name: str = "",
        rom_size: int = 0,
        system_id: int = 0,
    ) -> SSGameInfo | None:
        """
        Look up a game by ROM hash.

        Args:
            md5: MD5 hash of the ROM
            sha1: SHA1 hash of the ROM
            crc: CRC32 hash of the ROM
            rom_name: ROM filename (fallback search)
            rom_size: ROM file size in bytes
            system_id: ScreenScraper system ID (optional, improves accuracy)

        Returns:
            SSGameInfo if found, None otherwise
        """
        # Check cache first
        cache_key = f"game:{md5 or sha1 or crc}"
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if time.time() - cached_time < CACHE_TTL:
                return cached_data

        params = {}

        if md5:
            params["md5"] = md5.lower()
        if sha1:
            params["sha1"] = sha1.lower()
        if crc:
            params["crc"] = crc.lower()
        if rom_name:
            params["romnom"] = rom_name
        if rom_size:
            params["romtaille"] = str(rom_size)
        if system_id:
            params["systemeid"] = str(system_id)

        if not params:
            return None

        data = self._make_request("jeuInfos.php", params)
        if not data or "response" not in data:
            self._cache[cache_key] = (time.time(), None)
            return None

        response = data["response"]
        if "jeu" not in response:
            self._cache[cache_key] = (time.time(), None)
            return None

        game_info = SSGameInfo.from_api_response(response, hash_match=md5 or sha1 or crc)
        self._cache[cache_key] = (time.time(), game_info)
        return game_info

    def scrape_rom(self, filepath: str | Path, md5: str = "") -> SSScrapeResult:
        """
        Scrape game info for a ROM file.

        Args:
            filepath: Path to the ROM file
            md5: Pre-calculated MD5 hash (optional)

        Returns:
            SSScrapeResult with game info if found
        """
        filepath = Path(filepath)

        if not filepath.exists():
            return SSScrapeResult(
                filepath=str(filepath),
                md5="",
                found=False,
                error="File not found",
            )

        # Calculate MD5 if not provided
        if not md5:
            try:
                with open(filepath, "rb") as f:
                    md5 = hashlib.md5(f.read()).hexdigest()
            except Exception as e:
                return SSScrapeResult(
                    filepath=str(filepath),
                    md5="",
                    found=False,
                    error=str(e),
                )

        # Get file size for better matching
        rom_size = filepath.stat().st_size
        rom_name = filepath.name

        # Look up by hash
        game_info = self.get_game_by_hash(
            md5=md5,
            rom_name=rom_name,
            rom_size=rom_size,
        )

        if game_info:
            return SSScrapeResult(
                filepath=str(filepath),
                md5=md5,
                found=True,
                game_info=game_info,
            )
        else:
            return SSScrapeResult(
                filepath=str(filepath),
                md5=md5,
                found=False,
            )

    def download_media(
        self,
        url: str,
        save_path: str | Path,
        overwrite: bool = False,
    ) -> bool:
        """
        Download media from ScreenScraper.

        Args:
            url: Media URL from game info
            save_path: Path to save the file
            overwrite: Whether to overwrite existing files

        Returns:
            True if download succeeded
        """
        save_path = Path(save_path)

        if save_path.exists() and not overwrite:
            return True

        try:
            self._rate_limit()
            response = self.client.get(url)

            if response.status_code == 200:
                save_path.parent.mkdir(parents=True, exist_ok=True)
                save_path.write_bytes(response.content)
                return True
            else:
                return False

        except Exception as e:
            print(f"Failed to download media: {e}")
            return False

    def get_systems(self) -> list[dict]:
        """Get list of all supported systems."""
        cache_key = "systems"
        if cache_key in self._cache:
            cached_time, cached_data = self._cache[cache_key]
            if time.time() - cached_time < CACHE_TTL:
                return cached_data

        data = self._make_request("systemesListe.php", {})
        if not data or "response" not in data:
            return []

        systems = data["response"].get("systemes", [])
        result = []

        for sys in systems:
            names = sys.get("noms_commun", [])
            name = names[0].get("text", "") if names else ""

            result.append({
                "id": int(sys.get("id", 0)),
                "name": name,
                "company": sys.get("compagnie", ""),
                "type": sys.get("type", ""),
            })

        self._cache[cache_key] = (time.time(), result)
        return result

    def test_connection(self) -> dict:
        """
        Test the connection to ScreenScraper.

        Returns:
            Dict with connection status and user info
        """
        if not self.is_configured():
            return {
                "success": False,
                "error": "ScreenScraper credentials not configured",
            }

        try:
            # Use a simple request to test credentials
            data = self._make_request("ssuserInfos.php", {})

            if data and "response" in data:
                user_info = data["response"].get("ssuser", {})
                # Trigger tier detection to update rate limits
                self._detect_tier()
                return {
                    "success": True,
                    "username": user_info.get("id", ""),
                    "level": user_info.get("niveau", ""),
                    "requests_today": user_info.get("requeststoday", 0),
                    "requests_max": user_info.get("maxrequestsperday", 0),
                    "threads": user_info.get("maxthreads", 1),
                    "effective_rate_limit": self._rate_limit_delay,
                    "max_threads": self._max_threads,
                }
            else:
                return {
                    "success": False,
                    "error": "Invalid credentials or API error",
                }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }

    def batch_scrape(
        self,
        files: list[tuple[str, str]],
        media_dir: str = "",
        download_art: bool = True,
        callback=None,
    ) -> list[SSScrapeResult]:
        """
        Scrape multiple ROMs concurrently using max allowed threads.

        Args:
            files: List of (filepath, md5) tuples
            media_dir: Base directory for saving media
            download_art: Whether to download box art
            callback: Optional callback(result, index, total) for progress

        Returns:
            List of SSScrapeResult
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not self._tier_detected:
            self._detect_tier()

        max_workers = max(1, self._max_threads)
        results = [None] * len(files)
        total = len(files)

        def _scrape_one(index, filepath, md5):
            self._rate_limit()
            result = self.scrape_rom(filepath, md5=md5)

            # Download box art if found
            if result.found and download_art and media_dir and result.game_info:
                gi = result.game_info
                if gi.media.box_2d:
                    import os
                    system = os.path.basename(os.path.dirname(filepath))
                    save_dir = os.path.join(media_dir, system, "box2dfront")
                    os.makedirs(save_dir, exist_ok=True)
                    name = os.path.splitext(os.path.basename(filepath))[0]
                    save_path = os.path.join(save_dir, f"{name}.png")
                    self.download_media(gi.media.box_2d, save_path)

                if gi.media.screenshot:
                    import os
                    system = os.path.basename(os.path.dirname(filepath))
                    save_dir = os.path.join(media_dir, system, "screenshots")
                    os.makedirs(save_dir, exist_ok=True)
                    name = os.path.splitext(os.path.basename(filepath))[0]
                    save_path = os.path.join(save_dir, f"{name}.png")
                    self.download_media(gi.media.screenshot, save_path)

                if gi.media.wheel:
                    import os
                    system = os.path.basename(os.path.dirname(filepath))
                    save_dir = os.path.join(media_dir, system, "wheel")
                    os.makedirs(save_dir, exist_ok=True)
                    name = os.path.splitext(os.path.basename(filepath))[0]
                    save_path = os.path.join(save_dir, f"{name}.png")
                    self.download_media(gi.media.wheel, save_path)

            return index, result

        print(f"ScreenScraper batch scrape: {total} files, {max_workers} threads, {self._rate_limit_delay}s delay")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, (filepath, md5) in enumerate(files):
                future = executor.submit(_scrape_one, i, filepath, md5)
                futures[future] = i

            completed = 0
            for future in as_completed(futures):
                try:
                    index, result = future.result()
                    results[index] = result
                    completed += 1
                    if callback:
                        callback(result, completed, total)
                    elif completed % 10 == 0:
                        found = sum(1 for r in results if r and r.found)
                        print(f"  [{completed}/{total}] {found} found")
                except Exception as e:
                    print(f"  Scrape error: {e}")

        return [r for r in results if r is not None]

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()


# System ID mapping for common platforms
SYSTEM_IDS = {
    "nes": 3,
    "snes": 4,
    "n64": 14,
    "gb": 9,
    "gbc": 10,
    "gba": 12,
    "nds": 15,
    "3ds": 17,
    "gamecube": 13,
    "wii": 16,
    "genesis": 1,
    "megadrive": 1,
    "sms": 2,
    "gamegear": 21,
    "saturn": 22,
    "dreamcast": 23,
    "psx": 57,
    "ps2": 58,
    "psp": 61,
    "atari2600": 26,
    "atari7800": 41,
    "lynx": 28,
    "jaguar": 27,
    "neogeo": 142,
    "neogeopocket": 25,
    "pcengine": 31,
    "turbografx": 31,
    "msx": 113,
    "colecovision": 48,
    "intellivision": 115,
    "vectrex": 102,
    "wonderswan": 45,
    "wonderswancolor": 46,
    "arcade": 75,
    "mame": 75,
    "fba": 75,
}


def get_system_id(system_name: str) -> int:
    """Get ScreenScraper system ID from common system name."""
    name = system_name.lower().replace("-", "").replace("_", "").replace(" ", "")
    return SYSTEM_IDS.get(name, 0)


# Global client instance
_ss_client: ScreenScraperClient | None = None


def get_ss_client(
    username: str | None = None,
    password: str | None = None,
    dev_id: str = "",
    dev_password: str = "",
) -> ScreenScraperClient | None:
    """
    Get or create the global ScreenScraper client.

    Args:
        username: ScreenScraper username (if creating new client)
        password: ScreenScraper password (if creating new client)
        dev_id: Developer ID for higher rate limits (optional)
        dev_password: Developer password (optional)

    Returns:
        ScreenScraperClient instance or None if no credentials
    """
    global _ss_client

    # If credentials provided, create/update client
    if username and password:
        if _ss_client is None or _ss_client.user_id != username:
            if _ss_client:
                _ss_client.close()
            _ss_client = ScreenScraperClient(
                dev_id=dev_id,
                dev_password=dev_password,
                user_id=username,
                user_password=password,
            )
        return _ss_client

    # Return existing client if available
    return _ss_client


def set_ss_client(client: ScreenScraperClient | None) -> None:
    """Set the global ScreenScraper client."""
    global _ss_client
    if _ss_client and _ss_client is not client:
        _ss_client.close()
    _ss_client = client


def reset_ss_client() -> None:
    """Reset (close) the global ScreenScraper client."""
    global _ss_client
    if _ss_client:
        _ss_client.close()
        _ss_client = None
