"""System-related API routes."""

from __future__ import annotations

import xml.etree.ElementTree as _ET
from datetime import datetime as _datetime
from pathlib import Path as _PathImport

import httpx as _httpx

from fastapi import APIRouter, Depends

from duper import __codename__, __version__
from duper.api.auth import optional_auth
from duper.api.models import HealthResponse, SizeBreakdownItem, StatsResponse
from duper.core import DuperDatabase, get_config
from duper.utils.helpers import get_system_info

router = APIRouter(prefix="/api", tags=["system"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint - always accessible."""
    return HealthResponse(
        status="ok",
        version=__version__,
        codename=__codename__,
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    include_system: bool = False,
    include_metrics: bool = False,
    include_history: bool = False,
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
) -> StatsResponse:
    """Get database and system statistics."""
    stats = db.get_stats()

    # Convert size breakdown to proper model
    size_breakdown = [
        SizeBreakdownItem(**item)
        for item in stats.get("size_breakdown", [])
    ]

    response = StatsResponse(
        total_files=stats["total_files"],
        total_duplicates=stats["total_duplicates"],
        exact_duplicates=stats.get("exact_duplicates", 0),
        total_moved=stats["total_moved"],
        total_size_mb=stats["total_size_mb"],
        database_path=stats["database_path"],
        # Space statistics
        duplicate_space_mb=stats.get("duplicate_space_mb", 0.0),
        wasted_space_mb=stats.get("wasted_space_mb", 0.0),
        duplicate_groups=stats.get("duplicate_groups", 0),
        space_saved_mb=stats.get("space_saved_mb", 0.0),
        # Size breakdown
        size_breakdown=size_breakdown,
    )

    if include_system:
        response.system_info = get_system_info()

    if include_metrics and is_authed:
        metrics = db.get_latest_metrics()
        if metrics:
            response.latest_metrics = {
                "start_time": metrics.start_time,
                "end_time": metrics.end_time,
                "duration_seconds": metrics.scan_duration_seconds,
                "duration_verbose": metrics.scan_duration_verbose,
                "errors": metrics.errors_encountered,
                "files_processed": metrics.files_processed,
                "scan_directory": metrics.scan_directory,
            }

        statistics = db.get_latest_statistics()
        if statistics:
            response.latest_statistics = {
                "scan_id": statistics.scan_id,
                "total_files": statistics.total_files,
                "potential_duplicates": statistics.potential_duplicates,
                "scan_directory": statistics.scan_directory,
            }

    if include_history and is_authed:
        history = db.get_scan_history()
        response.scan_history = [
            {"directory": d, "last_scan_time": t}
            for d, t in history
        ]

    return response


# === Dashboard Gaming Stats ===

def _parse_esde_gamelists() -> list[dict]:
    """Parse all ES-DE gamelists and return games with lastplayed data."""
    gamelists_dir = _PathImport.home() / "ES-DE" / "gamelists"
    if not gamelists_dir.exists():
        # Try alternate location
        gamelists_dir = _PathImport.home() / "ES-DE" / "settings" / "gamelists"
    if not gamelists_dir.exists():
        return []

    played_games: list[dict] = []
    for sys_dir in gamelists_dir.iterdir():
        if not sys_dir.is_dir():
            continue
        gl_file = sys_dir / "gamelist.xml"
        if not gl_file.exists():
            continue
        try:
            tree = _ET.parse(gl_file)
            root = tree.getroot()
            if root.tag != "gameList":
                continue
            for game in root.findall("game"):
                lp = game.findtext("lastplayed")
                if not lp:
                    continue
                name = game.findtext("name", "Unknown")
                path = game.findtext("path", "")
                playcount = int(game.findtext("playcount", "0") or "0")
                playtime = int(game.findtext("playtime", "0") or "0")
                genre = game.findtext("genre", "")
                rating = game.findtext("rating", "")
                developer = game.findtext("developer", "")
                publisher = game.findtext("publisher", "")

                # Parse lastplayed timestamp: 20260316T195704
                try:
                    lp_dt = _datetime.strptime(lp, "%Y%m%dT%H%M%S")
                    lp_iso = lp_dt.isoformat()
                except (ValueError, TypeError):
                    lp_iso = lp
                    lp_dt = None

                played_games.append({
                    "name": name,
                    "system": sys_dir.name,
                    "path": path,
                    "lastplayed": lp_iso,
                    "lastplayed_raw": lp,
                    "playcount": playcount,
                    "playtime_minutes": playtime,
                    "genre": genre,
                    "rating": float(rating) if rating else None,
                    "developer": developer,
                    "publisher": publisher,
                    "_dt": lp_dt,
                })
        except _ET.ParseError:
            continue

    # Sort by lastplayed descending
    played_games.sort(key=lambda g: g.get("_dt") or _datetime.min, reverse=True)
    # Remove internal _dt field
    for g in played_games:
        g.pop("_dt", None)
    return played_games


@router.get("/dashboard/gaming")
async def dashboard_gaming(
    is_authed: bool = Depends(optional_auth),
):
    """Get gaming stats for dashboard: last played, collection metrics, play history."""
    played_games = _parse_esde_gamelists()

    # Last played game
    last_played = played_games[0] if played_games else None

    # Recently played (top 10)
    recently_played = played_games[:10]

    # Collection metrics by system
    gamelists_dir = _PathImport.home() / "ES-DE" / "gamelists"
    if not gamelists_dir.exists():
        gamelists_dir = _PathImport.home() / "ES-DE" / "settings" / "gamelists"

    system_stats: list[dict] = []
    total_games = 0
    total_played = 0
    total_playtime = 0

    if gamelists_dir.exists():
        for sys_dir in sorted(gamelists_dir.iterdir()):
            if not sys_dir.is_dir():
                continue
            gl_file = sys_dir / "gamelist.xml"
            if not gl_file.exists():
                continue
            try:
                tree = _ET.parse(gl_file)
                root = tree.getroot()
                if root.tag != "gameList":
                    continue
                games = root.findall("game")
                game_count = len(games)
                played_count = sum(
                    1 for g in games
                    if int(g.findtext("playcount", "0") or "0") > 0
                )
                sys_playtime = sum(
                    int(g.findtext("playtime", "0") or "0")
                    for g in games
                )
                if game_count > 0:
                    system_stats.append({
                        "system": sys_dir.name,
                        "total_games": game_count,
                        "played": played_count,
                        "completion_pct": round(played_count / game_count * 100, 1),
                        "playtime_minutes": sys_playtime,
                    })
                    total_games += game_count
                    total_played += played_count
                    total_playtime += sys_playtime
            except _ET.ParseError:
                continue

    # Sort system_stats by total_games descending
    system_stats.sort(key=lambda s: s["total_games"], reverse=True)

    # Most played games (by playcount)
    most_played = sorted(
        [g for g in played_games if g["playcount"] > 1],
        key=lambda g: g["playcount"],
        reverse=True,
    )[:5]

    return {
        "last_played": last_played,
        "recently_played": recently_played,
        "most_played": most_played,
        "collection": {
            "total_systems": len(system_stats),
            "total_games": total_games,
            "total_played": total_played,
            "completion_pct": round(total_played / total_games * 100, 1) if total_games else 0,
            "total_playtime_minutes": total_playtime,
        },
        "systems": system_stats,
    }


@router.get("/dashboard/ra-game-progress")
async def dashboard_ra_game_progress(
    game_id: int = 0,
    is_authed: bool = Depends(optional_auth),
):
    """Get RA achievement progress for a specific game. Returns earned/total, rarest, latest."""
    if game_id <= 0:
        return {"error": "game_id required"}

    config = get_config()
    ra = config.retroachievements
    if not ra.enabled or not ra.username or not ra.api_key:
        return {"error": "RA not configured"}

    try:
        async with _httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://retroachievements.org/API/API_GetGameInfoAndUserProgress.php",
                params={"z": ra.username, "y": ra.api_key, "u": ra.username, "g": game_id},
            )
            resp.raise_for_status()
            data = resp.json()

        achievements = data.get("Achievements", {})
        if isinstance(achievements, dict):
            ach_list = list(achievements.values())
        elif isinstance(achievements, list):
            ach_list = achievements
        else:
            ach_list = []

        earned = [a for a in ach_list if int(a.get("DateEarned", "0") != "") and a.get("DateEarned")]
        total = len(ach_list)

        # Find rarest earned achievement (lowest TrueRatio = hardest)
        rarest = None
        if earned:
            earned_sorted = sorted(earned, key=lambda a: float(a.get("TrueRatio", 9999)))
            if earned_sorted:
                r = earned_sorted[0]
                earn_pct = round(int(r.get("NumAwarded", 0)) / max(int(data.get("NumDistinctPlayersCasual", 1)), 1) * 100, 1)
                rarest = {"title": r.get("Title", ""), "earn_pct": earn_pct,
                          "points": r.get("Points", 0), "date": r.get("DateEarned", "")}

        # Find latest earned
        latest = None
        if earned:
            latest_sorted = sorted(earned, key=lambda a: a.get("DateEarned", ""), reverse=True)
            if latest_sorted:
                l = latest_sorted[0]
                latest = {"title": l.get("Title", ""), "points": l.get("Points", 0),
                          "date": l.get("DateEarned", "")}

        return {
            "game_id": game_id,
            "title": data.get("Title", ""),
            "earned": len(earned),
            "total": total,
            "points_earned": sum(int(a.get("Points", 0)) for a in earned),
            "points_total": sum(int(a.get("Points", 0)) for a in ach_list),
            "rarest_achievement": rarest,
            "latest_achievement": latest,
        }
    except Exception as e:
        return {"error": str(e), "game_id": game_id}


@router.get("/dashboard/ra-activity")
async def dashboard_ra_activity(
    minutes: int = 1440,
    is_authed: bool = Depends(optional_auth),
):
    """Get recent RetroAchievements activity for the dashboard.

    Calls the RA API for recent achievements earned by the user.
    Default: last 24 hours (1440 minutes).
    """
    config = get_config()
    ra = config.retroachievements

    if not ra.enabled or not ra.username or not ra.api_key:
        return {
            "enabled": False,
            "username": ra.username or "",
            "recent_achievements": [],
            "total_recent": 0,
            "error": None,
        }

    try:
        async with _httpx.AsyncClient(timeout=10.0) as client:
            # Get recent achievements
            resp = await client.get(
                "https://retroachievements.org/API/API_GetUserRecentAchievements.php",
                params={
                    "z": ra.username,
                    "y": ra.api_key,
                    "u": ra.username,
                    "m": minutes,
                },
            )
            resp.raise_for_status()
            achievements = resp.json()

            # Get user summary for overall stats
            summary_resp = await client.get(
                "https://retroachievements.org/API/API_GetUserSummary.php",
                params={
                    "z": ra.username,
                    "y": ra.api_key,
                    "u": ra.username,
                    "g": 5,  # number of recent games
                    "a": 10,  # number of recent achievements
                },
            )
            summary_resp.raise_for_status()
            summary = summary_resp.json()

        # Format achievements
        recent = []
        for ach in (achievements if isinstance(achievements, list) else []):
            recent.append({
                "title": ach.get("Title", ""),
                "description": ach.get("Description", ""),
                "points": ach.get("Points", 0),
                "game_title": ach.get("GameTitle", ""),
                "console_name": ach.get("ConsoleName", ""),
                "date": ach.get("Date", ""),
                "badge_url": f"https://media.retroachievements.org/Badge/{ach.get('BadgeName', '')}.png"
                if ach.get("BadgeName") else None,
                "game_icon": f"https://media.retroachievements.org{ach.get('GameIcon', '')}"
                if ach.get("GameIcon") else None,
                "type": ach.get("Type", ""),
            })

        # Extract key summary stats
        user_summary = {
            "total_points": summary.get("TotalPoints", 0),
            "total_softcore_points": summary.get("TotalSoftcorePoints", 0),
            "total_true_points": summary.get("TotalTruePoints", 0),
            "member_since": summary.get("MemberSince", ""),
            "rank": summary.get("Rank"),
            "user_pic": f"https://media.retroachievements.org{summary.get('UserPic', '')}"
            if summary.get("UserPic") else None,
            "motto": summary.get("Motto", ""),
            "recently_played": [
                {
                    "game_id": g.get("GameID"),
                    "title": g.get("Title", ""),
                    "console_name": g.get("ConsoleName", ""),
                    "last_played": g.get("LastPlayed", ""),
                    "icon": f"https://media.retroachievements.org{g.get('ImageIcon', '')}"
                    if g.get("ImageIcon") else None,
                    "achievements_earned": g.get("NumPossibleAchievements", 0),
                }
                for g in (summary.get("RecentlyPlayed", []) or [])
            ],
        }

        return {
            "enabled": True,
            "username": ra.username,
            "recent_achievements": recent,
            "total_recent": len(recent),
            "summary": user_summary,
            "error": None,
        }
    except Exception as exc:
        return {
            "enabled": True,
            "username": ra.username,
            "recent_achievements": [],
            "total_recent": 0,
            "summary": None,
            "error": str(exc),
        }


@router.get("/game-detail/{system}/{filename:path}")
async def game_detail(
    system: str,
    filename: str,
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Get full detail for a specific game.

    Combines:
      - File record from the DUPer DB (md5, size, ra_*, rom_serial)
      - ES-DE gamelist metadata (description, developer, publisher, etc.)
      - RetroAchievements game progress (if ra_game_id available)
    """
    # --- 1. File data from DB ---
    file_info: dict | None = None
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM files WHERE filename = ? AND filepath LIKE ?",
            (filename, f"%/{system}/%"),
        )
        row = cursor.fetchone()
        if row:
            file_info = dict(row)

    # --- 2. ES-DE gamelist metadata ---
    gl_meta: dict = {}
    clean_name = filename.rsplit(".", 1)[0] if "." in filename else filename

    gamelists_dir = _PathImport.home() / "ES-DE" / "gamelists"
    if not gamelists_dir.exists():
        gamelists_dir = _PathImport.home() / "ES-DE" / "settings" / "gamelists"

    gl_file = gamelists_dir / system / "gamelist.xml"
    if gl_file.exists():
        try:
            tree = _ET.parse(gl_file)
            root = tree.getroot()
            if root.tag == "gameList":
                for game in root.findall("game"):
                    path = game.findtext("path", "")
                    # Match by filename in path
                    if filename in path or clean_name in path:
                        gl_meta = {
                            "name": game.findtext("name", ""),
                            "description": game.findtext("desc", ""),
                            "developer": game.findtext("developer", ""),
                            "publisher": game.findtext("publisher", ""),
                            "genre": game.findtext("genre", ""),
                            "release_date": game.findtext("releasedate", ""),
                            "rating": game.findtext("rating", ""),
                            "players": game.findtext("players", ""),
                            "playcount": int(game.findtext("playcount", "0") or "0"),
                            "lastplayed": game.findtext("lastplayed", ""),
                            "playtime_minutes": int(game.findtext("playtime", "0") or "0"),
                        }
                        # Parse lastplayed for display
                        lp = gl_meta.get("lastplayed", "")
                        if lp:
                            try:
                                lp_dt = _datetime.strptime(lp, "%Y%m%dT%H%M%S")
                                gl_meta["lastplayed_iso"] = lp_dt.isoformat()
                            except (ValueError, TypeError):
                                gl_meta["lastplayed_iso"] = lp
                        break
        except _ET.ParseError:
            pass

    # --- 3. RetroAchievements game progress ---
    ra_progress: dict | None = None
    ra_game_id = (file_info or {}).get("ra_game_id", 0)

    if ra_game_id:
        config = get_config()
        ra = config.retroachievements
        if ra.enabled and ra.username and ra.api_key:
            try:
                async with _httpx.AsyncClient(timeout=8.0) as client:
                    resp = await client.get(
                        "https://retroachievements.org/API/API_GetGameInfoAndUserProgress.php",
                        params={
                            "z": ra.username,
                            "y": ra.api_key,
                            "u": ra.username,
                            "g": ra_game_id,
                        },
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    achievements = data.get("Achievements", {})
                    earned = sum(
                        1 for a in achievements.values()
                        if isinstance(a, dict) and a.get("DateEarned")
                    )
                    total = len(achievements)
                    points_earned = sum(
                        int(a.get("Points", 0)) for a in achievements.values()
                        if isinstance(a, dict) and a.get("DateEarned")
                    )
                    points_possible = sum(
                        int(a.get("Points", 0)) for a in achievements.values()
                        if isinstance(a, dict)
                    )

                    # Build earned achievements list (most recent first)
                    earned_list = []
                    for a in achievements.values():
                        if isinstance(a, dict) and a.get("DateEarned"):
                            earned_list.append({
                                "title": a.get("Title", ""),
                                "description": a.get("Description", ""),
                                "points": int(a.get("Points", 0)),
                                "badge_url": f"https://media.retroachievements.org/Badge/{a.get('BadgeName', '')}.png"
                                if a.get("BadgeName") else None,
                                "date_earned": a.get("DateEarned", ""),
                                "type": a.get("type", ""),
                            })
                    earned_list.sort(
                        key=lambda x: x.get("date_earned", ""), reverse=True
                    )

                    ra_progress = {
                        "game_id": ra_game_id,
                        "title": data.get("Title", ""),
                        "console_name": data.get("ConsoleName", ""),
                        "image_icon": f"https://media.retroachievements.org{data.get('ImageIcon', '')}"
                        if data.get("ImageIcon") else None,
                        "achievements_earned": earned,
                        "achievements_total": total,
                        "points_earned": points_earned,
                        "points_possible": points_possible,
                        "completion_pct": round(earned / total * 100, 1) if total > 0 else 0,
                        "earned_achievements": earned_list[:20],
                        "ra_url": f"https://retroachievements.org/game/{ra_game_id}",
                    }
            except Exception:
                pass

    return {
        "system": system,
        "filename": filename,
        "file": file_info,
        "gamelist": gl_meta,
        "ra_progress": ra_progress,
    }


@router.get("/retronas/summary")
async def retronas_summary(
    directory: str = "",
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Get RetroNAS dashboard summary with per-system breakdown."""
    collection = db.get_collection_stats()

    # Get per-system breakdown from directory structure
    systems = []
    if directory:
        systems = db.get_system_summary(directory)
    else:
        # Auto-detect: try scan history, then find common base from filepaths
        history = db.get_scan_history()
        if history:
            base_dir = history[0][0]
            systems = db.get_system_summary(base_dir)
        if not systems:
            # Infer base directory from file paths in DB
            with db.cursor() as cursor:
                cursor.execute("""
                    SELECT filepath FROM files
                    ORDER BY filepath LIMIT 1
                """)
                row = cursor.fetchone()
                if row:
                    import os
                    # Walk up to find a likely ROM root (dir containing system subdirs)
                    path = row[0]
                    parts = path.split("/")
                    # Try common patterns: /roms/, /Emulation/roms/, /retronas/roms/
                    for i, part in enumerate(parts):
                        if part.lower() == "roms" and i > 0:
                            base_dir = "/".join(parts[:i + 1])
                            systems = db.get_system_summary(base_dir)
                            if systems:
                                break

    return {
        "total_files": collection["total_files"],
        "total_size_mb": collection["total_size_mb"],
        "total_games": collection["total_games"],
        "total_systems": collection["total_systems"],
        "total_libraries": collection["total_libraries"],
        "total_media": collection["total_media"],
        "duplicate_groups": collection["duplicate_groups"],
        "space_saved_mb": collection["space_saved_mb"],
        "ra_supported": collection["ra_supported"],
        "ra_not_supported": collection["ra_not_supported"],
        "ra_unverified": collection["ra_unverified"],
        "systems": systems,
        "size_breakdown": collection["size_breakdown"],
    }


@router.get("/retronas/integrity")
async def retronas_integrity(
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Run database integrity check."""
    return db.integrity_check()


@router.post("/retronas/optimize")
async def retronas_optimize(
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Optimize database (vacuum, analyze, rebuild indexes)."""
    db.optimize()
    return {"status": "ok", "message": "Database optimized"}


# === File-based transfer & acquisition status ===
# Workers run as independent shell scripts. State is persisted to JSON files.
# DUPer API just reads these files — survives DUPer restarts.

import json as _json
import os as _os
import subprocess as _subprocess
from pathlib import Path as _Path

_STATE_DIR = _Path(_os.environ.get("DUPER_DATA_DIR", _Path.home() / ".local" / "share" / "duper")) / "transfers"
_STATE_DIR.mkdir(parents=True, exist_ok=True)

_TRANSFER_STATE_FILE = _STATE_DIR / "transfer.json"
_ACQ_STATE_FILE = _STATE_DIR / "acquisition.json"
_MEDIA_STATE_FILE = _STATE_DIR / "media.json"
_LIVE_STATE_FILE = _STATE_DIR / "live-systems.json"

_SCRIPTS_DIR = _Path(__file__).parent.parent.parent.parent / "scripts"

_EMPTY_TRANSFER = {
    "active": False, "source": "", "dest": "", "method": "scp",
    "total_files": 0, "transferred_files": 0, "total_bytes": 0, "transferred_bytes": 0,
    "current_file": "", "current_system": "", "speed_bps": 0, "eta_seconds": 0,
    "started_at": "", "pid": 0, "errors": [], "systems_done": [], "systems_remaining": [],
}

_EMPTY_ACQ = {
    "active": False, "paused": False, "collection": "", "collection_label": "",
    "dest_host": "", "total_files": 0, "completed_files": 0, "failed_files": 0,
    "total_bytes_downloaded": 0, "current_file": "", "current_file_size": 0,
    "current_file_downloaded": 0, "current_speed_bps": 0, "current_eta_seconds": 0,
    "started_at": "", "pid": 0, "queue": [], "completed": [], "errors": [],
}


def _read_state(path, default):
    """Read JSON state file, return default if missing/corrupt."""
    try:
        if path.exists():
            data = _json.loads(path.read_text())
            # Check if worker is still alive using pgrep (more reliable than os.kill)
            if data.get("active"):
                script_name = "transfer-worker" if "transfer" in str(path) else "acquisition-worker"
                result = _subprocess.run(
                    ["pgrep", "-f", script_name],
                    capture_output=True, text=True, timeout=2
                )
                if result.returncode != 0 or not result.stdout.strip():
                    data["active"] = False  # Process died
            return data
    except Exception:
        pass
    return dict(default)


def _is_worker_running(state_file):
    """Check if the worker process is still alive."""
    data = _read_state(state_file, {})
    pid = data.get("pid", 0)
    if pid > 0:
        try:
            _os.kill(pid, 0)
            return True
        except OSError:
            return False
    return False


# --- Live VM systems (background refresh) ---

_vm_live_cache = {"systems": [], "total_files": 0, "total_bytes": 0, "last_updated": ""}
_vm_live_thread_running = False


def _start_vm_live_monitor(dest_host="10.99.11.8"):
    """Background thread that polls VM filesystem every 5 seconds."""
    import threading
    import time

    global _vm_live_thread_running
    if _vm_live_thread_running:
        return
    _vm_live_thread_running = True

    def _monitor():
        global _vm_live_thread_running
        while _vm_live_thread_running:
            try:
                result = _subprocess.run(
                    ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
                     f"retronas@{dest_host}",
                     """cd /data/retronas/roms 2>/dev/null && for d in */; do
                         d="${d%/}"; count=$(find "$d" -maxdepth 1 -type f 2>/dev/null | wc -l)
                         if [ "$count" -gt 0 ]; then
                             size=$(du -sb "$d" 2>/dev/null | cut -f1)
                             echo "$d|$count|$size"
                         fi
                     done"""],
                    capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0 and result.stdout.strip():
                    systems = []
                    total_files = 0
                    total_bytes = 0
                    for line in result.stdout.strip().split("\n"):
                        parts = line.strip().split("|")
                        if len(parts) == 3:
                            name, count, size = parts[0], int(parts[1]), int(parts[2])
                            systems.append({"system": name, "file_count": count, "total_size_mb": size / (1024 * 1024)})
                            total_files += count
                            total_bytes += size
                    systems.sort(key=lambda s: -s["total_size_mb"])
                    _vm_live_cache["systems"] = systems
                    _vm_live_cache["total_files"] = total_files
                    _vm_live_cache["total_bytes"] = total_bytes
                    _vm_live_cache["last_updated"] = time.strftime("%H:%M:%S")
            except Exception:
                pass
            time.sleep(5)

    threading.Thread(target=_monitor, daemon=True).start()


# Auto-start the live monitor on import
_start_vm_live_monitor()


# --- Live file size monitor (polls current downloading file every 500ms) ---

def _start_file_monitor(dest_host="10.99.11.8"):
    """Polls current acquisition file size on VM for real-time progress."""
    import threading
    import time

    def _monitor():
        last_bytes = 0
        last_time = time.time()
        while True:
            try:
                acq = _read_state(_ACQ_STATE_FILE, _EMPTY_ACQ)
                if acq.get("active") and acq.get("current_file"):
                    dest_dir = "/data/retronas/roms/psx" if acq.get("collection") == "ps1" else "/data/retronas/roms/xbox"
                    fname = acq["current_file"]
                    result = _subprocess.run(
                        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=2",
                         f"retronas@{dest_host}",
                         f"stat -c '%s' '{dest_dir}/{fname}' 2>/dev/null || echo 0"],
                        capture_output=True, text=True, timeout=3
                    )
                    if result.returncode == 0:
                        current_size = int(result.stdout.strip())
                        now = time.time()
                        dt = now - last_time
                        if dt > 0.1:
                            speed = max(0, int((current_size - last_bytes) / dt))
                            # Write live overlay to state file
                            try:
                                data = _json.loads(_ACQ_STATE_FILE.read_text()) if _ACQ_STATE_FILE.exists() else {}
                                data["current_file_downloaded"] = current_size
                                data["current_speed_bps"] = speed
                                if speed > 0 and data.get("current_file_size", 0) > current_size:
                                    data["current_eta_seconds"] = int((data["current_file_size"] - current_size) / speed)
                                _ACQ_STATE_FILE.write_text(_json.dumps(data))
                            except Exception:
                                pass
                            last_bytes = current_size
                            last_time = now
                else:
                    last_bytes = 0
                    last_time = time.time()
            except Exception:
                pass
            time.sleep(0.5)

    threading.Thread(target=_monitor, daemon=True).start()


_start_file_monitor()


# === Transfer API endpoints ===

@router.get("/retronas/transfer")
async def retronas_transfer_status():
    """Read transfer state from worker's JSON file."""
    return _read_state(_TRANSFER_STATE_FILE, _EMPTY_TRANSFER)


@router.post("/retronas/transfer/start")
async def retronas_transfer_start(
    source: str = "",
    dest_host: str = "10.99.11.8",
    dest_path: str = "/data/retronas",
    is_authed: bool = Depends(optional_auth),
):
    """Launch the transfer worker script as a detached process."""
    if _is_worker_running(_TRANSFER_STATE_FILE):
        return {"error": "Transfer already in progress"}

    if not source:
        for candidate in ["/run/media/reaver/EXT-512/Emulation"]:
            if _os.path.isdir(candidate) and _os.path.isdir(_os.path.join(candidate, "roms")):
                source = candidate
                break
    if not source or not _os.path.isdir(source):
        return {"error": "SD card (EXT-512) not detected. Please insert the SD card."}

    script = str(_SCRIPTS_DIR / "transfer-worker.sh")
    state_file = str(_TRANSFER_STATE_FILE)

    # Pass the DB path so the worker can check/record transfers
    config = get_config()
    db_path = str(config.paths.database)

    # Use systemd-run to survive DUPer restarts (escapes cgroup kill)
    _subprocess.Popen(
        ["systemd-run", "--user", "--scope",
         "bash", script, source, dest_host, dest_path, state_file, db_path],
        stdout=open("/tmp/transfer-worker.log", "w"),
        stderr=_subprocess.STDOUT,
    )

    return {"status": "started", "source": source}


@router.post("/retronas/transfer/cancel")
async def retronas_transfer_cancel():
    """Kill the transfer worker process."""
    data = _read_state(_TRANSFER_STATE_FILE, _EMPTY_TRANSFER)
    pid = data.get("pid", 0)
    if pid > 0:
        try:
            _os.kill(pid, 15)  # SIGTERM
        except OSError:
            pass
    _subprocess.run(["pkill", "-f", "transfer-worker.sh"], capture_output=True)
    _subprocess.run(["pkill", "-f", "scp.*retronas"], capture_output=True)
    # Update state file
    data["active"] = False
    data["current_system"] = "CANCELLED"
    _TRANSFER_STATE_FILE.write_text(_json.dumps(data))
    return {"status": "cancelled"}


@router.get("/retronas/transfer/stats")
async def retronas_transfer_stats(
    dest_host: str = "10.99.11.8",
    db: DuperDatabase = Depends(get_db),
):
    """Get transfer statistics — how many files are tracked on the destination."""
    return db.get_transfer_stats(dest_host)


@router.get("/retronas/transfer/manifest")
async def retronas_transfer_manifest(
    source: str = "",
    dest_host: str = "10.99.11.8",
    db: DuperDatabase = Depends(get_db),
):
    """Preview what would be transferred (new files vs already-there)."""
    if not source:
        for candidate in ["/run/media/reaver/EXT-512/Emulation"]:
            if _os.path.isdir(candidate) and _os.path.isdir(_os.path.join(candidate, "roms")):
                source = candidate
                break
    if not source or not _os.path.isdir(source):
        return {"error": "Source not found"}

    manifest = db.build_transfer_manifest(source, dest_host)
    return {
        "source": source,
        "dest_host": dest_host,
        "new_files": len(manifest["to_transfer"]),
        "skipped_files": len(manifest["skipped"]),
        "new_bytes": sum(f["file_size"] for f in manifest["to_transfer"]),
        "skipped_bytes": sum(f["file_size"] for f in manifest["skipped"]),
        "systems_with_new": list(set(f["system"] for f in manifest["to_transfer"])),
    }


# === Media Cache ===

@router.post("/retronas/sync-media-cache")
async def retronas_sync_media_cache(
    is_authed: bool = Depends(optional_auth),
):
    """Sync browse media (covers, miximages, screenshots) locally for stutter-free menus."""
    script = str(_SCRIPTS_DIR / "sync-media-cache.sh")
    if not _Path(script).exists():
        return {"error": "sync-media-cache.sh not found"}

    _subprocess.Popen(
        ["bash", script],
        stdout=open("/tmp/sync-media-cache.log", "w"),
        stderr=_subprocess.STDOUT,
    )
    return {"status": "started"}


# === Game Index (Gamelist Generation) ===

@router.post("/retronas/rebuild-index")
async def retronas_rebuild_index(
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Rebuild ES-DE gamelists from the DUPer database and push to all devices.

    This regenerates gamelist.xml files for all systems, updates directory
    symlinks, and pushes to the Steam Deck if online.
    """
    script = str(_SCRIPTS_DIR / "build-game-index.sh")
    if not _Path(script).exists():
        return {"error": "build-game-index.sh not found"}

    config = get_config()
    db_path = str(config.paths.database)

    result = _subprocess.run(
        ["bash", script, "--db", db_path],
        capture_output=True, text=True, timeout=120,
    )

    return {
        "status": "ok" if result.returncode == 0 else "error",
        "output": result.stdout[-2000:] if result.stdout else "",
        "errors": result.stderr[-500:] if result.stderr else "",
    }


@router.get("/retronas/index-status")
async def retronas_index_status(
    db: DuperDatabase = Depends(get_db),
):
    """Check game index consistency — compare DB records vs gamelists on disk."""
    import os
    import xml.etree.ElementTree as ET

    gamelists_dir = _Path.home() / "ES-DE" / "gamelists"
    if not gamelists_dir.exists():
        return {"error": "ES-DE gamelists directory not found"}

    # Count games in DB per system
    with db.cursor() as cursor:
        cursor.execute("""
            SELECT system, COUNT(*) as count FROM device_transfers
            WHERE dest_host='10.99.11.8' AND dest_path='/data/retronas'
            AND status='transferred' AND (rom_serial IS NULL OR rom_serial != 'media')
            AND system != '' AND filename NOT IN ('metadata.txt', 'systeminfo.txt')
            GROUP BY system ORDER BY system
        """)
        db_counts = {row["system"]: row["count"] for row in cursor.fetchall()}

    # Count games in gamelists on disk
    gl_counts: dict[str, int] = {}
    for sys_dir in gamelists_dir.iterdir():
        if not sys_dir.is_dir():
            continue
        gl_file = sys_dir / "gamelist.xml"
        if gl_file.exists():
            try:
                tree = ET.parse(gl_file)
                root = tree.getroot()
                games = root.findall("game") if root.tag == "gameList" else []
                gl_counts[sys_dir.name] = len(games)
            except ET.ParseError:
                gl_counts[sys_dir.name] = -1  # Parse error

    # Find mismatches
    mismatches = []
    all_systems = sorted(set(db_counts.keys()) | set(gl_counts.keys()))
    for sys in all_systems:
        db_c = db_counts.get(sys, 0)
        gl_c = gl_counts.get(sys, 0)
        if db_c != gl_c:
            mismatches.append({"system": sys, "db": db_c, "gamelist": gl_c})

    return {
        "consistent": len(mismatches) == 0,
        "total_systems_db": len(db_counts),
        "total_systems_gamelists": len(gl_counts),
        "total_games_db": sum(db_counts.values()),
        "total_games_gamelists": sum(v for v in gl_counts.values() if v >= 0),
        "mismatches": mismatches,
    }


# === Live VM filesystem ===

@router.get("/retronas/live")
async def retronas_live():
    """Get live filesystem data from the RetroNAS VM."""
    return _vm_live_cache


# === File Acquisition (archive.org downloads) ===

@router.get("/retronas/acquisition")
async def retronas_acquisition_status():
    """Read acquisition state from worker's JSON file."""
    return _read_state(_ACQ_STATE_FILE, _EMPTY_ACQ)


@router.post("/retronas/acquisition/start")
async def retronas_acquisition_start(
    collection: str = "ps1",
    dest_host: str = "10.99.11.8",
    is_authed: bool = Depends(optional_auth),
):
    """Launch the acquisition worker script as a detached process."""
    if _is_worker_running(_ACQ_STATE_FILE):
        return {"error": "Acquisition already in progress"}

    script = str(_SCRIPTS_DIR / "acquisition-worker.sh")
    state_file = str(_ACQ_STATE_FILE)

    _subprocess.Popen(
        ["systemd-run", "--user", "--scope",
         "bash", script, collection, dest_host, state_file],
        stdout=open("/tmp/acquisition-worker.log", "w"),
        stderr=_subprocess.STDOUT,
    )

    # Wait briefly for the worker to write initial state
    import time
    time.sleep(2)
    return _read_state(_ACQ_STATE_FILE, {"status": "starting", "collection": collection})


@router.post("/retronas/acquisition/pause")
async def retronas_acquisition_pause():
    """Pause not supported in file-based workers yet."""
    return {"status": "pause not implemented for standalone workers"}


@router.post("/retronas/acquisition/cancel")
async def retronas_acquisition_cancel():
    """Kill the acquisition worker process."""
    data = _read_state(_ACQ_STATE_FILE, _EMPTY_ACQ)
    pid = data.get("pid", 0)
    if pid > 0:
        try:
            _os.kill(pid, 15)
        except OSError:
            pass
    _subprocess.run(["pkill", "-f", "acquisition-worker.sh"], capture_output=True)
    data["active"] = False
    _ACQ_STATE_FILE.write_text(_json.dumps(data))
    return {"status": "cancelled"}


@router.post("/retronas/acquisition/scrape-new")
async def retronas_acquisition_scrape_new(
    directory: str = "/data/retronas/roms",
    limit: int = 20,
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Scrape media for recently-acquired ROMs that have no media.

    Call this after acquisition completes (or periodically) to auto-scrape
    newly downloaded games. Only scrapes files already in the DUPer DB.
    """
    config = get_config()
    ss = config.screenscraper

    if not ss.enabled or not ss.username or not ss.password:
        return {"status": "skipped", "reason": "screenscraper_not_configured"}

    roms = db.get_files_without_media(directory=directory, limit=limit)
    files_to_scrape = [(r["filepath"], r["md5"]) for r in roms if r["md5"]]

    if not files_to_scrape:
        return {"status": "nothing_to_scrape", "count": 0}

    from duper.core.screenscraper import get_ss_client

    client = get_ss_client(
        username=ss.username,
        password=ss.password,
        dev_id=ss.dev_id,
        dev_password=ss.dev_password,
    )
    if not client:
        return {"status": "error", "reason": "client_init_failed"}

    media_dir = ss.media_path or ""

    found = 0
    def _cb(result, completed, total):
        nonlocal found
        if result and result.found:
            found += 1

    try:
        client.batch_scrape(
            files=files_to_scrape,
            media_dir=media_dir,
            download_art=True,
            callback=_cb,
        )
    except Exception as e:
        return {"status": "error", "reason": str(e), "found": found}

    return {"status": "completed", "scraped": len(files_to_scrape), "found": found}


# === Media Transfer ===

_EMPTY_MEDIA = {
    "active": False, "type": "media", "source": "", "dest": "",
    "total_files": 0, "transferred_files": 0, "skipped_files": 0,
    "total_bytes": 0, "transferred_bytes": 0, "skipped_bytes": 0,
    "current_file": "", "current_system": "", "speed_bps": 0, "eta_seconds": 0,
    "started_at": "", "pid": 0, "systems_done": [], "systems_remaining": [],
}


@router.get("/retronas/media-transfer")
async def retronas_media_transfer_status():
    """Get media transfer status."""
    return _read_state(_MEDIA_STATE_FILE, _EMPTY_MEDIA)


@router.post("/retronas/media-transfer/start")
async def retronas_media_transfer_start(
    source: str = "",
    dest_host: str = "10.99.11.8",
    dest_path: str = "/data/retronas/media",
    is_authed: bool = Depends(optional_auth),
):
    """Start media transfer (box art, screenshots, videos) to RetroNAS."""
    if _is_worker_running(_MEDIA_STATE_FILE):
        return {"error": "Media transfer already in progress"}

    if not source:
        for candidate in [
            "/run/media/reaver/EXT-512/Emulation/tools/downloaded_media",
        ]:
            if _os.path.isdir(candidate):
                source = candidate
                break
    if not source or not _os.path.isdir(source):
        return {"error": "Media source not found. Is the SD card inserted?"}

    script = str(_SCRIPTS_DIR / "media-worker.sh")
    state_file = str(_MEDIA_STATE_FILE)

    # Pass the DB path so the worker can check/record transfers
    config = get_config()
    db_path = str(config.paths.database)

    _subprocess.Popen(
        ["systemd-run", "--user", "--scope",
         "bash", script, source, dest_host, dest_path, state_file, db_path],
        stdout=open("/tmp/media-worker.log", "w"),
        stderr=_subprocess.STDOUT,
    )

    return {"status": "started", "source": source}


@router.post("/retronas/media-transfer/cancel")
async def retronas_media_transfer_cancel():
    """Cancel media transfer."""
    data = _read_state(_MEDIA_STATE_FILE, _EMPTY_MEDIA)
    pid = data.get("pid", 0)
    if pid > 0:
        try:
            _os.kill(pid, 15)
        except OSError:
            pass
    _subprocess.run(["pkill", "-f", "media-worker.sh"], capture_output=True)
    data["active"] = False
    _MEDIA_STATE_FILE.write_text(_json.dumps(data))
    return {"status": "cancelled"}


# === Custom Game Collections ===

@router.get("/collections")
async def list_custom_collections():
    """List all custom ES-DE game collections."""
    import os
    collections_dir = _PathImport.home() / "ES-DE" / "collections"
    if not collections_dir.exists():
        return []

    result = []
    for cfg in sorted(collections_dir.glob("custom-*.cfg")):
        name = cfg.stem.replace("custom-", "")
        games = [l.strip() for l in cfg.read_text().splitlines() if l.strip()]
        result.append({
            "name": name,
            "game_count": len(games),
            "file": str(cfg),
        })
    return result


@router.post("/collections/create")
async def create_collection(
    name: str = "",
    search: str = "",
    is_authed: bool = Depends(optional_auth),
):
    """Create a custom collection by searching game names."""
    if not name:
        return {"error": "name required"}

    import re
    gamelists_dir = _PathImport.home() / "ES-DE" / "gamelists"
    collections_dir = _PathImport.home() / "ES-DE" / "collections"
    collections_dir.mkdir(exist_ok=True)

    matches = []
    for sys_dir in sorted(gamelists_dir.iterdir()):
        if not sys_dir.is_dir():
            continue
        gl = sys_dir / "gamelist.xml"
        if not gl.exists():
            continue
        try:
            tree = _ET.parse(gl)
            root = tree.getroot()
            if root.tag != "gameList":
                continue
            for game in root.findall("game"):
                gname = game.findtext("name", "")
                path = game.findtext("path", "")
                if search and re.search(search, gname, re.I):
                    full_path = f"%ROMPATH%/{sys_dir.name}/{path.lstrip('./')}"
                    matches.append(full_path)
        except _ET.ParseError:
            continue

    cfg_path = collections_dir / f"custom-{name}.cfg"
    cfg_path.write_text("\n".join(matches) + "\n")

    return {"name": name, "games": len(matches), "file": str(cfg_path)}


@router.delete("/collections/{name}")
async def delete_collection(
    name: str,
    is_authed: bool = Depends(optional_auth),
):
    """Delete a custom collection."""
    cfg = _PathImport.home() / "ES-DE" / "collections" / f"custom-{name}.cfg"
    if cfg.exists():
        cfg.unlink()
        return {"status": "deleted", "name": name}
    return {"error": "not found"}
