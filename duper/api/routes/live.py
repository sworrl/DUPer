"""Live game capture API — serves screenshots and controls the capture service."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse

from duper.api.auth import optional_auth

router = APIRouter(prefix="/api/live", tags=["live"])

_LIVE_DIR = Path(os.environ.get("DUPER_DATA_DIR", Path.home() / ".local" / "share" / "duper")) / "live"
_LIVE_DIR.mkdir(parents=True, exist_ok=True)
_SCRIPTS_DIR = Path(__file__).parent.parent.parent.parent / "scripts"


def _read_state() -> dict:
    state_file = _LIVE_DIR / "state.json"
    try:
        if state_file.exists():
            data = json.loads(state_file.read_text())
            # Verify process is alive
            if data.get("active") and data.get("pid"):
                try:
                    os.kill(data["pid"], 0)
                except OSError:
                    data["active"] = False
            return data
    except Exception:
        pass
    return {"active": False}


@router.get("/status")
async def live_status():
    """Get live capture status."""
    state = _read_state()
    latest = _LIVE_DIR / "latest.jpg"
    state["has_frame"] = latest.exists()
    if latest.exists():
        state["frame_size"] = latest.stat().st_size
    return state


@router.get("/frame")
async def live_frame():
    """Get the latest captured frame as JPEG."""
    latest = _LIVE_DIR / "latest.jpg"
    if latest.exists():
        return FileResponse(
            str(latest),
            media_type="image/jpeg",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    return {"error": "no frame available"}


@router.get("/frame/{frame_num}")
async def live_frame_by_number(frame_num: int):
    """Get a specific frame from the buffer."""
    frame = _LIVE_DIR / f"frame_{frame_num:03d}.jpg"
    if frame.exists():
        return FileResponse(str(frame), media_type="image/jpeg")
    return {"error": "frame not found"}


@router.post("/start")
async def live_start(
    interval: int = 10,
    is_authed: bool = Depends(optional_auth),
):
    """Start live capture."""
    state = _read_state()
    if state.get("active"):
        return {"status": "already_running", "pid": state.get("pid")}

    script = str(_SCRIPTS_DIR / "live-capture.sh")
    if not Path(script).exists():
        return {"error": "live-capture.sh not found"}

    subprocess.Popen(
        ["bash", script, str(interval), str(_LIVE_DIR)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return {"status": "started", "interval": interval}


@router.post("/stop")
async def live_stop(
    is_authed: bool = Depends(optional_auth),
):
    """Stop live capture."""
    state = _read_state()
    pid = state.get("pid", 0)
    if pid > 0:
        try:
            os.kill(pid, 15)
        except OSError:
            pass

    state_file = _LIVE_DIR / "state.json"
    state_file.write_text('{"active": false}')
    return {"status": "stopped"}


@router.post("/screenshot")
async def take_screenshot(
    is_authed: bool = Depends(optional_auth),
):
    """Take a single screenshot right now (doesn't need capture running)."""
    output = _LIVE_DIR / "latest.jpg"
    png_tmp = _LIVE_DIR / "snap.png"

    # Take screenshot
    result = subprocess.run(
        ["spectacle", "-b", "-n", "-f", "-o", str(png_tmp)],
        capture_output=True, timeout=5,
    )

    if result.returncode == 0 and png_tmp.exists():
        # Convert to smaller JPEG
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(png_tmp), "-vf", "scale=1280:-1", "-q:v", "5", str(output)],
            capture_output=True, timeout=5,
        )
        png_tmp.unlink(missing_ok=True)

        if output.exists():
            return {"status": "ok", "size": output.stat().st_size}

    return {"error": "screenshot failed"}
