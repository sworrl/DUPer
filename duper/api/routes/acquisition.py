"""Acquisition API — download game collections from archive.org."""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends

from duper.api.auth import optional_auth
from duper.core import DuperDatabase, get_config
from duper.core.collections import (
    COLLECTIONS,
    filter_files_by_sub_collection,
    get_collection,
    list_collections,
)

router = APIRouter(prefix="/api/acquisition", tags=["acquisition"])

_STATE_DIR = Path(os.environ.get("DUPER_DATA_DIR", Path.home() / ".local" / "share" / "duper")) / "transfers"
_STATE_DIR.mkdir(parents=True, exist_ok=True)
_SCRIPTS_DIR = Path(__file__).parent.parent.parent.parent / "scripts"


def get_db() -> DuperDatabase:
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db


def _read_job_state(job_id: str) -> dict:
    """Read a job's state file."""
    state_file = _STATE_DIR / f"acq-{job_id}.json"
    try:
        if state_file.exists():
            data = json.loads(state_file.read_text())
            # Check if worker is alive
            if data.get("active"):
                pid = data.get("pid", 0)
                if pid > 0:
                    try:
                        os.kill(pid, 0)
                    except OSError:
                        data["active"] = False
            return data
    except Exception:
        pass
    return {}


@router.get("/collections")
async def get_collections():
    """List all available collections and their sub-collections."""
    return list_collections()


@router.get("/collections/{collection_id}")
async def get_collection_detail(
    collection_id: str,
    db: DuperDatabase = Depends(get_db),
):
    """Get detailed info for a collection including what's already downloaded."""
    coll = get_collection(collection_id)
    if not coll:
        return {"error": f"Collection '{collection_id}' not found"}

    # Count files already on NAS for this system
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) as count FROM device_transfers
            WHERE dest_host='10.99.11.8' AND system=? AND status='transferred'
            AND (rom_serial IS NULL OR rom_serial != 'media')
            """,
            (coll["system"],),
        )
        on_nas = cursor.fetchone()["count"]

    subs = [
        {"id": sub_id, "label": sub["label"], "has_filter": bool(sub.get("filter"))}
        for sub_id, sub in coll.get("sub_collections", {}).items()
    ]

    return {
        "id": collection_id,
        "label": coll["label"],
        "system": coll["system"],
        "region": coll["region"],
        "format": coll["format"],
        "ia_collections": coll["ia_collections"],
        "dest_dir": coll["dest_dir"],
        "on_nas": on_nas,
        "sub_collections": subs,
    }


@router.get("/jobs")
async def list_jobs(
    db: DuperDatabase = Depends(get_db),
):
    """List all acquisition jobs (active and completed)."""
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM acquisition_jobs ORDER BY started_at DESC LIMIT 50"
        )
        rows = cursor.fetchall()

    jobs = []
    for row in rows:
        job = dict(row)
        # Merge live state from state file
        live = _read_job_state(row["job_id"])
        if live:
            job["live"] = {
                "active": live.get("active", False),
                "current_file": live.get("current_file", ""),
                "current_speed_bps": live.get("current_speed_bps", 0),
                "current_eta_seconds": live.get("current_eta_seconds", 0),
                "queue": live.get("queue", [])[:5],
                "completed": live.get("completed", [])[:10],
            }
        jobs.append(job)

    return jobs


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, db: DuperDatabase = Depends(get_db)):
    """Get detailed status of a specific acquisition job."""
    with db.cursor() as cursor:
        cursor.execute("SELECT * FROM acquisition_jobs WHERE job_id=?", (job_id,))
        row = cursor.fetchone()

    if not row:
        return {"error": "Job not found"}

    job = dict(row)
    live = _read_job_state(job_id)
    if live:
        job["live"] = live
    return job


@router.post("/start")
async def start_acquisition(
    collection_id: str = "ps1",
    sub_collection: str = "all",
    dest_host: str = "10.99.11.8",
    is_authed: bool = Depends(optional_auth),
    db: DuperDatabase = Depends(get_db),
):
    """Start downloading a collection (or sub-collection) from archive.org."""
    coll = get_collection(collection_id)
    if not coll:
        return {"error": f"Unknown collection: {collection_id}"}

    # Check for existing active job for this collection
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT job_id FROM acquisition_jobs WHERE collection_id=? AND status='running'",
            (collection_id,),
        )
        existing = cursor.fetchone()
        if existing:
            live = _read_job_state(existing["job_id"])
            if live.get("active"):
                return {"error": f"Collection '{collection_id}' already downloading", "job_id": existing["job_id"]}

    job_id = str(uuid.uuid4())[:8]
    state_file = str(_STATE_DIR / f"acq-{job_id}.json")
    script = str(_SCRIPTS_DIR / "acquisition-worker.sh")

    config = get_config()
    db_path = str(config.paths.database)

    # Record job in DB
    from datetime import datetime
    with db.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO acquisition_jobs
            (job_id, collection_id, sub_collection, system, status, started_at)
            VALUES (?, ?, ?, ?, 'running', ?)
            """,
            (job_id, collection_id, sub_collection, coll["system"], datetime.now().isoformat()),
        )

    # Launch the worker
    subprocess.Popen(
        ["systemd-run", "--user", "--scope",
         "bash", script, collection_id, dest_host, state_file, sub_collection, db_path, job_id],
        stdout=open(f"/tmp/acq-{job_id}.log", "w"),
        stderr=subprocess.STDOUT,
    )

    return {
        "job_id": job_id,
        "collection": collection_id,
        "sub_collection": sub_collection,
        "status": "started",
    }


@router.post("/cancel/{job_id}")
async def cancel_job(
    job_id: str,
    db: DuperDatabase = Depends(get_db),
):
    """Cancel an active acquisition job."""
    live = _read_job_state(job_id)
    pid = live.get("pid", 0)
    if pid > 0:
        try:
            os.kill(pid, 15)
        except OSError:
            pass

    subprocess.run(["pkill", "-f", f"acq-{job_id}"], capture_output=True)

    with db.cursor() as cursor:
        cursor.execute(
            "UPDATE acquisition_jobs SET status='cancelled' WHERE job_id=?",
            (job_id,),
        )

    # Update state file
    state_file = _STATE_DIR / f"acq-{job_id}.json"
    if state_file.exists():
        try:
            data = json.loads(state_file.read_text())
            data["active"] = False
            state_file.write_text(json.dumps(data))
        except Exception:
            pass

    return {"status": "cancelled", "job_id": job_id}


@router.get("/summary")
async def acquisition_summary(
    db: DuperDatabase = Depends(get_db),
):
    """Summary of all acquisitions — what's downloaded per collection."""
    summary = []
    for coll_id, coll in COLLECTIONS.items():
        with db.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*) as count, COALESCE(SUM(file_size),0) as bytes
                FROM device_transfers
                WHERE dest_host='10.99.11.8' AND system=? AND status='transferred'
                AND (rom_serial IS NULL OR rom_serial != 'media')
                """,
                (coll["system"],),
            )
            row = cursor.fetchone()

            # Active jobs
            cursor.execute(
                "SELECT COUNT(*) as c FROM acquisition_jobs WHERE collection_id=? AND status='running'",
                (coll_id,),
            )
            active = cursor.fetchone()["c"]

        summary.append({
            "id": coll_id,
            "label": coll["label"],
            "system": coll["system"],
            "on_nas": row["count"],
            "total_bytes": row["bytes"],
            "active_jobs": active,
        })

    return summary
