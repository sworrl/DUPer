"""Game management API routes."""

from fastapi import APIRouter, Depends, HTTPException

from duper.api.auth import require_auth
from duper.api.models import (
    FileResponse,
    GameDetailResponse,
    GameListResponse,
    GameResponse,
)
from duper.core.config import get_config
from duper.core.database import DuperDatabase

router = APIRouter(prefix="/api/games", tags=["games"])


def get_db() -> DuperDatabase:
    """Get database instance."""
    config = get_config()
    db = DuperDatabase(config.paths.database)
    db.initialize()
    return db


@router.get("", response_model=GameListResponse)
def list_games(
    library_id: str,
    system: str | None = None,
    limit: int = 100,
    offset: int = 0,
    _: None = Depends(require_auth),
) -> GameListResponse:
    """List games in a library, optionally filtered by system."""
    db = get_db()

    # Verify library exists
    library = db.get_library(library_id)
    if not library:
        raise HTTPException(status_code=404, detail="Library not found")

    games = db.get_games_in_library(library_id, system=system, limit=limit, offset=offset)
    total = db.get_game_count_in_library(library_id)
    systems = db.get_games_by_system(library_id)

    return GameListResponse(
        total=total,
        library_id=library_id,
        games=[
            GameResponse(
                game_id=game.game_id,
                library_id=game.library_id,
                title=game.title,
                normalized_title=game.normalized_title,
                system=game.system,
                ra_game_id=game.ra_game_id,
                primary_file_path=game.primary_file_path,
                primary_rom_serial=game.primary_rom_serial,
                region=game.region,
                release_year=game.release_year,
                genre=game.genre,
                developer=game.developer,
                publisher=game.publisher,
                file_count=game.file_count,
                total_size_mb=game.total_size_mb,
                has_media=game.has_media,
                ra_supported=game.ra_supported,
                cover_url=f"/api/games/{game.game_id}/cover" if game.has_media else None,
            )
            for game in games
        ],
        systems=systems,
    )


@router.get("/{game_id}", response_model=GameDetailResponse)
def get_game(
    game_id: str,
    _: None = Depends(require_auth),
) -> GameDetailResponse:
    """Get detailed game information including files and media."""
    db = get_db()
    game = db.get_game(game_id)

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # Get all files for this game
    files = []
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM files WHERE game_id = ?",
            (game_id,),
        )
        from duper.core.database import FileRecord
        files = [FileRecord.from_row(row) for row in cursor.fetchall()]

    # Get all media for this game
    media = []
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM media_files WHERE game_id = ?",
            (game_id,),
        )
        from duper.core.database import MediaRecord
        media = [MediaRecord.from_row(row).to_dict() for row in cursor.fetchall()]

    return GameDetailResponse(
        game_id=game.game_id,
        library_id=game.library_id,
        title=game.title,
        normalized_title=game.normalized_title,
        system=game.system,
        ra_game_id=game.ra_game_id,
        primary_file_path=game.primary_file_path,
        primary_rom_serial=game.primary_rom_serial,
        region=game.region,
        release_year=game.release_year,
        genre=game.genre,
        developer=game.developer,
        publisher=game.publisher,
        file_count=game.file_count,
        total_size_mb=game.total_size_mb,
        has_media=game.has_media,
        ra_supported=game.ra_supported,
        cover_url=f"/api/games/{game.game_id}/cover" if game.has_media else None,
        files=[
            FileResponse(
                filepath=f.filepath,
                filename=f.filename,
                md5=f.md5,
                size_mb=f.size_mb,
                extension=f.extension,
                is_duplicate=f.is_potential_duplicate,
                rom_serial=f.rom_serial,
            )
            for f in files
        ],
        media=media,
    )


@router.get("/{game_id}/cover")
def get_game_cover(
    game_id: str,
    _: None = Depends(require_auth),
):
    """Get the cover image for a game."""
    from fastapi.responses import FileResponse as FastAPIFileResponse
    from pathlib import Path

    db = get_db()
    game = db.get_game(game_id)

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    if not game.primary_file_path:
        raise HTTPException(status_code=404, detail="No primary file set for game")

    # Find the best cover image
    cover = db.get_best_cover_for_rom(game.primary_file_path)

    if not cover:
        raise HTTPException(status_code=404, detail="No cover image found")

    cover_path = Path(cover.media_path)
    if not cover_path.exists():
        raise HTTPException(status_code=404, detail="Cover file not found")

    return FastAPIFileResponse(
        path=str(cover_path),
        media_type=cover.mime_type or "image/png",
        filename=cover.filename,
    )


@router.put("/{game_id}/primary")
def set_primary_file(
    game_id: str,
    filepath: str,
    _: None = Depends(require_auth),
) -> dict:
    """Set the primary ROM file for a game."""
    db = get_db()
    game = db.get_game(game_id)

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    # Verify the file exists and belongs to this game
    file = db.get_file(filepath)
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    if file.game_id != game_id:
        raise HTTPException(status_code=400, detail="File does not belong to this game")

    game.primary_file_path = filepath
    game.primary_rom_serial = file.rom_serial

    if not db.update_game(game):
        raise HTTPException(status_code=500, detail="Failed to update game")

    return {"status": "updated", "primary_file_path": filepath}


@router.delete("/{game_id}")
def delete_game(
    game_id: str,
    _: None = Depends(require_auth),
) -> dict:
    """Delete a game (does not delete the ROM files)."""
    db = get_db()
    game = db.get_game(game_id)

    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    if not db.delete_game(game_id):
        raise HTTPException(status_code=500, detail="Failed to delete game")

    return {"status": "deleted", "game_id": game_id}
