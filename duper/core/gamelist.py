"""Generate ES-DE gamelist.xml files from the DUPer database.

Instead of ES-DE scanning the filesystem (slow over NFS/SMB), we generate
complete gamelist.xml files from what DUPer knows is on the NAS. Combined
with ES-DE's ParseGamelistOnly=true setting, this eliminates network
scanning entirely — instant startup.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from xml.dom import minidom

from duper.core.database import DuperDatabase

# Files to exclude from gamelists (not actual games)
SKIP_FILENAMES = frozenset({
    "metadata.txt", "systeminfo.txt", "noload.txt",
    ".gitkeep", ".gitignore", "desktop.ini", "Thumbs.db",
})

SKIP_EXTENSIONS = frozenset({
    ".txt", ".xml", ".json", ".cfg", ".ini", ".log", ".md",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp",
    ".mp3", ".flac", ".wav", ".ogg",
})


def _game_name_from_filename(filename: str) -> str:
    """Extract a clean game name from a ROM filename.

    'Super Mario Bros (USA) [!].nes' -> 'Super Mario Bros'
    """
    name = os.path.splitext(filename)[0]
    # Strip region/revision tags: (USA), [!], (Rev A), etc.
    import re
    name = re.sub(r'\s*[\(\[][^\)\]]*[\)\]]', '', name)
    return name.strip() or filename


def _should_skip(filename: str) -> bool:
    """Check if a file should be excluded from gamelists."""
    if filename in SKIP_FILENAMES:
        return True
    ext = os.path.splitext(filename)[1].lower()
    return ext in SKIP_EXTENSIONS


def generate_gamelists(
    db: DuperDatabase,
    output_dir: str | Path,
    dest_host: str = "10.99.11.8",
    dest_path: str = "/data/retronas",
) -> dict[str, int]:
    """Generate gamelist.xml files for all systems from the DUPer database.

    Merges with existing gamelists to preserve user metadata (play counts,
    favorites, last played, descriptions from scraping).

    Args:
        db: Connected DuperDatabase instance
        output_dir: ES-DE gamelists directory (e.g. ~/ES-DE/gamelists)
        dest_host: NAS host to query transfers for
        dest_path: NAS destination path

    Returns:
        Dict of {system: game_count} for systems that were generated
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get all transferred ROM files grouped by system
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT system, filename, filepath
            FROM device_transfers
            WHERE dest_host = ? AND dest_path = ?
              AND status = 'transferred'
              AND (rom_serial IS NULL OR rom_serial != 'media')
              AND system != ''
            ORDER BY system, filename
            """,
            (dest_host, dest_path),
        )
        rows = cursor.fetchall()

    # Group by system
    systems: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        system = row["system"]
        filename = row["filename"]
        if _should_skip(filename):
            continue
        systems.setdefault(system, []).append({
            "filename": filename,
            "filepath": row["filepath"],
        })

    results: dict[str, int] = {}

    for system, files in systems.items():
        if not files:
            continue

        sys_dir = output_dir / system
        sys_dir.mkdir(parents=True, exist_ok=True)
        gamelist_path = sys_dir / "gamelist.xml"

        # Load existing gamelist to preserve user metadata
        existing_games = _load_existing_gamelist(gamelist_path)

        # Build the new gamelist
        root = ET.Element("gameList")

        for file_info in files:
            filename = file_info["filename"]
            rel_path = f"./{filename}"

            # Check if this game exists in the old gamelist
            existing = existing_games.get(rel_path)

            game_el = ET.SubElement(root, "game")
            path_el = ET.SubElement(game_el, "path")
            path_el.text = rel_path

            if existing:
                # Preserve all existing metadata
                for key, value in existing.items():
                    if key == "path":
                        continue  # Already set
                    child = ET.SubElement(game_el, key)
                    child.text = value
            else:
                # New game — generate a clean name
                name_el = ET.SubElement(game_el, "name")
                name_el.text = _game_name_from_filename(filename)

        # Write the gamelist
        xml_str = _pretty_xml(root)
        gamelist_path.write_text(xml_str, encoding="utf-8")
        results[system] = len(files)

    return results


def _load_existing_gamelist(path: Path) -> dict[str, dict[str, str]]:
    """Load an existing gamelist.xml and return a dict keyed by path.

    Returns {path_text: {tag: text}} for merging.
    """
    games: dict[str, dict[str, str]] = {}
    if not path.exists():
        return games

    try:
        tree = ET.parse(path)
        root = tree.getroot()
        # Handle case where root might be <gameList> or file starts with other elements
        game_elements = root.findall("game") if root.tag == "gameList" else []
        if not game_elements:
            # Try finding gameList as a child
            gl = root.find("gameList")
            if gl is not None:
                game_elements = gl.findall("game")

        for game_el in game_elements:
            path_el = game_el.find("path")
            if path_el is None or not path_el.text:
                continue
            game_data: dict[str, str] = {}
            for child in game_el:
                if child.text:
                    game_data[child.tag] = child.text
            games[path_el.text] = game_data
    except (ET.ParseError, OSError):
        pass

    return games


def _pretty_xml(root: ET.Element) -> str:
    """Convert an ElementTree to a nicely formatted XML string."""
    rough = ET.tostring(root, encoding="unicode", xml_declaration=False)
    dom = minidom.parseString(f'<?xml version="1.0"?>\n{rough}')
    lines = dom.toprettyxml(indent="\t", encoding=None)
    # minidom adds an extra xml declaration, remove duplicate
    result = "\n".join(
        line for line in lines.split("\n")
        if line.strip() and not (line.strip() == '<?xml version="1.0" ?>' and lines.index(line) > 0)
    )
    return result


def set_parse_gamelist_only(settings_path: str | Path, enabled: bool = True) -> bool:
    """Set ES-DE's ParseGamelistOnly setting.

    Args:
        settings_path: Path to es_settings.xml
        enabled: True to enable gamelist-only mode

    Returns:
        True if the setting was changed
    """
    settings_path = Path(settings_path)
    if not settings_path.exists():
        return False

    try:
        content = settings_path.read_text()
        old_value = "true" if not enabled else "false"
        new_value = "true" if enabled else "false"
        target = f'<bool name="ParseGamelistOnly" value="{old_value}" />'
        replacement = f'<bool name="ParseGamelistOnly" value="{new_value}" />'

        if target in content:
            content = content.replace(target, replacement)
            settings_path.write_text(content)
            return True
    except OSError:
        pass

    return False
