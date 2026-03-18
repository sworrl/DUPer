"""Game collection catalog for archive.org acquisition.

Defines available collections, sub-collections (greatest hits, top games, etc.),
and tracks acquisition state per collection in the database.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Archive.org collection definitions
# Each system maps to one or more IA collections with glob patterns

COLLECTIONS: dict[str, dict] = {
    "ps1": {
        "label": "PlayStation 1",
        "system": "psx",
        "region": "USA",
        "format": "CHD",
        "ia_collections": [
            f"psx-chd-roms-{letter}" for letter in "bcdefghiklmnopqrs"
        ],
        "glob": "*(USA)*.chd",
        "dest_dir": "/data/retronas/roms/psx",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
            "greatest_hits": {
                "label": "Greatest Hits",
                "filter": [
                    "Crash Bandicoot", "Crash Bandicoot 2", "Crash Bandicoot - Warped",
                    "Spyro the Dragon", "Spyro 2", "Spyro - Year of the Dragon",
                    "Metal Gear Solid", "Final Fantasy VII", "Final Fantasy VIII",
                    "Final Fantasy IX", "Resident Evil", "Resident Evil 2",
                    "Resident Evil 3", "Silent Hill", "Tekken 3",
                    "Gran Turismo", "Gran Turismo 2", "Tomb Raider",
                    "Tomb Raider II", "Castlevania - Symphony of the Night",
                    "Tony Hawk's Pro Skater", "Tony Hawk's Pro Skater 2",
                    "Twisted Metal", "Twisted Metal 2", "Syphon Filter",
                    "MediEvil", "Ape Escape", "Parappa the Rapper",
                    "Jet Moto", "Cool Boarders 2", "Medievil",
                ],
            },
            "rpgs": {
                "label": "RPGs",
                "filter": [
                    "Final Fantasy", "Chrono Cross", "Xenogears",
                    "Legend of Dragoon", "Vagrant Story", "Suikoden",
                    "Star Ocean", "Breath of Fire", "Wild Arms",
                    "Parasite Eve", "Legend of Mana", "Valkyrie Profile",
                    "Grandia", "Lunar", "Tales of",
                ],
            },
            "fighting": {
                "label": "Fighting Games",
                "filter": [
                    "Tekken", "Street Fighter", "Soul Blade",
                    "Dead or Alive", "Mortal Kombat", "Marvel vs",
                    "Rival Schools", "Bloody Roar", "Ehrgeiz",
                    "Bushido Blade", "Tobal",
                ],
            },
        },
    },
    "ps2": {
        "label": "PlayStation 2",
        "system": "ps2",
        "region": "USA",
        "format": "CHD",
        "ia_collections": [
            f"ps2-chd-roms-{letter}" for letter in "abcdefghijklmnopqrstuvwxyz"
        ],
        "glob": "*(USA)*.chd",
        "dest_dir": "/data/retronas/roms/ps2",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
            "greatest_hits": {
                "label": "Greatest Hits",
                "filter": [
                    "Grand Theft Auto - San Andreas", "Grand Theft Auto - Vice City",
                    "Grand Theft Auto III", "God of War", "God of War II",
                    "Kingdom Hearts", "Kingdom Hearts II", "Final Fantasy X",
                    "Final Fantasy XII", "Metal Gear Solid 2", "Metal Gear Solid 3",
                    "Shadow of the Colossus", "Ico", "Ratchet", "Jak and Daxter",
                    "Sly Cooper", "Devil May Cry", "Resident Evil 4",
                    "Silent Hill 2", "Silent Hill 3", "Tekken 5", "Tekken Tag",
                    "Gran Turismo 3", "Gran Turismo 4", "Guitar Hero",
                    "Tony Hawk", "SSX", "Burnout 3", "Need for Speed",
                    "Madden", "NBA Street", "Katamari Damacy",
                ],
            },
            "top_50": {
                "label": "Top 50 Rated",
                "filter": [
                    "Grand Theft Auto - San Andreas", "Metal Gear Solid 3",
                    "God of War", "Shadow of the Colossus", "Resident Evil 4",
                    "Final Fantasy X", "Kingdom Hearts", "God of War II",
                    "Grand Theft Auto - Vice City", "Ratchet & Clank - Up Your Arsenal",
                    "Jak 3", "Sly 3", "Devil May Cry 3",
                    "Prince of Persia - The Sands of Time", "Beyond Good & Evil",
                    "Okami", "Psychonauts", "Katamari Damacy",
                    "Persona 3", "Persona 4", "Dragon Quest VIII",
                    "Dark Cloud 2", "Disgaea", "Xenosaga",
                ],
            },
        },
    },
    "xbox": {
        "label": "Xbox Original",
        "system": "xbox",
        "region": "USA",
        "format": "ISO",
        "ia_collections": ["list-of-xbox-original-games-xiso.iso-format"],
        "glob": "*(USA)*.iso",
        "dest_dir": "/data/retronas/roms/xbox",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
            "top_25": {
                "label": "Top 25",
                "filter": [
                    "Halo - Combat Evolved", "Halo 2",
                    "Fable", "Star Wars - Knights of the Old Republic",
                    "Ninja Gaiden", "Jade Empire", "Panzer Dragoon Orta",
                    "Jet Set Radio Future", "Crimson Skies",
                    "MechAssault", "Project Gotham Racing",
                    "Forza Motorsport", "Splinter Cell",
                    "Prince of Persia", "Beyond Good",
                    "Psychonauts", "Oddworld", "Conker",
                    "Dead or Alive 3", "Burnout 3",
                ],
            },
        },
    },
    "dreamcast": {
        "label": "Dreamcast",
        "system": "dreamcast",
        "region": "USA",
        "format": "CHD",
        "ia_collections": ["chd_dc"],
        "glob": "*(USA)*.chd",
        "dest_dir": "/data/retronas/roms/dreamcast",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
            "essentials": {
                "label": "Essentials",
                "filter": [
                    "Shenmue", "Jet Set Radio", "Crazy Taxi",
                    "Sonic Adventure", "Soul Calibur", "Power Stone",
                    "Skies of Arcadia", "Grandia II", "Phantasy Star Online",
                    "Marvel vs. Capcom 2", "Ikaruga", "Rez",
                ],
            },
        },
    },
    "saturn": {
        "label": "Sega Saturn",
        "system": "saturn",
        "region": "USA",
        "format": "CHD",
        "ia_collections": ["chd_saturn"],
        "glob": "*(USA)*.chd",
        "dest_dir": "/data/retronas/roms/saturn",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
        },
    },
    "gamecube": {
        "label": "GameCube",
        "system": "gc",
        "region": "USA",
        "format": "ISO/NKit",
        "ia_collections": ["gamecube-usa"],
        "glob": "*(USA)*.{iso,nkit.iso,rvz}",
        "dest_dir": "/data/retronas/roms/gc",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
            "essentials": {
                "label": "Essentials",
                "filter": [
                    "Super Smash Bros", "Mario Kart", "Metroid Prime",
                    "Zelda", "Wind Waker", "Twilight Princess",
                    "Resident Evil", "Eternal Darkness", "F-Zero GX",
                    "Paper Mario", "Mario Sunshine", "Luigi's Mansion",
                    "Pikmin", "Animal Crossing", "Soul Calibur II",
                    "Tales of Symphonia",
                ],
            },
        },
    },
    "psp": {
        "label": "PlayStation Portable",
        "system": "psp",
        "region": "USA",
        "format": "ISO",
        "ia_collections": ["psp-collection"],
        "glob": "*(USA)*.iso",
        "dest_dir": "/data/retronas/roms/psp",
        "sub_collections": {
            "all": {"label": "All USA Games", "filter": None},
        },
    },
}


def get_collection(collection_id: str) -> dict | None:
    """Get a collection definition by ID."""
    return COLLECTIONS.get(collection_id)


def list_collections() -> list[dict]:
    """List all available collections with their sub-collections."""
    result = []
    for coll_id, coll in COLLECTIONS.items():
        subs = [
            {"id": sub_id, "label": sub["label"]}
            for sub_id, sub in coll.get("sub_collections", {}).items()
        ]
        result.append({
            "id": coll_id,
            "label": coll["label"],
            "system": coll["system"],
            "region": coll["region"],
            "format": coll["format"],
            "sub_collections": subs,
        })
    return result


def filter_files_by_sub_collection(
    files: list[str],
    collection_id: str,
    sub_collection_id: str = "all",
) -> list[str]:
    """Filter a file list by sub-collection criteria."""
    coll = COLLECTIONS.get(collection_id)
    if not coll:
        return files

    subs = coll.get("sub_collections", {})
    sub = subs.get(sub_collection_id)
    if not sub or not sub.get("filter"):
        return files  # "all" or no filter = return everything

    # Filter: filename must contain one of the filter strings
    filter_terms = sub["filter"]
    return [
        f for f in files
        if any(term.lower() in f.lower() for term in filter_terms)
    ]
