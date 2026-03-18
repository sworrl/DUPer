#!/bin/bash
# Build ES-DE game index: generate gamelists + set up directory symlinks.
#
# Replaces per-file symlinks with a smarter approach:
#   1. Generate gamelist.xml files from DUPer DB (ES-DE reads these instead of scanning)
#   2. Create per-system directory symlinks (ES-DE resolves paths through these)
#   3. Enable ParseGamelistOnly in ES-DE settings (skip filesystem scanning)
#
# Usage: build-game-index.sh [options]
#   --local-roms DIR    Local roms directory (default: ~/Emulation/roms)
#   --nas-roms DIR      NAS roms mount point (default: /var/mnt/retronas/roms)
#   --gamelists DIR     ES-DE gamelists directory (default: ~/ES-DE/gamelists)
#   --settings FILE     ES-DE settings file (default: ~/ES-DE/settings/es_settings.xml)
#   --db PATH           DUPer database path (default: ~/.local/share/duper/duper.db)
#   --deck-host IP      Steam Deck IP (auto-detected if omitted)
#   --no-deck           Skip Steam Deck push

set -euo pipefail

# Defaults
LOCAL_ROMS="${HOME}/Emulation/roms"
NAS_ROMS="/var/mnt/retronas/roms"
GAMELISTS="${HOME}/ES-DE/gamelists"
SETTINGS="${HOME}/ES-DE/settings/es_settings.xml"
DB_PATH="${HOME}/.local/share/duper/duper.db"
DECK_HOST=""
SKIP_DECK=false
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
DUPER_ROOT="$(cd "$SCRIPTS_DIR/.." && pwd)"

# Parse args
while [ $# -gt 0 ]; do
    case "$1" in
        --local-roms) LOCAL_ROMS="$2"; shift 2 ;;
        --nas-roms) NAS_ROMS="$2"; shift 2 ;;
        --gamelists) GAMELISTS="$2"; shift 2 ;;
        --settings) SETTINGS="$2"; shift 2 ;;
        --db) DB_PATH="$2"; shift 2 ;;
        --deck-host) DECK_HOST="$2"; shift 2 ;;
        --no-deck) SKIP_DECK=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo "Building ES-DE game index"
echo "  Gamelists: $GAMELISTS"
echo "  Local ROMs: $LOCAL_ROMS"
echo "  NAS ROMs:   $NAS_ROMS"
echo ""

# === Step 1: Generate gamelists from DUPer DB ===
echo "Generating gamelists from database..."

VENV_PYTHON="${DUPER_ROOT}/.venv/bin/python3"
if [ ! -x "$VENV_PYTHON" ]; then
    VENV_PYTHON="python3"
fi

GENERATED=$("$VENV_PYTHON" -c "
import sys
sys.path.insert(0, '${DUPER_ROOT}')
from duper.core.database import DuperDatabase
from duper.core.gamelist import generate_gamelists, set_parse_gamelist_only

db = DuperDatabase('${DB_PATH}')
db.connect()
db.initialize()

results = generate_gamelists(db, '${GAMELISTS}')
total_games = sum(results.values())
print(f'{len(results)} systems, {total_games} games')

for sys_name, count in sorted(results.items()):
    if count > 2:  # Only show systems with actual games
        print(f'  {sys_name}: {count}')

# Enable ParseGamelistOnly
changed = set_parse_gamelist_only('${SETTINGS}', enabled=True)
if changed:
    print('Enabled ParseGamelistOnly in ES-DE settings')
else:
    print('ParseGamelistOnly already enabled (or settings not found)')

db.close()
" 2>&1)

echo "$GENERATED"
echo ""

# === Step 2: Set up directory symlinks ===
echo "Setting up directory symlinks..."

# If local roms dir is full of per-file symlinks, convert to dir symlinks
# First, get list of systems from the DB
SYSTEMS=$(sqlite3 "$DB_PATH" "
    SELECT DISTINCT system FROM device_transfers
    WHERE dest_host='10.99.11.8' AND dest_path='/data/retronas'
    AND system != '' AND (rom_serial IS NULL OR rom_serial != 'media')
    ORDER BY system;
" 2>/dev/null)

CREATED=0
KEPT=0

# Ensure local roms is a real directory (not a symlink to NAS)
if [ -L "$LOCAL_ROMS" ]; then
    echo "  Replacing NAS symlink with local directory..."
    rm "$LOCAL_ROMS"
    mkdir -p "$LOCAL_ROMS"
fi
mkdir -p "$LOCAL_ROMS"

while IFS= read -r sys; do
    [ -z "$sys" ] && continue
    target="$NAS_ROMS/$sys"
    link="$LOCAL_ROMS/$sys"

    if [ -L "$link" ]; then
        # Already a symlink — check if it points to the right place
        current=$(readlink "$link" 2>/dev/null)
        if [ "$current" = "$target" ]; then
            KEPT=$((KEPT + 1))
            continue
        fi
        rm "$link"
    elif [ -d "$link" ]; then
        # Real directory with per-file symlinks — replace with dir symlink
        # Check if it only contains symlinks (safe to replace)
        real_files=$(find "$link" -maxdepth 1 -type f 2>/dev/null | wc -l)
        if [ "$real_files" -eq 0 ]; then
            rm -rf "$link"
        else
            # Has real files (offline mode?) — keep as-is
            KEPT=$((KEPT + 1))
            continue
        fi
    fi

    ln -s "$target" "$link"
    CREATED=$((CREATED + 1))
done <<< "$SYSTEMS"

echo "  $CREATED directory symlinks created, $KEPT unchanged"

# Prune system symlinks with no gamelist (ES-DE wastes time checking empty systems)
PRUNED=0
for link in "$LOCAL_ROMS"/*/; do
    [ -L "${link%/}" ] || continue
    sys=$(basename "$link")
    if [ ! -d "$GAMELISTS/$sys" ]; then
        rm "$LOCAL_ROMS/$sys"
        PRUNED=$((PRUNED + 1))
    fi
done
[ "$PRUNED" -gt 0 ] && echo "  Pruned $PRUNED empty system symlinks"
echo ""

# === Step 3: Push to Steam Deck if online ===
if [ "$SKIP_DECK" = false ]; then
    if [ -z "$DECK_HOST" ]; then
        DECK_HOST=$(arp -a 2>/dev/null | grep -i "deamsteck\|steamdeck" | grep -oP '\d+\.\d+\.\d+\.\d+' | head -1)
    fi

    if [ -n "$DECK_HOST" ]; then
        echo "Pushing to Steam Deck ($DECK_HOST)..."

        # Find Deck paths
        DECK_GAMELISTS=$(ssh -o BatchMode=yes -o ConnectTimeout=3 "deck@${DECK_HOST}" \
            "find /home/deck -maxdepth 3 -type d -name gamelists 2>/dev/null | head -1" 2>/dev/null || echo "")
        DECK_ROMS=$(ssh -o BatchMode=yes -o ConnectTimeout=3 "deck@${DECK_HOST}" \
            "find /home/deck -maxdepth 4 -type d -name roms 2>/dev/null | head -1" 2>/dev/null || echo "")
        DECK_SETTINGS=$(ssh -o BatchMode=yes -o ConnectTimeout=3 "deck@${DECK_HOST}" \
            "find /home/deck -maxdepth 4 -name es_settings.xml 2>/dev/null | head -1" 2>/dev/null || echo "")

        if [ -n "$DECK_GAMELISTS" ]; then
            # Sync gamelists
            rsync -az --delete -e "ssh -o BatchMode=yes" \
                "$GAMELISTS/" "deck@${DECK_HOST}:${DECK_GAMELISTS}/" 2>/dev/null && \
                echo "  Gamelists synced" || echo "  Gamelist sync failed"

            # Copy DB
            ssh -o BatchMode=yes "deck@${DECK_HOST}" "mkdir -p /home/deck/.local/share/duper" 2>/dev/null
            scp -o BatchMode=yes "$DB_PATH" "deck@${DECK_HOST}:/home/deck/.local/share/duper/duper.db" 2>/dev/null && \
                echo "  Database synced" || echo "  DB sync failed"

            # Set up dir symlinks on Deck
            if [ -n "$DECK_ROMS" ]; then
                ssh -o BatchMode=yes "deck@${DECK_HOST}" "
                    NAS='/var/mnt/retronas/roms'
                    ROMS='${DECK_ROMS}'
                    # Replace per-file symlinks with dir symlinks
                    for sys_dir in \"\$ROMS\"/*/; do
                        [ -d \"\$sys_dir\" ] || continue
                        sys=\$(basename \"\$sys_dir\")
                        # Skip if already a dir symlink to NAS
                        if [ -L \"\$ROMS/\$sys\" ]; then continue; fi
                        # If dir only has symlinks, replace with dir symlink
                        real_files=\$(find \"\$sys_dir\" -maxdepth 1 -type f 2>/dev/null | wc -l)
                        if [ \"\$real_files\" -eq 0 ] && [ -d \"\$NAS/\$sys\" ]; then
                            rm -rf \"\$ROMS/\$sys\"
                            ln -s \"\$NAS/\$sys\" \"\$ROMS/\$sys\"
                        fi
                    done
                    # Prune empty system symlinks
                    for link in \"\$ROMS\"/*/; do
                        [ -L \"\${link%/}\" ] || continue
                        sys=\$(basename \"\$link\")
                        if [ ! -d \"${DECK_GAMELISTS}/\$sys\" ]; then
                            rm \"\$ROMS/\$sys\"
                        fi
                    done
                    echo 'Dir symlinks updated + pruned'
                " 2>/dev/null
            fi

            # Enable ParseGamelistOnly on Deck
            if [ -n "$DECK_SETTINGS" ]; then
                ssh -o BatchMode=yes "deck@${DECK_HOST}" \
                    "sed -i 's/ParseGamelistOnly\" value=\"false\"/ParseGamelistOnly\" value=\"true\"/' '${DECK_SETTINGS}'" 2>/dev/null && \
                    echo "  ParseGamelistOnly enabled on Deck" || echo "  Settings update failed"
            fi
        else
            echo "  Could not find ES-DE gamelists dir on Deck"
        fi
    else
        echo "Steam Deck not found on network (skipping)"
    fi
fi

echo ""
echo "Done. ES-DE will load games from gamelists (no filesystem scanning)."
