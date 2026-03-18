#!/bin/bash
# Build a local symlink index of RetroNAS ROMs for fast ES-DE startup.
#
# Instead of ES-DE scanning the NAS over the network (slow readdir+stat),
# this creates a local directory tree with symlinks pointing to NAS files.
# ES-DE readdir = local ext4 (instant). Game launch = follows symlink to NAS.
#
# Usage: build-rom-index.sh [local_roms_dir] [nas_roms_dir] [db_path]
#
# Typically called once, then re-run after new games are added.
# Safe to re-run: only creates missing symlinks, removes stale ones.

set -euo pipefail

LOCAL_ROMS="${1:-${HOME}/Emulation/roms}"
NAS_ROMS="${2:-/var/mnt/retronas/roms}"
DB_PATH="${3:-${HOME}/.local/share/duper/duper.db}"

echo "Building ROM symlink index"
echo "  Local: $LOCAL_ROMS"
echo "  NAS:   $NAS_ROMS"
echo "  DB:    $DB_PATH"
echo ""

# If local roms is currently a symlink to the NAS, replace it with a real dir
if [ -L "$LOCAL_ROMS" ]; then
    echo "Replacing NAS symlink with local directory..."
    rm "$LOCAL_ROMS"
    mkdir -p "$LOCAL_ROMS"
fi

mkdir -p "$LOCAL_ROMS"

# Method 1: Use DUPer DB (fast, no network needed)
# Method 2: Fall back to scanning NAS if no DB
CREATED=0
SKIPPED=0
STALE=0

if [ -f "$DB_PATH" ]; then
    echo "Using DUPer database for file index..."

    # Get all unique systems from device_transfers (files on NAS)
    SYSTEMS=$(sqlite3 "$DB_PATH" "
        SELECT DISTINCT system FROM device_transfers
        WHERE dest_host='10.99.11.8' AND dest_path='/data/retronas'
        AND system != '' AND rom_serial != 'media'
        ORDER BY system;
    " 2>/dev/null)

    # Also get systems from files table (scanned from NAS)
    SYSTEMS2=$(sqlite3 "$DB_PATH" "
        SELECT DISTINCT SUBSTR(filepath, LENGTH('${NAS_ROMS}/') + 1,
               INSTR(SUBSTR(filepath, LENGTH('${NAS_ROMS}/') + 1), '/') - 1)
        FROM files WHERE filepath LIKE '${NAS_ROMS}/%'
        ORDER BY 1;
    " 2>/dev/null)

    # Merge and deduplicate
    ALL_SYSTEMS=$(echo -e "${SYSTEMS}\n${SYSTEMS2}" | sort -u | grep -v '^$')
    SYS_COUNT=$(echo "$ALL_SYSTEMS" | wc -l)
    echo "Found $SYS_COUNT systems in database"
    echo ""

    while IFS= read -r sys; do
        [ -z "$sys" ] && continue
        mkdir -p "$LOCAL_ROMS/$sys"

        # Get filenames for this system from transfers table
        # These are the files that were transferred to the NAS
        FILENAMES=$(sqlite3 "$DB_PATH" "
            SELECT DISTINCT filename FROM device_transfers
            WHERE dest_host='10.99.11.8' AND dest_path='/data/retronas'
            AND system='${sys}' AND rom_serial != 'media' AND filename != '';
        " 2>/dev/null)

        # Also get filenames from files table (direct NAS scans)
        FILENAMES2=$(sqlite3 "$DB_PATH" "
            SELECT filename FROM files
            WHERE filepath LIKE '${NAS_ROMS}/${sys}/%';
        " 2>/dev/null)

        ALL_FILES=$(echo -e "${FILENAMES}\n${FILENAMES2}" | sort -u | grep -v '^$')

        sys_created=0
        sys_skipped=0
        while IFS= read -r fname; do
            [ -z "$fname" ] && continue
            link_path="$LOCAL_ROMS/$sys/$fname"
            target="$NAS_ROMS/$sys/$fname"

            if [ -L "$link_path" ]; then
                # Symlink exists — check if target is correct
                current_target=$(readlink "$link_path" 2>/dev/null)
                if [ "$current_target" = "$target" ]; then
                    sys_skipped=$((sys_skipped + 1))
                    continue
                fi
                rm "$link_path"
            elif [ -e "$link_path" ]; then
                # Physical file exists (offline mode?) — don't touch
                sys_skipped=$((sys_skipped + 1))
                continue
            fi

            ln -s "$target" "$link_path"
            sys_created=$((sys_created + 1))
        done <<< "$ALL_FILES"

        total_in_sys=$(echo "$ALL_FILES" | wc -l)
        if [ "$sys_created" -gt 0 ]; then
            echo "  $sys: ${sys_created} created, ${sys_skipped} existing (${total_in_sys} total)"
        fi
        CREATED=$((CREATED + sys_created))
        SKIPPED=$((SKIPPED + sys_skipped))
    done <<< "$ALL_SYSTEMS"

else
    echo "No database found — scanning NAS directly (this will be slow)..."

    for sys_dir in "$NAS_ROMS"/*/; do
        [ -d "$sys_dir" ] || continue
        sys=$(basename "$sys_dir")
        mkdir -p "$LOCAL_ROMS/$sys"

        for filepath in "$sys_dir"*; do
            [ -f "$filepath" ] || continue
            fname=$(basename "$filepath")
            link_path="$LOCAL_ROMS/$sys/$fname"

            if [ -L "$link_path" ] || [ -e "$link_path" ]; then
                SKIPPED=$((SKIPPED + 1))
                continue
            fi

            ln -s "$filepath" "$link_path"
            CREATED=$((CREATED + 1))
        done
    done
fi

# Clean up stale symlinks (point to files that no longer exist on NAS)
echo ""
echo "Checking for stale symlinks..."
while IFS= read -r link; do
    [ -L "$link" ] || continue
    target=$(readlink "$link")
    # Only remove if target looks like a NAS path and is broken
    if [[ "$target" == "$NAS_ROMS"* ]] && [ ! -e "$link" ]; then
        rm "$link"
        STALE=$((STALE + 1))
    fi
done < <(find "$LOCAL_ROMS" -type l 2>/dev/null)

# Remove empty system dirs
find "$LOCAL_ROMS" -mindepth 1 -maxdepth 1 -type d -empty -delete 2>/dev/null

echo ""
echo "Done:"
echo "  Created: $CREATED symlinks"
echo "  Existing: $SKIPPED (unchanged)"
echo "  Stale removed: $STALE"
echo ""
echo "ES-DE will now read local directories (fast) and access games via symlinks."
