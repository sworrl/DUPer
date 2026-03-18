#!/bin/bash
# Sync ES-DE browse media (covers, miximages, screenshots, marquees) locally.
# Videos, manuals, fanart stay on NAS — only loaded when viewing a specific game.
#
# This eliminates menu stutter: ES-DE reads local images during scrolling,
# only hits NAS when launching a game or viewing its video.
#
# Usage: sync-media-cache.sh [local_media_dir] [nas_media_dir]
# Runs automatically after transfers/acquisitions. Safe to re-run anytime.

set -euo pipefail

LOCAL_MEDIA="${1:-${HOME}/Emulation/tools/downloaded_media}"
NAS_MEDIA="${2:-/var/mnt/retronas/media}"

# Categories to cache locally (small images used during browsing)
CACHE_CATEGORIES="covers miximages screenshots marquees titlescreens 3dboxes"

# Categories to leave on NAS (large files, only loaded on-demand)
# videos, manuals, fanart, backcovers, physicalmedia

echo "Syncing browse media locally"
echo "  Local: $LOCAL_MEDIA"
echo "  NAS:   $NAS_MEDIA"
echo ""

# Ensure local media dir is a real directory (not a NAS symlink)
if [ -L "$LOCAL_MEDIA" ]; then
    echo "Replacing NAS symlink with local directory..."
    rm "$LOCAL_MEDIA"
    mkdir -p "$LOCAL_MEDIA"

    # Immediately create per-system symlinks to NAS as fallback
    # so media is never missing during sync
    for sys_dir in "$NAS_MEDIA"/*/; do
        [ -d "$sys_dir" ] || continue
        sys=$(basename "$sys_dir")
        [ "$sys" = "Emulation" ] && continue
        ln -s "$NAS_MEDIA/$sys" "$LOCAL_MEDIA/$sys" 2>/dev/null
    done
    echo "  Created NAS fallback symlinks for all systems"
fi
mkdir -p "$LOCAL_MEDIA"

TOTAL_SYNCED=0

# Get list of systems that have media on NAS
for sys_dir in "$NAS_MEDIA"/*/; do
    [ -d "$sys_dir" ] || continue
    sys=$(basename "$sys_dir")
    [ "$sys" = "Emulation" ] && continue  # Skip stray dir

    # If this system dir is still a NAS symlink, replace with real dir
    if [ -L "$LOCAL_MEDIA/$sys" ]; then
        rm "$LOCAL_MEDIA/$sys"
        mkdir -p "$LOCAL_MEDIA/$sys"
    fi

    sys_synced=0
    for cat in $CACHE_CATEGORIES; do
        nas_cat="$NAS_MEDIA/$sys/$cat"
        [ -d "$nas_cat" ] || continue

        local_cat="$LOCAL_MEDIA/$sys/$cat"
        mkdir -p "$local_cat"

        # rsync: only copy new/changed files, skip existing
        rsync -a --ignore-existing "$nas_cat/" "$local_cat/" 2>/dev/null
        sys_synced=$((sys_synced + 1))
    done

    # For categories NOT cached locally, create symlinks to NAS
    for cat in videos manuals fanart backcovers physicalmedia; do
        nas_cat="$NAS_MEDIA/$sys/$cat"
        local_cat="$LOCAL_MEDIA/$sys/$cat"
        [ -d "$nas_cat" ] || continue
        # Only create symlink if local dir doesn't exist (or is already a symlink)
        if [ ! -e "$local_cat" ]; then
            ln -s "$nas_cat" "$local_cat"
        fi
    done

    [ "$sys_synced" -gt 0 ] && TOTAL_SYNCED=$((TOTAL_SYNCED + 1))
done

echo "Synced $TOTAL_SYNCED systems"
echo ""

# Show local cache size
LOCAL_SIZE=$(du -sh "$LOCAL_MEDIA" 2>/dev/null | cut -f1)
echo "Local media cache: $LOCAL_SIZE"
echo "Done. ES-DE will read browse images locally (no NFS stutter)."
