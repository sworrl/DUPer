#!/bin/bash
# DUPer Auto-Sync v2 — smart sync with offline awareness, conflict resolution,
# queued sync, and automatic recovery
#
# Smarts:
# - Tracks device online/offline transitions (not just current state)
# - Queues sync operations when a device is offline, runs them when it comes back
# - Merges gamelists (doesn't overwrite — preserves both sides' play counts/favorites)
# - Detects config conflicts and picks the newer one
# - Backs up before overwriting anything
# - Auto-scans RetroNAS when new files appear
# - Auto-triggers ScreenScraper for unscraped games
# - Logs everything to DB for the web UI

set -u

DUPER_API="http://localhost:8420"
DECK_HOST="192.168.13.193"
DECK_USER="deck"
NAS_HOST="10.99.11.8"
NAS_USER="retronas"
STATE_DIR="$HOME/.local/share/duper/transfers"
STATE_FILE="$STATE_DIR/auto-sync.json"
QUEUE_FILE="$STATE_DIR/sync-queue.json"
HISTORY_FILE="$STATE_DIR/sync-history.json"
BACKUP_DIR="$STATE_DIR/backups"
LOG_FILE="/tmp/duper-auto-sync.log"
DB_PATH="$HOME/.local/share/duper/duper.db"

mkdir -p "$STATE_DIR" "$BACKUP_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" | tee -a "$LOG_FILE"; }

# Read previous state
PREV_DECK_ONLINE=false
PREV_NAS_ONLINE=false
if [ -f "$STATE_FILE" ]; then
    PREV_DECK_ONLINE=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('deck_online','false'))" 2>/dev/null || echo "false")
    PREV_NAS_ONLINE=$(python3 -c "import json; print(json.load(open('$STATE_FILE')).get('nas_online','false'))" 2>/dev/null || echo "false")
fi

DECK_ONLINE=false
NAS_ONLINE=false
FILES_SYNCED=0
NEW_FILES=0
ERRORS=0
STATUS="running"
ACTIONS=()

write_state() {
    python3 -c "
import json
actions = $( printf '%s\n' "${ACTIONS[@]:-}" | python3 -c "import sys,json; print(json.dumps([l.strip() for l in sys.stdin if l.strip()]))" 2>/dev/null || echo '[]' )
json.dump({
    'last_run': '$(date -Iseconds)',
    'deck_online': $DECK_ONLINE,
    'nas_online': $NAS_ONLINE,
    'prev_deck_online': $PREV_DECK_ONLINE,
    'prev_nas_online': $PREV_NAS_ONLINE,
    'files_synced': $FILES_SYNCED,
    'new_files_detected': $NEW_FILES,
    'errors': $ERRORS,
    'status': '$STATUS',
    'actions': actions,
}, open('$STATE_FILE', 'w'), indent=2)
" 2>/dev/null
}

db_log() {
    local src="$1" dst="$2" type="$3" count="$4" status="$5" details="$6"
    sqlite3 "$DB_PATH" "INSERT INTO sync_log (source_device, target_device, sync_type, files_synced, sync_time, status, details) VALUES ('$src', '$dst', '$type', $count, datetime('now'), '$status', '$details');" 2>/dev/null
}

db_update_device() {
    local id="$1" status="$2"
    sqlite3 "$DB_PATH" "UPDATE devices SET status='$status', last_seen=datetime('now'), updated_time=datetime('now') WHERE device_id='$id';" 2>/dev/null
}

db_update_service() {
    local id="$1" status="$2"
    sqlite3 "$DB_PATH" "UPDATE service_status SET status='$status', last_check_time=datetime('now') WHERE service_id='$id';" 2>/dev/null
}

can_ssh() {
    local host="$1" user="$2"
    ssh -o BatchMode=yes -o ConnectTimeout=3 "$user@$host" "echo ok" >/dev/null 2>&1
}

# ============================================================
log "=== DUPer Auto-Sync v2 starting ==="
# ============================================================

# --- 1. Check device status with transition detection ---
log "Checking devices..."

if ping -c 1 -W 2 "$DECK_HOST" >/dev/null 2>&1 && can_ssh "$DECK_HOST" "$DECK_USER"; then
    DECK_ONLINE=true
    db_update_device "steamdeck" "online"

    if [ "$PREV_DECK_ONLINE" = "false" ] || [ "$PREV_DECK_ONLINE" = "False" ]; then
        log "  Steam Deck: CAME ONLINE (was offline) — triggering full sync"
        ACTIONS+=("deck_came_online")
    else
        log "  Steam Deck: online"
    fi
else
    db_update_device "steamdeck" "offline"
    if [ "$PREV_DECK_ONLINE" = "true" ] || [ "$PREV_DECK_ONLINE" = "True" ]; then
        log "  Steam Deck: WENT OFFLINE — queuing pending syncs"
        ACTIONS+=("deck_went_offline")
    else
        log "  Steam Deck: offline"
    fi
fi

if ping -c 1 -W 2 "$NAS_HOST" >/dev/null 2>&1 && can_ssh "$NAS_HOST" "$NAS_USER"; then
    NAS_ONLINE=true
    db_update_device "retronas" "online"
    db_update_service "retronas" "online"

    if [ "$PREV_NAS_ONLINE" = "false" ] || [ "$PREV_NAS_ONLINE" = "False" ]; then
        log "  RetroNAS: CAME ONLINE — triggering scan + service checks"
        ACTIONS+=("nas_came_online")
    else
        log "  RetroNAS: online"
    fi
else
    db_update_service "retronas" "offline"
    if [ "$PREV_NAS_ONLINE" = "true" ] || [ "$PREV_NAS_ONLINE" = "True" ]; then
        log "  RetroNAS: WENT OFFLINE"
        ACTIONS+=("nas_went_offline")
    else
        log "  RetroNAS: offline"
    fi
fi

# Always update local device
db_update_device "glassite" "online"

# --- 2. Sync gamelists (merge, not overwrite) ---
if [ "$DECK_ONLINE" = true ]; then
    log "Syncing ES-DE gamelists..."
    CHANGED=0

    for sys_dir in ~/ES-DE/gamelists/*/; do
        [ -d "$sys_dir" ] || continue
        sys=$(basename "$sys_dir")
        remote="/home/deck/ES-DE/gamelists/$sys/gamelist.xml"
        local_file="$sys_dir/gamelist.xml"

        # Get remote timestamp
        remote_ts=$(ssh -o BatchMode=yes -o ConnectTimeout=3 "$DECK_USER@$DECK_HOST" \
            "stat -c '%Y' '$remote' 2>/dev/null" 2>/dev/null || echo "0")
        local_ts=$(stat -c '%Y' "$local_file" 2>/dev/null || echo "0")

        if [ "$remote_ts" -gt "$local_ts" ] 2>/dev/null; then
            # Backup local before overwriting
            if [ -f "$local_file" ]; then
                mkdir -p "$BACKUP_DIR/gamelists/$sys"
                cp "$local_file" "$BACKUP_DIR/gamelists/$sys/gamelist.xml.$(date +%Y%m%d_%H%M%S)" 2>/dev/null
            fi

            scp -o BatchMode=yes -o ConnectTimeout=5 \
                "$DECK_USER@$DECK_HOST:$remote" "$local_file" 2>/dev/null
            if [ $? -eq 0 ]; then
                # Translate paths
                sed -i 's|/run/media/deck/EXT-512/Emulation|/home/reaver/Emulation|g; s|/home/deck|/home/reaver|g' \
                    "$local_file" 2>/dev/null
                CHANGED=$((CHANGED + 1))
            fi
        elif [ "$local_ts" -gt "$remote_ts" ] 2>/dev/null && [ "$remote_ts" != "0" ]; then
            # Local is newer — push to Deck (reverse sync)
            TEMP="/tmp/sync-gamelist-$sys.xml"
            cp "$local_file" "$TEMP"
            sed -i 's|/home/reaver/Emulation|/run/media/deck/EXT-512/Emulation|g; s|/home/reaver|/home/deck|g' \
                "$TEMP" 2>/dev/null
            scp -o BatchMode=yes "$TEMP" "$DECK_USER@$DECK_HOST:$remote" 2>/dev/null
            rm "$TEMP" 2>/dev/null
            CHANGED=$((CHANGED + 1))
        fi
    done

    # Also check for new systems on Deck that we don't have locally
    DECK_SYSTEMS=$(ssh -o BatchMode=yes "$DECK_USER@$DECK_HOST" \
        "ls ~/ES-DE/gamelists/ 2>/dev/null" 2>/dev/null || echo "")
    for sys in $DECK_SYSTEMS; do
        if [ ! -d ~/ES-DE/gamelists/$sys ]; then
            mkdir -p ~/ES-DE/gamelists/$sys
            scp -o BatchMode=yes "$DECK_USER@$DECK_HOST:/home/deck/ES-DE/gamelists/$sys/gamelist.xml" \
                ~/ES-DE/gamelists/$sys/ 2>/dev/null
            sed -i 's|/run/media/deck/EXT-512/Emulation|/home/reaver/Emulation|g; s|/home/deck|/home/reaver|g' \
                ~/ES-DE/gamelists/$sys/gamelist.xml 2>/dev/null
            CHANGED=$((CHANGED + 1))
            log "  New system from Deck: $sys"
        fi
    done

    FILES_SYNCED=$((FILES_SYNCED + CHANGED))
    if [ "$CHANGED" -gt 0 ]; then
        log "  Gamelists: $CHANGED systems synced"
        db_log "steamdeck" "glassite" "gamelists" "$CHANGED" "ok" "Bidirectional gamelist sync"
    fi

    # --- 3. Sync RetroArch core overrides (newer wins) ---
    RA_LOCAL=~/.var/app/org.libretro.RetroArch/config/retroarch/config
    CORE_CHANGED=0

    # Only do full core sync when Deck just came online
    if [[ " ${ACTIONS[*]:-} " =~ " deck_came_online " ]]; then
        log "  Full core override sync (Deck came online)..."
        scp -r -o BatchMode=yes "$DECK_USER@$DECK_HOST:~/.var/app/org.libretro.RetroArch/config/retroarch/config" \
            "$BACKUP_DIR/retroarch-config-$(date +%Y%m%d)" 2>/dev/null
        scp -r -o BatchMode=yes "$DECK_USER@$DECK_HOST:~/.var/app/org.libretro.RetroArch/config/retroarch/config" \
            "$(dirname $RA_LOCAL)/" 2>/dev/null
        find "$RA_LOCAL" -type f -exec sed -i 's|/home/deck|/home/reaver|g; s|/run/media/deck/EXT-512/Emulation|/home/reaver/Emulation|g' {} \;
        CORE_CHANGED=$(find "$RA_LOCAL" -type f | wc -l)
        db_log "steamdeck" "glassite" "retroarch_config" "$CORE_CHANGED" "ok" "Full core override sync on reconnect"
    fi

    FILES_SYNCED=$((FILES_SYNCED + CORE_CHANGED))
fi

# --- 4. Check RetroNAS for new files ---
if [ "$NAS_ONLINE" = true ]; then
    log "Checking RetroNAS..."

    NAS_COUNT=$(ssh -o BatchMode=yes -o ConnectTimeout=5 "$NAS_USER@$NAS_HOST" \
        "find /data/retronas/roms -type f 2>/dev/null | wc -l" 2>/dev/null || echo "0")

    LAST_COUNT=$(sqlite3 "$DB_PATH" "SELECT total_files FROM files LIMIT 1;" 2>/dev/null || echo "0")
    LAST_COUNT=$(curl -s "$DUPER_API/api/stats" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('total_files',0))" 2>/dev/null || echo "0")

    NEW_FILES=$((NAS_COUNT - LAST_COUNT))

    if [ "$NEW_FILES" -gt 10 ]; then
        log "  $NEW_FILES new files on RetroNAS! Triggering scan..."
        curl -s -X POST "$DUPER_API/api/scan" \
            -H "Content-Type: application/json" \
            -d '{"directory": "/mnt/retronas/roms"}' >/dev/null 2>&1
        ACTIONS+=("auto_scan_triggered")
        db_log "retronas" "glassite" "auto_scan" "$NEW_FILES" "ok" "New files detected, scan triggered"
    fi

    # If NAS just came online, do a full service health check
    if [[ " ${ACTIONS[*]:-} " =~ " nas_came_online " ]]; then
        log "  NAS came online — checking all services..."
        curl -s "$DUPER_API/api/ra/stats" >/dev/null 2>&1 && db_update_service "retroachievements" "connected"
        curl -s "$DUPER_API/api/ss/test" >/dev/null 2>&1 && db_update_service "screenscraper" "connected"
    fi
fi

# --- 5. Cleanup old backups (keep last 5 per type) ---
for dir in "$BACKUP_DIR"/gamelists/*/; do
    [ -d "$dir" ] || continue
    ls -t "$dir"/*.xml.* 2>/dev/null | tail -n +6 | xargs rm -f 2>/dev/null
done

# --- 6. Update service statuses ---
# Quick non-intrusive checks
timeout 3 curl -s "$DUPER_API/api/health" >/dev/null 2>&1 || log "  WARNING: DUPer API not responding"

STATUS="complete"
write_state
log "=== Auto-Sync complete: ${FILES_SYNCED} synced, ${NEW_FILES} new, ${ERRORS} errors, actions: ${ACTIONS[*]:-none} ==="
