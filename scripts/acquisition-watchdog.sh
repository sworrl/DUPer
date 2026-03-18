#!/bin/bash
# DUPer Acquisition Watchdog — continuously acquires games for specified collections.
# Runs each collection sequentially, loops forever. Handles failures gracefully.
# After each collection completes: registers files, rebuilds gamelists, scrapes, RA verifies.
#
# Usage: acquisition-watchdog.sh [--once] [--interval MINUTES]
#   --once       Run through all collections once then exit
#   --interval   Minutes between collection cycles (default: 30)

set -u

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
DB_PATH="${HOME}/.local/share/duper/duper.db"
DUPER_API="http://127.0.0.1:8420"
ONCE=false
INTERVAL=30

# Parse args
while [ $# -gt 0 ]; do
    case "$1" in
        --once) ONCE=true; shift ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        *) shift ;;
    esac
done

# Collections to acquire (in order)
COLLECTIONS=(
    "xbox:all"
    "saturn:all"
    "dreamcast:all"
    "ps1:all"
    "psp:all"
)

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

# Wait for DUPer API to be available
wait_for_api() {
    local tries=0
    while [ $tries -lt 30 ]; do
        curl -s "$DUPER_API/api/health" >/dev/null 2>&1 && return 0
        sleep 2
        tries=$((tries + 1))
    done
    log "ERROR: DUPer API not available"
    return 1
}

# Register all files on NAS for a system into device_transfers
register_nas_files() {
    local system="$1"
    local dest_dir="/data/retronas/roms/$system"

    ssh -o BatchMode=yes -o ConnectTimeout=5 retronas@10.99.11.8 \
        "find '$dest_dir' -maxdepth 1 -type f -printf '%f|%s\n'" 2>/dev/null | \
    while IFS='|' read -r fname fsize; do
        [ -z "$fname" ] && continue
        safe="${fname//\'/\'\'}"
        sqlite3 "$DB_PATH" "INSERT OR IGNORE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, system, status, transferred_at) VALUES ('/var/mnt/retronas/roms/${system}/${safe}', '${safe}', '10.99.11.8', '/data/retronas', ${fsize}, '${system}', 'transferred', datetime('now'));" 2>/dev/null
    done
}

# Run post-acquisition pipeline
post_acquire() {
    local system="$1"
    log "  Registering $system files in DB..."
    register_nas_files "$system"

    log "  Rebuilding gamelists..."
    "$SCRIPTS_DIR/build-game-index.sh" --db "$DB_PATH" --no-deck 2>&1 | tail -1

    log "  Scraping missing media..."
    curl -s -X POST "$DUPER_API/api/ss/scrape-missing?limit=200" >/dev/null 2>&1

    log "  RA verification..."
    curl -s -X POST "$DUPER_API/api/ra/verify-unchecked?limit=500" >/dev/null 2>&1
}

# Acquire a single collection
acquire_collection() {
    local collection="$1"
    local sub="$2"
    local coll_info

    # Get collection info
    coll_info=$(curl -s "$DUPER_API/api/acquisition/collections/$collection" 2>/dev/null)
    local system
    system=$(echo "$coll_info" | python3 -c "import json,sys; print(json.load(sys.stdin).get('system',''))" 2>/dev/null)
    local on_nas
    on_nas=$(echo "$coll_info" | python3 -c "import json,sys; print(json.load(sys.stdin).get('on_nas',0))" 2>/dev/null)

    log "Starting $collection/$sub (system: $system, on NAS: $on_nas)"

    # Cancel any stale running jobs for this collection
    sqlite3 "$DB_PATH" "UPDATE acquisition_jobs SET status='cancelled' WHERE collection_id='$collection' AND status='running';" 2>/dev/null

    # Start acquisition
    local result
    result=$(curl -s -X POST "$DUPER_API/api/acquisition/start?collection_id=$collection&sub_collection=$sub&dest_host=10.99.11.8" 2>/dev/null)
    local job_id
    job_id=$(echo "$result" | python3 -c "import json,sys; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
    local error
    error=$(echo "$result" | python3 -c "import json,sys; print(json.load(sys.stdin).get('error',''))" 2>/dev/null)

    if [ -n "$error" ] && [ "$error" != "None" ]; then
        log "  Error: $error"
        return 1
    fi

    if [ -z "$job_id" ]; then
        log "  No job ID returned"
        return 1
    fi

    log "  Job $job_id started"

    # Wait for completion
    local state_file="${HOME}/.local/share/duper/transfers/acq-${job_id}.json"
    local last_progress=""
    local stall_count=0

    while true; do
        sleep 30

        if [ ! -f "$state_file" ]; then
            stall_count=$((stall_count + 1))
            [ $stall_count -gt 10 ] && { log "  Stalled (no state file)"; break; }
            continue
        fi

        local active completed total current
        active=$(python3 -c "import json; d=json.load(open('$state_file')); print(d.get('active',False))" 2>/dev/null)
        completed=$(python3 -c "import json; d=json.load(open('$state_file')); print(d.get('completed_files',0))" 2>/dev/null)
        total=$(python3 -c "import json; d=json.load(open('$state_file')); print(d.get('total_files',0))" 2>/dev/null)
        current=$(python3 -c "import json; d=json.load(open('$state_file')); print(d.get('current_file','')[:40])" 2>/dev/null)

        local progress="${completed}/${total}"
        if [ "$progress" != "$last_progress" ]; then
            log "  Progress: $progress — $current"
            last_progress="$progress"
            stall_count=0
        else
            stall_count=$((stall_count + 1))
        fi

        if [ "$active" = "False" ] || [ "$active" = "false" ]; then
            log "  Completed: $completed/$total"
            break
        fi

        # If stalled for 15 minutes, break
        if [ $stall_count -gt 30 ]; then
            log "  Stalled for 15 min, moving on"
            break
        fi
    done

    # Post-acquisition pipeline
    post_acquire "$system"

    local new_on_nas
    new_on_nas=$(ssh -o BatchMode=yes retronas@10.99.11.8 "find /data/retronas/roms/$system -maxdepth 1 -type f 2>/dev/null | wc -l" 2>/dev/null)
    log "  $system: $new_on_nas files on NAS (was $on_nas)"
}

# ── Main Loop ────────────────────────────────────────────────────────────────

log "DUPer Acquisition Watchdog starting"
log "Collections: ${COLLECTIONS[*]}"
log "Mode: $([ "$ONCE" = true ] && echo 'single run' || echo "continuous (${INTERVAL}min interval)")"

wait_for_api || exit 1

CYCLE=0
while true; do
    CYCLE=$((CYCLE + 1))
    log "=== Cycle $CYCLE ==="

    for entry in "${COLLECTIONS[@]}"; do
        collection="${entry%%:*}"
        sub="${entry#*:}"
        acquire_collection "$collection" "$sub"
    done

    log "=== Cycle $CYCLE complete ==="

    if [ "$ONCE" = true ]; then
        log "Single run mode — exiting"
        exit 0
    fi

    log "Sleeping ${INTERVAL} minutes before next cycle..."
    sleep $((INTERVAL * 60))
done
