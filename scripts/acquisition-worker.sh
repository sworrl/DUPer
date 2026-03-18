#!/bin/bash
# DUPer Acquisition Worker — downloads from archive.org, writes JSON status
# Runs independently of DUPer. One file at a time.
#
# Usage: acquisition-worker.sh <collection> <dest_host> <state_file> [sub_collection] [db_path] [job_id]

set -u
COLLECTION="$1"
DEST_HOST="$2"
STATE_FILE="$3"
SUB_COLLECTION="${4:-all}"
DB_PATH="${5:-${HOME}/.local/share/duper/duper.db}"
JOB_ID="${6:-}"
IA="/var/home/reaver/Documents/GitHub/DUPer/.venv/bin/ia"
SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"

write_state() {
    local tmp="${STATE_FILE}.tmp"
    cat > "$tmp" << STATEEOF
{
  "active": $ACTIVE,
  "paused": false,
  "collection": "$COLLECTION",
  "collection_label": "$COLLECTION_LABEL",
  "sub_collection": "$SUB_COLLECTION",
  "job_id": "$JOB_ID",
  "dest_host": "$DEST_HOST",
  "total_files": $TOTAL_FILES,
  "completed_files": $COMPLETED_FILES,
  "failed_files": $FAILED_FILES,
  "skipped_files": $SKIPPED_FILES,
  "total_bytes_downloaded": $TOTAL_BYTES_DL,
  "current_file": "$CURRENT_FILE",
  "current_file_size": $CURRENT_FILE_SIZE,
  "current_file_downloaded": $CURRENT_FILE_DL,
  "current_speed_bps": $SPEED_BPS,
  "current_eta_seconds": $ETA_SECONDS,
  "started_at": "$STARTED_AT",
  "pid": $$,
  "queue": [$QUEUE_JSON],
  "completed": [$COMPLETED_JSON],
  "errors": []
}
STATEEOF
    mv "$tmp" "$STATE_FILE"

    # Update job in DB
    if [ -n "$JOB_ID" ] && [ -f "$DB_PATH" ]; then
        sqlite3 "$DB_PATH" "UPDATE acquisition_jobs SET total_files=$TOTAL_FILES, completed_files=$COMPLETED_FILES, failed_files=$FAILED_FILES, skipped_files=$SKIPPED_FILES, total_bytes=$TOTAL_BYTES_DL, pid=$$ WHERE job_id='$JOB_ID';" 2>/dev/null
    fi
}

add_completed() {
    local name="$1" size="$2" status="$3" speed="$4"
    local time_str=$(date +%H:%M:%S)
    local entry="{\"name\":\"$name\",\"size\":$size,\"status\":\"$status\",\"time\":\"$time_str\",\"speed\":$speed}"
    if [ -n "$COMPLETED_JSON" ]; then
        COMPLETED_JSON="$entry,$COMPLETED_JSON"
    else
        COMPLETED_JSON="$entry"
    fi
    # Keep last 20
    COMPLETED_JSON=$(echo "[$COMPLETED_JSON]" | python3 -c "import json,sys; d=json.load(sys.stdin)[:20]; print(','.join(json.dumps(x) for x in d))" 2>/dev/null || echo "$COMPLETED_JSON")
}

# Build file list from collections catalog (Python-driven)
VENV_PYTHON="${SCRIPTS_DIR}/../.venv/bin/python3"

COLL_INFO=$("$VENV_PYTHON" -c "
import sys; sys.path.insert(0, '${SCRIPTS_DIR}/..')
from duper.core.collections import get_collection
c = get_collection('${COLLECTION}')
if not c:
    print('ERROR')
else:
    print(f'{c[\"label\"]}')
    print(f'{c[\"dest_dir\"]}')
    print(f'{c[\"glob\"]}')
    for ia in c['ia_collections']:
        print(f'IA:{ia}')
" 2>/dev/null)

COLLECTION_LABEL=$(echo "$COLL_INFO" | head -1)
DEST_DIR=$(echo "$COLL_INFO" | sed -n '2p')
GLOB_PATTERN=$(echo "$COLL_INFO" | sed -n '3p')
IA_COLLECTIONS=()
while IFS= read -r line; do
    [[ "$line" == IA:* ]] && IA_COLLECTIONS+=("${line#IA:}")
done <<< "$COLL_INFO"

if [ "$COLLECTION_LABEL" = "ERROR" ] || [ -z "$DEST_DIR" ]; then
    echo "Unknown collection: $COLLECTION" >&2
    exit 1
fi

# Get existing files on NAS
EXISTING=$(ssh -o BatchMode=yes -o ConnectTimeout=5 "retronas@${DEST_HOST}" \
    "ls '$DEST_DIR' 2>/dev/null" 2>/dev/null || echo "")

# Scan all IA collections for files matching glob
FILES=()
ALL_FOUND=()
for ia_coll in "${IA_COLLECTIONS[@]}"; do
    while IFS= read -r fname; do
        [ -z "$fname" ] && continue
        # Skip if already on NAS
        if echo "$EXISTING" | grep -qF "$fname"; then
            continue
        fi
        ALL_FOUND+=("$ia_coll|$fname")
    done < <($IA list "$ia_coll" --glob="$GLOB_PATTERN" 2>/dev/null)
done

# Apply sub-collection filter if not "all"
if [ "$SUB_COLLECTION" != "all" ]; then
    FILTER_FILE=$(mktemp /tmp/duper-acq-filter-XXXXXX.txt)
    printf '%s\n' "${ALL_FOUND[@]}" > "$FILTER_FILE"
    FILTERED=$("$VENV_PYTHON" -c "
import sys; sys.path.insert(0, '${SCRIPTS_DIR}/..')
from duper.core.collections import filter_files_by_sub_collection
files = [line.split('|',1)[1] for line in open('${FILTER_FILE}').read().strip().split('\n') if '|' in line]
colls = [line.split('|',1)[0] for line in open('${FILTER_FILE}').read().strip().split('\n') if '|' in line]
filtered = filter_files_by_sub_collection(files, '${COLLECTION}', '${SUB_COLLECTION}')
filtered_set = set(filtered)
for c, f in zip(colls, files):
    if f in filtered_set:
        print(f'{c}|{f}')
" 2>/dev/null)
    rm -f "$FILTER_FILE"
    while IFS= read -r line; do
        [ -n "$line" ] && FILES+=("$line")
    done <<< "$FILTERED"
else
    FILES=("${ALL_FOUND[@]}")
fi

TOTAL_FILES=${#FILES[@]}
COMPLETED_FILES=0
FAILED_FILES=0
SKIPPED_FILES=0
TOTAL_BYTES_DL=0
CURRENT_FILE=""
CURRENT_FILE_SIZE=0
CURRENT_FILE_DL=0
SPEED_BPS=0
ETA_SECONDS=0
STARTED_AT=$(date -Iseconds)
ACTIVE=true
QUEUE_JSON=""
COMPLETED_JSON=""

# Build initial queue JSON (first 50)
q_items=""
for i in $(seq 0 $((${#FILES[@]} < 50 ? ${#FILES[@]} - 1 : 49))); do
    fname="${FILES[$i]#*|}"
    [ -n "$q_items" ] && q_items="$q_items,"
    q_items="$q_items\"$fname\""
done
QUEUE_JSON="$q_items"

ssh -o BatchMode=yes "retronas@${DEST_HOST}" "mkdir -p '$DEST_DIR'" 2>/dev/null
write_state

# Download loop — 4 concurrent downloads (archive.org limit is ~5)
MAX_PARALLEL=4
ACTIVE_PIDS=()
RESULT_DIR=$(mktemp -d /tmp/duper-acq-results-XXXXXX)

download_one() {
    local idx="$1"
    local entry="${FILES[$idx]}"
    local coll="${entry%%|*}"
    local fname="${entry#*|}"
    local result_file="$RESULT_DIR/$idx"

    local encoded
    encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$fname'))" 2>/dev/null)
    local url="https://archive.org/download/${coll}/${encoded}"

    local start_time=$(date +%s)
    local dl_result
    dl_result=$(ssh -o BatchMode=yes "retronas@${DEST_HOST}" \
        "curl -sL -o '${DEST_DIR}/${fname}' -w '%{size_download}' '${url}'" 2>/dev/null)

    local end_time=$(date +%s)
    local elapsed=$((end_time - start_time))
    [ "$elapsed" -lt 1 ] && elapsed=1

    local size=0
    [ -n "$dl_result" ] && size=$(echo "$dl_result" | tr -cd '0-9')
    [ -z "$size" ] && size=0

    if [ "$size" -gt 1000 ]; then
        local speed=$((size / elapsed))
        echo "ok|$fname|$size|$speed" > "$result_file"
    else
        ssh -o BatchMode=yes "retronas@${DEST_HOST}" "rm -f '${DEST_DIR}/${fname}'" 2>/dev/null
        echo "fail|$fname|0|0" > "$result_file"
    fi
}

collect_results() {
    for rf in "$RESULT_DIR"/*; do
        [ -f "$rf" ] || continue
        IFS='|' read -r status fname size speed < "$rf"
        if [ "$status" = "ok" ]; then
            COMPLETED_FILES=$((COMPLETED_FILES + 1))
            TOTAL_BYTES_DL=$((TOTAL_BYTES_DL + size))
            SPEED_BPS=$speed
            add_completed "$fname" "$size" "ok" "$speed"
        else
            FAILED_FILES=$((FAILED_FILES + 1))
            add_completed "$fname" "0" "fail" "0"
        fi
        rm "$rf"
    done
}

i=0
while [ $i -lt $TOTAL_FILES ]; do
    # Launch up to MAX_PARALLEL downloads
    ACTIVE_PIDS=()
    BATCH_END=$((i + MAX_PARALLEL))
    [ "$BATCH_END" -gt "$TOTAL_FILES" ] && BATCH_END=$TOTAL_FILES

    CURRENT_FILE="downloading ${BATCH_END}/${TOTAL_FILES} (${MAX_PARALLEL} parallel)"

    # Update queue
    q_items=""
    for j in $(seq $BATCH_END $((BATCH_END + 10 < TOTAL_FILES ? BATCH_END + 10 : TOTAL_FILES - 1))); do
        qfname="${FILES[$j]#*|}"
        [ -n "$q_items" ] && q_items="$q_items,"
        q_items="$q_items\"$qfname\""
    done
    QUEUE_JSON="$q_items"
    write_state

    for j in $(seq $i $((BATCH_END - 1))); do
        download_one "$j" &
        ACTIVE_PIDS+=($!)
    done

    # Wait for all in this batch
    for pid in "${ACTIVE_PIDS[@]}"; do
        wait "$pid" 2>/dev/null
    done

    # Collect results
    collect_results
    write_state

    i=$BATCH_END
done

rm -rf "$RESULT_DIR"

# Register acquired files in DB and rebuild indexes
if [ "$COMPLETED_FILES" -gt 0 ] && [ -f "$DB_PATH" ]; then
    # Record acquired files in device_transfers
    NOW=$(date -Iseconds)
    SQL_BATCH=$(mktemp /tmp/duper-acq-record-XXXXXX.sql)
    echo "BEGIN TRANSACTION;" > "$SQL_BATCH"
    for i in $(seq 0 $((TOTAL_FILES - 1))); do
        entry="${FILES[$i]}"
        fname="${entry#*|}"
        safe_name="${fname//\'/\'\'}"
        echo "INSERT OR IGNORE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, system, status, transferred_at) VALUES ('/var/mnt/retronas/roms/${DEST_DIR##*/}/${safe_name}', '${safe_name}', '${DEST_HOST}', '/data/retronas', 0, '${DEST_DIR##*/}', 'transferred', '${NOW}');" >> "$SQL_BATCH"
    done
    echo "COMMIT;" >> "$SQL_BATCH"
    sqlite3 "$DB_PATH" < "$SQL_BATCH" 2>/dev/null
    rm -f "$SQL_BATCH"

    # Rebuild ES-DE game index on all devices
    if [ -x "$SCRIPTS_DIR/build-game-index.sh" ]; then
        "$SCRIPTS_DIR/build-game-index.sh" --db "$DB_PATH" 2>&1 | tail -5
    fi
fi

# Sync browse media locally
if [ -x "$SCRIPTS_DIR/sync-media-cache.sh" ]; then
    "$SCRIPTS_DIR/sync-media-cache.sh" 2>&1 | tail -3
fi

ACTIVE=false
CURRENT_FILE=""
CURRENT_SYSTEM="COMPLETE"
write_state

# Mark job complete in DB
if [ -n "$JOB_ID" ] && [ -f "$DB_PATH" ]; then
    sqlite3 "$DB_PATH" "UPDATE acquisition_jobs SET status='completed', completed_at='$(date -Iseconds)' WHERE job_id='$JOB_ID';" 2>/dev/null
fi
