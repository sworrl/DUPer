#!/bin/bash
# DUPer Media Transfer Worker — transfers downloaded_media (box art, screenshots, videos)
# Uses rsync --ignore-existing for smart incremental sync + DB tracking.
#
# Usage: media-worker.sh <source_media_dir> <dest_host> <dest_path> <state_file> [db_path]

set -u
SOURCE="$1"
DEST_HOST="$2"
DEST_PATH="$3"
STATE_FILE="$4"
DB_PATH="${5:-${HOME}/.local/share/duper/duper.db}"

TRANSFERRED_FILES=0
TRANSFERRED_BYTES=0
SKIPPED_FILES=0
SKIPPED_BYTES=0
TOTAL_FILES=0
TOTAL_BYTES=0
CURRENT_FILE=""
CURRENT_SYSTEM=""
SPEED_BPS=0
ETA_SECONDS=0
STARTED_AT=$(date -Iseconds)
SYSTEMS_DONE_JSON=""
SYSTEMS_REMAINING_JSON=""
SYSTEMS_DONE=()

write_state() {
    local tmp="${STATE_FILE}.tmp"
    cat > "$tmp" << STATEEOF
{
  "active": $1,
  "type": "media",
  "source": "$SOURCE",
  "dest": "${DEST_HOST}:${DEST_PATH}",
  "total_files": $TOTAL_FILES,
  "transferred_files": $TRANSFERRED_FILES,
  "skipped_files": $SKIPPED_FILES,
  "total_bytes": $TOTAL_BYTES,
  "transferred_bytes": $TRANSFERRED_BYTES,
  "skipped_bytes": $SKIPPED_BYTES,
  "current_file": "$CURRENT_FILE",
  "current_system": "$CURRENT_SYSTEM",
  "speed_bps": $SPEED_BPS,
  "eta_seconds": $ETA_SECONDS,
  "started_at": "$STARTED_AT",
  "pid": $$,
  "systems_done": [$SYSTEMS_DONE_JSON],
  "systems_remaining": [$SYSTEMS_REMAINING_JSON]
}
STATEEOF
    mv "$tmp" "$STATE_FILE"
}

systems_to_json() {
    local result=""
    for s in "$@"; do
        [ -z "$s" ] && continue
        [ -n "$result" ] && result="$result,"
        result="$result\"$s\""
    done
    echo "$result"
}

# === Phase 0: Verify DB against remote and sync records ===
# First run: full remote scan to bootstrap DB.
# Subsequent runs: quick per-system count check. Only rescan systems with mismatches.

CURRENT_SYSTEM="VERIFYING"
write_state true

if [ -f "$DB_PATH" ]; then
    DB_TOTAL=$(sqlite3 "$DB_PATH" \
        "SELECT COUNT(*) FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}';" 2>/dev/null || echo 0)
else
    DB_TOTAL=0
fi

# Get per-system file counts from remote (fast: one SSH, no per-file stat)
REMOTE_COUNTS=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "retronas@${DEST_HOST}" \
    "cd '${DEST_PATH}' 2>/dev/null && for d in */; do [ -d \"\$d\" ] || continue; d=\"\${d%/}\"; c=\$(find \"\$d\" -type f 2>/dev/null | wc -l); [ \"\$c\" -gt 0 ] && echo \"\$d|\$c\"; done" 2>/dev/null || echo "")

REMOTE_FILE_TOTAL=0
declare -A REMOTE_SYS_COUNTS
while IFS='|' read -r rsys rcount; do
    [ -z "$rsys" ] && continue
    REMOTE_SYS_COUNTS[$rsys]=$rcount
    REMOTE_FILE_TOTAL=$((REMOTE_FILE_TOTAL + rcount))
done <<< "$REMOTE_COUNTS"
echo "Remote: ${REMOTE_FILE_TOTAL} files across ${#REMOTE_SYS_COUNTS[@]} systems, DB: ${DB_TOTAL} records" >&2

# Find systems with count mismatches — only rescan those
RESCAN_SYSTEMS=()
if [ "$DB_TOTAL" -eq 0 ]; then
    # Bootstrap: need to scan everything
    for rsys in "${!REMOTE_SYS_COUNTS[@]}"; do
        RESCAN_SYSTEMS+=("$rsys")
    done
    echo "Bootstrap mode: scanning all ${#RESCAN_SYSTEMS[@]} remote systems" >&2
else
    for rsys in "${!REMOTE_SYS_COUNTS[@]}"; do
        remote_c=${REMOTE_SYS_COUNTS[$rsys]}
        db_c=$(sqlite3 "$DB_PATH" \
            "SELECT COUNT(*) FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}' AND system='${rsys}';" 2>/dev/null || echo 0)
        if [ "$remote_c" -ne "$db_c" ]; then
            RESCAN_SYSTEMS+=("$rsys")
            echo "  Mismatch: $rsys — remote=$remote_c DB=$db_c" >&2
        fi
    done
    if [ ${#RESCAN_SYSTEMS[@]} -eq 0 ]; then
        echo "All systems verified — DB matches remote" >&2
    else
        echo "Rescanning ${#RESCAN_SYSTEMS[@]} mismatched systems" >&2
    fi
fi

# Rescan only mismatched systems
if [ ${#RESCAN_SYSTEMS[@]} -gt 0 ] && [ -f "$DB_PATH" ]; then
    CURRENT_SYSTEM="SCANNING_REMOTE"
    write_state true

    for rsys in "${RESCAN_SYSTEMS[@]}"; do
        REMOTE_SYS_FILES=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "retronas@${DEST_HOST}" \
            "cd '${DEST_PATH}/${rsys}' 2>/dev/null && find . -type f -printf '%P|%s\n' 2>/dev/null" 2>/dev/null || echo "")

        [ -z "$REMOTE_SYS_FILES" ] && continue

        NOW=$(date -Iseconds)
        SQL_BATCH=$(mktemp /tmp/duper-media-batch-XXXXXX.sql)
        echo "BEGIN TRANSACTION;" > "$SQL_BATCH"
        # Clear stale records for this system, then re-insert
        echo "DELETE FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}' AND system='${rsys}';" >> "$SQL_BATCH"

        while IFS='|' read -r rel_path filesize; do
            [ -z "$rel_path" ] && continue
            [ -z "$filesize" ] && filesize=0
            local_path="${SOURCE}/${rsys}/${rel_path}"
            safe_path="${local_path//\'/\'\'}"
            safe_name="$(basename "$rel_path")"
            safe_name="${safe_name//\'/\'\'}"
            echo "INSERT OR IGNORE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, md5, rom_serial, system, status, transferred_at) VALUES ('${safe_path}', '${safe_name}', '${DEST_HOST}', '${DEST_PATH}', ${filesize}, '', 'media', '${rsys}', 'transferred', '${NOW}');" >> "$SQL_BATCH"
        done <<< "$REMOTE_SYS_FILES"

        echo "COMMIT;" >> "$SQL_BATCH"
        sqlite3 "$DB_PATH" < "$SQL_BATCH" 2>/dev/null
        rm -f "$SQL_BATCH"
    done
    echo "Rescan complete" >&2
fi

# === Phase 1: Pre-load transferred media set from DB ===
declare -A ALREADY_TRANSFERRED
if [ -f "$DB_PATH" ]; then
    while IFS='|' read -r tpath tsize; do
        [ -n "$tpath" ] && ALREADY_TRANSFERRED["${tpath}|${tsize}"]=1
    done < <(sqlite3 "$DB_PATH" "SELECT filepath, file_size FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}' AND status='transferred';" 2>/dev/null)
    echo "Loaded ${#ALREADY_TRANSFERRED[@]} media transfer records from DB" >&2
fi

# === Phase 2: Count per system using find + du (fast, no per-file stat) ===
# For skip counting, compare per-system totals: remote vs local.
# Per-file skip tracking only matters for the transfer phase (rsync handles it).
SYSTEMS=()
declare -A SYS_FILES SYS_BYTES SYS_NEW_FILES SYS_NEW_BYTES

for sys_dir in "$SOURCE"/*/; do
    [ -d "$sys_dir" ] || continue
    sys=$(basename "$sys_dir")

    sys_total=$(find "$sys_dir" -type f 2>/dev/null | wc -l)
    [ "$sys_total" -eq 0 ] && continue

    sys_bytes=$(du -sb "$sys_dir" 2>/dev/null | cut -f1)
    [ -z "$sys_bytes" ] && sys_bytes=0

    # Count how many of this system's files are in the transferred set
    sys_skipped=0
    sys_skipped_bytes=0
    while IFS='|' read -r tpath tsize; do
        [ -n "$tpath" ] && sys_skipped=$((sys_skipped + 1)) && sys_skipped_bytes=$((sys_skipped_bytes + tsize))
    done < <(sqlite3 "$DB_PATH" "SELECT filepath, file_size FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}' AND system='${sys}' AND status='transferred';" 2>/dev/null)

    sys_new=$((sys_total - sys_skipped))
    [ "$sys_new" -lt 0 ] && sys_new=0
    sys_new_bytes=$((sys_bytes - sys_skipped_bytes))
    [ "$sys_new_bytes" -lt 0 ] && sys_new_bytes=0

    SYSTEMS+=("$sys")
    SYS_FILES[$sys]=$sys_total
    SYS_BYTES[$sys]=$sys_bytes
    SYS_NEW_FILES[$sys]=$sys_new
    SYS_NEW_BYTES[$sys]=$sys_new_bytes
    TOTAL_FILES=$((TOTAL_FILES + sys_new))
    TOTAL_BYTES=$((TOTAL_BYTES + sys_new_bytes))
    SKIPPED_FILES=$((SKIPPED_FILES + sys_skipped))
    SKIPPED_BYTES=$((SKIPPED_BYTES + sys_skipped_bytes))
done

SYSTEMS_REMAINING=("${SYSTEMS[@]}")
SYSTEMS_REMAINING_JSON=$(systems_to_json "${SYSTEMS_REMAINING[@]}")
write_state true

START_TIME=$(date +%s)

ssh -o BatchMode=yes -o ConnectTimeout=5 "retronas@${DEST_HOST}" \
    "mkdir -p '${DEST_PATH}'" 2>/dev/null

# === Phase 3: Transfer new media per system ===
for sys in "${SYSTEMS[@]}"; do
    CURRENT_SYSTEM="$sys"
    new_count=${SYS_NEW_FILES[$sys]:-0}
    new_bytes=${SYS_NEW_BYTES[$sys]:-0}

    # Remove from remaining
    SYSTEMS_REMAINING=("${SYSTEMS_REMAINING[@]/$sys/}")
    SYSTEMS_REMAINING=($(echo "${SYSTEMS_REMAINING[@]}" | tr ' ' '\n' | grep -v '^$'))
    SYSTEMS_REMAINING_JSON=$(systems_to_json "${SYSTEMS_REMAINING[@]}")

    if [ "$new_count" -eq 0 ]; then
        # All media for this system already transferred
        SYSTEMS_DONE+=("$sys")
        SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
        write_state true
        continue
    fi

    CURRENT_FILE="$sys/ (${new_count} new files)"
    write_state true

    # Use rsync --ignore-existing for smart sync
    if command -v rsync &>/dev/null; then
        rsync -az --ignore-existing -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
            "$SOURCE/$sys/" "retronas@${DEST_HOST}:${DEST_PATH}/${sys}/" 2>/dev/null
    else
        ssh -o BatchMode=yes "retronas@${DEST_HOST}" "mkdir -p '${DEST_PATH}/${sys}'" 2>/dev/null
        scp -r -o BatchMode=yes -o StrictHostKeyChecking=no \
            "$SOURCE/$sys" "retronas@${DEST_HOST}:${DEST_PATH}/" 2>/dev/null
    fi

    if [ $? -eq 0 ]; then
        TRANSFERRED_FILES=$((TRANSFERRED_FILES + new_count))
        TRANSFERRED_BYTES=$((TRANSFERRED_BYTES + new_bytes))

        # Record in DB (batch insert)
        if [ -f "$DB_PATH" ]; then
            NOW=$(date -Iseconds)
            SQL_BATCH=$(mktemp /tmp/duper-media-record-XXXXXX.sql)
            echo "BEGIN TRANSACTION;" > "$SQL_BATCH"
            while IFS= read -r filepath; do
                [ -f "$filepath" ] || continue
                filesize=$(stat -c '%s' "$filepath" 2>/dev/null || echo 0)
                safe_path="${filepath//\'/\'\'}"
                safe_name="$(basename "$filepath")"
                safe_name="${safe_name//\'/\'\'}"
                echo "INSERT OR IGNORE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, md5, rom_serial, system, status, transferred_at) VALUES ('${safe_path}', '${safe_name}', '${DEST_HOST}', '${DEST_PATH}', ${filesize}, '', 'media', '${sys}', 'transferred', '${NOW}');" >> "$SQL_BATCH"
            done < <(find "$SOURCE/$sys" -type f 2>/dev/null)
            echo "COMMIT;" >> "$SQL_BATCH"
            sqlite3 "$DB_PATH" < "$SQL_BATCH" 2>/dev/null
            rm -f "$SQL_BATCH"
        fi
    fi

    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TIME))
    if [ "$ELAPSED" -gt 0 ]; then
        SPEED_BPS=$((TRANSFERRED_BYTES / ELAPSED))
        REMAINING=$((TOTAL_BYTES - TRANSFERRED_BYTES))
        if [ "$SPEED_BPS" -gt 0 ]; then
            ETA_SECONDS=$((REMAINING / SPEED_BPS))
        fi
    fi

    SYSTEMS_DONE+=("$sys")
    SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
    write_state true
done

CURRENT_FILE=""
CURRENT_SYSTEM="COMPLETE"
write_state false
