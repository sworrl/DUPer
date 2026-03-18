#!/bin/bash
# DUPer Transfer Worker — runs independently, writes JSON status to STATE_FILE
# Now checks DUPer database to skip already-transferred files.
#
# Usage: transfer-worker.sh <source_dir> <dest_host> <dest_path> <state_file> [db_path]

set -u
SOURCE="$1"
DEST_HOST="$2"
DEST_PATH="$3"
STATE_FILE="$4"
DB_PATH="${5:-${HOME}/.local/share/duper/duper.db}"
ROMS_DIR="$SOURCE/roms"

write_state() {
    local tmp="${STATE_FILE}.tmp"
    cat > "$tmp" << STATEEOF
{
  "active": $1,
  "source": "$SOURCE",
  "dest": "${DEST_HOST}:${DEST_PATH}",
  "method": "scp",
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
  "errors": [],
  "systems_done": [$SYSTEMS_DONE_JSON],
  "systems_remaining": [$SYSTEMS_REMAINING_JSON]
}
STATEEOF
    mv "$tmp" "$STATE_FILE"
}

# Check if a file is already recorded as transferred in the DB
is_transferred() {
    local filepath="$1"
    local filesize="$2"
    if [ ! -f "$DB_PATH" ]; then
        return 1  # No DB = not transferred
    fi
    local count
    count=$(sqlite3 "$DB_PATH" \
        "SELECT COUNT(*) FROM device_transfers WHERE filepath='${filepath}' AND dest_host='${DEST_HOST}' AND file_size=${filesize} AND status='transferred';" 2>/dev/null)
    [ "$count" = "1" ]
}

# Record a successful transfer in the DB
record_transfer() {
    local filepath="$1"
    local filename="$2"
    local filesize="$3"
    local system="$4"
    local now
    now=$(date -Iseconds)

    if [ ! -f "$DB_PATH" ]; then
        return
    fi

    # Look up md5 and rom_serial from files table if available
    local md5="" rom_serial=""
    md5=$(sqlite3 "$DB_PATH" "SELECT COALESCE(md5,'') FROM files WHERE filepath='${filepath}';" 2>/dev/null || echo "")
    rom_serial=$(sqlite3 "$DB_PATH" "SELECT COALESCE(rom_serial,'') FROM files WHERE filepath='${filepath}';" 2>/dev/null || echo "")

    sqlite3 "$DB_PATH" "INSERT OR REPLACE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, md5, rom_serial, system, status, transferred_at) VALUES ('${filepath}', '${filename}', '${DEST_HOST}', '${DEST_PATH}', ${filesize}, '${md5}', '${rom_serial}', '${system}', 'transferred', '${now}');" 2>/dev/null
}

# === Phase 0: Verify DB against remote, rescan only mismatched systems ===
# First run: full scan. Subsequent runs: quick count-based verification.

CURRENT_FILE=""
CURRENT_SYSTEM="VERIFYING"
TOTAL_FILES=0
TRANSFERRED_FILES=0
SKIPPED_FILES=0
TOTAL_BYTES=0
TRANSFERRED_BYTES=0
SKIPPED_BYTES=0
SPEED_BPS=0
ETA_SECONDS=0
STARTED_AT=$(date -Iseconds)
SYSTEMS_DONE_JSON=""
SYSTEMS_REMAINING_JSON=""

if [ -f "$DB_PATH" ]; then
    # Ensure the device_transfers table exists
    sqlite3 "$DB_PATH" "CREATE TABLE IF NOT EXISTS device_transfers (
        transfer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        filepath TEXT NOT NULL,
        filename TEXT NOT NULL,
        dest_host TEXT NOT NULL,
        dest_path TEXT NOT NULL,
        file_size INTEGER DEFAULT 0,
        md5 TEXT DEFAULT '',
        rom_serial TEXT DEFAULT '',
        system TEXT DEFAULT '',
        status TEXT DEFAULT 'transferred',
        transferred_at TEXT NOT NULL,
        verified_at TEXT,
        UNIQUE(filepath, dest_host, dest_path)
    );" 2>/dev/null

    DB_TOTAL=$(sqlite3 "$DB_PATH" \
        "SELECT COUNT(*) FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}';" 2>/dev/null || echo 0)

    # Get per-system counts from remote (fast: one SSH call)
    REMOTE_COUNTS=$(ssh -o BatchMode=yes -o ConnectTimeout=10 "retronas@${DEST_HOST}" \
        "cd ${DEST_PATH}/roms 2>/dev/null && for d in */; do [ -d \"\$d\" ] || continue; d=\"\${d%/}\"; c=\$(find \"\$d\" -maxdepth 1 -type f 2>/dev/null | wc -l); [ \"\$c\" -gt 0 ] && echo \"\$d|\$c\"; done" 2>/dev/null || echo "")

    declare -A REMOTE_SYS_COUNTS
    REMOTE_FILE_TOTAL=0
    while IFS='|' read -r rsys rcount; do
        [ -z "$rsys" ] && continue
        REMOTE_SYS_COUNTS[$rsys]=$rcount
        REMOTE_FILE_TOTAL=$((REMOTE_FILE_TOTAL + rcount))
    done <<< "$REMOTE_COUNTS"
    echo "Remote: ${REMOTE_FILE_TOTAL} files, DB: ${DB_TOTAL} records" >&2

    # Find mismatched systems
    RESCAN_SYSTEMS=()
    if [ "$DB_TOTAL" -eq 0 ]; then
        for rsys in "${!REMOTE_SYS_COUNTS[@]}"; do
            RESCAN_SYSTEMS+=("$rsys")
        done
        echo "Bootstrap: scanning all ${#RESCAN_SYSTEMS[@]} remote systems" >&2
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
        [ ${#RESCAN_SYSTEMS[@]} -eq 0 ] && echo "All systems verified" >&2
    fi

    # Rescan only mismatched systems
    if [ ${#RESCAN_SYSTEMS[@]} -gt 0 ]; then
        CURRENT_SYSTEM="SCANNING_REMOTE"
        write_state true

        for rsys in "${RESCAN_SYSTEMS[@]}"; do
            SYS_FILES=$(ssh -o BatchMode=yes -o ConnectTimeout=5 "retronas@${DEST_HOST}" \
                "cd ${DEST_PATH}/roms/${rsys} 2>/dev/null && find . -maxdepth 1 -type f -printf '%f|%s\n' 2>/dev/null" 2>/dev/null || echo "")
            [ -z "$SYS_FILES" ] && continue

            NOW=$(date -Iseconds)
            SQL_BATCH=$(mktemp /tmp/duper-transfer-batch-XXXXXX.sql)
            echo "BEGIN TRANSACTION;" > "$SQL_BATCH"
            echo "DELETE FROM device_transfers WHERE dest_host='${DEST_HOST}' AND dest_path='${DEST_PATH}' AND system='${rsys}';" >> "$SQL_BATCH"

            while IFS='|' read -r filename filesize; do
                [ -z "$filename" ] && continue
                local_path="${ROMS_DIR}/${rsys}/${filename}"
                [ -f "$local_path" ] || continue
                local_size=$(stat -c '%s' "$local_path" 2>/dev/null || echo 0)
                safe_path="${local_path//\'/\'\'}"
                safe_name="${filename//\'/\'\'}"
                echo "INSERT OR IGNORE INTO device_transfers (filepath, filename, dest_host, dest_path, file_size, md5, rom_serial, system, status, transferred_at) SELECT '${safe_path}', '${safe_name}', '${DEST_HOST}', '${DEST_PATH}', ${local_size}, COALESCE((SELECT md5 FROM files WHERE filepath='${safe_path}'),''), COALESCE((SELECT rom_serial FROM files WHERE filepath='${safe_path}'),''), '${rsys}', 'transferred', '${NOW}';" >> "$SQL_BATCH"
            done <<< "$SYS_FILES"

            echo "COMMIT;" >> "$SQL_BATCH"
            sqlite3 "$DB_PATH" < "$SQL_BATCH" 2>/dev/null
            rm -f "$SQL_BATCH"
        done
        echo "Rescan of ${#RESCAN_SYSTEMS[@]} systems complete" >&2
    fi
fi

# Pre-load all transferred filepaths from DB into a set (one query)
declare -A ALREADY_TRANSFERRED
if [ -f "$DB_PATH" ]; then
    while IFS='|' read -r tpath tsize; do
        [ -n "$tpath" ] && ALREADY_TRANSFERRED["${tpath}|${tsize}"]=1
    done < <(sqlite3 "$DB_PATH" "SELECT filepath, file_size FROM device_transfers WHERE dest_host='${DEST_HOST}' AND status='transferred';" 2>/dev/null)
    echo "Loaded ${#ALREADY_TRANSFERRED[@]} transfer records from DB" >&2
fi

# First pass: count files and check which need transferring
TOTAL_FILES=0
TOTAL_BYTES=0
NEED_TRANSFER_FILES=0
NEED_TRANSFER_BYTES=0
SKIPPED_FILES=0
SKIPPED_BYTES=0
SYSTEMS=()

declare -A SKIP_MAP  # filepath -> 1 if should skip

for sys_dir in "$ROMS_DIR"/*/; do
    [ -d "$sys_dir" ] || continue
    sys=$(basename "$sys_dir")
    sys_has_files=false

    for raw_filepath in "$sys_dir"*; do
        [ -f "$raw_filepath" ] || continue
        sys_has_files=true
        filename=$(basename "$raw_filepath")
        # Canonical path: avoids double-slash from glob trailing /
        filepath="${ROMS_DIR}/${sys}/${filename}"
        filesize=$(stat -c '%s' "$filepath" 2>/dev/null || echo 0)
        TOTAL_FILES=$((TOTAL_FILES + 1))
        TOTAL_BYTES=$((TOTAL_BYTES + filesize))

        if [ -n "${ALREADY_TRANSFERRED["${filepath}|${filesize}"]+x}" ]; then
            SKIP_MAP["$filepath"]=1
            SKIPPED_FILES=$((SKIPPED_FILES + 1))
            SKIPPED_BYTES=$((SKIPPED_BYTES + filesize))
        else
            NEED_TRANSFER_FILES=$((NEED_TRANSFER_FILES + 1))
            NEED_TRANSFER_BYTES=$((NEED_TRANSFER_BYTES + filesize))
        fi
    done

    if [ "$sys_has_files" = true ]; then
        SYSTEMS+=("$sys")
    fi
done

TRANSFERRED_FILES=0
TRANSFERRED_BYTES=0
CURRENT_FILE=""
CURRENT_SYSTEM=""
SPEED_BPS=0
ETA_SECONDS=0
STARTED_AT=$(date -Iseconds)
SYSTEMS_DONE=()
SYSTEMS_REMAINING=("${SYSTEMS[@]}")

# Build JSON arrays
systems_to_json() {
    local arr=("$@")
    local result=""
    for s in "${arr[@]}"; do
        [ -n "$result" ] && result="$result,"
        result="$result\"$s\""
    done
    echo "$result"
}

SYSTEMS_DONE_JSON=""
SYSTEMS_REMAINING_JSON=$(systems_to_json "${SYSTEMS_REMAINING[@]}")

# Update total to reflect only files needing transfer for progress tracking
# But keep original totals for the state output
TOTAL_FILES=$NEED_TRANSFER_FILES
TOTAL_BYTES=$NEED_TRANSFER_BYTES
write_state true

START_TIME=$(date +%s)

for sys in "${SYSTEMS[@]}"; do
    CURRENT_SYSTEM="$sys"
    sys_dir="$ROMS_DIR/$sys"

    # Remove from remaining, track in JSON
    SYSTEMS_REMAINING=("${SYSTEMS_REMAINING[@]/$sys/}")
    SYSTEMS_REMAINING=($(echo "${SYSTEMS_REMAINING[@]}" | tr ' ' '\n' | grep -v '^$'))
    SYSTEMS_REMAINING_JSON=$(systems_to_json "${SYSTEMS_REMAINING[@]}")

    # Check if this system has any files to transfer
    sys_needs_transfer=false
    for raw_fp in "$sys_dir"/*; do
        [ -f "$raw_fp" ] || continue
        canon="${ROMS_DIR}/${sys}/$(basename "$raw_fp")"
        if [ -z "${SKIP_MAP[$canon]+x}" ]; then
            sys_needs_transfer=true
            break
        fi
    done

    if [ "$sys_needs_transfer" = false ]; then
        # All files in this system already transferred, skip entirely
        SYSTEMS_DONE+=("$sys")
        SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
        write_state true
        continue
    fi

    # Create remote dir
    ssh -o BatchMode=yes -o ConnectTimeout=5 "retronas@${DEST_HOST}" \
        "mkdir -p ${DEST_PATH}/roms/${sys}" 2>/dev/null

    # Transfer each file that isn't already there
    for raw_fp in "$sys_dir"/*; do
        [ -f "$raw_fp" ] || continue
        filename=$(basename "$raw_fp")
        filepath="${ROMS_DIR}/${sys}/${filename}"
        filesize=$(stat -c '%s' "$filepath" 2>/dev/null || echo 0)

        # Skip if already transferred
        if [ -n "${SKIP_MAP[$filepath]+x}" ]; then
            continue
        fi

        CURRENT_FILE="$filename"
        write_state true

        scp -o BatchMode=yes -o StrictHostKeyChecking=no \
            "$filepath" "retronas@${DEST_HOST}:${DEST_PATH}/roms/${sys}/" 2>/dev/null

        if [ $? -eq 0 ]; then
            TRANSFERRED_FILES=$((TRANSFERRED_FILES + 1))
            TRANSFERRED_BYTES=$((TRANSFERRED_BYTES + filesize))
            # Record in database
            record_transfer "$filepath" "$filename" "$filesize" "$sys"
        fi

        # Calculate speed and ETA
        NOW=$(date +%s)
        ELAPSED=$((NOW - START_TIME))
        if [ "$ELAPSED" -gt 0 ]; then
            SPEED_BPS=$((TRANSFERRED_BYTES / ELAPSED))
            REMAINING=$((TOTAL_BYTES - TRANSFERRED_BYTES))
            if [ "$SPEED_BPS" -gt 0 ]; then
                ETA_SECONDS=$((REMAINING / SPEED_BPS))
            fi
        fi

        write_state true
    done

    # System complete
    SYSTEMS_DONE+=("$sys")
    SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
    write_state true
done

# Also transfer bios, saves, and media
for subdir in bios saves; do
    src="$SOURCE/$subdir"
    if [ -d "$src" ]; then
        CURRENT_SYSTEM="$subdir"
        CURRENT_FILE="$subdir"
        write_state true
        # Use rsync if available for smarter sync, fall back to scp
        if command -v rsync &>/dev/null; then
            rsync -az --ignore-existing -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
                "$src/" "retronas@${DEST_HOST}:${DEST_PATH}/${subdir}/" 2>/dev/null
        else
            scp -r -o BatchMode=yes -o StrictHostKeyChecking=no \
                "$src" "retronas@${DEST_HOST}:${DEST_PATH}/${subdir}" 2>/dev/null
        fi
        SYSTEMS_DONE+=("$subdir")
        SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
    fi
done

# Transfer downloaded media (box art, screenshots, videos)
MEDIA_DIR="$SOURCE/tools/downloaded_media"
if [ -d "$MEDIA_DIR" ]; then
    CURRENT_SYSTEM="media"
    ssh -o BatchMode=yes -o ConnectTimeout=5 "retronas@${DEST_HOST}" \
        "mkdir -p ${DEST_PATH}/media" 2>/dev/null
    for media_sys in "$MEDIA_DIR"/*/; do
        [ -d "$media_sys" ] || continue
        sys=$(basename "$media_sys")
        CURRENT_FILE="media/$sys"
        write_state true
        if command -v rsync &>/dev/null; then
            rsync -az --ignore-existing -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
                "$media_sys" "retronas@${DEST_HOST}:${DEST_PATH}/media/${sys}/" 2>/dev/null
        else
            scp -r -o BatchMode=yes -o StrictHostKeyChecking=no \
                "$media_sys" "retronas@${DEST_HOST}:${DEST_PATH}/media/${sys}" 2>/dev/null
        fi
    done
    SYSTEMS_DONE+=("media")
    SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
fi

# Transfer storage (RetroArch configs, emulator data)
STORAGE_DIR="$SOURCE/storage"
if [ -d "$STORAGE_DIR" ]; then
    CURRENT_SYSTEM="storage"
    CURRENT_FILE="storage"
    write_state true
    if command -v rsync &>/dev/null; then
        rsync -az --ignore-existing -e "ssh -o BatchMode=yes -o StrictHostKeyChecking=no" \
            "$STORAGE_DIR/" "retronas@${DEST_HOST}:${DEST_PATH}/storage/" 2>/dev/null
    else
        scp -r -o BatchMode=yes -o StrictHostKeyChecking=no \
            "$STORAGE_DIR" "retronas@${DEST_HOST}:${DEST_PATH}/storage" 2>/dev/null
    fi
    SYSTEMS_DONE+=("storage")
    SYSTEMS_DONE_JSON=$(systems_to_json "${SYSTEMS_DONE[@]}")
fi

# Rebuild ES-DE game index on all devices
CURRENT_SYSTEM="REBUILDING_INDEX"
CURRENT_FILE="Updating gamelists..."
write_state true

SCRIPTS_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -x "$SCRIPTS_DIR/build-game-index.sh" ]; then
    "$SCRIPTS_DIR/build-game-index.sh" --db "$DB_PATH" 2>&1 | tail -5
fi

# Sync browse media locally for stutter-free menus
CURRENT_FILE="Syncing browse media..."
write_state true
if [ -x "$SCRIPTS_DIR/sync-media-cache.sh" ]; then
    "$SCRIPTS_DIR/sync-media-cache.sh" 2>&1 | tail -3
fi

CURRENT_FILE=""
CURRENT_SYSTEM="COMPLETE"
write_state false
