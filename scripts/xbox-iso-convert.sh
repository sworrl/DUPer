#!/bin/bash
# Convert redump Xbox ISOs to XISO format for xemu compatibility.
# Redump ISOs have a video/dashboard partition before the game data.
# XISO is just the game partition extracted out.
#
# Usage: xbox-iso-convert.sh <input.iso> [output.iso]
#        xbox-iso-convert.sh --batch <directory>

set -euo pipefail

# XDVDFS magic string
MAGIC="MICROSOFT*XBOX*MEDIA"

find_game_partition() {
    local file="$1"
    # Common game partition offsets (sector * 2048):
    # 0x30600 * 0x800 = 0x18300000 (standard redump)
    # 0x00000 (already an XISO)
    for base_offset in 0x18300000 0x00000; do
        magic_offset=$(( base_offset + 0x10000 ))
        local found
        found=$(dd if="$file" bs=1 skip=$magic_offset count=20 2>/dev/null | strings -n 10)
        if echo "$found" | grep -q "MICROSOFT"; then
            echo "$base_offset"
            return 0
        fi
    done
    return 1
}

convert_one() {
    local input="$1"
    local output="$2"
    local filename
    filename=$(basename "$input")

    # Find the game partition
    local offset
    offset=$(find_game_partition "$input") || {
        echo "SKIP: $filename — no XDVDFS signature found (not an Xbox ISO?)"
        return 1
    }

    if [ "$offset" = "0x00000" ] || [ "$offset" = "0" ]; then
        echo "SKIP: $filename — already in XISO format"
        # If output differs from input, just copy
        if [ "$input" != "$output" ]; then
            cp "$input" "$output"
        fi
        return 0
    fi

    local input_size
    input_size=$(stat -c '%s' "$input")
    local game_size=$(( input_size - offset ))

    echo "CONVERT: $filename"
    echo "  Partition offset: $offset ($((offset / 1024 / 1024))MB)"
    echo "  Game data size: $((game_size / 1024 / 1024))MB"

    # Extract game partition using dd
    dd if="$input" of="$output" bs=1M skip=$((offset / 1024 / 1024)) status=progress 2>&1

    # Verify the output
    local verify
    verify=$(dd if="$output" bs=1 skip=$((0x10000)) count=20 2>/dev/null | strings -n 10)
    if echo "$verify" | grep -q "MICROSOFT"; then
        echo "  OK: Verified XDVDFS signature in output"
        return 0
    else
        echo "  ERROR: Output verification failed"
        rm -f "$output"
        return 1
    fi
}

# --- Main ---

if [ "${1:-}" = "--batch" ]; then
    DIR="${2:-.}"
    echo "Batch converting Xbox ISOs in: $DIR"
    converted=0
    skipped=0
    failed=0

    for iso in "$DIR"/*.iso; do
        [ -f "$iso" ] || continue
        filename=$(basename "$iso")

        # Check if already XISO
        offset=$(find_game_partition "$iso" 2>/dev/null) || { echo "SKIP: $filename — not Xbox"; skipped=$((skipped+1)); continue; }

        if [ "$offset" = "0x00000" ] || [ "$offset" = "0" ]; then
            echo "SKIP: $filename — already XISO"
            skipped=$((skipped+1))
            continue
        fi

        # Convert in-place: write to temp, then replace
        tmp="${iso}.converting"
        if convert_one "$iso" "$tmp"; then
            mv "$tmp" "$iso"
            converted=$((converted+1))
        else
            rm -f "$tmp"
            failed=$((failed+1))
        fi
    done

    echo ""
    echo "Done: $converted converted, $skipped skipped, $failed failed"
else
    INPUT="${1:?Usage: xbox-iso-convert.sh <input.iso> [output.iso]}"
    OUTPUT="${2:-${INPUT%.iso}.xiso.iso}"

    if [ "$INPUT" = "$OUTPUT" ]; then
        # In-place conversion
        TMP="${INPUT}.converting"
        convert_one "$INPUT" "$TMP" && mv "$TMP" "$INPUT"
    else
        convert_one "$INPUT" "$OUTPUT"
    fi
fi
