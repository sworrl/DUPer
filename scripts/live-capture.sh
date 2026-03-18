#!/bin/bash
# DUPer Live Game Capture — captures ONLY RetroArch game content.
# Uses RetroArch's network command interface to take in-game screenshots.
# NEVER captures the desktop — only active RetroArch game frames.
#
# Usage: live-capture.sh [interval_seconds] [output_dir]

INTERVAL="${1:-10}"
OUTPUT_DIR="${2:-${HOME}/.local/share/duper/live}"
RA_SCREENSHOT_DIR="${HOME}/.var/app/org.libretro.RetroArch/config/retroarch/screenshots"

mkdir -p "$OUTPUT_DIR" "$RA_SCREENSHOT_DIR"

echo '{"active": true, "interval": '$INTERVAL', "pid": '$$', "mode": "retroarch_only"}' > "$OUTPUT_DIR/state.json"

cleanup() {
    echo '{"active": false}' > "$OUTPUT_DIR/state.json"
    exit 0
}
trap cleanup SIGTERM SIGINT

FRAME=0
while true; do
    # Only capture if RetroArch is running with a game loaded
    RA_PID=$(pgrep -f "retroarch" | head -1)

    if [ -n "$RA_PID" ]; then
        # Use RetroArch's UDP command interface to trigger screenshot
        # RetroArch listens on UDP port 55355 by default when network_cmd_enable is true
        echo "SCREENSHOT" | socat - UDP:127.0.0.1:55355 2>/dev/null

        # Wait for screenshot to be written
        sleep 1

        # Find the latest screenshot RetroArch saved
        LATEST=$(ls -t "$RA_SCREENSHOT_DIR"/*.png 2>/dev/null | head -1)

        if [ -n "$LATEST" ] && [ -f "$LATEST" ]; then
            # Convert to small JPEG for web
            ffmpeg -y -i "$LATEST" -vf "scale=1280:-1" -q:v 5 "$OUTPUT_DIR/latest.jpg" 2>/dev/null
            rm "$LATEST"  # Clean up the full-size PNG

            echo "{\"active\": true, \"interval\": $INTERVAL, \"pid\": $$, \"mode\": \"retroarch_only\", \"frame\": $FRAME, \"timestamp\": \"$(date -Iseconds)\"}" > "$OUTPUT_DIR/state.json"
            FRAME=$((FRAME + 1))
        fi
    else
        # RetroArch not running — do nothing, just wait
        echo "{\"active\": true, \"interval\": $INTERVAL, \"pid\": $$, \"mode\": \"retroarch_only\", \"frame\": $FRAME, \"waiting\": true, \"timestamp\": \"$(date -Iseconds)\"}" > "$OUTPUT_DIR/state.json"
    fi

    sleep "$INTERVAL"
done
