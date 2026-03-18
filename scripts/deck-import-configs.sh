#!/bin/bash
# Run this on Bazzite after exporting configs from Steam Deck.
# Imports configs from the SD card and adapts paths for Bazzite.
#
# Usage: ./deck-import-configs.sh

set -e
IMPORT_DIR="/run/media/reaver/EXT-512/Emulation/deck-configs"

if [ ! -d "$IMPORT_DIR" ]; then
    echo "ERROR: $IMPORT_DIR not found. Run deck-export-configs.sh on the Steam Deck first."
    exit 1
fi

echo "Importing Steam Deck configs from SD card..."

# Path translation: /home/deck → /home/reaver, SD card paths → Emulation symlinks
fix_paths() {
    local file="$1"
    if [ -f "$file" ]; then
        sed -i 's|/home/deck|/home/reaver|g' "$file"
        sed -i 's|/run/media/mmcblk0p1/Emulation|/home/reaver/Emulation|g' "$file"
    fi
}

# ES-DE
echo "  ES-DE..."
mkdir -p ~/.config/es-de
cp -a "$IMPORT_DIR/ES-DE/"* ~/.config/es-de/ 2>/dev/null || true
# Also copy to where EmuDeck flatpak ES-DE looks
mkdir -p ~/ES-DE
cp -a "$IMPORT_DIR/ES-DE/"* ~/ES-DE/ 2>/dev/null || true
find ~/.config/es-de ~/ES-DE -name "*.xml" -o -name "*.cfg" 2>/dev/null | while read f; do fix_paths "$f"; done

# RetroArch
echo "  RetroArch..."
RA_DIR=~/.var/app/org.libretro.RetroArch/config/retroarch
cp "$IMPORT_DIR/retroarch/retroarch.cfg" "$RA_DIR/retroarch-deck.cfg" 2>/dev/null || true
cp -a "$IMPORT_DIR/retroarch/config" "$RA_DIR/" 2>/dev/null || true
cp -a "$IMPORT_DIR/retroarch/playlists" "$RA_DIR/" 2>/dev/null || true
cp -a "$IMPORT_DIR/retroarch/autoconfig" "$RA_DIR/" 2>/dev/null || true
# Fix paths in core overrides
find "$RA_DIR/config" -name "*.cfg" -o -name "*.opt" 2>/dev/null | while read f; do fix_paths "$f"; done

# Dolphin
echo "  Dolphin..."
cp -a "$IMPORT_DIR/dolphin/"* ~/.var/app/org.DolphinEmu.dolphin-emu/config/dolphin-emu/ 2>/dev/null || true
find ~/.var/app/org.DolphinEmu.dolphin-emu/config -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# DuckStation
echo "  DuckStation..."
mkdir -p ~/.var/app/org.duckstation.DuckStation/config/duckstation
cp -a "$IMPORT_DIR/duckstation/"* ~/.var/app/org.duckstation.DuckStation/config/duckstation/ 2>/dev/null || true
find ~/.var/app/org.duckstation.DuckStation -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# PCSX2
echo "  PCSX2..."
mkdir -p ~/.var/app/net.pcsx2.PCSX2/config/PCSX2
cp -a "$IMPORT_DIR/pcsx2/"* ~/.var/app/net.pcsx2.PCSX2/config/PCSX2/ 2>/dev/null || true
find ~/.var/app/net.pcsx2.PCSX2 -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# Flycast
echo "  Flycast..."
cp -a "$IMPORT_DIR/flycast/"* ~/.var/app/org.flycast.Flycast/config/flycast/ 2>/dev/null || true
find ~/.var/app/org.flycast.Flycast -name "*.cfg" 2>/dev/null | while read f; do fix_paths "$f"; done

# PPSSPP
echo "  PPSSPP..."
cp -a "$IMPORT_DIR/ppsspp/"* ~/.var/app/org.ppsspp.PPSSPP/config/ppsspp/PSP/SYSTEM/ 2>/dev/null || true
find ~/.var/app/org.ppsspp.PPSSPP -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# melonDS
echo "  melonDS..."
cp -a "$IMPORT_DIR/melonds/"* ~/.var/app/net.kuribo64.melonDS/config/melonDS/ 2>/dev/null || true
find ~/.var/app/net.kuribo64.melonDS -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# mGBA
echo "  mGBA..."
mkdir -p ~/.var/app/io.mgba.mGBA/config/mgba
cp -a "$IMPORT_DIR/mgba/"* ~/.var/app/io.mgba.mGBA/config/mgba/ 2>/dev/null || true

# RMG/N64
echo "  RMG/N64..."
cp -a "$IMPORT_DIR/rmg/"* ~/.var/app/com.github.Rosalie241.RMG/config/RMG/ 2>/dev/null || true
find ~/.var/app/com.github.Rosalie241.RMG -name "*.cfg" -o -name "*.ini" 2>/dev/null | while read f; do fix_paths "$f"; done

# xemu
echo "  xemu..."
cp -a "$IMPORT_DIR/xemu/"* ~/.var/app/app.xemu.xemu/data/xemu/xemu/ 2>/dev/null || true
find ~/.var/app/app.xemu.xemu -name "*.toml" 2>/dev/null | while read f; do fix_paths "$f"; done

# Also copy configs to RetroNAS for centralized storage
echo "  Syncing to RetroNAS..."
scp -r -o BatchMode=yes "$IMPORT_DIR" retronas@10.99.11.8:/data/retronas/configs/ 2>/dev/null || true

echo ""
echo "Import complete! All paths translated from /home/deck to /home/reaver"
echo "RetroArch Deck config saved as retroarch-deck.cfg (not overwriting your current one)"
echo ""
echo "To use the Deck's RetroArch config: cp $RA_DIR/retroarch-deck.cfg $RA_DIR/retroarch.cfg"
