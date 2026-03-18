#!/bin/bash
# Run this ON the Steam Deck to export all emulator configs to the SD card.
# Then plug the SD card into Bazzite and run deck-import-configs.sh
#
# Usage: ./deck-export-configs.sh

set -e
EXPORT_DIR="/run/media/mmcblk0p1/Emulation/deck-configs"
mkdir -p "$EXPORT_DIR"

echo "Exporting Steam Deck emulator configs to SD card..."

# ES-DE (gamelists, favorites, settings, custom systems)
echo "  ES-DE..."
mkdir -p "$EXPORT_DIR/ES-DE"
cp -a ~/ES-DE/* "$EXPORT_DIR/ES-DE/" 2>/dev/null || true
cp -a ~/.config/es-de/* "$EXPORT_DIR/ES-DE/" 2>/dev/null || true

# RetroArch (main config, core overrides, remaps, playlists)
echo "  RetroArch..."
mkdir -p "$EXPORT_DIR/retroarch"
cp ~/.var/app/org.libretro.RetroArch/config/retroarch/retroarch.cfg "$EXPORT_DIR/retroarch/" 2>/dev/null || true
cp -a ~/.var/app/org.libretro.RetroArch/config/retroarch/config "$EXPORT_DIR/retroarch/" 2>/dev/null || true
cp -a ~/.var/app/org.libretro.RetroArch/config/retroarch/playlists "$EXPORT_DIR/retroarch/" 2>/dev/null || true
cp -a ~/.var/app/org.libretro.RetroArch/config/retroarch/autoconfig "$EXPORT_DIR/retroarch/" 2>/dev/null || true

# Dolphin
echo "  Dolphin..."
mkdir -p "$EXPORT_DIR/dolphin"
cp -a ~/.var/app/org.DolphinEmu.dolphin-emu/config/dolphin-emu/* "$EXPORT_DIR/dolphin/" 2>/dev/null || true

# DuckStation (PS1)
echo "  DuckStation..."
mkdir -p "$EXPORT_DIR/duckstation"
cp -a ~/.var/app/org.duckstation.DuckStation/config/duckstation/* "$EXPORT_DIR/duckstation/" 2>/dev/null || true

# PCSX2 (PS2)
echo "  PCSX2..."
mkdir -p "$EXPORT_DIR/pcsx2"
cp -a ~/.var/app/net.pcsx2.PCSX2/config/PCSX2/* "$EXPORT_DIR/pcsx2/" 2>/dev/null || true

# Flycast (Dreamcast)
echo "  Flycast..."
mkdir -p "$EXPORT_DIR/flycast"
cp -a ~/.var/app/org.flycast.Flycast/config/flycast/* "$EXPORT_DIR/flycast/" 2>/dev/null || true

# PPSSPP
echo "  PPSSPP..."
mkdir -p "$EXPORT_DIR/ppsspp"
cp -a ~/.var/app/org.ppsspp.PPSSPP/config/ppsspp/PSP/SYSTEM/* "$EXPORT_DIR/ppsspp/" 2>/dev/null || true

# melonDS (NDS)
echo "  melonDS..."
mkdir -p "$EXPORT_DIR/melonds"
cp -a ~/.var/app/net.kuribo64.melonDS/config/melonDS/* "$EXPORT_DIR/melonds/" 2>/dev/null || true

# mGBA (GBA)
echo "  mGBA..."
mkdir -p "$EXPORT_DIR/mgba"
cp -a ~/.var/app/io.mgba.mGBA/config/mgba/* "$EXPORT_DIR/mgba/" 2>/dev/null || true

# Mupen64Plus/RMG (N64)
echo "  RMG/N64..."
mkdir -p "$EXPORT_DIR/rmg"
cp -a ~/.var/app/com.github.Rosalie241.RMG/config/RMG/* "$EXPORT_DIR/rmg/" 2>/dev/null || true

# xemu (Xbox)
echo "  xemu..."
mkdir -p "$EXPORT_DIR/xemu"
cp -a ~/.var/app/app.xemu.xemu/data/xemu/xemu/* "$EXPORT_DIR/xemu/" 2>/dev/null || true

# Steam ROM Manager shortcuts
echo "  Steam shortcuts..."
mkdir -p "$EXPORT_DIR/steam"
cp ~/.local/share/Steam/userdata/*/config/shortcuts.vdf "$EXPORT_DIR/steam/" 2>/dev/null || true

echo ""
echo "Export complete: $(du -sh "$EXPORT_DIR" | cut -f1)"
echo "Files saved to: $EXPORT_DIR"
echo ""
echo "Now plug the SD card into your Bazzite PC and run:"
echo "  ~/Documents/GitHub/DUPer/scripts/deck-import-configs.sh"
