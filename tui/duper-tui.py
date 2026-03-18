#!/usr/bin/env python3
"""
DUPer TUI Dashboard
Real-time terminal monitoring for DUPer ROM management engine.
Connects via REST API to display library, gaming, acquisition, and operations data.

Usage:
    python3 tui/duper-tui.py
    python3 tui/duper-tui.py --host localhost --port 8420
"""

# -- Auto-install missing deps ------------------------------------------------
import importlib, subprocess, sys

_DEPS = {
    "textual":  "textual",
    "rich":     "rich",
    "aiohttp":  "aiohttp",
}

def _ensure_deps():
    missing = []
    for mod, pkg in _DEPS.items():
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"Installing missing deps: {', '.join(missing)}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", *missing]
        )

_ensure_deps()

# -- Imports -------------------------------------------------------------------
import argparse, asyncio, json, time, os, traceback
from datetime import datetime, timedelta
from collections import deque

import aiohttp

from textual.app import App
from textual.binding import Binding
from textual.css.query import NoMatches
from textual.widget import Widget
from textual.widgets import (
    Header, Footer, Static, RichLog, TabbedContent, TabPane,
)

from rich.text import Text
from rich.table import Table
from rich.panel import Panel
from rich.console import Group
from rich.columns import Columns

# -- Theme: Pixel Forge --------------------------------------------------------

ORANGE     = "#e88a3a"
DARK_ORANGE = "#9c5c1e"
CYAN       = "#4ac8e8"
BRASS      = "#c49a5c"
GREEN      = "#5cc87a"
RED        = "#d45858"
YELLOW     = "#e0b830"
WHITE      = "#ede4d8"
DIM        = "#666666"
PANEL_BG   = "#111111"

# Unicode block characters for sparklines
SPARK_CHARS = " \u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"
BAR_FULL = "\u2588"

# Animated spinner frames (Braille)
SPINNER_FRAMES = [
    "\u280b", "\u2819", "\u2839", "\u2838",
    "\u283c", "\u2834", "\u2826", "\u2827",
    "\u2807", "\u280f",
]

# -- CSS ----------------------------------------------------------------------

CSS = """
Screen {
    background: #0a0908;
}

Header {
    background: #1a120a;
    color: #e88a3a;
    text-style: bold;
}

Footer {
    background: #1a120a;
    color: #888888;
}

#status-bar {
    height: 1;
    background: #111111;
    color: #888888;
    padding: 0 1;
}

TabbedContent {
    height: 1fr;
}

TabPane {
    padding: 0;
}

RichLog {
    height: 1fr;
    background: #0a0908;
    border: round #333333;
    scrollbar-size: 1 1;
}
"""

# ==============================================================================
# Helper utilities
# ==============================================================================

def fmt_size(val):
    """Format bytes to human-readable size."""
    if val is None or val == 0:
        return "0 B"
    v = float(val)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(v) < 1024:
            if unit == "B":
                return f"{int(v)} B"
            return f"{v:.1f} {unit}"
        v /= 1024
    return f"{v:.1f} PB"


def fmt_size_mb(val):
    """Format MB to human-readable size."""
    if val is None or val == 0:
        return "0 MB"
    v = float(val)
    if v < 1:
        return f"{v * 1024:.0f} KB"
    if v < 1024:
        return f"{v:.1f} MB"
    return f"{v / 1024:.1f} GB"


def fmt_speed(bps):
    """Format bytes/s to human-readable speed."""
    if not bps or bps <= 0:
        return "0 B/s"
    v = float(bps)
    for unit in ("B/s", "KB/s", "MB/s", "GB/s"):
        if abs(v) < 1024:
            if unit == "B/s":
                return f"{int(v)} B/s"
            return f"{v:.1f} {unit}"
        v /= 1024
    return f"{v:.1f} TB/s"


def fmt_time(seconds):
    """Format seconds to h:m:s."""
    if not seconds or seconds <= 0:
        return "--"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m {s % 60}s"


def fmt_playtime(minutes):
    """Format minutes to hours/minutes."""
    if not minutes or minutes <= 0:
        return "--"
    m = int(minutes)
    if m < 60:
        return f"{m}m"
    return f"{m // 60}h {m % 60}m"


def time_ago(iso_str):
    """Human-readable time-ago from ISO datetime string."""
    if not iso_str:
        return "never"
    try:
        dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
        delta = datetime.now() - dt.replace(tzinfo=None)
        s = delta.total_seconds()
        if s < 0:
            s = 0
    except Exception:
        try:
            dt = datetime.strptime(str(iso_str)[:15], "%Y%m%dT%H%M%S")
            delta = datetime.now() - dt
            s = delta.total_seconds()
        except Exception:
            return str(iso_str)[:19]
    if s < 60:
        return f"{int(s)}s ago"
    if s < 3600:
        return f"{int(s / 60)}m ago"
    if s < 86400:
        return f"{int(s / 3600)}h ago"
    return f"{int(s / 86400)}d ago"


def status_dot(active):
    """Green/red dot."""
    return f"[{GREEN}]\u25cf[/]" if active else f"[{RED}]\u25cf[/]"


# -- Visual rendering helpers -------------------------------------------------

def sparkline(data, width=40, color=GREEN):
    """Render a sparkline from a list of floats. Returns Rich markup string."""
    if not data or len(data) < 2:
        return f"[{DIM}]{'.' * width}[/]"
    pts = list(data[-width:])
    lo, hi = min(pts), max(pts)
    rng = hi - lo if hi != lo else 1.0
    chars = []
    for v in pts:
        idx = int((v - lo) / rng * 7)
        idx = max(0, min(7, idx))
        chars.append(SPARK_CHARS[idx + 1])
    return f"[{color}]{''.join(chars)}[/]"


def hbar(value, max_val, width=30, fill_color=GREEN, label=""):
    """Render a horizontal progress bar."""
    if max_val <= 0:
        pct = 0.0
    else:
        pct = min(1.0, max(0.0, value / max_val))
    filled = int(pct * width)
    empty = width - filled
    bar = f"[{fill_color}]{BAR_FULL * filled}[/][#333333]{chr(0x2591) * empty}[/]"
    if label:
        bar += f" {label}"
    return bar


def pct_bar(pct, width=20, thresholds=None):
    """Render a percentage bar with color thresholds."""
    if thresholds is None:
        thresholds = [(0, RED), (30, YELLOW), (60, GREEN)]
    color = thresholds[0][1]
    for thresh, c in thresholds:
        if pct >= thresh:
            color = c
    filled = int(pct / 100 * width)
    empty = width - filled
    return f"[{color}]{BAR_FULL * filled}[/][#333333]{chr(0x2591) * empty}[/] [{color}]{pct:.0f}%[/]"


# ==============================================================================
# Data Store
# ==============================================================================

class DuperData:
    """Central data store updated by REST API polling."""

    def __init__(self):
        self.connected = False
        self.server_start_time = None

        # /api/health
        self.health = {}
        # /api/stats
        self.stats = {}
        # /api/dashboard/gaming
        self.gaming = {}
        # /api/dashboard/ra-activity
        self.ra_activity = {}
        # /api/retronas/transfer
        self.transfer = {}
        # /api/retronas/media-transfer
        self.media_transfer = {}
        # /api/retronas/acquisition
        self.acquisition = {}
        # /api/ra/stats
        self.ra_stats = {}
        # /api/ra/verify-unchecked/status
        self.ra_verify = {}
        # /api/ss/missing-media-count
        self.ss_missing = {}
        # /api/retronas/index-status
        self.index_status = {}
        # /api/retronas/live
        self.live_vm = {}
        # /api/collections
        self.collections = []
        # /api/retronas/summary
        self.retronas_summary = {}

        # Rolling data
        self.speed_history = deque(maxlen=60)
        self.acq_speed_history = deque(maxlen=60)

        # Event log entries
        self.event_log = []

        # Animation
        self._anim_frame = 0
        self.prev_stats = {}

    def tick_animation(self):
        """Advance the animation frame counter."""
        self._anim_frame = (self._anim_frame + 1) % len(SPINNER_FRAMES)
        return self._anim_frame

    def log_event(self, category, message, color=DIM):
        """Add an event to the log."""
        ts = datetime.now().strftime("%H:%M:%S")
        self.event_log.append({
            "ts": ts,
            "category": category,
            "message": message,
            "color": color,
        })
        if len(self.event_log) > 500:
            self.event_log = self.event_log[-500:]


# ==============================================================================
# Custom Widgets
# ==============================================================================

class StatusBar(Static):
    """Bottom status bar showing connection and key info."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        if d.connected:
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            conn = f"[{GREEN}]{spin} CONNECTED[/]"
        else:
            conn = f"[{RED}]\u25cf OFFLINE[/]"

        # Version
        ver = d.health.get("version", "?")
        codename = d.health.get("codename", "")
        ver_str = f"v{ver}" + (f" ({codename})" if codename else "")

        # Active ops count
        ops = []
        if d.transfer.get("active"):
            ops.append(f"[{CYAN}]XFER[/]")
        if d.media_transfer.get("active"):
            ops.append(f"[{CYAN}]MEDIA[/]")
        if d.acquisition.get("active"):
            ops.append(f"[{CYAN}]ACQ[/]")
        if d.ra_verify.get("active"):
            ops.append(f"[{CYAN}]RA-VFY[/]")
        ops_str = " ".join(ops) if ops else f"[{DIM}]idle[/]"

        sep_c = ORANGE if frame % 2 == 0 else DARK_ORANGE
        sep = f"[{sep_c}]\u2502[/]"

        return Text.from_markup(
            f" {conn}  {sep}  [{BRASS}]DUPer {ver_str}[/]  {sep}  {ops_str}  {sep}  "
            f"[{DIM}]q:quit  1-6:tabs  r:refresh[/]"
        )


class DashboardPanel(Static):
    """Tab 1: Dashboard -- health, last played, collection, RA, active ops."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        parts = []

        # -- Server Health --
        health = d.health or {}
        status = health.get("status", "unknown")
        version = health.get("version", "?")
        codename = health.get("codename", "")

        if d.connected:
            status_display = f"[{GREEN}]\u25cf ONLINE[/]"
        else:
            status_display = f"[{RED}]\u25cf OFFLINE[/]"

        stats = d.stats or {}
        total_files = stats.get("total_files", 0)
        total_dupes = stats.get("total_duplicates", 0)
        total_size = stats.get("total_size_mb", 0)
        wasted = stats.get("wasted_space_mb", 0)
        saved = stats.get("space_saved_mb", 0)

        # RetroNAS summary
        rns = d.retronas_summary or {}
        rns_games = rns.get("total_games", 0)
        rns_systems = rns.get("total_systems", 0)
        rns_media = rns.get("total_media", 0)
        rns_libraries = rns.get("total_libraries", 0)

        t = Table(show_header=False, box=None, padding=(0, 2), expand=True)
        t.add_column("Label", style=DIM, width=22)
        t.add_column("Value", style=WHITE)
        t.add_column("Label2", style=DIM, width=22)
        t.add_column("Value2", style=WHITE)

        t.add_row(
            f"\u2588 Server", status_display,
            f"\u2588 Version", f"[bold {ORANGE}]v{version}[/]" + (f" [{BRASS}]{codename}[/]" if codename else ""),
        )
        t.add_row(
            "  Total Files", f"[bold]{total_files:,}[/]",
            "  Total Size", f"[bold]{fmt_size_mb(total_size)}[/]",
        )
        t.add_row(
            "  Duplicates", f"[{YELLOW}]{total_dupes:,}[/]",
            "  Space Saved", f"[{GREEN}]{fmt_size_mb(saved)}[/]",
        )
        t.add_row(
            "  Games", f"[bold]{rns_games:,}[/]",
            "  Systems", f"[bold]{rns_systems:,}[/]",
        )
        t.add_row(
            "  Libraries", f"{rns_libraries:,}",
            "  Media Files", f"{rns_media:,}",
        )

        parts.append(t)

        # -- Last Played Game --
        gaming = d.gaming or {}
        last_played = gaming.get("last_played")
        if last_played:
            lp_name = last_played.get("name", "?")
            lp_sys = last_played.get("system", "?")
            lp_time = time_ago(last_played.get("lastplayed"))
            lp_count = last_played.get("playcount", 0)
            lp_playtime = fmt_playtime(last_played.get("playtime_minutes", 0))

            lp_text = (
                f"\n  [{ORANGE}]\u2588 Last Played[/]\n"
                f"  [{WHITE}][bold]{lp_name}[/bold][/]  [{CYAN}][{lp_sys}][/]  "
                f"[{DIM}]{lp_time}[/]\n"
                f"  [{DIM}]Play count:[/] {lp_count}  "
                f"[{DIM}]Playtime:[/] {lp_playtime}"
            )
            parts.append(Text.from_markup(lp_text))
        else:
            dots = "." * ((frame % 3) + 1)
            parts.append(Text.from_markup(
                f"\n  [{ORANGE}]\u2588 Last Played[/]\n"
                f"  [{DIM}]No play data yet{dots}[/]"
            ))

        # -- Collection Stats --
        collection = gaming.get("collection", {})
        col_games = collection.get("total_games", 0)
        col_played = collection.get("total_played", 0)
        col_pct = collection.get("completion_pct", 0)
        col_playtime = fmt_playtime(collection.get("total_playtime_minutes", 0))

        col_bar = pct_bar(col_pct, width=20) if col_games > 0 else f"[{DIM}]--[/]"

        col_text = (
            f"\n  [{ORANGE}]\u2588 Collection[/]\n"
            f"  [{DIM}]Games:[/] [{WHITE}]{col_games:,}[/]  "
            f"[{DIM}]Played:[/] [{WHITE}]{col_played:,}[/]  "
            f"[{DIM}]Completion:[/] {col_bar}\n"
            f"  [{DIM}]Total Playtime:[/] [{WHITE}]{col_playtime}[/]"
        )
        parts.append(Text.from_markup(col_text))

        # -- RA Profile --
        ra = d.ra_activity or {}
        summary = ra.get("summary") or {}
        if ra.get("enabled") and summary:
            ra_points = summary.get("total_points", 0)
            ra_rank = summary.get("rank", "?")
            ra_motto = summary.get("motto", "")

            recent_ach = ra.get("recent_achievements", [])
            recent_lines = ""
            for ach in recent_ach[:3]:
                ach_name = ach.get("title", "")
                ach_game = ach.get("game_title", "")
                ach_pts = ach.get("points", 0)
                recent_lines += (
                    f"\n    [{GREEN}]\u2605[/] [{WHITE}]{ach_name}[/] "
                    f"[{DIM}]({ach_game})[/] [{YELLOW}]+{ach_pts}[/]"
                )

            ra_text = (
                f"\n  [{ORANGE}]\u2588 RetroAchievements[/]  "
                f"[{DIM}]@{ra.get('username', '?')}[/]\n"
                f"  [{DIM}]Points:[/] [{YELLOW}]{ra_points:,}[/]  "
                f"[{DIM}]Rank:[/] [{CYAN}]#{ra_rank}[/]"
            )
            if ra_motto:
                ra_text += f"  [{DIM}]\"{ra_motto}\"[/]"
            if recent_lines:
                ra_text += f"\n  [{DIM}]Recent:[/]{recent_lines}"
            parts.append(Text.from_markup(ra_text))
        else:
            parts.append(Text.from_markup(
                f"\n  [{ORANGE}]\u2588 RetroAchievements[/]  [{DIM}]not configured[/]"
            ))

        # -- Active Operations Summary --
        ops_lines = f"\n  [{ORANGE}]\u2588 Active Operations[/]\n"
        any_op = False

        xfer = d.transfer or {}
        if xfer.get("active"):
            any_op = True
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            xf_sys = xfer.get("current_system", "?")
            xf_pct = 0
            if xfer.get("total_files", 0) > 0:
                xf_pct = xfer.get("transferred_files", 0) / xfer["total_files"] * 100
            ops_lines += (
                f"  [{GREEN}]{spin}[/] [{WHITE}]ROM Transfer[/]  "
                f"[{CYAN}]{xf_sys}[/]  {pct_bar(xf_pct, width=15)}\n"
            )

        media_xfer = d.media_transfer or {}
        if media_xfer.get("active"):
            any_op = True
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            mx_sys = media_xfer.get("current_system", "?")
            mx_pct = 0
            if media_xfer.get("total_files", 0) > 0:
                mx_pct = media_xfer.get("transferred_files", 0) / media_xfer["total_files"] * 100
            ops_lines += (
                f"  [{GREEN}]{spin}[/] [{WHITE}]Media Transfer[/]  "
                f"[{CYAN}]{mx_sys}[/]  {pct_bar(mx_pct, width=15)}\n"
            )

        acq = d.acquisition or {}
        if acq.get("active"):
            any_op = True
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            acq_col = acq.get("collection_label") or acq.get("collection", "?")
            acq_done = acq.get("completed_files", 0)
            acq_total = acq.get("total_files", 0)
            ops_lines += (
                f"  [{GREEN}]{spin}[/] [{WHITE}]Acquisition[/]  "
                f"[{CYAN}]{acq_col}[/]  {acq_done}/{acq_total}\n"
            )

        ra_vfy = d.ra_verify or {}
        if ra_vfy.get("active"):
            any_op = True
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            rv_done = ra_vfy.get("verified", 0)
            rv_total = ra_vfy.get("total", 0)
            rv_pct = rv_done / rv_total * 100 if rv_total > 0 else 0
            ops_lines += (
                f"  [{GREEN}]{spin}[/] [{WHITE}]RA Verification[/]  "
                f"{pct_bar(rv_pct, width=15)}\n"
            )

        if not any_op:
            ops_lines += f"  [{DIM}]No active operations[/]\n"

        parts.append(Text.from_markup(ops_lines))

        return Panel(
            Group(*parts),
            title=f"[bold {ORANGE}]\u2726 DUPer DASHBOARD[/]",
            border_style=DARK_ORANGE,
            padding=(1, 2),
        )


class GamesPanel(Static):
    """Tab 2: Games -- system table, RA verification progress."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        parts = []

        # System breakdown from gaming data
        gaming = d.gaming or {}
        systems = gaming.get("systems", [])
        collection = gaming.get("collection", {})

        # Summary line
        total_games = collection.get("total_games", 0)
        total_played = collection.get("total_played", 0)
        total_systems = collection.get("total_systems", len(systems))

        summary = (
            f"  [{ORANGE}]\u2588 Library Overview[/]  "
            f"[{WHITE}]{total_games:,}[/] games across "
            f"[{WHITE}]{total_systems}[/] systems  "
            f"[{DIM}]Played:[/] [{GREEN}]{total_played:,}[/]"
        )
        parts.append(Text.from_markup(summary))

        # RA verification stats
        ra_s = d.ra_stats or {}
        ra_supported = ra_s.get("ra_supported", 0)
        ra_not = ra_s.get("ra_not_supported", 0)
        ra_unverified = ra_s.get("ra_unverified", 0)
        ra_total = ra_supported + ra_not + ra_unverified
        ra_pct = ra_supported / ra_total * 100 if ra_total > 0 else 0
        ra_checked_pct = (ra_supported + ra_not) / ra_total * 100 if ra_total > 0 else 0

        ra_bar = hbar(ra_supported + ra_not, ra_total, width=25, fill_color=GREEN,
                      label=f"[{GREEN}]{ra_checked_pct:.0f}%[/]  [{DIM}]{ra_unverified:,} unchecked[/]")

        # RA verify job
        ra_vfy = d.ra_verify or {}
        vfy_line = ""
        if ra_vfy.get("active"):
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            vfy_done = ra_vfy.get("verified", 0)
            vfy_total = ra_vfy.get("total", 0)
            vfy_found = ra_vfy.get("supported", 0)
            vfy_line = (
                f"\n  [{GREEN}]{spin}[/] [{WHITE}]Verifying...[/]  "
                f"{vfy_done}/{vfy_total}  [{GREEN}]{vfy_found} supported[/]"
            )

        ra_text = (
            f"\n\n  [{ORANGE}]\u2588 RA Verification[/]  "
            f"[{GREEN}]\u2713 {ra_supported:,}[/] supported  "
            f"[{RED}]\u2717 {ra_not:,}[/] unsupported  "
            f"[{YELLOW}]? {ra_unverified:,}[/] unchecked\n"
            f"  [{DIM}]Progress:[/] {ra_bar}{vfy_line}"
        )
        parts.append(Text.from_markup(ra_text))

        # Systems table
        sys_table = Table(
            title=f"Systems ({len(systems)})",
            title_style=f"bold {ORANGE}",
            border_style="#444444",
            expand=True,
            show_lines=False,
            padding=(0, 1),
        )
        sys_table.add_column("System", style=WHITE, ratio=2)
        sys_table.add_column("Games", style=WHITE, width=8, justify="right")
        sys_table.add_column("Played", style=WHITE, width=8, justify="right")
        sys_table.add_column("Completion", style=WHITE, width=30)
        sys_table.add_column("Playtime", style=WHITE, width=10, justify="right")

        for sys_info in systems[:30]:
            sys_name = sys_info.get("system", "?")
            sys_games = sys_info.get("total_games", 0)
            sys_played = sys_info.get("played", 0)
            sys_pct = sys_info.get("completion_pct", 0)
            sys_playtime = fmt_playtime(sys_info.get("playtime_minutes", 0))

            comp_bar = pct_bar(sys_pct, width=12)

            sys_table.add_row(
                f"[{CYAN}]{sys_name}[/]",
                str(sys_games),
                str(sys_played),
                comp_bar,
                sys_playtime,
            )

        if not systems:
            sys_table.add_row(f"[{DIM}]No systems found[/]", "", "", "", "")

        parts.append(Text(""))
        parts.append(sys_table)

        return Panel(
            Group(*parts),
            title=f"[bold {ORANGE}]\U0001f3ae GAMES[/]",
            border_style=DARK_ORANGE,
            padding=(1, 1),
        )


class AcquisitionPanel(Static):
    """Tab 3: Acquisition -- download progress, collection summary, log."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        parts = []

        acq = d.acquisition or {}
        is_active = acq.get("active", False)

        # -- Active Download --
        if is_active:
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            col_label = acq.get("collection_label") or acq.get("collection", "?")
            dest = acq.get("dest_host", "?")
            done = acq.get("completed_files", 0)
            total = acq.get("total_files", 0)
            failed = acq.get("failed_files", 0)
            current_file = acq.get("current_file", "")
            speed = acq.get("current_speed_bps", 0)
            eta = acq.get("current_eta_seconds", 0)
            file_size = acq.get("current_file_size", 0)
            file_dl = acq.get("current_file_downloaded", 0)

            # Record speed for sparkline
            if speed > 0:
                d.acq_speed_history.append(speed)

            # Overall progress
            overall_pct = done / total * 100 if total > 0 else 0
            overall_bar = hbar(done, total, width=35, fill_color=GREEN,
                              label=f"[{GREEN}]{done}[/]/{total}")

            # File progress
            file_pct = file_dl / file_size * 100 if file_size > 0 else 0
            file_bar = hbar(file_dl, file_size, width=30, fill_color=CYAN,
                           label=f"[{CYAN}]{fmt_size(file_dl)}[/] / {fmt_size(file_size)}")

            # Speed sparkline
            speed_spark = sparkline(list(d.acq_speed_history), width=35, color=CYAN)

            active_text = (
                f"  [{GREEN}]{spin}[/] [{WHITE}][bold]DOWNLOADING[/bold][/]  "
                f"[{CYAN}]{col_label}[/] -> [{BRASS}]{dest}[/]\n"
                f"\n"
                f"  [{ORANGE}]Overall[/]   {overall_bar}\n"
                f"  [{ORANGE}]File[/]      {file_bar}\n"
                f"  [{ORANGE}]Current[/]   [{WHITE}]{current_file[:60]}[/]\n"
                f"  [{ORANGE}]Speed[/]     [{CYAN}]{fmt_speed(speed)}[/]  "
                f"[{ORANGE}]ETA[/] [{WHITE}]{fmt_time(eta)}[/]  "
                f"[{ORANGE}]Failed[/] [{RED}]{failed}[/]\n"
                f"\n"
                f"  [{ORANGE}]Speed History[/]\n  {speed_spark}"
            )
            parts.append(Text.from_markup(active_text))
        else:
            parts.append(Text.from_markup(
                f"  [{DIM}]\u25cf No active acquisition[/]"
            ))

        # -- Queue Preview --
        queue = acq.get("queue", [])
        if queue:
            parts.append(Text.from_markup(
                f"\n\n  [{ORANGE}]\u2588 Queue[/] [{DIM}]({len(queue)} pending)[/]"
            ))
            for item in queue[:5]:
                if isinstance(item, str):
                    parts.append(Text.from_markup(
                        f"    [{DIM}]\u25b8[/] [{WHITE}]{item[:70]}[/]"
                    ))
                elif isinstance(item, dict):
                    parts.append(Text.from_markup(
                        f"    [{DIM}]\u25b8[/] [{WHITE}]{item.get('name', str(item))[:70]}[/]"
                    ))

        # -- Live VM Filesystem Stats --
        live = d.live_vm or {}
        vm_systems = live.get("systems", [])
        if vm_systems:
            vm_table = Table(
                title=f"RetroNAS Contents ({live.get('total_files', 0):,} files, {fmt_size(live.get('total_bytes', 0))})",
                title_style=f"bold {BRASS}",
                border_style="#444444",
                expand=True,
                show_lines=False,
                padding=(0, 1),
            )
            vm_table.add_column("System", style=CYAN, ratio=2)
            vm_table.add_column("Files", style=WHITE, width=8, justify="right")
            vm_table.add_column("Size", style=WHITE, width=12, justify="right")

            for vms in vm_systems[:20]:
                vm_table.add_row(
                    vms.get("system", "?"),
                    str(vms.get("file_count", 0)),
                    fmt_size_mb(vms.get("total_size_mb", 0)),
                )

            parts.append(Text(""))
            parts.append(vm_table)

        # -- Completed Downloads --
        completed = acq.get("completed", [])
        if completed:
            parts.append(Text.from_markup(
                f"\n  [{ORANGE}]\u2588 Recently Completed[/] [{DIM}]({len(completed)})[/]"
            ))
            for item in completed[-5:]:
                name = item if isinstance(item, str) else item.get("name", str(item))
                parts.append(Text.from_markup(
                    f"    [{GREEN}]\u2713[/] [{DIM}]{name[:70]}[/]"
                ))

        # -- Errors --
        errors = acq.get("errors", [])
        if errors:
            parts.append(Text.from_markup(
                f"\n  [{RED}]\u2588 Errors[/] [{DIM}]({len(errors)})[/]"
            ))
            for err in errors[-3:]:
                err_text = err if isinstance(err, str) else str(err)
                parts.append(Text.from_markup(
                    f"    [{RED}]\u2717[/] [{DIM}]{err_text[:70]}[/]"
                ))

        return Panel(
            Group(*parts),
            title=f"[bold {ORANGE}]\u2b07 ACQUISITION[/]",
            border_style=DARK_ORANGE,
            padding=(1, 2),
        )


class OperationsPanel(Static):
    """Tab 4: Operations -- transfer, media, scraper, RA verify, live capture."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        parts = []

        # -- ROM Transfer --
        xfer = d.transfer or {}
        if xfer.get("active"):
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            xf_files = xfer.get("transferred_files", 0)
            xf_total = xfer.get("total_files", 0)
            xf_sys = xfer.get("current_system", "?")
            xf_file = xfer.get("current_file", "")
            xf_speed = xfer.get("speed_bps", 0)
            xf_eta = xfer.get("eta_seconds", 0)
            xf_pct = xf_files / xf_total * 100 if xf_total > 0 else 0

            # Speed history
            if xf_speed > 0:
                d.speed_history.append(xf_speed)

            xf_bar = hbar(xf_files, xf_total, width=35, fill_color=GREEN,
                         label=f"[{GREEN}]{xf_files}[/]/{xf_total}")
            speed_spark = sparkline(list(d.speed_history), width=30, color=CYAN)

            # Systems progress
            sys_done = xfer.get("systems_done", [])
            sys_remain = xfer.get("systems_remaining", [])
            sys_line = ""
            if sys_done:
                done_str = ", ".join(sys_done[-5:])
                sys_line += f"  [{DIM}]Done:[/] [{GREEN}]{done_str}[/]"
            if sys_remain:
                remain_str = ", ".join(sys_remain[:5])
                sys_line += f"  [{DIM}]Remaining:[/] [{YELLOW}]{remain_str}[/]"

            xf_text = (
                f"  [{GREEN}]{spin}[/] [{WHITE}][bold]ROM Transfer[/bold][/]  "
                f"[{CYAN}]{xf_sys}[/]\n"
                f"  [{ORANGE}]Progress[/]  {xf_bar}\n"
                f"  [{ORANGE}]Current[/]   [{WHITE}]{xf_file[:55]}[/]\n"
                f"  [{ORANGE}]Speed[/]     [{CYAN}]{fmt_speed(xf_speed)}[/]  "
                f"[{ORANGE}]ETA[/] [{WHITE}]{fmt_time(xf_eta)}[/]\n"
                f"  [{ORANGE}]Speed[/]     {speed_spark}"
            )
            if sys_line:
                xf_text += f"\n{sys_line}"
            parts.append(Text.from_markup(xf_text))
        else:
            parts.append(Text.from_markup(
                f"  [{DIM}]\u25cf ROM Transfer[/]  [{DIM}]inactive[/]"
            ))

        parts.append(Text(""))

        # -- Media Transfer --
        mx = d.media_transfer or {}
        if mx.get("active"):
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            mx_files = mx.get("transferred_files", 0)
            mx_total = mx.get("total_files", 0)
            mx_sys = mx.get("current_system", "?")
            mx_speed = mx.get("speed_bps", 0)
            mx_eta = mx.get("eta_seconds", 0)
            mx_pct = mx_files / mx_total * 100 if mx_total > 0 else 0

            mx_bar = hbar(mx_files, mx_total, width=35, fill_color=CYAN,
                         label=f"[{CYAN}]{mx_files}[/]/{mx_total}")

            mx_text = (
                f"  [{GREEN}]{spin}[/] [{WHITE}][bold]Media Transfer[/bold][/]  "
                f"[{CYAN}]{mx_sys}[/]\n"
                f"  [{ORANGE}]Progress[/]  {mx_bar}\n"
                f"  [{ORANGE}]Speed[/]     [{CYAN}]{fmt_speed(mx_speed)}[/]  "
                f"[{ORANGE}]ETA[/] [{WHITE}]{fmt_time(mx_eta)}[/]"
            )
            parts.append(Text.from_markup(mx_text))
        else:
            parts.append(Text.from_markup(
                f"  [{DIM}]\u25cf Media Transfer[/]  [{DIM}]inactive[/]"
            ))

        parts.append(Text(""))

        # -- ScreenScraper Status --
        ss_missing = d.ss_missing or {}
        ss_total = ss_missing.get("total_without_media", 0)
        ss_md5 = ss_missing.get("with_md5", 0)
        ss_nomd5 = ss_missing.get("without_md5", 0)

        ss_text = (
            f"  [{ORANGE}]\u2588 Scraper Status[/]\n"
            f"  [{DIM}]Games needing media:[/] [{YELLOW}]{ss_total:,}[/]  "
            f"[{DIM}]Scrapeable (MD5):[/] [{GREEN}]{ss_md5:,}[/]  "
            f"[{DIM}]No MD5:[/] [{RED}]{ss_nomd5:,}[/]"
        )
        parts.append(Text.from_markup(ss_text))

        parts.append(Text(""))

        # -- RA Verification --
        ra_s = d.ra_stats or {}
        ra_vfy = d.ra_verify or {}
        ra_supported = ra_s.get("ra_supported", 0)
        ra_not = ra_s.get("ra_not_supported", 0)
        ra_unverified = ra_s.get("ra_unverified", 0)

        if ra_vfy.get("active"):
            spin = SPINNER_FRAMES[frame % len(SPINNER_FRAMES)]
            vfy_done = ra_vfy.get("verified", 0)
            vfy_total = ra_vfy.get("total", 0)
            vfy_found = ra_vfy.get("supported", 0)
            vfy_pct = vfy_done / vfy_total * 100 if vfy_total > 0 else 0
            vfy_bar = hbar(vfy_done, vfy_total, width=30, fill_color=GREEN,
                          label=f"[{GREEN}]{vfy_done}[/]/{vfy_total}  [{GREEN}]{vfy_found} supported[/]")
            ra_text = (
                f"  [{GREEN}]{spin}[/] [{WHITE}][bold]RA Verification[/bold][/]\n"
                f"  [{ORANGE}]Progress[/]  {vfy_bar}"
            )
        else:
            ra_text = (
                f"  [{ORANGE}]\u2588 RA Verification[/]\n"
                f"  [{GREEN}]\u2713 {ra_supported:,}[/]  "
                f"[{RED}]\u2717 {ra_not:,}[/]  "
                f"[{YELLOW}]? {ra_unverified:,}[/] unchecked"
            )
        parts.append(Text.from_markup(ra_text))

        parts.append(Text(""))

        # -- Live Capture Status --
        live = d.live_vm or {}
        vm_total = live.get("total_files", 0)
        vm_bytes = live.get("total_bytes", 0)
        vm_updated = live.get("last_updated", "")

        live_text = (
            f"  [{ORANGE}]\u2588 RetroNAS Live[/]  "
            f"[{DIM}]last polled:[/] [{WHITE}]{vm_updated or '--'}[/]\n"
            f"  [{DIM}]Files:[/] [{WHITE}]{vm_total:,}[/]  "
            f"[{DIM}]Size:[/] [{WHITE}]{fmt_size(vm_bytes)}[/]"
        )
        parts.append(Text.from_markup(live_text))

        return Panel(
            Group(*parts),
            title=f"[bold {ORANGE}]\u2699 OPERATIONS[/]",
            border_style=DARK_ORANGE,
            padding=(1, 2),
        )


class CollectionsPanel(Static):
    """Tab 5: Collections -- ES-DE custom collections, gamelist integrity."""

    def render(self):
        d = self.app.data
        frame = d._anim_frame

        parts = []

        # -- Custom Collections --
        collections = d.collections or []

        col_table = Table(
            title=f"Custom ES-DE Collections ({len(collections)})",
            title_style=f"bold {ORANGE}",
            border_style="#444444",
            expand=True,
            show_lines=False,
            padding=(0, 1),
        )
        col_table.add_column("Collection", style=CYAN, ratio=3)
        col_table.add_column("Games", style=WHITE, width=8, justify="right")
        col_table.add_column("File", style=DIM, ratio=4)

        for col in collections:
            col_table.add_row(
                col.get("name", "?"),
                str(col.get("game_count", 0)),
                col.get("file", ""),
            )

        if not collections:
            col_table.add_row(f"[{DIM}]No custom collections[/]", "", "")

        parts.append(col_table)

        # -- Gamelist Integrity --
        idx = d.index_status or {}
        if idx and "error" not in idx:
            consistent = idx.get("consistent", False)
            db_systems = idx.get("total_systems_db", 0)
            gl_systems = idx.get("total_systems_gamelists", 0)
            db_games = idx.get("total_games_db", 0)
            gl_games = idx.get("total_games_gamelists", 0)
            mismatches = idx.get("mismatches", [])

            if consistent:
                status_line = f"[{GREEN}]\u2713 CONSISTENT[/]"
            else:
                status_line = f"[{YELLOW}]\u26a0 {len(mismatches)} MISMATCHES[/]"

            idx_text = (
                f"\n\n  [{ORANGE}]\u2588 Gamelist Index Status[/]  {status_line}\n"
                f"  [{DIM}]DB Systems:[/] [{WHITE}]{db_systems}[/]  "
                f"[{DIM}]Gamelist Systems:[/] [{WHITE}]{gl_systems}[/]\n"
                f"  [{DIM}]DB Games:[/] [{WHITE}]{db_games:,}[/]  "
                f"[{DIM}]Gamelist Games:[/] [{WHITE}]{gl_games:,}[/]"
            )
            parts.append(Text.from_markup(idx_text))

            if mismatches:
                mm_table = Table(
                    title="Mismatches",
                    title_style=f"bold {YELLOW}",
                    border_style="#444444",
                    expand=True,
                    show_lines=False,
                    padding=(0, 1),
                )
                mm_table.add_column("System", style=CYAN, ratio=2)
                mm_table.add_column("DB Count", style=WHITE, width=10, justify="right")
                mm_table.add_column("Gamelist Count", style=WHITE, width=14, justify="right")
                mm_table.add_column("Diff", style=WHITE, width=8, justify="right")

                for mm in mismatches[:15]:
                    diff = mm.get("db", 0) - mm.get("gamelist", 0)
                    diff_c = RED if diff != 0 else GREEN
                    mm_table.add_row(
                        mm.get("system", "?"),
                        str(mm.get("db", 0)),
                        str(mm.get("gamelist", 0)),
                        f"[{diff_c}]{diff:+d}[/]",
                    )

                parts.append(Text(""))
                parts.append(mm_table)
        else:
            error = idx.get("error", "")
            if error:
                parts.append(Text.from_markup(
                    f"\n\n  [{ORANGE}]\u2588 Gamelist Index[/]  [{RED}]{error}[/]"
                ))
            else:
                dots = "." * ((frame % 3) + 1)
                parts.append(Text.from_markup(
                    f"\n\n  [{ORANGE}]\u2588 Gamelist Index[/]  [{DIM}]Loading{dots}[/]"
                ))

        return Panel(
            Group(*parts),
            title=f"[bold {ORANGE}]\U0001f4da COLLECTIONS[/]",
            border_style=DARK_ORANGE,
            padding=(1, 1),
        )


class EventLogPanel(Widget):
    """Tab 6: Scrollable event log."""

    def compose(self):
        yield RichLog(id="event-richlog", wrap=True, highlight=True, markup=True)

    def on_mount(self):
        self._last_count = 0

    def update_log(self):
        """Push new entries to the RichLog."""
        d = self.app.data
        log = d.event_log
        new_count = len(log)
        if new_count <= self._last_count:
            return

        try:
            rlog = self.query_one("#event-richlog", RichLog)
        except NoMatches:
            return

        for entry in log[self._last_count:]:
            ts = entry.get("ts", "--:--:--")
            cat = entry.get("category", "info")
            msg = entry.get("message", "")
            color = entry.get("color", DIM)

            # Category icon
            icons = {
                "health": "\u25cf",
                "transfer": "\u25b6",
                "acquisition": "\u2b07",
                "gaming": "\U0001f3ae",
                "ra": "\u2605",
                "scraper": "\u25ce",
                "error": "\u2717",
                "info": "\u25b8",
            }
            icon = icons.get(cat, "\u25b8")

            line = (
                f"[{DIM}]{ts}[/] "
                f"[{color}]{icon} [{color}][bold]{cat:>12s}[/bold][/] "
                f"{msg}"
            )
            rlog.write(Text.from_markup(line))

        self._last_count = new_count


# ==============================================================================
# Main TUI App
# ==============================================================================

class DuperTUI(App):
    """DUPer Terminal Dashboard."""

    TITLE = "DUPer TUI"
    SUB_TITLE = "ROM Management Engine"
    CSS = CSS

    BINDINGS = [
        Binding("1", "switch_tab('dashboard')", "Dashboard", show=True),
        Binding("2", "switch_tab('games')", "Games", show=True),
        Binding("3", "switch_tab('acquisition')", "Acquisition", show=True),
        Binding("4", "switch_tab('operations')", "Operations", show=True),
        Binding("5", "switch_tab('collections')", "Collections", show=True),
        Binding("6", "switch_tab('log')", "Log", show=True),
        Binding("q", "quit", "Quit", show=True),
        Binding("r", "refresh", "Refresh", show=False),
    ]

    data = DuperData()

    def __init__(self, host="127.0.0.1", port=8420, **kwargs):
        super().__init__(**kwargs)
        self.api_base = f"http://{host}:{port}"
        self._session = None

    def compose(self):
        yield Header(show_clock=True)
        with TabbedContent(initial="dashboard"):
            with TabPane("1: Dashboard", id="dashboard"):
                yield DashboardPanel(id="dashboard-panel")
            with TabPane("2: Games", id="games"):
                yield GamesPanel(id="games-panel")
            with TabPane("3: Acquisition", id="acquisition"):
                yield AcquisitionPanel(id="acquisition-panel")
            with TabPane("4: Operations", id="operations"):
                yield OperationsPanel(id="operations-panel")
            with TabPane("5: Collections", id="collections"):
                yield CollectionsPanel(id="collections-panel")
            with TabPane("6: Log", id="log"):
                yield EventLogPanel(id="log-panel")
        yield StatusBar(id="status-bar")

    # -- API Fetching ----------------------------------------------------------

    async def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8)
            )
        return self._session

    async def _fetch(self, path):
        """Fetch JSON from API endpoint. Returns dict or None on error."""
        try:
            session = await self._get_session()
            async with session.get(f"{self.api_base}{path}") as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            pass
        return None

    async def _poll_fast(self):
        """Poll endpoints that need frequent updates (2s)."""
        d = self.data

        # Health check (also serves as connectivity test)
        health = await self._fetch("/api/health")
        was_connected = d.connected
        if health:
            d.connected = True
            d.health = health
            if not was_connected:
                d.log_event("health", "Connected to DUPer server", GREEN)
        else:
            if was_connected:
                d.log_event("error", "Lost connection to DUPer server", RED)
            d.connected = False

        if not d.connected:
            return

        # Active operation endpoints (fast poll)
        transfer = await self._fetch("/api/retronas/transfer")
        if transfer is not None:
            was_active = d.transfer.get("active", False)
            d.transfer = transfer
            if transfer.get("active") and not was_active:
                d.log_event("transfer", f"ROM transfer started: {transfer.get('current_system', '?')}", GREEN)
            elif not transfer.get("active") and was_active:
                d.log_event("transfer", "ROM transfer completed", CYAN)

        media_xfer = await self._fetch("/api/retronas/media-transfer")
        if media_xfer is not None:
            was_active = d.media_transfer.get("active", False)
            d.media_transfer = media_xfer
            if media_xfer.get("active") and not was_active:
                d.log_event("transfer", "Media transfer started", GREEN)

        acq = await self._fetch("/api/retronas/acquisition")
        if acq is not None:
            was_active = d.acquisition.get("active", False)
            d.acquisition = acq
            if acq.get("active") and not was_active:
                col = acq.get("collection_label") or acq.get("collection", "?")
                d.log_event("acquisition", f"Acquisition started: {col}", GREEN)

        ra_vfy = await self._fetch("/api/ra/verify-unchecked/status")
        if ra_vfy is not None:
            d.ra_verify = ra_vfy

    async def _poll_slow(self):
        """Poll endpoints that need less frequent updates (10s)."""
        d = self.data

        if not d.connected:
            return

        stats = await self._fetch("/api/stats")
        if stats is not None:
            d.stats = stats

        gaming = await self._fetch("/api/dashboard/gaming")
        if gaming is not None:
            d.gaming = gaming

        ra_activity = await self._fetch("/api/dashboard/ra-activity")
        if ra_activity is not None:
            d.ra_activity = ra_activity

        ra_stats = await self._fetch("/api/ra/stats")
        if ra_stats is not None:
            d.ra_stats = ra_stats

        ss_missing = await self._fetch("/api/ss/missing-media-count")
        if ss_missing is not None:
            d.ss_missing = ss_missing

        index_status = await self._fetch("/api/retronas/index-status")
        if index_status is not None:
            d.index_status = index_status

        live_vm = await self._fetch("/api/retronas/live")
        if live_vm is not None:
            d.live_vm = live_vm

        collections = await self._fetch("/api/collections")
        if collections is not None:
            d.collections = collections

        rns = await self._fetch("/api/retronas/summary")
        if rns is not None:
            d.retronas_summary = rns

    # -- Lifecycle -------------------------------------------------------------

    async def on_mount(self):
        """Start polling loops."""
        self._fast_tick = 0
        self._slow_tick = 0
        self.set_interval(1.0, self._periodic_refresh)
        self.run_worker(self._initial_fetch(), exclusive=True, name="init")

    async def _initial_fetch(self):
        """Fetch all data on startup."""
        await self._poll_fast()
        await self._poll_slow()
        self._do_refresh()

    async def _periodic_refresh(self):
        """Periodic refresh: fast every 2s, slow every 10s, animation every 1s."""
        self.data.tick_animation()
        self._fast_tick += 1
        self._slow_tick += 1

        if self._fast_tick >= 2:
            self._fast_tick = 0
            self.run_worker(self._poll_fast(), exclusive=False, name="fast_poll")

        if self._slow_tick >= 10:
            self._slow_tick = 0
            self.run_worker(self._poll_slow(), exclusive=False, name="slow_poll")

        self._do_refresh()

    def _do_refresh(self):
        """Refresh visible widgets."""
        self.data.tick_animation()

        try:
            tabs = self.query_one(TabbedContent)
            active = tabs.active
        except NoMatches:
            return

        widget_map = {
            "dashboard": "dashboard-panel",
            "games": "games-panel",
            "acquisition": "acquisition-panel",
            "operations": "operations-panel",
            "collections": "collections-panel",
        }

        # Always refresh status bar
        try:
            self.query_one("#status-bar", StatusBar).refresh()
        except NoMatches:
            pass

        # Refresh active tab
        if active in widget_map:
            try:
                self.query_one(f"#{widget_map[active]}", Static).refresh()
            except NoMatches:
                pass
        elif active == "log":
            try:
                self.query_one("#log-panel", EventLogPanel).update_log()
            except NoMatches:
                pass

    # -- Actions ---------------------------------------------------------------

    def action_switch_tab(self, tab_id: str):
        """Switch to a named tab."""
        try:
            tabs = self.query_one(TabbedContent)
            tabs.active = tab_id
        except NoMatches:
            pass

    async def action_refresh(self):
        """Force refresh all data."""
        self.data.log_event("info", "Manual refresh triggered", BRASS)
        await self._poll_fast()
        await self._poll_slow()
        self._do_refresh()

    async def on_unmount(self):
        """Clean up aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()


# ==============================================================================
# Entry point
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="DUPer TUI Dashboard")
    parser.add_argument(
        "--host", default="127.0.0.1",
        help="DUPer API server host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port", type=int, default=8420,
        help="DUPer API server port (default: 8420)",
    )
    args = parser.parse_args()

    app = DuperTUI(host=args.host, port=args.port)
    app.run()


if __name__ == "__main__":
    main()
