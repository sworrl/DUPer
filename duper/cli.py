"""Command-line interface for DUPer."""

from __future__ import annotations

import webbrowser
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from duper import __codename__, __version__
from duper.core import (
    Deduper,
    DuperConfig,
    DuperDatabase,
    MediaCorrelator,
    Scanner,
    ScanProgress,
    get_config,
    set_config,
)
from duper.utils.helpers import format_size, generate_api_key

# Create CLI app
app = typer.Typer(
    name="duper",
    help="DUPer - Duplicate file finder and manager",
    add_completion=False,
)

# Subcommands
config_app = typer.Typer(help="Configuration commands")
remote_app = typer.Typer(help="Remote host commands")
duplicates_app = typer.Typer(help="Duplicate management commands")
media_app = typer.Typer(help="Media file management commands")
saves_app = typer.Typer(help="Save game/state management (NEVER deletes, only moves)")

app.add_typer(config_app, name="config")
app.add_typer(remote_app, name="remote")
app.add_typer(duplicates_app, name="duplicates")
app.add_typer(media_app, name="media")
app.add_typer(saves_app, name="saves")

console = Console()


def get_db_and_config() -> tuple[DuperDatabase, DuperConfig]:
    """Get database and config instances."""
    config = get_config()
    config.ensure_directories()
    db = DuperDatabase(config.paths.database)
    db.connect()
    db.initialize()
    return db, config


@app.command()
def version():
    """Show version information."""
    console.print(f"[bold cyan]DUPer[/bold cyan] v{__version__}")
    console.print(f'Code name: "{__codename__}"')


@app.command()
def scan(
    directory: str = typer.Argument(..., help="Directory to scan"),
    full: bool = typer.Option(False, "--full", "-f", help="Force full rescan"),
):
    """Scan a directory for duplicate files."""
    db, config = get_db_and_config()

    directory = str(Path(directory).resolve())

    if not Path(directory).is_dir():
        console.print(f"[red]Error:[/red] Directory does not exist: {directory}")
        raise typer.Exit(1)

    console.print(f"[cyan]Scanning:[/cyan] {directory}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Scanning...", total=None)

        def progress_callback(p: ScanProgress):
            if p.total_files > 0:
                pct = (p.processed_files / p.total_files) * 100
                progress.update(
                    task,
                    description=f"[{p.status}] {p.processed_files}/{p.total_files} ({pct:.1f}%)"
                )

        scanner = Scanner(db=db, config=config, progress_callback=progress_callback)

        if full:
            result = scanner.scan(directory)
        else:
            result = scanner.scan_or_update(directory)

        # Mark duplicates
        deduper = Deduper(db=db, config=config)
        dup_count = deduper.mark_duplicates(directory)
        deduper.log_statistics(directory)

    console.print()
    console.print(f"[green]Scan complete![/green]")
    console.print(f"  Files processed: {result.files_processed}")
    console.print(f"  Duration: {result.duration_seconds}s")
    console.print(f"  Duplicates found: {dup_count}")
    if result.errors:
        console.print(f"  [yellow]Errors: {result.errors}[/yellow]")

    # Save last scan directory
    config.last_scan_directory = directory
    config.save()

    db.close()


@app.command()
def status():
    """Show current status and statistics."""
    db, config = get_db_and_config()

    stats = db.get_stats()
    metrics = db.get_latest_metrics()

    table = Table(title="DUPer Status")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Total Files", str(stats["total_files"]))
    table.add_row("Duplicates", str(stats["total_duplicates"]))
    table.add_row("Moved Files", str(stats["total_moved"]))
    table.add_row("Total Size", format_size(stats["total_size_mb"] * 1024 * 1024))
    table.add_row("Database", stats["database_path"])

    if metrics:
        table.add_row("Last Scan", metrics.scan_directory)
        table.add_row("Last Scan Time", metrics.start_time)

    if config.last_scan_directory:
        table.add_row("Last Directory", config.last_scan_directory)

    console.print(table)
    db.close()


@app.command()
def serve(
    host: str = typer.Option(None, "--host", "-h", help="Host to bind to"),
    port: int = typer.Option(None, "--port", "-p", help="Port to listen on"),
    no_auth: bool = typer.Option(False, "--no-auth", help="Disable authentication"),
    show_key: bool = typer.Option(False, "--show-key", help="Show API key on startup"),
):
    """Start the DUPer API server."""
    from duper.api import run_server

    config = get_config()
    config.ensure_directories()

    # Only override auth_enabled if --no-auth flag is explicitly set
    if no_auth:
        config.server.auth_enabled = False

    if host:
        config.server.host = host
    if port:
        config.server.port = port

    console.print(f"[bold cyan]DUPer Server[/bold cyan] v{__version__}")
    console.print(f"Starting server on [green]{config.server.host}:{config.server.port}[/green]")

    # Auth is always bypassed for localhost, only needed for remote access
    console.print(f"Authentication: [green]localhost=open[/green], [yellow]remote=API key[/yellow]")
    if show_key:
        console.print(f"API Key: [yellow]{config.server.api_key}[/yellow]")

    if config.server.web_ui_enabled:
        console.print(f"Web UI: http://{config.server.host}:{config.server.port}/")

    console.print()
    run_server(config=config)


@app.command()
def web():
    """Open the web UI in the default browser."""
    config = get_config()
    url = f"http://localhost:{config.server.port}/"
    console.print(f"Opening [cyan]{url}[/cyan] in browser...")
    webbrowser.open(url)


# === Duplicates Commands ===


@duplicates_app.command("list")
def duplicates_list(
    directory: str = typer.Option(None, "--dir", "-d", help="Directory to check"),
):
    """List duplicate files."""
    db, config = get_db_and_config()

    if not directory:
        directory = config.last_scan_directory
        if not directory:
            console.print("[red]Error:[/red] No directory specified and no last scan found.")
            console.print("Use --dir to specify a directory or run a scan first.")
            raise typer.Exit(1)

    deduper = Deduper(db=db, config=config)
    summary = deduper.get_duplicate_summary(directory)

    if summary["total_groups"] == 0:
        console.print("[green]No duplicates found![/green]")
        db.close()
        return

    console.print(f"\n[bold]Duplicate Summary for {directory}[/bold]")
    console.print(f"  Groups: {summary['total_groups']}")
    console.print(f"  Total files: {summary['total_duplicate_files']}")
    console.print(f"  Files to remove: {summary['files_to_remove']}")
    console.print(f"  Wasted space: {format_size(summary['wasted_space_mb'] * 1024 * 1024)}")

    for group in summary["groups"]:
        console.print(f"\n[cyan]MD5: {group['md5']}[/cyan] ({group['file_count']} files)")
        for f in group["files"]:
            marker = "[green]*[/green]" if f["filepath"] == group["recommended_keep"] else "[red]-[/red]"
            console.print(f"  {marker} {f['filename']} (score: {f['score']:.2f})")
            console.print(f"      {f['filepath']}")

    db.close()


@duplicates_app.command("process")
def duplicates_process(
    directory: str = typer.Option(None, "--dir", "-d", help="Directory to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be moved"),
):
    """Process duplicates by moving them to the duplicates folder."""
    db, config = get_db_and_config()

    if not directory:
        directory = config.last_scan_directory
        if not directory:
            console.print("[red]Error:[/red] No directory specified and no last scan found.")
            raise typer.Exit(1)

    deduper = Deduper(db=db, config=config)

    if dry_run:
        console.print("[yellow]DRY RUN - no files will be moved[/yellow]\n")

    result = deduper.process_duplicates(directory, dry_run=dry_run)

    if dry_run:
        console.print(f"\n[yellow]Would move {result.moved_count} files[/yellow]")
    else:
        console.print(f"\n[green]Moved {result.moved_count} files[/green]")

    if result.errors:
        console.print(f"[red]Errors: {len(result.errors)}[/red]")
        for err in result.errors:
            console.print(f"  - {err}")

    db.close()


@duplicates_app.command("restore")
def duplicates_restore(
    move_id: int = typer.Argument(..., help="ID of the file to restore"),
):
    """Restore a moved file."""
    db, config = get_db_and_config()
    deduper = Deduper(db=db, config=config)

    result = deduper.restore_file(move_id)

    if result.restored_count > 0:
        console.print(f"[green]Restored file successfully[/green]")
    else:
        console.print(f"[red]Failed to restore file[/red]")
        for err in result.errors:
            console.print(f"  - {err}")

    db.close()


@duplicates_app.command("restore-all")
def duplicates_restore_all():
    """Restore all moved files."""
    db, config = get_db_and_config()
    deduper = Deduper(db=db, config=config)

    result = deduper.restore_all_files()

    console.print(f"[green]Restored {result.restored_count} files[/green]")
    if result.errors:
        console.print(f"[red]Errors: {len(result.errors)}[/red]")

    db.close()


@duplicates_app.command("moved")
def duplicates_moved():
    """List all moved files."""
    db, config = get_db_and_config()
    deduper = Deduper(db=db, config=config)

    summary = deduper.get_moved_files_summary()

    if summary["total_moved"] == 0:
        console.print("[green]No moved files[/green]")
        db.close()
        return

    table = Table(title=f"Moved Files ({summary['total_moved']} total)")
    table.add_column("ID", style="cyan")
    table.add_column("Filename", style="green")
    table.add_column("Original Path")
    table.add_column("Moved Time")

    for f in summary["files"]:
        table.add_row(
            str(f["move_id"]),
            f["filename"],
            f["original_filepath"],
            f["moved_time"],
        )

    console.print(table)
    db.close()


# === Media Commands ===


@media_app.command("orphaned")
def media_orphaned(
    directory: str = typer.Argument(..., help="ROM directory to scan for orphaned media"),
):
    """Find orphaned media files (media without corresponding ROMs)."""
    db, config = get_db_and_config()

    console.print(f"[cyan]Scanning for orphaned media in:[/cyan] {directory}")

    correlator = MediaCorrelator(db=db, config=config)
    result = correlator.find_orphaned_media(directory)

    if result.total_files == 0:
        console.print("[green]No orphaned media files found![/green]")
        db.close()
        return

    console.print(f"\n[bold]Orphaned Media Summary[/bold]")
    console.print(f"  Total files: {result.total_files}")
    console.print(f"  Total size: {format_size(result.total_size_bytes)}")
    console.print(f"  ROM names: {len(result.orphaned)}")

    for orphan in result.orphaned:
        console.print(f"\n[cyan]{orphan.rom_name}[/cyan] ({len(orphan.media_files)} files, {format_size(orphan.total_size_bytes)})")
        for media in orphan.media_files:
            console.print(f"  [{media.category}] {media.path}")

    db.close()


@media_app.command("for-moved")
def media_for_moved():
    """Find media files for ROMs that have been moved as duplicates."""
    db, config = get_db_and_config()

    console.print("[cyan]Finding media for moved ROMs...[/cyan]")

    correlator = MediaCorrelator(db=db, config=config)
    result = correlator.find_media_for_moved_roms()

    if result.total_files == 0:
        console.print("[green]No media files found for moved ROMs![/green]")
        db.close()
        return

    console.print(f"\n[bold]Media for Moved ROMs[/bold]")
    console.print(f"  Total files: {result.total_files}")
    console.print(f"  Total size: {format_size(result.total_size_bytes)}")

    for orphan in result.orphaned:
        console.print(f"\n[cyan]{orphan.rom_name}[/cyan] ({len(orphan.media_files)} files)")
        console.print(f"  Original ROM: {orphan.rom_path}")
        for media in orphan.media_files:
            console.print(f"  [{media.category}] {media.path}")

    db.close()


@media_app.command("cleanup")
def media_cleanup(
    directory: str = typer.Option(None, "--dir", "-d", help="ROM directory to clean orphaned media"),
    moved_roms: bool = typer.Option(False, "--moved-roms", "-m", help="Clean up media for moved ROMs"),
    move_to: str = typer.Option(None, "--move-to", help="Move files here instead of deleting"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without making changes"),
):
    """Clean up orphaned media files."""
    db, config = get_db_and_config()

    if not directory and not moved_roms:
        console.print("[red]Error:[/red] Must specify either --dir or --moved-roms")
        raise typer.Exit(1)

    if dry_run:
        console.print("[yellow]DRY RUN - no files will be modified[/yellow]\n")

    correlator = MediaCorrelator(db=db, config=config)

    if moved_roms:
        console.print("[cyan]Cleaning up media for moved ROMs...[/cyan]")
        result = correlator.cleanup_moved_rom_media(move_to=move_to, dry_run=dry_run)
    else:
        console.print(f"[cyan]Cleaning up orphaned media in:[/cyan] {directory}")
        result = correlator.cleanup_orphaned_media(
            rom_directory=directory,
            move_to=move_to,
            dry_run=dry_run,
        )

    if dry_run:
        console.print(f"\n[yellow]Would {'move' if move_to else 'remove'} {result.removed_count} files ({format_size(result.removed_size_bytes)})[/yellow]")
    else:
        action = "Moved" if move_to else "Removed"
        console.print(f"\n[green]{action} {result.removed_count} files ({format_size(result.removed_size_bytes)})[/green]")

    if result.errors:
        console.print(f"[red]Errors: {len(result.errors)}[/red]")
        for err in result.errors:
            console.print(f"  - {err}")

    db.close()


@media_app.command("for-rom")
def media_for_rom(
    rom_path: str = typer.Argument(..., help="Path to the ROM file"),
):
    """Find all media files associated with a specific ROM."""
    db, config = get_db_and_config()

    console.print(f"[cyan]Finding media for:[/cyan] {rom_path}")

    correlator = MediaCorrelator(db=db, config=config)
    media_files = correlator.find_media_for_rom(rom_path)

    if not media_files:
        console.print("[yellow]No media files found for this ROM[/yellow]")
        db.close()
        return

    total_size = sum(m.size_bytes for m in media_files)
    console.print(f"\n[bold]Found {len(media_files)} media files ({format_size(total_size)})[/bold]")

    table = Table()
    table.add_column("Category", style="cyan")
    table.add_column("Type")
    table.add_column("Size", justify="right")
    table.add_column("Path")

    for media in media_files:
        table.add_row(
            media.category,
            media.media_type,
            format_size(media.size_bytes),
            media.path,
        )

    console.print(table)
    db.close()


# === Saves Commands ===
# IMPORTANT: Saves are NEVER deleted - only moved or preserved


@saves_app.command("orphaned")
def saves_orphaned(
    directory: str = typer.Argument(..., help="ROM directory to scan for orphaned saves"),
):
    """Find orphaned save files (saves without corresponding ROMs)."""
    db, config = get_db_and_config()

    console.print(f"[cyan]Scanning for orphaned saves in:[/cyan] {directory}")

    correlator = MediaCorrelator(db=db, config=config)
    result = correlator.find_orphaned_saves(directory)

    if result.total_saves == 0 and result.total_states == 0:
        console.print("[green]No orphaned save files found![/green]")
        db.close()
        return

    console.print(f"\n[bold]Orphaned Saves Summary[/bold]")
    console.print(f"  Save games: {result.total_saves}")
    console.print(f"  Save states: {result.total_states}")
    console.print(f"  Total size: {format_size(result.total_size_bytes)}")
    console.print(f"  ROM names: {len(result.orphaned)}")

    for orphan in result.orphaned:
        total = len(orphan.save_files) + len(orphan.state_files)
        console.print(f"\n[cyan]{orphan.rom_name}[/cyan] ({total} files, {format_size(orphan.total_size_bytes)})")
        for save in orphan.save_files:
            console.print(f"  [yellow][SAVE][/yellow] {save.path}")
        for state in orphan.state_files:
            console.print(f"  [blue][STATE][/blue] {state.path}")

    db.close()


@saves_app.command("for-moved")
def saves_for_moved():
    """Find save files for ROMs that have been moved as duplicates."""
    db, config = get_db_and_config()

    console.print("[cyan]Finding saves for moved ROMs...[/cyan]")

    correlator = MediaCorrelator(db=db, config=config)
    result = correlator.find_saves_for_moved_roms()

    if result.total_saves == 0 and result.total_states == 0:
        console.print("[green]No save files found for moved ROMs![/green]")
        db.close()
        return

    console.print(f"\n[bold]Saves for Moved ROMs[/bold]")
    console.print(f"  Save games: {result.total_saves}")
    console.print(f"  Save states: {result.total_states}")
    console.print(f"  Total size: {format_size(result.total_size_bytes)}")

    for orphan in result.orphaned:
        total = len(orphan.save_files) + len(orphan.state_files)
        console.print(f"\n[cyan]{orphan.rom_name}[/cyan] ({total} files)")
        console.print(f"  Original ROM: {orphan.rom_path}")
        for save in orphan.save_files:
            console.print(f"  [yellow][SAVE][/yellow] {save.path}")
        for state in orphan.state_files:
            console.print(f"  [blue][STATE][/blue] {state.path}")

    db.close()


@saves_app.command("preserve")
def saves_preserve(
    directory: str = typer.Option(None, "--dir", "-d", help="ROM directory to preserve orphaned saves"),
    moved_roms: bool = typer.Option(False, "--moved-roms", "-m", help="Preserve saves for moved ROMs"),
    move_to: str = typer.Option(..., "--move-to", "-t", help="Directory to move saves to (REQUIRED)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without making changes"),
):
    """
    Preserve orphaned save files by moving them to a safe location.

    IMPORTANT: This command NEVER deletes saves - it only moves them.
    """
    db, config = get_db_and_config()

    if not directory and not moved_roms:
        console.print("[red]Error:[/red] Must specify either --dir or --moved-roms")
        raise typer.Exit(1)

    if dry_run:
        console.print("[yellow]DRY RUN - no files will be moved[/yellow]\n")

    correlator = MediaCorrelator(db=db, config=config)

    if moved_roms:
        console.print(f"[cyan]Preserving saves for moved ROMs to:[/cyan] {move_to}")
        result = correlator.preserve_moved_rom_saves(move_to=move_to, dry_run=dry_run)
    else:
        console.print(f"[cyan]Preserving orphaned saves from:[/cyan] {directory}")
        console.print(f"[cyan]Moving to:[/cyan] {move_to}")
        result = correlator.preserve_orphaned_saves(
            rom_directory=directory,
            move_to=move_to,
            dry_run=dry_run,
        )

    if dry_run:
        console.print(f"\n[yellow]Would move {result.moved_count} files ({format_size(result.moved_size_bytes)})[/yellow]")
    else:
        console.print(f"\n[green]Moved {result.moved_count} files ({format_size(result.moved_size_bytes)})[/green]")

    if result.errors:
        console.print(f"[red]Errors: {len(result.errors)}[/red]")
        for err in result.errors:
            console.print(f"  - {err}")

    db.close()


@saves_app.command("rename")
def saves_rename(
    old_name: str = typer.Argument(..., help="Original ROM name (without extension)"),
    new_name: str = typer.Argument(..., help="New ROM name (without extension)"),
    directory: str = typer.Option(..., "--dir", "-d", help="ROM directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without making changes"),
):
    """
    Rename save files to match a new ROM name.

    Use this when you rename a ROM and want its saves to follow.
    """
    db, config = get_db_and_config()

    if dry_run:
        console.print("[yellow]DRY RUN - no files will be renamed[/yellow]\n")

    console.print(f"[cyan]Renaming saves:[/cyan] {old_name} -> {new_name}")
    console.print(f"[cyan]In directory:[/cyan] {directory}")

    correlator = MediaCorrelator(db=db, config=config)
    result = correlator.rename_saves_for_rom(
        old_rom_name=old_name,
        new_rom_name=new_name,
        rom_directory=directory,
        dry_run=dry_run,
    )

    if dry_run:
        console.print(f"\n[yellow]Would rename {result.renamed_count} files[/yellow]")
    else:
        console.print(f"\n[green]Renamed {result.renamed_count} files[/green]")

    if result.errors:
        console.print(f"[red]Errors: {len(result.errors)}[/red]")
        for err in result.errors:
            console.print(f"  - {err}")

    db.close()


@saves_app.command("for-rom")
def saves_for_rom(
    rom_path: str = typer.Argument(..., help="Path to the ROM file"),
):
    """Find all save files associated with a specific ROM."""
    db, config = get_db_and_config()

    console.print(f"[cyan]Finding saves for:[/cyan] {rom_path}")

    correlator = MediaCorrelator(db=db, config=config)
    save_files, state_files = correlator.find_saves_for_rom(rom_path)

    if not save_files and not state_files:
        console.print("[yellow]No save files found for this ROM[/yellow]")
        db.close()
        return

    total_size = sum(s.size_bytes for s in save_files + state_files)
    console.print(f"\n[bold]Found {len(save_files)} saves + {len(state_files)} states ({format_size(total_size)})[/bold]")

    if save_files:
        console.print("\n[yellow]Save Games:[/yellow]")
        for save in save_files:
            console.print(f"  {save.extension}: {save.path} ({format_size(save.size_bytes)})")

    if state_files:
        console.print("\n[blue]Save States:[/blue]")
        for state in state_files:
            console.print(f"  {state.extension}: {state.path} ({format_size(state.size_bytes)})")

    db.close()


# === Config Commands ===


@config_app.command("show")
def config_show():
    """Show current configuration."""
    config = get_config()

    console.print("[bold cyan]Server[/bold cyan]")
    console.print(f"  Host: {config.server.host}")
    console.print(f"  Port: {config.server.port}")
    console.print(f"  Web UI: {config.server.web_ui_enabled}")
    console.print(f"  Auth: {config.server.auth_enabled}")

    console.print("\n[bold cyan]Scanner[/bold cyan]")
    console.print(f"  Ignore fodder: {config.scanner.ignore_fodder}")
    console.print(f"  Ignore video: {config.scanner.ignore_video}")
    console.print(f"  Ignore music: {config.scanner.ignore_music}")
    console.print(f"  Ignore pictures: {config.scanner.ignore_pictures}")
    console.print(f"  RetroArch mode: {config.scanner.retroarch_mode}")

    console.print("\n[bold cyan]Paths[/bold cyan]")
    console.print(f"  Working dir: {config.paths.working_dir}")
    console.print(f"  Database: {config.paths.database}")
    console.print(f"  Duplicates dir: {config.paths.duplicates_dir}")

    if config.remotes:
        console.print("\n[bold cyan]Remotes[/bold cyan]")
        for name, remote in config.remotes.items():
            console.print(f"  {name}: {remote.host}:{remote.port}")


@config_app.command("set")
def config_set(
    key: str = typer.Argument(..., help="Config key (e.g., scanner.retroarch_mode)"),
    value: str = typer.Argument(..., help="Value to set"),
):
    """Set a configuration value."""
    config = get_config()

    # Parse key
    parts = key.split(".")
    if len(parts) != 2:
        console.print("[red]Error:[/red] Key must be in format 'section.key'")
        raise typer.Exit(1)

    section, setting = parts

    # Convert value
    if value.lower() in ("true", "yes", "1"):
        typed_value: str | int | bool = True
    elif value.lower() in ("false", "no", "0"):
        typed_value = False
    elif value.isdigit():
        typed_value = int(value)
    else:
        typed_value = value

    # Apply setting
    if section == "server":
        setattr(config.server, setting, typed_value)
    elif section == "scanner":
        setattr(config.scanner, setting, typed_value)
    elif section == "paths":
        setattr(config.paths, setting, typed_value)
    else:
        console.print(f"[red]Error:[/red] Unknown section '{section}'")
        raise typer.Exit(1)

    config.save()
    console.print(f"[green]Set {key} = {typed_value}[/green]")


@config_app.command("generate-key")
def config_generate_key():
    """Generate a new API key."""
    config = get_config()
    config.server.api_key = generate_api_key()
    config.save()
    console.print(f"[green]New API key:[/green] {config.server.api_key}")


@config_app.command("path")
def config_path():
    """Show configuration file path."""
    config = get_config()
    console.print(config.config_file)


# === Remote Commands ===


@remote_app.command("add")
def remote_add(
    name: str = typer.Argument(..., help="Name for the remote host"),
    host: str = typer.Argument(..., help="Host address (e.g., 192.168.1.50)"),
    port: int = typer.Option(8420, "--port", "-p", help="Port number"),
    api_key: str = typer.Option("", "--key", "-k", help="API key"),
):
    """Add a remote host configuration."""
    config = get_config()

    if name in config.remotes:
        console.print(f"[red]Error:[/red] Remote '{name}' already exists")
        raise typer.Exit(1)

    config.add_remote(name=name, host=host, port=port, api_key=api_key)
    config.save()

    console.print(f"[green]Added remote:[/green] {name} ({host}:{port})")


@remote_app.command("remove")
def remote_remove(
    name: str = typer.Argument(..., help="Name of the remote to remove"),
):
    """Remove a remote host configuration."""
    config = get_config()

    if config.remove_remote(name):
        config.save()
        console.print(f"[green]Removed remote:[/green] {name}")
    else:
        console.print(f"[red]Error:[/red] Remote '{name}' not found")
        raise typer.Exit(1)


@remote_app.command("list")
def remote_list():
    """List configured remote hosts."""
    config = get_config()

    if not config.remotes:
        console.print("No remote hosts configured")
        return

    table = Table(title="Remote Hosts")
    table.add_column("Name", style="cyan")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("Has Key")

    for name, remote in config.remotes.items():
        table.add_row(
            name,
            remote.host,
            str(remote.port),
            "Yes" if remote.api_key else "No",
        )

    console.print(table)


@remote_app.command("scan")
def remote_scan(
    name: str = typer.Argument(..., help="Name of the remote host"),
    directory: str = typer.Argument(..., help="Directory to scan on remote"),
):
    """Scan a directory on a remote host."""
    import httpx

    config = get_config()
    remote = config.get_remote(name)

    if not remote:
        console.print(f"[red]Error:[/red] Remote '{name}' not found")
        raise typer.Exit(1)

    url = f"http://{remote.host}:{remote.port}/api/scan/sync"
    headers = {}
    if remote.api_key:
        headers["X-API-Key"] = remote.api_key

    console.print(f"[cyan]Scanning on {name}:[/cyan] {directory}")

    try:
        with httpx.Client(timeout=300) as client:
            response = client.post(
                url,
                json={"directory": directory, "full_scan": False},
                headers=headers,
            )
            response.raise_for_status()
            result = response.json()

        console.print(f"\n[green]Scan complete![/green]")
        console.print(f"  Files: {result['files_processed']}")
        console.print(f"  Duration: {result['duration_seconds']}s")

    except httpx.HTTPStatusError as e:
        console.print(f"[red]Error:[/red] {e.response.status_code} - {e.response.text}")
        raise typer.Exit(1)
    except httpx.RequestError as e:
        console.print(f"[red]Connection error:[/red] {e}")
        raise typer.Exit(1)


@remote_app.command("status")
def remote_status(
    name: str = typer.Argument(..., help="Name of the remote host"),
):
    """Check status of a remote host."""
    import httpx

    config = get_config()
    remote = config.get_remote(name)

    if not remote:
        console.print(f"[red]Error:[/red] Remote '{name}' not found")
        raise typer.Exit(1)

    url = f"http://{remote.host}:{remote.port}/api/health"

    try:
        with httpx.Client(timeout=10) as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()

        console.print(f"[green]Connected to {name}[/green]")
        console.print(f"  Version: {data['version']}")
        console.print(f"  Codename: {data['codename']}")

    except httpx.RequestError as e:
        console.print(f"[red]Connection failed:[/red] {e}")
        raise typer.Exit(1)


def main():
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()
