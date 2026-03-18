#!/usr/bin/env python3
"""Build script for DUPer packaging.

This script handles building DUPer in various formats:
- PyInstaller single binary
- pip-installable wheel
- Docker image
- Portable zip bundle

Usage:
    python scripts/build.py binary      # Build PyInstaller binary
    python scripts/build.py wheel       # Build pip wheel
    python scripts/build.py docker      # Build Docker image
    python scripts/build.py portable    # Build portable zip
    python scripts/build.py all         # Build all formats
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent.parent
DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"


def run_command(cmd: list[str], cwd: Path | None = None) -> bool:
    """Run a command and return success status."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    return result.returncode == 0


def clean_build_dirs():
    """Clean build directories."""
    print("Cleaning build directories...")
    for d in [DIST_DIR, BUILD_DIR]:
        if d.exists():
            shutil.rmtree(d)
    DIST_DIR.mkdir(parents=True, exist_ok=True)


def get_version() -> str:
    """Get version from package."""
    sys.path.insert(0, str(PROJECT_ROOT))
    from duper import __version__
    return __version__


def build_binary():
    """Build PyInstaller single-file binary."""
    print("\n=== Building PyInstaller Binary ===\n")

    # Check if PyInstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("PyInstaller not installed. Installing...")
        if not run_command([sys.executable, "-m", "pip", "install", "pyinstaller"]):
            print("Failed to install PyInstaller")
            return False

    version = get_version()
    system = platform.system().lower()
    arch = platform.machine().lower()

    # Output filename
    if system == "windows":
        output_name = f"duper-{version}-{system}-{arch}.exe"
    else:
        output_name = f"duper-{version}-{system}-{arch}"

    # PyInstaller command
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--name", output_name.replace(f".exe" if system == "windows" else "", "duper"),
        "--clean",
        # Include web static files
        "--add-data", f"duper/web/static{os.pathsep}duper/web/static",
        # Hidden imports for FastAPI/uvicorn
        "--hidden-import", "uvicorn.logging",
        "--hidden-import", "uvicorn.loops",
        "--hidden-import", "uvicorn.loops.auto",
        "--hidden-import", "uvicorn.protocols",
        "--hidden-import", "uvicorn.protocols.http",
        "--hidden-import", "uvicorn.protocols.http.auto",
        "--hidden-import", "uvicorn.protocols.websockets",
        "--hidden-import", "uvicorn.protocols.websockets.auto",
        "--hidden-import", "uvicorn.lifespan",
        "--hidden-import", "uvicorn.lifespan.on",
        # Entry point
        "duper/cli.py",
    ]

    if not run_command(cmd, cwd=PROJECT_ROOT):
        print("PyInstaller build failed")
        return False

    # Move to dist with proper name
    built_binary = DIST_DIR / ("duper.exe" if system == "windows" else "duper")
    if built_binary.exists():
        final_path = DIST_DIR / output_name
        shutil.move(built_binary, final_path)
        print(f"\nBinary built: {final_path}")
        print(f"Size: {final_path.stat().st_size / (1024*1024):.1f} MB")

    return True


def build_wheel():
    """Build pip-installable wheel."""
    print("\n=== Building Wheel ===\n")

    # Check if build is installed
    try:
        import build
    except ImportError:
        print("build not installed. Installing...")
        if not run_command([sys.executable, "-m", "pip", "install", "build"]):
            print("Failed to install build")
            return False

    if not run_command([sys.executable, "-m", "build", "--wheel"], cwd=PROJECT_ROOT):
        print("Wheel build failed")
        return False

    # List built wheels
    for wheel in DIST_DIR.glob("*.whl"):
        print(f"\nWheel built: {wheel}")
        print(f"Size: {wheel.stat().st_size / 1024:.1f} KB")

    return True


def build_docker():
    """Build Docker image."""
    print("\n=== Building Docker Image ===\n")

    version = get_version()

    # Check if Docker is available
    if shutil.which("docker") is None:
        print("Docker not found. Please install Docker.")
        return False

    # Build image
    cmd = [
        "docker", "build",
        "-t", f"duper:{version}",
        "-t", "duper:latest",
        ".",
    ]

    if not run_command(cmd, cwd=PROJECT_ROOT):
        print("Docker build failed")
        return False

    print(f"\nDocker image built: duper:{version}")
    return True


def build_portable():
    """Build portable zip bundle."""
    print("\n=== Building Portable Bundle ===\n")

    version = get_version()
    system = platform.system().lower()
    bundle_name = f"duper-{version}-portable-{system}"
    bundle_dir = BUILD_DIR / bundle_name

    # Create bundle directory
    bundle_dir.mkdir(parents=True, exist_ok=True)

    # Copy source files
    src_dir = bundle_dir / "duper"
    shutil.copytree(
        PROJECT_ROOT / "duper",
        src_dir,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    # Copy pyproject.toml
    shutil.copy(PROJECT_ROOT / "pyproject.toml", bundle_dir)

    # Create requirements.txt
    requirements = """fastapi>=0.109.0
uvicorn[standard]>=0.27.0
typer>=0.9.0
httpx>=0.26.0
paramiko>=3.4.0
pydantic>=2.5.0
toml>=0.10.2
rich>=13.0.0
"""
    (bundle_dir / "requirements.txt").write_text(requirements)

    # Create run script
    if system == "windows":
        run_script = """@echo off
python -m pip install -r requirements.txt
python -m duper %*
"""
        (bundle_dir / "run.bat").write_text(run_script)
    else:
        run_script = """#!/bin/bash
python3 -m pip install -r requirements.txt
python3 -m duper "$@"
"""
        run_path = bundle_dir / "run.sh"
        run_path.write_text(run_script)
        run_path.chmod(0o755)

    # Create README
    readme = f"""# DUPer {version} - Portable Bundle

## Quick Start

1. Install Python 3.10+ if not already installed
2. Run the setup script:
   - Linux/Mac: ./run.sh
   - Windows: run.bat

3. Or install manually:
   pip install -r requirements.txt
   python -m duper --help

## Commands

- Start server: python -m duper serve
- Scan directory: python -m duper scan /path/to/dir
- Show status: python -m duper status
- Web UI: http://localhost:8420

## Documentation

See https://github.com/eurrl/DUPer for full documentation.
"""
    (bundle_dir / "README.txt").write_text(readme)

    # Create zip
    zip_path = DIST_DIR / f"{bundle_name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in bundle_dir.rglob("*"):
            if file.is_file():
                arcname = file.relative_to(BUILD_DIR)
                zf.write(file, arcname)

    print(f"\nPortable bundle created: {zip_path}")
    print(f"Size: {zip_path.stat().st_size / (1024*1024):.1f} MB")

    # Cleanup
    shutil.rmtree(bundle_dir)

    return True


def main():
    parser = argparse.ArgumentParser(description="Build DUPer packages")
    parser.add_argument(
        "target",
        choices=["binary", "wheel", "docker", "portable", "all", "clean"],
        help="Build target",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean build directories first",
    )

    args = parser.parse_args()

    if args.clean or args.target == "clean":
        clean_build_dirs()
        if args.target == "clean":
            print("Cleaned build directories")
            return

    # Ensure dist directory exists
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    success = True

    if args.target in ("binary", "all"):
        success = build_binary() and success

    if args.target in ("wheel", "all"):
        success = build_wheel() and success

    if args.target in ("docker", "all"):
        success = build_docker() and success

    if args.target in ("portable", "all"):
        success = build_portable() and success

    if success:
        print("\n=== Build Complete ===")
        print(f"\nOutput directory: {DIST_DIR}")
        for f in DIST_DIR.iterdir():
            print(f"  - {f.name}")
    else:
        print("\n=== Build Failed ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
