#!/usr/bin/env python3
"""
DUPer Deployment Script - Idempotent installer and manager.

Commands:
    install           Install DUPer and set up HTTPS
    start             Start the DUPer server and watchdog
    stop              Stop all DUPer services
    restart           Restart all DUPer services
    status            Show service status
    logs              Show server logs
    uninstall         Remove DUPer installation

Options:
    --no-service      Don't create systemd services
    --no-https        Don't set up HTTPS (nginx, SSL)
    --no-watchdog     Don't create acquisition watchdog service
    --port PORT       Server port (default: 8420)
    --show-key        Show API key after install

Examples:
    ./deploy.py install          # Fresh install with HTTPS
    ./deploy.py install --no-https   # Install without HTTPS setup
    ./deploy.py start            # Start the server + watchdog
    ./deploy.py logs -f          # Follow logs
"""

import argparse
import getpass
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path


# === Configuration ===
REPO_URL = "https://github.com/eurrl/DUPer.git"
REPO_BRANCH = "main"
HOSTNAME = "duper.localhost"
DEFAULT_PORT = 8420

# Installation paths
INSTALL_DIR = Path.home() / ".local" / "share" / "duper"
VENV_DIR = INSTALL_DIR / "venv"
REPO_DIR = INSTALL_DIR / "repo"
CONFIG_DIR = Path.home() / ".config" / "duper"
DATA_DIR = Path.home() / ".local" / "share" / "duper" / "data"
BIN_DIR = Path.home() / ".local" / "bin"

# System paths (require sudo)
SSL_DIR = Path("/opt/duper/ssl")
NGINX_AVAILABLE = Path("/etc/nginx/sites-available")
NGINX_ENABLED = Path("/etc/nginx/sites-enabled")
HOSTS_FILE = Path("/etc/hosts")

# Shell scripts to install
SHELL_SCRIPTS = [
    "transfer-worker.sh",
    "media-worker.sh",
    "acquisition-worker.sh",
    "acquisition-watchdog.sh",
    "build-game-index.sh",
    "build-rom-index.sh",
    "sync-media-cache.sh",
    "auto-sync.sh",
    "xbox-iso-convert.sh",
    "live-capture.sh",
    "deck-export-configs.sh",
    "deck-import-configs.sh",
]


class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_status(msg: str, status: str = "info"):
    """Print a status message with color."""
    colors = {
        "info": Colors.CYAN,
        "success": Colors.GREEN,
        "warning": Colors.YELLOW,
        "error": Colors.RED,
    }
    color = colors.get(status, Colors.CYAN)
    prefix = {"info": "->", "success": "OK", "warning": "!!", "error": "XX"}.get(status, "->")
    print(f"{color}[{prefix}]{Colors.END} {msg}")


def print_header(msg: str):
    """Print a header."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}=== {msg} ==={Colors.END}\n")


def run_cmd(cmd: list[str], check: bool = True, capture: bool = False, **kwargs) -> subprocess.CompletedProcess:
    """Run a command."""
    if capture:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    result = subprocess.run(cmd, **kwargs)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def run_sudo(cmd: list[str], password: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command with sudo."""
    full_cmd = ["sudo", "-S"] + cmd
    result = subprocess.run(
        full_cmd,
        input=password + "\n",
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"Sudo command failed: {' '.join(cmd)}\n{result.stderr}")
    return result


def get_sudo_password() -> str:
    """Prompt for sudo password and verify it works."""
    print_header("Administrator Access Required")
    print("HTTPS setup requires sudo to:")
    print("  - Create SSL certificates in /opt/duper/ssl/")
    print("  - Add nginx configuration")
    print(f"  - Add {HOSTNAME} to /etc/hosts")
    print()

    for attempt in range(3):
        password = getpass.getpass("Enter sudo password: ")

        # Test the password
        result = subprocess.run(
            ["sudo", "-S", "-v"],
            input=password + "\n",
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            print_status("Sudo access verified", "success")
            return password
        else:
            print_status("Invalid password, try again", "error")

    print_status("Too many failed attempts", "error")
    sys.exit(1)


def check_prerequisites() -> bool:
    """Check that required tools are available."""
    print_header("Checking Prerequisites")

    all_ok = True

    # Check Python version
    py_version = sys.version_info
    if py_version >= (3, 10):
        print_status(f"Python {py_version.major}.{py_version.minor}.{py_version.micro}", "success")
    else:
        print_status(f"Python {py_version.major}.{py_version.minor} (need 3.10+)", "error")
        all_ok = False

    # Check git
    if shutil.which("git"):
        result = run_cmd(["git", "--version"], capture=True, check=False)
        print_status(f"Git: {result.stdout.strip()}", "success")
    else:
        print_status("Git not found", "error")
        all_ok = False

    # Check pip/venv
    try:
        import venv
        print_status("Python venv module available", "success")
    except ImportError:
        print_status("Python venv module not found", "error")
        all_ok = False

    return all_ok


def check_https_prerequisites() -> bool:
    """Check prerequisites for HTTPS setup."""
    all_ok = True

    # Check nginx
    if shutil.which("nginx"):
        print_status("nginx available", "success")
    else:
        print_status("nginx not found - HTTPS setup will be skipped", "warning")
        all_ok = False

    # Check openssl
    if shutil.which("openssl"):
        print_status("openssl available", "success")
    else:
        print_status("openssl not found - HTTPS setup will be skipped", "warning")
        all_ok = False

    return all_ok


def clone_or_update_repo() -> bool:
    """Clone the repository or update if it exists."""
    print_header("Setting Up Repository")

    REPO_DIR.parent.mkdir(parents=True, exist_ok=True)

    if (REPO_DIR / ".git").exists():
        print_status("Repository exists, updating...")
        try:
            run_cmd(["git", "fetch", "origin"], cwd=REPO_DIR)
            run_cmd(["git", "reset", "--hard", f"origin/{REPO_BRANCH}"], cwd=REPO_DIR)
            run_cmd(["git", "clean", "-fd"], cwd=REPO_DIR)
            print_status("Repository updated", "success")
        except Exception as e:
            print_status(f"Update failed: {e}", "error")
            return False
    else:
        print_status(f"Cloning from {REPO_URL}...")
        try:
            run_cmd(["git", "clone", "--branch", REPO_BRANCH, REPO_URL, str(REPO_DIR)])
            print_status("Repository cloned", "success")
        except Exception as e:
            print_status(f"Clone failed: {e}", "error")
            return False

    return True


def setup_venv() -> bool:
    """Create or update the virtual environment."""
    print_header("Setting Up Virtual Environment")

    if not VENV_DIR.exists():
        print_status("Creating virtual environment...")
        try:
            import venv
            venv.create(VENV_DIR, with_pip=True)
            print_status("Virtual environment created", "success")
        except Exception as e:
            print_status(f"Failed to create venv: {e}", "error")
            return False
    else:
        print_status("Virtual environment exists", "success")

    # Get pip path
    if os.name == "nt":
        pip_path = VENV_DIR / "Scripts" / "pip"
        python_path = VENV_DIR / "Scripts" / "python"
    else:
        pip_path = VENV_DIR / "bin" / "pip"
        python_path = VENV_DIR / "bin" / "python"

    # Upgrade pip
    print_status("Upgrading pip...")
    try:
        run_cmd([str(python_path), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    except Exception:
        pass  # Non-critical

    # Install package (includes textual, aiohttp, and all dependencies from pyproject.toml)
    print_status("Installing DUPer and dependencies...")
    try:
        run_cmd([str(pip_path), "install", "-e", str(REPO_DIR), "-q"])
        print_status("Dependencies installed (fastapi, uvicorn, typer, textual, aiohttp, ...)", "success")
    except Exception as e:
        print_status(f"Installation failed: {e}", "error")
        return False

    return True


def create_wrapper_scripts() -> bool:
    """Create wrapper scripts in ~/.local/bin."""
    print_header("Creating CLI Wrappers")

    BIN_DIR.mkdir(parents=True, exist_ok=True)

    if os.name == "nt":
        venv_python = VENV_DIR / "Scripts" / "python"
    else:
        venv_python = VENV_DIR / "bin" / "python"

    # Main duper wrapper
    wrapper_path = BIN_DIR / "duper"
    if os.name == "nt":
        wrapper_path = BIN_DIR / "duper.bat"
        content = f'@echo off\n"{venv_python}" -m duper %*\n'
    else:
        content = f'''#!/bin/bash
# DUPer CLI wrapper - auto-generated by deploy.py
exec "{venv_python}" -m duper "$@"
'''

    wrapper_path.write_text(content)
    if os.name != "nt":
        wrapper_path.chmod(0o755)
    print_status(f"Created {wrapper_path}", "success")

    # TUI wrapper (duper-tui)
    tui_wrapper_path = BIN_DIR / "duper-tui"
    if os.name == "nt":
        tui_wrapper_path = BIN_DIR / "duper-tui.bat"
        tui_content = f'@echo off\n"{venv_python}" -c "from duper.tui import main; main()" %*\n'
    else:
        tui_content = f'''#!/bin/bash
# DUPer TUI wrapper - auto-generated by deploy.py
exec "{venv_python}" -c "from duper.tui import main; main()" "$@"
'''

    tui_wrapper_path.write_text(tui_content)
    if os.name != "nt":
        tui_wrapper_path.chmod(0o755)
    print_status(f"Created {tui_wrapper_path}", "success")

    # Check if ~/.local/bin is in PATH
    path_dirs = os.environ.get("PATH", "").split(os.pathsep)
    if str(BIN_DIR) not in path_dirs:
        print_status(f"Add {BIN_DIR} to your PATH:", "warning")
        print(f"    export PATH=\"{BIN_DIR}:$PATH\"")

    return True


def install_shell_scripts() -> bool:
    """Install shell scripts to ~/.local/bin."""
    print_header("Installing Shell Scripts")

    BIN_DIR.mkdir(parents=True, exist_ok=True)
    scripts_dir = REPO_DIR / "scripts"

    installed = 0
    for script_name in SHELL_SCRIPTS:
        src = scripts_dir / script_name
        dst = BIN_DIR / f"duper-{script_name}"

        if src.exists():
            shutil.copy2(src, dst)
            dst.chmod(0o755)
            installed += 1
        else:
            print_status(f"Script not found: {script_name}", "warning")

    print_status(f"Installed {installed}/{len(SHELL_SCRIPTS)} scripts to {BIN_DIR}", "success")
    return True


def setup_config(port: int = 8420) -> str:
    """Create configuration file with API key."""
    print_header("Configuring DUPer")

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    config_file = CONFIG_DIR / "config.toml"

    # Generate API key if config doesn't exist or doesn't have one
    api_key = ""
    if config_file.exists():
        content = config_file.read_text()
        for line in content.split("\n"):
            if line.strip().startswith("api_key"):
                if '"' in line:
                    api_key = line.split('"')[1]
                break
        print_status("Existing config found, preserving API key", "success")

    if not api_key:
        api_key = secrets.token_urlsafe(32)
        print_status("Generated new API key", "success")

    config_content = f'''# DUPer Configuration
# Auto-generated by deploy.py

[server]
port = {port}
host = "0.0.0.0"
web_ui_enabled = true
auth_enabled = true
api_key = "{api_key}"

[scanner]
ignore_fodder = true
ignore_video = true
ignore_music = true
ignore_pictures = true
retroarch_mode = true

[paths]
working_dir = "{DATA_DIR}"
database = "{DATA_DIR / "duper.db"}"
duplicates_dir = "{DATA_DIR / "duplicates"}"

[retroachievements]
enabled = false
username = ""
api_key = ""
ra_score_bonus = 1000
verify_on_scan = true

[screenscraper]
enabled = false
username = ""
password = ""
'''

    config_file.write_text(config_content)
    print_status(f"Config saved to {config_file}", "success")

    return api_key


def setup_https(password: str, port: int = 8420) -> bool:
    """Set up HTTPS with nginx and self-signed certificates."""
    print_header("Setting Up HTTPS")

    # Create SSL directory
    print_status("Creating SSL directory...")
    run_sudo(["mkdir", "-p", str(SSL_DIR)], password)

    # Generate self-signed certificate
    print_status("Generating SSL certificate...")
    run_sudo([
        "openssl", "req", "-x509", "-nodes", "-days", "365",
        "-newkey", "rsa:2048",
        "-keyout", str(SSL_DIR / "key.pem"),
        "-out", str(SSL_DIR / "cert.pem"),
        "-subj", f"/CN={HOSTNAME}",
        "-addext", f"subjectAltName=DNS:{HOSTNAME}"
    ], password)
    print_status("SSL certificate generated", "success")

    # Check if hosts entry exists
    hosts_content = HOSTS_FILE.read_text()
    if HOSTNAME not in hosts_content:
        print_status(f"Adding {HOSTNAME} to /etc/hosts...")
        run_sudo(["bash", "-c", f'echo "127.0.0.1 {HOSTNAME}" >> /etc/hosts'], password)
        print_status(f"Added {HOSTNAME} to /etc/hosts", "success")
    else:
        print_status(f"{HOSTNAME} already in /etc/hosts", "success")

    # Create nginx config
    nginx_config = f'''# DUPer - Auto-generated by deploy.py
# HTTP server - redirect to HTTPS
server {{
    listen 80;
    listen [::]:80;
    server_name {HOSTNAME};

    return 301 https://$host$request_uri;
}}

# HTTPS server
server {{
    listen 443 ssl;
    listen [::]:443 ssl;
    http2 on;

    server_name {HOSTNAME};

    ssl_certificate {SSL_DIR}/cert.pem;
    ssl_certificate_key {SSL_DIR}/key.pem;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;

    client_max_body_size 100M;

    # Proxy all requests to DUPer server
    location / {{
        proxy_pass http://127.0.0.1:{port};
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }}
}}
'''

    # Write nginx config via sudo
    config_path = NGINX_AVAILABLE / "duper.conf"
    print_status("Creating nginx configuration...")

    # Write to temp file then move with sudo
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write(nginx_config)
        temp_path = f.name

    run_sudo(["cp", temp_path, str(config_path)], password)
    os.unlink(temp_path)
    print_status(f"Created {config_path}", "success")

    # Enable the site
    enabled_path = NGINX_ENABLED / "duper.conf"
    run_sudo(["ln", "-sf", str(config_path), str(enabled_path)], password, check=False)
    print_status("Enabled nginx site", "success")

    # Test nginx config
    print_status("Testing nginx configuration...")
    result = run_sudo(["nginx", "-t"], password, check=False)
    if result.returncode != 0:
        print_status("nginx config test failed", "error")
        print(result.stderr)
        return False
    print_status("nginx config test passed", "success")

    # Reload nginx
    print_status("Reloading nginx...")
    run_sudo(["systemctl", "reload", "nginx"], password)
    print_status("nginx reloaded", "success")

    return True


def setup_systemd_services(port: int = 8420, autostart: bool = True, watchdog: bool = True) -> bool:
    """Create systemd user services for DUPer server and watchdog."""
    print_header("Setting Up Systemd Services")

    if not shutil.which("systemctl"):
        print_status("systemctl not found, skipping service setup", "warning")
        return True

    service_dir = Path.home() / ".config" / "systemd" / "user"
    service_dir.mkdir(parents=True, exist_ok=True)

    venv_python = VENV_DIR / "bin" / "python"

    # --- DUPer main service ---
    service_file = service_dir / "duper.service"
    service_content = f'''[Unit]
Description=DUPer - ROM Collection Manager
Documentation=https://github.com/eurrl/DUPer
After=network.target

[Service]
Type=simple
Environment=HOME={Path.home()}
ExecStart={venv_python} -m duper serve --host 127.0.0.1 --port {port} --no-auth
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
'''

    service_file.write_text(service_content)
    print_status(f"Created {service_file}", "success")

    # --- Acquisition watchdog service ---
    if watchdog:
        watchdog_file = service_dir / "duper-watchdog.service"
        watchdog_script = BIN_DIR / "duper-acquisition-watchdog.sh"
        watchdog_content = f'''[Unit]
Description=DUPer Acquisition Watchdog - Continuous background downloading
Documentation=https://github.com/eurrl/DUPer
After=network.target duper.service
Wants=duper.service

[Service]
Type=simple
Environment=HOME={Path.home()}
ExecStart={watchdog_script}
Restart=on-failure
RestartSec=30

[Install]
WantedBy=default.target
'''

        watchdog_file.write_text(watchdog_content)
        print_status(f"Created {watchdog_file}", "success")

    # Reload systemd
    try:
        run_cmd(["systemctl", "--user", "daemon-reload"], check=False)
        print_status("Systemd daemon reloaded", "success")
    except Exception:
        print_status("Could not reload systemd", "warning")
        return True

    if autostart:
        try:
            run_cmd(["systemctl", "--user", "enable", "duper"], check=False)
            print_status("DUPer service enabled for autostart", "success")
        except Exception:
            print_status("Could not enable DUPer service", "warning")

        if watchdog:
            try:
                run_cmd(["systemctl", "--user", "enable", "duper-watchdog"], check=False)
                print_status("Watchdog service enabled for autostart", "success")
            except Exception:
                print_status("Could not enable watchdog service", "warning")

    return True


def start_service() -> bool:
    """Start the DUPer services."""
    print_header("Starting DUPer")

    if not shutil.which("systemctl"):
        print_status("systemctl not found", "error")
        return False

    success = True

    try:
        run_cmd(["systemctl", "--user", "start", "duper"])
        print_status("DUPer server started", "success")
    except Exception as e:
        print_status(f"Could not start DUPer server: {e}", "error")
        success = False

    # Start watchdog if service exists
    watchdog_file = Path.home() / ".config" / "systemd" / "user" / "duper-watchdog.service"
    if watchdog_file.exists():
        try:
            run_cmd(["systemctl", "--user", "start", "duper-watchdog"], check=False)
            print_status("Acquisition watchdog started", "success")
        except Exception:
            print_status("Could not start watchdog (non-critical)", "warning")

    if success:
        print(f"\n  Access DUPer at: {Colors.GREEN}https://{HOSTNAME}{Colors.END}")
        print(f"  Or: {Colors.GREEN}http://localhost:{DEFAULT_PORT}{Colors.END}\n")

    return success


def stop_service() -> bool:
    """Stop all DUPer services."""
    print_header("Stopping DUPer")

    if not shutil.which("systemctl"):
        print_status("systemctl not found", "error")
        return False

    # Stop watchdog first
    run_cmd(["systemctl", "--user", "stop", "duper-watchdog"], check=False)
    run_cmd(["systemctl", "--user", "stop", "duper"], check=False)
    print_status("All services stopped", "success")
    return True


def restart_service() -> bool:
    """Restart all DUPer services."""
    print_header("Restarting DUPer")

    if not shutil.which("systemctl"):
        print_status("systemctl not found", "error")
        return False

    try:
        run_cmd(["systemctl", "--user", "restart", "duper"])
        print_status("DUPer server restarted", "success")
    except Exception as e:
        print_status(f"Could not restart server: {e}", "error")
        return False

    # Restart watchdog if service exists
    watchdog_file = Path.home() / ".config" / "systemd" / "user" / "duper-watchdog.service"
    if watchdog_file.exists():
        run_cmd(["systemctl", "--user", "restart", "duper-watchdog"], check=False)
        print_status("Acquisition watchdog restarted", "success")

    print(f"\n  Access DUPer at: {Colors.GREEN}https://{HOSTNAME}{Colors.END}")
    print(f"  Or: {Colors.GREEN}http://localhost:{DEFAULT_PORT}{Colors.END}\n")
    return True


def show_status() -> bool:
    """Show service status."""
    if not shutil.which("systemctl"):
        print_status("systemctl not found", "error")
        return False

    print_header("DUPer Service Status")
    result = run_cmd(["systemctl", "--user", "status", "duper"], check=False)

    # Also show watchdog status
    watchdog_file = Path.home() / ".config" / "systemd" / "user" / "duper-watchdog.service"
    if watchdog_file.exists():
        print()
        print_header("Watchdog Service Status")
        run_cmd(["systemctl", "--user", "status", "duper-watchdog"], check=False)

    return result.returncode == 0


def show_logs(follow: bool = False, service: str = "duper") -> bool:
    """Show service logs."""
    if not shutil.which("journalctl"):
        print_status("journalctl not found", "error")
        return False

    cmd = ["journalctl", "--user", "-u", service, "-n", "50"]
    if follow:
        cmd.append("-f")

    try:
        subprocess.run(cmd)
        return True
    except KeyboardInterrupt:
        return True
    except Exception:
        return False


def uninstall(password: str = None) -> bool:
    """Remove DUPer installation."""
    print_header("Uninstalling DUPer")

    # Stop services
    if shutil.which("systemctl"):
        print_status("Stopping services...")
        run_cmd(["systemctl", "--user", "stop", "duper-watchdog"], check=False)
        run_cmd(["systemctl", "--user", "stop", "duper"], check=False)
        run_cmd(["systemctl", "--user", "disable", "duper-watchdog"], check=False)
        run_cmd(["systemctl", "--user", "disable", "duper"], check=False)

        service_dir = Path.home() / ".config" / "systemd" / "user"
        for svc in ["duper.service", "duper-watchdog.service"]:
            svc_file = service_dir / svc
            if svc_file.exists():
                svc_file.unlink()
                print_status(f"Removed {svc}", "success")

    # Remove HTTPS setup if password provided
    if password:
        print_status("Removing HTTPS configuration...")
        run_sudo(["rm", "-f", str(NGINX_ENABLED / "duper.conf")], password, check=False)
        run_sudo(["rm", "-f", str(NGINX_AVAILABLE / "duper.conf")], password, check=False)
        run_sudo(["rm", "-rf", str(SSL_DIR.parent)], password, check=False)
        run_sudo(["systemctl", "reload", "nginx"], password, check=False)
        print_status("Removed nginx configuration", "success")

    # Remove wrappers and installed scripts
    for name in ["duper", "duper-tui"]:
        wrapper = BIN_DIR / name
        if wrapper.exists():
            wrapper.unlink()
            print_status(f"Removed {name} wrapper", "success")

    for script_name in SHELL_SCRIPTS:
        script = BIN_DIR / f"duper-{script_name}"
        if script.exists():
            script.unlink()

    print_status("Removed installed scripts", "success")

    # Remove installation directory
    if INSTALL_DIR.exists():
        shutil.rmtree(INSTALL_DIR)
        print_status(f"Removed {INSTALL_DIR}", "success")

    print_status(f"Config directory: {CONFIG_DIR}", "info")
    print_status(f"Data directory: {DATA_DIR}", "info")
    print("Remove manually for complete uninstall:")
    print(f"    rm -rf {CONFIG_DIR}")
    print(f"    rm -rf {DATA_DIR}")

    print_status("Uninstall complete", "success")
    return True


def get_local_ip() -> str:
    """Get local IP address for display."""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


def print_success_message(api_key: str, port: int, show_key: bool, https_enabled: bool):
    """Print success message with access info."""
    print_header("Installation Complete!")

    local_ip = get_local_ip()

    print(f"{Colors.GREEN}DUPer v2.3.5 has been installed successfully!{Colors.END}\n")

    print(f"{Colors.BOLD}Access URLs:{Colors.END}")
    if https_enabled:
        print(f"  {Colors.GREEN}https://{HOSTNAME}{Colors.END}  (recommended)")
    print(f"  http://localhost:{port}")
    print(f"  http://{local_ip}:{port}")
    print()

    if show_key:
        print(f"{Colors.BOLD}API Key:{Colors.END}")
        print(f"  {Colors.YELLOW}{api_key}{Colors.END}\n")
    else:
        print(f"{Colors.BOLD}API Key:{Colors.END}")
        print(f"  Stored in {CONFIG_DIR / 'config.toml'}")
        print(f"  Run with --show-key to display\n")

    print(f"{Colors.BOLD}Commands:{Colors.END}")
    print("  duper serve            # Start server (foreground)")
    print("  duper-tui              # Launch terminal dashboard")
    print("  ./deploy.py start      # Start server + watchdog (systemd)")
    print("  ./deploy.py stop       # Stop all services")
    print("  ./deploy.py restart    # Restart all services")
    print("  ./deploy.py status     # Check status")
    print("  ./deploy.py logs -f    # Follow logs")
    print()

    print(f"{Colors.BOLD}Installed scripts:{Colors.END}")
    for script_name in SHELL_SCRIPTS:
        print(f"  duper-{script_name}")
    print()


def cmd_install(args):
    """Install command handler."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}+---------------------------------------+{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}|     DUPer v2.3.5 Installation         |{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}+---------------------------------------+{Colors.END}")

    # Check prerequisites
    if not check_prerequisites():
        print_status("Prerequisites not met", "error")
        sys.exit(1)

    # Check HTTPS prerequisites
    https_available = check_https_prerequisites() and not args.no_https

    # Get sudo password if HTTPS is enabled
    sudo_password = None
    if https_available:
        sudo_password = get_sudo_password()

    # Stop services if updating
    stop_service()

    # Clone or update repo
    if not clone_or_update_repo():
        sys.exit(1)

    # Setup virtual environment
    if not setup_venv():
        sys.exit(1)

    # Create wrapper scripts (duper + duper-tui)
    if not create_wrapper_scripts():
        sys.exit(1)

    # Install shell scripts
    if not install_shell_scripts():
        sys.exit(1)

    # Setup configuration
    api_key = setup_config(port=args.port)

    # Setup systemd services (server + watchdog)
    if not args.no_service:
        setup_systemd_services(
            port=args.port,
            autostart=True,
            watchdog=not args.no_watchdog,
        )

    # Setup HTTPS
    https_enabled = False
    if https_available and sudo_password:
        if setup_https(sudo_password, port=args.port):
            https_enabled = True
        else:
            print_status("HTTPS setup failed, continuing without it", "warning")

    # Start services
    if not args.no_service:
        start_service()

    # Print success message
    print_success_message(api_key, args.port, args.show_key, https_enabled)


def cmd_start(args):
    """Start command handler."""
    if not start_service():
        sys.exit(1)


def cmd_stop(args):
    """Stop command handler."""
    if not stop_service():
        sys.exit(1)


def cmd_restart(args):
    """Restart command handler."""
    if not restart_service():
        sys.exit(1)


def cmd_status(args):
    """Status command handler."""
    show_status()


def cmd_logs(args):
    """Logs command handler."""
    service = "duper-watchdog" if args.watchdog else "duper"
    show_logs(follow=args.follow, service=service)


def cmd_uninstall(args):
    """Uninstall command handler."""
    print(f"\n{Colors.BOLD}{Colors.RED}+---------------------------------------+{Colors.END}")
    print(f"{Colors.BOLD}{Colors.RED}|     DUPer Uninstall                   |{Colors.END}")
    print(f"{Colors.BOLD}{Colors.RED}+---------------------------------------+{Colors.END}")

    # Check if HTTPS was set up
    has_https = (NGINX_AVAILABLE / "duper.conf").exists()

    sudo_password = None
    if has_https:
        print("\nHTTPS configuration detected. Sudo required for complete removal.")
        sudo_password = get_sudo_password()

    uninstall(sudo_password)


def main():
    parser = argparse.ArgumentParser(
        description="DUPer Deployment Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Install command
    install_parser = subparsers.add_parser("install", help="Install DUPer")
    install_parser.add_argument("--no-service", action="store_true", help="Don't create systemd services")
    install_parser.add_argument("--no-https", action="store_true", help="Don't set up HTTPS")
    install_parser.add_argument("--no-watchdog", action="store_true", help="Don't create acquisition watchdog service")
    install_parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Server port (default: {DEFAULT_PORT})")
    install_parser.add_argument("--show-key", action="store_true", help="Show API key after install")
    install_parser.set_defaults(func=cmd_install)

    # Start command
    start_parser = subparsers.add_parser("start", help="Start DUPer server and watchdog")
    start_parser.set_defaults(func=cmd_start)

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop all DUPer services")
    stop_parser.set_defaults(func=cmd_stop)

    # Restart command
    restart_parser = subparsers.add_parser("restart", help="Restart all DUPer services")
    restart_parser.set_defaults(func=cmd_restart)

    # Status command
    status_parser = subparsers.add_parser("status", help="Show service status")
    status_parser.set_defaults(func=cmd_status)

    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Show service logs")
    logs_parser.add_argument("-f", "--follow", action="store_true", help="Follow log output")
    logs_parser.add_argument("--watchdog", action="store_true", help="Show watchdog logs instead of server logs")
    logs_parser.set_defaults(func=cmd_logs)

    # Uninstall command
    uninstall_parser = subparsers.add_parser("uninstall", help="Uninstall DUPer")
    uninstall_parser.set_defaults(func=cmd_uninstall)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
