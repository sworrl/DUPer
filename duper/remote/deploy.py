"""Deployment utilities for installing DUPer on remote hosts."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from duper import __version__
from duper.remote.ssh import SSHConnectionInfo, SSHManager
from duper.utils.helpers import generate_api_key


@dataclass
class DeploymentResult:
    """Result of a deployment operation."""

    success: bool
    message: str
    api_key: str = ""
    install_path: str = ""
    errors: list[str] | None = None


@dataclass
class DeploymentOptions:
    """Options for deployment."""

    # Installation method
    method: str = "pip"  # pip, binary, manual

    # Installation paths
    install_dir: str = "~/.local/bin"
    config_dir: str = "~/.config/duper"
    data_dir: str = "~/.local/share/duper"

    # Server settings
    port: int = 8420
    enable_auth: bool = True
    api_key: str = ""

    # Systemd service
    create_service: bool = False
    service_name: str = "duper"

    # Local binary path (for binary method)
    local_binary: str = ""


SYSTEMD_SERVICE_TEMPLATE = """[Unit]
Description=DUPer Duplicate File Manager
After=network.target

[Service]
Type=simple
User={user}
Environment=HOME={home}
ExecStart={exec_path} serve --host 0.0.0.0 --port {port}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


class Deployer:
    """Handles deployment of DUPer to remote hosts."""

    def __init__(
        self,
        ssh: SSHManager,
        options: DeploymentOptions | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ):
        self.ssh = ssh
        self.options = options or DeploymentOptions()
        self.progress_callback = progress_callback

    def _log(self, message: str) -> None:
        """Log a message via callback if set."""
        if self.progress_callback:
            self.progress_callback(message)

    def deploy(self) -> DeploymentResult:
        """Deploy DUPer to the remote host."""
        try:
            self._log("Connecting to remote host...")
            self.ssh.connect()

            # Check prerequisites
            self._log("Checking prerequisites...")
            prereq_result = self._check_prerequisites()
            if not prereq_result.success:
                return prereq_result

            # Install based on method
            if self.options.method == "pip":
                install_result = self._install_pip()
            elif self.options.method == "binary":
                install_result = self._install_binary()
            else:
                install_result = self._install_manual()

            if not install_result.success:
                return install_result

            # Configure
            self._log("Configuring DUPer...")
            config_result = self._configure()
            if not config_result.success:
                return config_result

            # Create systemd service if requested
            if self.options.create_service:
                self._log("Creating systemd service...")
                service_result = self._create_systemd_service(install_result.install_path)
                if not service_result.success:
                    self._log(f"Warning: Failed to create service: {service_result.message}")

            return DeploymentResult(
                success=True,
                message="Deployment successful",
                api_key=config_result.api_key,
                install_path=install_result.install_path,
            )

        except Exception as e:
            return DeploymentResult(
                success=False,
                message=f"Deployment failed: {e}",
            )

    def _check_prerequisites(self) -> DeploymentResult:
        """Check that prerequisites are met on the remote host."""
        errors = []

        # Check Python
        has_python, python_version = self.ssh.check_python()
        if not has_python:
            errors.append("Python 3 is not installed")
        else:
            self._log(f"Found {python_version}")

        # Check pip for pip installation method
        if self.options.method == "pip":
            has_pip, pip_version = self.ssh.check_pip()
            if not has_pip:
                errors.append("pip is not installed")
            else:
                self._log(f"Found {pip_version}")

        if errors:
            return DeploymentResult(
                success=False,
                message="Prerequisites not met",
                errors=errors,
            )

        return DeploymentResult(success=True, message="Prerequisites OK")

    def _install_pip(self) -> DeploymentResult:
        """Install DUPer via pip."""
        self._log("Installing DUPer via pip...")

        # Try pip3 first, then python3 -m pip
        result = self.ssh.execute(
            "pip3 install --user duper || python3 -m pip install --user duper"
        )

        if not result.success:
            return DeploymentResult(
                success=False,
                message=f"pip install failed: {result.stderr}",
            )

        # Find the installed binary
        result = self.ssh.execute("python3 -c 'import duper; print(duper.__file__)'")
        if result.success:
            install_path = "~/.local/bin/duper"
        else:
            install_path = "duper"  # Assume it's in PATH

        return DeploymentResult(
            success=True,
            message="pip install successful",
            install_path=install_path,
        )

    def _install_binary(self) -> DeploymentResult:
        """Install DUPer by uploading a binary."""
        if not self.options.local_binary:
            return DeploymentResult(
                success=False,
                message="No local binary specified",
            )

        local_path = Path(self.options.local_binary)
        if not local_path.exists():
            return DeploymentResult(
                success=False,
                message=f"Binary not found: {local_path}",
            )

        self._log(f"Uploading binary from {local_path}...")

        # Expand remote path
        result = self.ssh.execute(f"echo {self.options.install_dir}")
        remote_dir = result.stdout.strip()

        # Ensure directory exists
        self.ssh.execute(f"mkdir -p {remote_dir}")

        remote_path = f"{remote_dir}/duper"

        # Upload binary
        def progress(transferred: int, total: int):
            pct = (transferred / total) * 100 if total else 0
            self._log(f"Uploading: {pct:.1f}%")

        self.ssh.upload_file(local_path, remote_path, progress_callback=progress)

        # Make executable
        self.ssh.execute(f"chmod +x {remote_path}")

        return DeploymentResult(
            success=True,
            message="Binary upload successful",
            install_path=remote_path,
        )

    def _install_manual(self) -> DeploymentResult:
        """Install DUPer by copying source files."""
        self._log("Manual installation...")

        # For manual install, we assume duper is already available
        # Just return a basic result
        return DeploymentResult(
            success=True,
            message="Manual installation mode",
            install_path="duper",
        )

    def _configure(self) -> DeploymentResult:
        """Configure DUPer on the remote host."""
        # Generate API key if needed
        api_key = self.options.api_key
        if self.options.enable_auth and not api_key:
            api_key = generate_api_key()

        # Expand paths
        result = self.ssh.execute(f"echo {self.options.config_dir}")
        config_dir = result.stdout.strip()

        # Create config directory
        self.ssh.execute(f"mkdir -p {config_dir}")

        # Create config file
        config_content = f"""[server]
port = {self.options.port}
host = "0.0.0.0"
web_ui_enabled = true
auth_enabled = {str(self.options.enable_auth).lower()}
api_key = "{api_key}"

[scanner]
ignore_fodder = true
ignore_video = true
ignore_music = true
ignore_pictures = true
retroarch_mode = true

[paths]
working_dir = "{self.options.data_dir}"
database = "{self.options.data_dir}/duper.db"
duplicates_dir = "{self.options.data_dir}/duplicates"
"""

        config_path = f"{config_dir}/config.toml"
        self.ssh.upload_string(config_content, config_path)

        # Create data directory
        result = self.ssh.execute(f"echo {self.options.data_dir}")
        data_dir = result.stdout.strip()
        self.ssh.execute(f"mkdir -p {data_dir}")

        return DeploymentResult(
            success=True,
            message="Configuration created",
            api_key=api_key,
        )

    def _create_systemd_service(self, exec_path: str) -> DeploymentResult:
        """Create a systemd user service."""
        # Get user info
        result = self.ssh.execute("whoami")
        user = result.stdout.strip()

        result = self.ssh.execute("echo $HOME")
        home = result.stdout.strip()

        # Expand exec path
        result = self.ssh.execute(f"echo {exec_path}")
        exec_path = result.stdout.strip()

        # Generate service file
        service_content = SYSTEMD_SERVICE_TEMPLATE.format(
            user=user,
            home=home,
            exec_path=exec_path,
            port=self.options.port,
        )

        # Create systemd user directory
        service_dir = f"{home}/.config/systemd/user"
        self.ssh.execute(f"mkdir -p {service_dir}")

        # Write service file
        service_file = f"{service_dir}/{self.options.service_name}.service"
        self.ssh.upload_string(service_content, service_file)

        # Reload systemd
        result = self.ssh.execute("systemctl --user daemon-reload")
        if not result.success:
            return DeploymentResult(
                success=False,
                message=f"Failed to reload systemd: {result.stderr}",
            )

        # Enable service
        result = self.ssh.execute(f"systemctl --user enable {self.options.service_name}")
        if not result.success:
            self._log(f"Warning: Failed to enable service: {result.stderr}")

        return DeploymentResult(
            success=True,
            message="Systemd service created",
        )

    def start_service(self) -> DeploymentResult:
        """Start the DUPer systemd service."""
        result = self.ssh.execute(f"systemctl --user start {self.options.service_name}")
        if result.success:
            return DeploymentResult(success=True, message="Service started")
        return DeploymentResult(success=False, message=f"Failed to start: {result.stderr}")

    def stop_service(self) -> DeploymentResult:
        """Stop the DUPer systemd service."""
        result = self.ssh.execute(f"systemctl --user stop {self.options.service_name}")
        if result.success:
            return DeploymentResult(success=True, message="Service stopped")
        return DeploymentResult(success=False, message=f"Failed to stop: {result.stderr}")

    def service_status(self) -> DeploymentResult:
        """Get the status of the DUPer systemd service."""
        result = self.ssh.execute(f"systemctl --user status {self.options.service_name}")
        return DeploymentResult(
            success=result.success,
            message=result.stdout,
        )


def deploy_to_host(
    ssh: SSHManager,
    options: DeploymentOptions | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> DeploymentResult:
    """Deploy DUPer to a remote host via SSH."""
    deployer = Deployer(ssh=ssh, options=options, progress_callback=progress_callback)
    return deployer.deploy()
