"""SSH connection manager for remote operations."""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import paramiko


@dataclass
class SSHResult:
    """Result of an SSH command."""

    exit_code: int
    stdout: str
    stderr: str

    @property
    def success(self) -> bool:
        return self.exit_code == 0


@dataclass
class SSHConnectionInfo:
    """SSH connection information."""

    host: str
    port: int = 22
    username: str = ""
    password: str | None = None
    key_file: str | None = None


class SSHManager:
    """Manager for SSH connections and remote operations."""

    def __init__(self, connection: SSHConnectionInfo):
        self.connection = connection
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def connect(self) -> None:
        """Establish SSH connection."""
        if self._client is not None:
            return

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs = {
            "hostname": self.connection.host,
            "port": self.connection.port,
            "username": self.connection.username,
        }

        if self.connection.key_file:
            connect_kwargs["key_filename"] = self.connection.key_file
        elif self.connection.password:
            connect_kwargs["password"] = self.connection.password
        else:
            # Try to use SSH agent or default keys
            connect_kwargs["look_for_keys"] = True
            connect_kwargs["allow_agent"] = True

        self._client.connect(**connect_kwargs)

    def disconnect(self) -> None:
        """Close SSH connection."""
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None

        if self._client is not None:
            self._client.close()
            self._client = None

    def _get_sftp(self) -> paramiko.SFTPClient:
        """Get SFTP client, connecting if needed."""
        self.connect()
        if self._sftp is None:
            self._sftp = self._client.open_sftp()
        return self._sftp

    def execute(
        self,
        command: str,
        timeout: float | None = None,
        get_pty: bool = False,
    ) -> SSHResult:
        """Execute a command on the remote host."""
        self.connect()

        stdin, stdout, stderr = self._client.exec_command(
            command,
            timeout=timeout,
            get_pty=get_pty,
        )

        stdout_str = stdout.read().decode("utf-8", errors="replace")
        stderr_str = stderr.read().decode("utf-8", errors="replace")
        exit_code = stdout.channel.recv_exit_status()

        return SSHResult(
            exit_code=exit_code,
            stdout=stdout_str,
            stderr=stderr_str,
        )

    def execute_stream(
        self,
        command: str,
        output_callback: Callable[[str], None] | None = None,
        timeout: float | None = None,
    ) -> SSHResult:
        """Execute a command and stream output."""
        self.connect()

        stdin, stdout, stderr = self._client.exec_command(
            command,
            timeout=timeout,
            get_pty=True,
        )

        output_lines = []

        while not stdout.channel.exit_status_ready():
            if stdout.channel.recv_ready():
                chunk = stdout.channel.recv(1024).decode("utf-8", errors="replace")
                output_lines.append(chunk)
                if output_callback:
                    output_callback(chunk)

        # Get any remaining output
        remaining = stdout.read().decode("utf-8", errors="replace")
        if remaining:
            output_lines.append(remaining)
            if output_callback:
                output_callback(remaining)

        stderr_str = stderr.read().decode("utf-8", errors="replace")
        exit_code = stdout.channel.recv_exit_status()

        return SSHResult(
            exit_code=exit_code,
            stdout="".join(output_lines),
            stderr=stderr_str,
        )

    def upload_file(
        self,
        local_path: str | Path,
        remote_path: str,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> bool:
        """Upload a file to the remote host."""
        sftp = self._get_sftp()
        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Ensure remote directory exists
        remote_dir = os.path.dirname(remote_path)
        if remote_dir:
            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                self._mkdir_p(remote_dir)

        # Upload with progress callback
        if progress_callback:
            sftp.put(str(local_path), remote_path, callback=progress_callback)
        else:
            sftp.put(str(local_path), remote_path)

        return True

    def download_file(
        self,
        remote_path: str,
        local_path: str | Path,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> bool:
        """Download a file from the remote host."""
        sftp = self._get_sftp()
        local_path = Path(local_path)

        # Ensure local directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if progress_callback:
            sftp.get(remote_path, str(local_path), callback=progress_callback)
        else:
            sftp.get(remote_path, str(local_path))

        return True

    def upload_string(self, content: str, remote_path: str) -> bool:
        """Upload a string as a file to the remote host."""
        sftp = self._get_sftp()

        # Ensure remote directory exists
        remote_dir = os.path.dirname(remote_path)
        if remote_dir:
            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                self._mkdir_p(remote_dir)

        with sftp.file(remote_path, "w") as f:
            f.write(content)

        return True

    def read_file(self, remote_path: str) -> str:
        """Read a file from the remote host."""
        sftp = self._get_sftp()

        with sftp.file(remote_path, "r") as f:
            return f.read().decode("utf-8", errors="replace")

    def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on the remote host."""
        sftp = self._get_sftp()
        try:
            sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def _mkdir_p(self, remote_path: str) -> None:
        """Create directory and parents on remote host."""
        sftp = self._get_sftp()
        dirs = remote_path.split("/")
        current = ""

        for d in dirs:
            if not d:
                current = "/"
                continue
            current = f"{current}/{d}" if current != "/" else f"/{d}"
            try:
                sftp.stat(current)
            except FileNotFoundError:
                sftp.mkdir(current)

    def list_dir(self, remote_path: str) -> list[str]:
        """List directory contents on remote host."""
        sftp = self._get_sftp()
        return sftp.listdir(remote_path)

    def check_python(self) -> tuple[bool, str]:
        """Check if Python 3 is available on the remote host."""
        result = self.execute("python3 --version")
        if result.success:
            return True, result.stdout.strip()

        result = self.execute("python --version")
        if result.success and "Python 3" in result.stdout:
            return True, result.stdout.strip()

        return False, "Python 3 not found"

    def check_pip(self) -> tuple[bool, str]:
        """Check if pip is available on the remote host."""
        result = self.execute("pip3 --version")
        if result.success:
            return True, result.stdout.strip()

        result = self.execute("python3 -m pip --version")
        if result.success:
            return True, result.stdout.strip()

        return False, "pip not found"

    def __enter__(self) -> "SSHManager":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


def create_ssh_connection(
    host: str,
    username: str = "",
    port: int = 22,
    password: str | None = None,
    key_file: str | None = None,
) -> SSHManager:
    """Create an SSH connection manager."""
    connection = SSHConnectionInfo(
        host=host,
        port=port,
        username=username or os.getenv("USER", ""),
        password=password,
        key_file=key_file,
    )
    return SSHManager(connection)


def parse_ssh_target(target: str) -> SSHConnectionInfo:
    """
    Parse an SSH target string like user@host:port.

    Examples:
        deck@192.168.1.50
        deck@steamdeck:22
        192.168.1.50
    """
    username = ""
    port = 22

    if "@" in target:
        username, target = target.split("@", 1)

    if ":" in target:
        host, port_str = target.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            host = target
    else:
        host = target

    return SSHConnectionInfo(
        host=host,
        port=port,
        username=username or os.getenv("USER", ""),
    )
