"""HTTP agent client for connecting to remote DUPer instances."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from duper.core import RemoteHost


@dataclass
class AgentResponse:
    """Response from an agent request."""

    success: bool
    data: dict[str, Any] | None = None
    error: str | None = None


class AgentClient:
    """HTTP client for communicating with remote DUPer agents."""

    def __init__(self, remote: RemoteHost, timeout: float = 30.0):
        self.remote = remote
        self.timeout = timeout
        self._base_url = f"http://{remote.host}:{remote.port}"

    def _get_headers(self) -> dict[str, str]:
        """Get request headers including API key if set."""
        headers = {"Content-Type": "application/json"}
        if self.remote.api_key:
            headers["X-API-Key"] = self.remote.api_key
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> AgentResponse:
        """Make a request to the remote agent."""
        url = f"{self._base_url}{endpoint}"

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    json=json_data,
                    params=params,
                )
                response.raise_for_status()
                return AgentResponse(success=True, data=response.json())

        except httpx.HTTPStatusError as e:
            error_detail = e.response.text
            try:
                error_json = e.response.json()
                error_detail = error_json.get("detail", error_detail)
            except Exception:
                pass
            return AgentResponse(success=False, error=f"HTTP {e.response.status_code}: {error_detail}")

        except httpx.RequestError as e:
            return AgentResponse(success=False, error=f"Connection error: {e}")

    # === Health & Status ===

    def health(self) -> AgentResponse:
        """Check agent health."""
        return self._request("GET", "/api/health")

    def stats(
        self,
        include_system: bool = False,
        include_metrics: bool = False,
        include_history: bool = False,
    ) -> AgentResponse:
        """Get agent statistics."""
        params = {
            "include_system": include_system,
            "include_metrics": include_metrics,
            "include_history": include_history,
        }
        return self._request("GET", "/api/stats", params=params)

    # === Scanning ===

    def scan(self, directory: str, full_scan: bool = False) -> AgentResponse:
        """Start an async scan on the remote agent."""
        return self._request(
            "POST",
            "/api/scan",
            json_data={"directory": directory, "full_scan": full_scan},
        )

    def scan_sync(self, directory: str, full_scan: bool = False) -> AgentResponse:
        """Run a synchronous scan on the remote agent."""
        # Use longer timeout for sync scan
        old_timeout = self.timeout
        self.timeout = 600.0  # 10 minutes
        try:
            return self._request(
                "POST",
                "/api/scan/sync",
                json_data={"directory": directory, "full_scan": full_scan},
            )
        finally:
            self.timeout = old_timeout

    def scan_status(self, scan_id: str) -> AgentResponse:
        """Get the status of an ongoing scan."""
        return self._request("GET", f"/api/scan/{scan_id}/status")

    def scan_result(self, scan_id: str) -> AgentResponse:
        """Get the result of a completed scan."""
        return self._request("GET", f"/api/scan/{scan_id}/result")

    # === Files ===

    def list_files(
        self,
        directory: str,
        duplicates_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> AgentResponse:
        """List files in a directory."""
        return self._request(
            "GET",
            "/api/files",
            params={
                "directory": directory,
                "duplicates_only": duplicates_only,
                "limit": limit,
                "offset": offset,
            },
        )

    # === Duplicates ===

    def get_duplicates(self, directory: str) -> AgentResponse:
        """Get duplicate groups in a directory."""
        return self._request(
            "GET",
            "/api/duplicates",
            params={"directory": directory},
        )

    def process_duplicates(
        self,
        directory: str,
        move_location: str | None = None,
        dry_run: bool = False,
    ) -> AgentResponse:
        """Process duplicates by moving them."""
        return self._request(
            "POST",
            "/api/duplicates/process",
            json_data={
                "directory": directory,
                "move_location": move_location,
                "dry_run": dry_run,
            },
        )

    def get_moved_files(self) -> AgentResponse:
        """Get all moved files."""
        return self._request("GET", "/api/duplicates/moved")

    def restore_file(self, move_id: int) -> AgentResponse:
        """Restore a moved file."""
        return self._request(
            "POST",
            "/api/duplicates/restore",
            json_data={"move_id": move_id},
        )

    def restore_all(self) -> AgentResponse:
        """Restore all moved files."""
        return self._request("POST", "/api/duplicates/restore-all")

    # === Configuration ===

    def get_config(self) -> AgentResponse:
        """Get agent configuration."""
        return self._request("GET", "/api/config")

    def update_config(self, **kwargs) -> AgentResponse:
        """Update agent configuration."""
        return self._request("PUT", "/api/config", json_data=kwargs)


def connect_to_agent(remote: RemoteHost, timeout: float = 30.0) -> AgentClient:
    """Create an agent client for a remote host."""
    return AgentClient(remote=remote, timeout=timeout)
