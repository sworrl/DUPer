"""API authentication for DUPer."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader, APIKeyQuery

from duper.core.config import DuperConfig, get_config

# API key can be provided via header or query parameter
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_query = APIKeyQuery(name="api_key", auto_error=False)


def get_api_key(
    api_key_header_value: str | None = Depends(api_key_header),
    api_key_query_value: str | None = Depends(api_key_query),
) -> str | None:
    """Extract API key from header or query parameter."""
    return api_key_header_value or api_key_query_value


class AuthManager:
    """Manages API authentication."""

    def __init__(self, config: DuperConfig | None = None):
        self.config = config

    def get_config(self) -> DuperConfig:
        """Get configuration (lazy load if not set)."""
        if self.config is None:
            self.config = get_config()
        return self.config

    def verify_api_key(self, api_key: str | None) -> bool:
        """Verify an API key against the configured key."""
        config = self.get_config()

        # If auth is disabled, always allow
        if not config.server.auth_enabled:
            return True

        # If auth is enabled, require valid key
        if not api_key:
            return False

        return api_key == config.server.api_key

    async def __call__(
        self,
        request: Request,
        api_key: str | None = Depends(get_api_key),
    ) -> None:
        """
        FastAPI dependency for authentication.

        Use as: Depends(auth_manager)
        """
        if not self.verify_api_key(api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key",
                headers={"WWW-Authenticate": "ApiKey"},
            )


# Global auth manager instance
auth_manager = AuthManager()


def _is_localhost(request: Request) -> bool:
    """Check if the request is from localhost."""
    client_host = request.client.host if request.client else ""
    # Check for various localhost representations
    localhost_addresses = {"127.0.0.1", "::1", "localhost"}
    return client_host in localhost_addresses


def require_auth(
    request: Request,
    api_key: str | None = Depends(get_api_key),
) -> None:
    """
    Dependency that requires authentication for remote access.

    Localhost connections are always allowed without authentication.
    Remote connections require a valid API key.

    Usage:
        @app.get("/protected")
        def protected_endpoint(_: None = Depends(require_auth)):
            ...
    """
    # Localhost connections are always allowed
    if _is_localhost(request):
        return

    config = get_config()

    # If auth is disabled globally, allow all
    if not config.server.auth_enabled:
        return

    # Remote connections require valid API key
    if not api_key or api_key != config.server.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required for remote access",
            headers={"WWW-Authenticate": "ApiKey"},
        )


def optional_auth(api_key: str | None = Depends(get_api_key)) -> bool:
    """
    Dependency that checks auth but doesn't require it.

    Returns True if authenticated, False otherwise.

    Usage:
        @app.get("/endpoint")
        def endpoint(is_authed: bool = Depends(optional_auth)):
            if is_authed:
                ...
    """
    config = get_config()

    if not config.server.auth_enabled:
        return True

    if not api_key:
        return False

    return api_key == config.server.api_key
