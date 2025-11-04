"""
BotSpot API Client - Main Client Class

Provides the primary interface for interacting with the BotSpot API.
"""

import logging
import os
from typing import TYPE_CHECKING, Optional

from dotenv import load_dotenv

from .auth import AuthManager
from .exceptions import AuthenticationError, ValidationError
from .token_cache import TokenManager

if TYPE_CHECKING:
    from .resources.backtests import BacktestsResource
    from .resources.deployments import DeploymentsResource
    from .resources.strategies import StrategiesResource
    from .resources.users import UsersResource

logger = logging.getLogger(__name__)


class BotSpot:
    """
    BotSpot API Client.

    Provides elegant, Stripe-inspired API for interacting with BotSpot:

    Usage:
        # Basic usage
        client = BotSpot()
        profile = client.users.get_profile()

        # Context manager
        with BotSpot() as client:
            strategies = client.strategies.list()

        # Custom credentials
        client = BotSpot(username="user@example.com", password="secret")

    Features:
    - Automatic authentication with token caching
    - Lazy authentication (only authenticates on first API call)
    - Thread-safe token management
    - Resource-based API organization
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        env_path: Optional[str] = None,
        cache_tokens: bool = True,
        headless: bool = True,
    ):
        """
        Initialize BotSpot client.

        Args:
            username: BotSpot email. If None, loads from BOTSPOT_USERNAME env var.
            password: BotSpot password. If None, loads from BOTSPOT_PASSWORD env var.
            env_path: Path to .env file. If None, uses root .env at /Users/marvin/repos/lumibot/.env
            cache_tokens: If True, cache tokens to avoid repeated authentication.
            headless: If True, run Selenium in headless mode (no visible browser).

        Raises:
            ValidationError: If credentials are missing
        """
        # Load environment variables
        if env_path is None:
            env_path = "/Users/marvin/repos/lumibot/.env"

        load_dotenv(env_path)

        # Get credentials from args or environment
        self.username = username or os.getenv("BOTSPOT_USERNAME")
        self.password = password or os.getenv("BOTSPOT_PASSWORD")

        if not self.username or not self.password:
            raise ValidationError(
                "BotSpot credentials not found. "
                "Set BOTSPOT_USERNAME and BOTSPOT_PASSWORD in .env or pass to constructor."
            )

        # Initialize managers
        self.token_manager = TokenManager() if cache_tokens else None
        self.auth_manager = AuthManager(headless=headless)

        # Token storage
        self._access_token: Optional[str] = None
        self._id_token: Optional[str] = None
        self._authenticated = False

        # Lazy-load resources (imported when accessed)
        self._users: Optional[UsersResource] = None
        self._strategies: Optional[StrategiesResource] = None
        self._backtests: Optional[BacktestsResource] = None
        self._deployments: Optional[DeploymentsResource] = None

        logger.info("BotSpot client initialized (lazy authentication enabled)")

    @property
    def users(self) -> "UsersResource":
        """Access users API resource."""
        if self._users is None:
            from .resources.users import UsersResource

            self._users = UsersResource(self)
        return self._users

    @property
    def strategies(self) -> "StrategiesResource":
        """Access strategies API resource."""
        if self._strategies is None:
            from .resources.strategies import StrategiesResource

            self._strategies = StrategiesResource(self)
        return self._strategies

    @property
    def backtests(self) -> "BacktestsResource":
        """Access backtests API resource."""
        if self._backtests is None:
            from .resources.backtests import BacktestsResource

            self._backtests = BacktestsResource(self)
        return self._backtests

    @property
    def deployments(self) -> "DeploymentsResource":
        """Access deployments API resource."""
        if self._deployments is None:
            from .resources.deployments import DeploymentsResource

            self._deployments = DeploymentsResource(self)
        return self._deployments

    def _get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get valid access token, authenticating if necessary.

        This method is called internally by resource classes.
        It handles:
        1. Checking if already authenticated in this session
        2. Loading cached tokens if available
        3. Performing fresh authentication if needed
        4. Auto re-authentication on expiration

        Args:
            force_refresh: If True, force fresh authentication even if token exists

        Returns:
            Valid access token

        Raises:
            AuthenticationError: If authentication fails
        """
        # Force refresh if requested (e.g., after 401 error)
        if force_refresh:
            logger.info("Forcing fresh authentication (token may have expired)")
            self._authenticate()
            return self._access_token

        # If already authenticated in this session, check if still valid
        if self._authenticated and self._access_token:
            # Check if cached token is still valid
            if self.token_manager and not self.token_manager.is_valid():
                logger.info("Token has expired, re-authenticating...")
                print("\n\033[93m⚠️  Token expired - automatically re-authenticating...\033[0m")
                self._authenticate()
            return self._access_token

        # Try to load cached tokens
        if self.token_manager:
            cached_tokens = self.token_manager.load()
            if cached_tokens:
                self._access_token = cached_tokens["access_token"]
                self._id_token = cached_tokens.get("id_token")
                self._authenticated = True
                logger.info("Using cached authentication tokens")
                return self._access_token

        # Perform fresh authentication
        logger.info("No valid cached tokens, performing fresh authentication")
        self._authenticate()

        return self._access_token

    def _authenticate(self) -> None:
        """
        Perform authentication via Selenium.

        Raises:
            AuthenticationError: If authentication fails
        """
        try:
            access_token, id_token, expires_in, refresh_token = self.auth_manager.authenticate(
                self.username, self.password
            )

            self._access_token = access_token
            self._id_token = id_token
            self._authenticated = True

            # Cache tokens if enabled
            if self.token_manager:
                self.token_manager.save(
                    access_token=access_token,
                    expires_in=expires_in,
                    id_token=id_token,
                    refresh_token=refresh_token,
                )

            logger.info("Authentication successful")

        except AuthenticationError:
            raise
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {e}") from e

    def clear_cache(self) -> None:
        """
        Clear cached tokens.

        This forces re-authentication on the next API call.
        """
        if self.token_manager:
            self.token_manager.clear()

        self._access_token = None
        self._id_token = None
        self._authenticated = False

        logger.info("Token cache cleared")

    def get_cache_info(self) -> Optional[dict]:
        """
        Get information about cached tokens.

        Returns:
            Dictionary with cache info, or None if no cache or no tokens
        """
        if not self.token_manager:
            return None

        return self.token_manager.get_expiry_info()

    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Clean up if needed (currently nothing to clean up)
        pass
