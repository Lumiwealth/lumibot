"""
BotSpot API Client - Base Resource Class

Provides common HTTP methods and error handling for all API resources.
"""

import logging
from typing import Any, Dict, Optional

import requests

from .exceptions import APIError, NetworkError, RateLimitError, ResourceNotFoundError

logger = logging.getLogger(__name__)


class BaseResource:
    """
    Base class for all API resource classes.

    Provides common HTTP methods (_get, _post, _put, _delete) with:
    - Automatic Authorization header injection
    - Consistent error handling
    - Request/response logging
    """

    API_BASE = "https://api.botspot.trade"

    def __init__(self, client):
        """
        Initialize the resource.

        Args:
            client: BotSpot client instance (provides access to session and tokens)
        """
        self.client = client

    def _get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests.

        Returns:
            Dictionary with Authorization and Content-Type headers
        """
        # This will trigger authentication if not already done
        access_token = self.client._get_access_token()

        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "User-Agent": "BotSpot-Python-Client/1.0",
        }

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform GET request.

        Args:
            path: API endpoint path (e.g., "/users/profile")
            params: Optional query parameters

        Returns:
            Response data as dictionary

        Raises:
            NetworkError: If request fails due to network issues
            APIError: If API returns an error response
        """
        url = f"{self.API_BASE}{path}"

        try:
            logger.debug(f"GET {url}")
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=30)
            return self._handle_response(response)

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timeout: {url}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {url}") from e
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed: {e}") from e

    def _post(self, path: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform POST request.

        Args:
            path: API endpoint path (e.g., "/strategies")
            data: Optional request body data
            params: Optional query parameters

        Returns:
            Response data as dictionary

        Raises:
            NetworkError: If request fails due to network issues
            APIError: If API returns an error response
        """
        url = f"{self.API_BASE}{path}"

        try:
            logger.debug(f"POST {url}")
            response = requests.post(url, headers=self._get_headers(), json=data, params=params, timeout=30)
            return self._handle_response(response)

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timeout: {url}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {url}") from e
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed: {e}") from e

    def _put(self, path: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform PUT request.

        Args:
            path: API endpoint path (e.g., "/strategies/123")
            data: Optional request body data
            params: Optional query parameters

        Returns:
            Response data as dictionary

        Raises:
            NetworkError: If request fails due to network issues
            APIError: If API returns an error response
        """
        url = f"{self.API_BASE}{path}"

        try:
            logger.debug(f"PUT {url}")
            response = requests.put(url, headers=self._get_headers(), json=data, params=params, timeout=30)
            return self._handle_response(response)

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timeout: {url}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {url}") from e
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed: {e}") from e

    def _delete(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Perform DELETE request.

        Args:
            path: API endpoint path (e.g., "/strategies/123")
            params: Optional query parameters

        Returns:
            Response data as dictionary

        Raises:
            NetworkError: If request fails due to network issues
            APIError: If API returns an error response
        """
        url = f"{self.API_BASE}{path}"

        try:
            logger.debug(f"DELETE {url}")
            response = requests.delete(url, headers=self._get_headers(), params=params, timeout=30)
            return self._handle_response(response)

        except requests.exceptions.Timeout as e:
            raise NetworkError(f"Request timeout: {url}") from e
        except requests.exceptions.ConnectionError as e:
            raise NetworkError(f"Connection error: {url}") from e
        except requests.exceptions.RequestException as e:
            raise NetworkError(f"Request failed: {e}") from e

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Handle API response and raise appropriate exceptions.

        Args:
            response: requests Response object

        Returns:
            Response data as dictionary

        Raises:
            RateLimitError: If rate limit is exceeded (429)
            ResourceNotFoundError: If resource not found (404)
            APIError: For other error responses
        """
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            retry_after_int = int(retry_after) if retry_after else None
            raise RateLimitError("Rate limit exceeded. Please try again later.", retry_after=retry_after_int)

        # Handle resource not found
        if response.status_code == 404:
            try:
                error_data = response.json()
                message = error_data.get("message", "Resource not found")
            except Exception:
                message = "Resource not found"

            # Generic not found - could enhance this with resource type detection
            raise ResourceNotFoundError(
                resource_type="Resource",
                resource_id="unknown",
                response_data=response.text,
            )

        # Handle other client/server errors
        if not response.ok:
            try:
                error_data = response.json()
                message = error_data.get("message") or error_data.get("error") or response.text
            except Exception:
                message = response.text or f"HTTP {response.status_code}"

            raise APIError(
                message=message,
                status_code=response.status_code,
                response_data=response.text,
            )

        # Parse successful response
        try:
            return response.json()
        except ValueError as e:
            # Response is not JSON
            logger.warning(f"Response is not JSON: {response.text[:100]}")
            raise APIError(
                f"Invalid JSON response: {e}",
                status_code=response.status_code,
                response_data=response.text,
            ) from e
