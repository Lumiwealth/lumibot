"""
BotSpot API Client - Exception Classes

Exception hierarchy for clear error handling and debugging.
"""


class BotSpotError(Exception):
    """Base exception for all BotSpot API errors."""

    def __init__(self, message, **kwargs):
        super().__init__(message)
        self.message = message
        for key, value in kwargs.items():
            setattr(self, key, value)


class AuthenticationError(BotSpotError):
    """
    Raised when authentication fails.

    Common causes:
    - Invalid credentials (BOTSPOT_USERNAME or BOTSPOT_PASSWORD)
    - Auth0 login page not loading correctly
    - Network connectivity issues during login
    """

    pass


class TokenExpiredError(BotSpotError):
    """
    Raised when the access token has expired.

    The client will automatically attempt to refresh the token.
    If you see this error, it means the refresh also failed.
    """

    pass


class APIError(BotSpotError):
    """
    Raised when the BotSpot API returns an error response.

    Attributes:
        status_code: HTTP status code (e.g., 400, 404, 500)
        response_data: Raw response data from the API
        request_id: Request ID if provided by the API
    """

    def __init__(self, message, status_code=None, response_data=None, request_id=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        self.request_id = request_id

    def __str__(self):
        parts = [self.message]
        if self.status_code:
            parts.append(f"Status: {self.status_code}")
        if self.request_id:
            parts.append(f"Request ID: {self.request_id}")
        return " | ".join(parts)


class NetworkError(BotSpotError):
    """
    Raised when a network request fails.

    Common causes:
    - No internet connection
    - BotSpot API is down
    - DNS resolution failure
    - Request timeout
    """

    pass


class ValidationError(BotSpotError):
    """
    Raised when request parameters fail validation.

    This is raised before making an API request when the client
    detects invalid parameters (e.g., missing required fields).
    """

    pass


class ResourceNotFoundError(APIError):
    """
    Raised when a requested resource is not found (404).

    Examples:
    - Strategy with given ID doesn't exist
    - Backtest result not available
    - Deployment not found
    """

    def __init__(self, resource_type, resource_id, **kwargs):
        message = f"{resource_type} not found: {resource_id}"
        super().__init__(message, status_code=404, **kwargs)
        self.resource_type = resource_type
        self.resource_id = resource_id


class RateLimitError(APIError):
    """
    Raised when rate limit is exceeded (429).

    Attributes:
        retry_after: Number of seconds to wait before retrying (if provided)
    """

    def __init__(self, message, retry_after=None, **kwargs):
        super().__init__(message, status_code=429, **kwargs)
        self.retry_after = retry_after
