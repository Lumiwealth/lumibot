"""
BotSpot API Client

A simple, elegant Python client for the BotSpot trading platform API.

Usage:
    from botspot_api_class import BotSpot

    # Basic usage
    client = BotSpot()
    profile = client.users.get_profile()
    print(f"Logged in as: {profile['firstName']} {profile['lastName']}")

    # Context manager
    with BotSpot() as client:
        strategies = client.strategies.list()
        for strategy in strategies:
            print(strategy['name'])

Features:
- Automatic authentication with token caching
- Lazy authentication (only authenticates on first API call)
- Resource-based API organization (users, strategies, backtests, deployments)
- Comprehensive exception handling
- Thread-safe token management
"""

import logging

from .client import BotSpot
from .exceptions import (
    APIError,
    AuthenticationError,
    BotSpotError,
    NetworkError,
    RateLimitError,
    ResourceNotFoundError,
    TokenExpiredError,
    ValidationError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Public API
__all__ = [
    "BotSpot",
    "BotSpotError",
    "AuthenticationError",
    "TokenExpiredError",
    "APIError",
    "NetworkError",
    "ValidationError",
    "ResourceNotFoundError",
    "RateLimitError",
]

__version__ = "1.0.0"
