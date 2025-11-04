"""
Pytest configuration and fixtures for BotSpot API tests
"""

import os
import sys

import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add botspot_api_class to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

try:
    from botspot_api_class import BotSpot

    HAS_BOTSPOT_CLIENT = True
except ImportError:
    HAS_BOTSPOT_CLIENT = False


@pytest.fixture(scope="session")
def api_config():
    """API configuration fixture"""
    return {
        "base_url": "https://api.botspot.trade",
        "auth0_domain": "botspot.us.auth0.com",
        "client_id": "sys7COPgURwmEVYFi5Wc5U9rXJEsx55d",
    }


@pytest.fixture(scope="session")
def access_token():
    """
    Access token fixture

    Priority:
    1. Use ACCESS_TOKEN environment variable if set
    2. Use BotSpot API client for automatic authentication

    Token expires after 24 hours
    """
    # Try environment variable first
    token = os.getenv("ACCESS_TOKEN")
    if token:
        return token

    # Fall back to BotSpot API client
    if not HAS_BOTSPOT_CLIENT:
        pytest.skip("ACCESS_TOKEN not set and botspot_api_class not available")

    # Check if credentials are available
    username = os.getenv("BOTSPOT_USERNAME")
    password = os.getenv("BOTSPOT_PASSWORD")

    if not username or not password:
        pytest.skip("BOTSPOT_USERNAME/BOTSPOT_PASSWORD not set in .env")

    # Authenticate and get token
    try:
        client = BotSpot()
        token = client._get_access_token()
        return token
    except Exception as e:
        pytest.skip(f"Failed to authenticate with BotSpot API: {e}")


@pytest.fixture(scope="session")
def auth_headers(access_token):
    """Authenticated request headers"""
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": "BotSpot-API-Discovery-Tests/1.0",
    }


@pytest.fixture(scope="session")
def test_results():
    """Shared test results collector"""
    return {"passed": [], "failed": [], "skipped": []}
