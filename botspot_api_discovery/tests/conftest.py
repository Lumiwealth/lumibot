"""
Pytest configuration and fixtures for BotSpot API tests
"""

import os

import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


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
    NOTE: Token must be set in environment variable ACCESS_TOKEN
    Token expires after 24 hours
    """
    token = os.getenv("ACCESS_TOKEN")
    if not token:
        pytest.skip("ACCESS_TOKEN not set in environment")
    return token


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
