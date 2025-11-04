"""
TC-001 Authentication Endpoint Tests
Tests for endpoints discovered during authentication flow
"""

import pytest
import requests


class TestUserProfile:
    """Tests for user profile endpoints"""

    def test_get_user_profile(self, api_config, auth_headers):
        """
        Test GET /users/user_profile endpoint
        Verifies user profile retrieval with authentication
        """
        url = f"{api_config['base_url']}/users/user_profile"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert data["success"] is True, "Response should indicate success"
        assert "profile" in data, "Response should contain profile"

        profile = data["profile"]
        # Verify required fields
        required_fields = [
            "id",
            "email",
            "nickname",
            "firstName",
            "lastName",
            "role",
            "loginCount",
            "lastLoginAt",
        ]
        for field in required_fields:
            assert field in profile, f"Profile missing required field: {field}"

        # Verify onboarding flags exist
        onboarding_flags = [
            "hasCreatedStrategy",
            "hasRunBacktest",
            "hasDeployedBot",
            "hasRunningBot",
            "hasWatchedVideo",
            "hasDismissedOnboarding",
        ]
        for flag in onboarding_flags:
            assert flag in profile, f"Profile missing onboarding flag: {flag}"

    def test_get_user_profile_alt(self, api_config, auth_headers):
        """
        Test GET /users/profile endpoint (alternative)
        Should return same data as /users/user_profile
        """
        url = f"{api_config['base_url']}/users/profile"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert data["success"] is True
        assert "profile" in data


class TestStrategies:
    """Tests for strategy-related endpoints"""

    def test_get_onboarding_strategies(self, api_config, auth_headers):
        """
        Test GET /strategies/onboarding endpoint
        Retrieves example strategies for new users
        """
        url = f"{api_config['base_url']}/strategies/onboarding"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "strategies" in data, "Response should contain strategies array"

        strategies = data["strategies"]
        assert isinstance(strategies, list), "Strategies should be a list"

        if len(strategies) > 0:
            strategy = strategies[0]
            # Verify strategy structure
            assert "id" in strategy, "Strategy should have id"
            assert "name" in strategy, "Strategy should have name"
            assert "description" in strategy, "Strategy should have description"
            assert "codeOut" in strategy, "Strategy should have code"
            assert "performanceData" in strategy, "Strategy should have performance data"


class TestAuthentication:
    """Tests for authentication behavior"""

    def test_unauthorized_access(self, api_config):
        """
        Test that endpoints require authentication
        Should return 401 or 403 without valid token
        """
        url = f"{api_config['base_url']}/users/user_profile"
        headers = {"Content-Type": "application/json"}

        response = requests.get(url, headers=headers, timeout=10)

        # Should be unauthorized
        assert response.status_code in [
            401,
            403,
        ], f"Expected 401/403 for unauthorized, got {response.status_code}"

    def test_invalid_token(self, api_config):
        """Test that invalid tokens are rejected"""
        url = f"{api_config['base_url']}/users/user_profile"
        headers = {
            "Authorization": "Bearer invalid_token_12345",
            "Content-Type": "application/json",
        }

        response = requests.get(url, headers=headers, timeout=10)

        assert response.status_code in [
            401,
            403,
        ], f"Expected 401/403 for invalid token, got {response.status_code}"


@pytest.mark.integration
class TestEndToEndFlow:
    """Integration tests for complete workflows"""

    def test_login_flow_simulation(self, api_config, auth_headers):
        """
        Simulate the login flow by calling endpoints in order
        Tests the typical sequence: profile -> strategies -> login stats
        """
        base_url = api_config["base_url"]

        # Step 1: Get user profile
        response1 = requests.get(f"{base_url}/users/user_profile", headers=auth_headers, timeout=10)
        assert response1.status_code == 200, "User profile request failed"

        # Step 2: Get onboarding strategies
        response2 = requests.get(f"{base_url}/strategies/onboarding", headers=auth_headers, timeout=10)
        assert response2.status_code == 200, "Onboarding strategies request failed"

        # Both should succeed
        assert response1.json()["success"] is True
        assert "strategies" in response2.json()
