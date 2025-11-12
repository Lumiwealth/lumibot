"""
TC-004 Backtesting Tests
Tests for backtest submission, status polling, and results retrieval
"""

import pytest
import requests


class TestBacktestSubmission:
    """Tests for submitting backtests"""

    def test_submit_backtest_requires_auth(self, api_config):
        """Test that backtest submission requires authentication"""
        url = f"{api_config['base_url']}/backtests"
        headers = {"Content-Type": "application/json"}

        # Minimal payload
        payload = {
            "bot_id": "test-id",
            "main": "print('test')",
            "requirements": "lumibot",
            "start_date": "2024-01-01T00:00:00.000Z",
            "end_date": "2024-12-31T00:00:00.000Z",
            "revisionId": "test-revision",
            "dataProvider": "theta_data",
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}"

    def test_submit_backtest_with_valid_strategy(self, api_config, auth_headers):
        """
        Test POST /backtests with a valid strategy
        This will initiate a real backtest
        """
        # Get a strategy to backtest
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for backtesting")

        # Get strategy details
        ai_strategy = ai_strategies[0]
        ai_strategy_id = ai_strategy.get("id")

        # Get strategy code
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        versions_response = requests.get(
            versions_url, headers=auth_headers, params={"aiStrategyId": ai_strategy_id}, timeout=10
        )

        versions_data = versions_response.json()
        versions = versions_data.get("versions", [])

        if len(versions) == 0:
            pytest.skip("Strategy has no versions")

        latest_version = versions[0]
        code = latest_version.get("code_out")

        # Prepare backtest submission
        url = f"{api_config['base_url']}/backtests"

        payload = {
            "bot_id": ai_strategy_id,
            "main": code,
            "requirements": "lumibot",
            "start_date": "2024-11-01T00:00:00.000Z",
            "end_date": "2024-11-30T00:00:00.000Z",  # Short date range for faster test
            "revisionId": latest_version.get("version", "1"),
            "dataProvider": "theta_data",
        }

        response = requests.post(url, headers=auth_headers, json=payload, timeout=30)

        # Should return 202 Accepted
        assert response.status_code == 202, f"Expected 202, got {response.status_code}"

        data = response.json()

        # Verify response structure
        assert "status" in data, "Response should contain status"
        assert "backtestId" in data, "Response should contain backtestId"
        assert "message" in data, "Response should contain message"

        assert data["status"] == "initiated", "Status should be 'initiated'"
        assert data["backtestId"], "backtestId should not be empty"

    def test_submit_backtest_invalid_payload(self, api_config, auth_headers):
        """Test that invalid payloads are rejected with proper client error codes"""
        url = f"{api_config['base_url']}/backtests"

        # Missing required fields
        payload = {"bot_id": "test"}

        response = requests.post(url, headers=auth_headers, json=payload, timeout=10)

        # Should fail with 400 or 422 (client error codes for validation failures)
        assert response.status_code in [400, 422], f"Expected 400/422 for invalid payload, got {response.status_code}"


class TestBacktestStatusPolling:
    """Tests for polling backtest status"""

    def test_get_backtest_status_requires_auth(self, api_config):
        """Test that status endpoint requires authentication"""
        fake_id = "00000000-0000-0000-0000-000000000000"
        url = f"{api_config['base_url']}/backtests/{fake_id}/status"
        headers = {"Content-Type": "application/json"}

        response = requests.get(url, headers=headers, timeout=10)

        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}"

    def test_get_backtest_status_fake_id(self, api_config, auth_headers):
        """Test status endpoint with non-existent backtest ID"""
        fake_id = "00000000-0000-0000-0000-000000000000"
        url = f"{api_config['base_url']}/backtests/{fake_id}/status"

        response = requests.get(url, headers=auth_headers, timeout=10)

        # Should return 404 or error response
        if response.status_code == 200:
            data = response.json()
            # If 200, should indicate backtest doesn't exist
            assert data.get("running") is False or "error" in data
        else:
            assert response.status_code in [404, 400], f"Expected 404/400 for fake ID, got {response.status_code}"

    @pytest.mark.slow
    def test_backtest_status_structure(self, api_config, auth_headers):
        """
        Test backtest status response structure
        Requires a running backtest
        """
        # This test is marked slow because it requires submitting a backtest first
        # In real usage, you would get the backtestId from a previous submission

        # For now, we'll skip if we can't find a running backtest
        pytest.skip("Requires manual backtest submission - implement with fixture if needed")


class TestDataProviders:
    """Tests for data provider endpoints"""

    def test_list_data_providers(self, api_config, auth_headers):
        """Test GET /data-providers?includeProducts=true"""
        url = f"{api_config['base_url']}/data-providers"
        params = {"includeProducts": "true"}

        response = requests.get(url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "providers" in data, "Response should contain providers array"

        providers = data["providers"]
        assert isinstance(providers, list), "Providers should be a list"

        # Verify provider structure
        if len(providers) > 0:
            provider = providers[0]
            required_fields = ["id", "slug", "name", "description", "category", "enabled"]
            for field in required_fields:
                assert field in provider, f"Provider missing required field: {field}"

            # Check for products if includeProducts=true
            assert "products" in provider, "Provider should have products array"
            products = provider["products"]
            assert isinstance(products, list), "Products should be a list"

    def test_list_data_providers_with_requirements(self, api_config, auth_headers):
        """Test filtering data providers by capability requirements"""
        url = f"{api_config['base_url']}/data-providers"
        params = {"includeProducts": "true", "requirements": "stocks"}

        response = requests.get(url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        providers = data.get("providers", [])

        # Verify returned providers support stocks
        for provider in providers:
            capabilities = provider.get("capabilities", {})
            supported = capabilities.get("supported", [])
            assert "stocks" in supported, f"Provider {provider['name']} should support stocks"

    def test_get_data_provider_access(self, api_config, auth_headers):
        """Test GET /data-providers/access?provider={slug}"""
        url = f"{api_config['base_url']}/data-providers/access"
        params = {"provider": "theta_data"}

        response = requests.get(url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        # Response structure varies based on user's access status
        # Just verify it returns valid JSON
        assert data is not None, "Response should contain valid JSON"


class TestBacktestResults:
    """Tests for retrieving backtest results"""

    def test_get_backtest_stats(self, api_config, auth_headers):
        """
        Test GET /backtests/{strategyId}/stats
        This endpoint may return empty if no backtests exist
        """
        # Get a strategy ID
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available")

        strategy = ai_strategies[0].get("strategy", {})
        strategy_id = strategy.get("id")

        # Get backtest stats
        url = f"{api_config['base_url']}/backtests/{strategy_id}/stats"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "backtests" in data, "Response should contain backtests array"
        assert "updated_count" in data, "Response should contain updated_count"

        # backtests may be empty if none have been run
        assert isinstance(data["backtests"], list), "backtests should be a list"


@pytest.mark.integration
class TestBacktestWorkflow:
    """Integration tests for complete backtest lifecycle"""

    @pytest.mark.slow
    def test_complete_backtest_workflow(self, api_config, auth_headers):
        """
        Test complete workflow: submit backtest -> poll status -> get results
        WARNING: This test is slow (backtest takes 10-30+ minutes)
        """
        pytest.skip("Skipping slow integration test - backtests take 10-30+ minutes")

        # Step 1: Get a strategy
        # Step 2: Submit backtest
        # Step 3: Poll status until complete
        # Step 4: Retrieve results
        # (Implementation would go here)
