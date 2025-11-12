"""
TC-005 Historical Data Tests
Tests for listing strategies, backtests, and viewing historical data
"""

import pytest
import requests


class TestListStrategies:
    """Tests for listing AI strategies"""

    def test_list_strategies(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-strategies
        Lists all user strategies
        """
        url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "user_id" in data, "Response should contain user_id"
        assert "aiStrategies" in data, "Response should contain aiStrategies array"

        ai_strategies = data["aiStrategies"]
        assert isinstance(ai_strategies, list), "aiStrategies should be a list"

        # If strategies exist, verify structure
        if len(ai_strategies) > 0:
            strategy = ai_strategies[0]
            required_fields = ["id", "strategy", "createdAt", "updatedAt", "revisionCount"]
            for field in required_fields:
                assert field in strategy, f"Strategy missing required field: {field}"

            # Verify nested strategy object
            nested_strategy = strategy["strategy"]
            nested_required = ["id", "name", "strategyType", "isPublic", "createdAt", "updatedAt"]
            for field in nested_required:
                assert field in nested_strategy, f"Nested strategy missing required field: {field}"

    def test_list_strategies_requires_auth(self, api_config):
        """Test that listing strategies requires authentication"""
        url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        headers = {"Content-Type": "application/json"}

        response = requests.get(url, headers=headers, timeout=10)

        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}"


class TestGetStrategyDetails:
    """Tests for retrieving specific strategy details"""

    def test_get_strategy_versions(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-versions?aiStrategyId={id}
        Gets complete strategy data including all versions
        """
        # First get a strategy
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        required_top_level = ["user_id", "aiStrategyId", "strategy", "versions"]
        for field in required_top_level:
            assert field in data, f"Response missing required field: {field}"

        # Verify versions array
        versions = data["versions"]
        assert isinstance(versions, list), "versions should be a list"
        assert len(versions) > 0, "Should have at least one version"

        # Verify version structure
        version = versions[0]
        version_required = ["version", "code_out"]
        for field in version_required:
            assert field in version, f"Version missing required field: {field}"

    def test_get_strategy_versions_requires_id(self, api_config, auth_headers):
        """Test that get versions requires aiStrategyId parameter"""
        url = f"{api_config['base_url']}/ai-bot-builder/list-versions"

        # No parameters
        response = requests.get(url, headers=auth_headers, timeout=10)

        # Should fail without required parameter
        assert response.status_code in [400, 404, 500], "Should require aiStrategyId parameter"


class TestListBacktests:
    """Tests for listing backtests"""

    def test_list_backtest_stats_for_strategy(self, api_config, auth_headers):
        """
        Test GET /backtests/{strategyId}/stats
        Gets backtest history for a specific strategy
        """
        # Get a strategy
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

        backtests = data["backtests"]
        assert isinstance(backtests, list), "backtests should be a list"

        # backtests array may be empty if no backtests run yet
        # If not empty, verify structure
        if len(backtests) > 0:
            backtest = backtests[0]
            # Verify backtest has expected fields
            assert isinstance(backtest, dict), "Each backtest should be a dictionary"


class TestSearchAndFilter:
    """Tests for search and filtering functionality"""

    def test_strategy_list_search_client_side(self, api_config, auth_headers):
        """
        Test that strategy list returns all strategies
        (Search appears to be client-side in UI)
        """
        url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"

        # No search parameters observed in API
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        # Should return all strategies without pagination
        assert "aiStrategies" in data, "Should return all strategies"


@pytest.mark.integration
class TestHistoricalDataWorkflow:
    """Integration tests for complete historical data retrieval"""

    def test_complete_strategy_history_workflow(self, api_config, auth_headers):
        """
        Test complete workflow: list strategies -> get specific strategy -> get backtests
        """
        # Step 1: List all strategies
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        assert list_response.status_code == 200, "Failed to list strategies"

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available")

        # Step 2: Get details for first strategy
        ai_strategy_id = ai_strategies[0].get("id")
        strategy_id = ai_strategies[0].get("strategy", {}).get("id")

        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        versions_response = requests.get(
            versions_url, headers=auth_headers, params={"aiStrategyId": ai_strategy_id}, timeout=10
        )

        assert versions_response.status_code == 200, "Failed to get strategy versions"

        versions_data = versions_response.json()
        assert "versions" in versions_data, "Should have versions"
        assert len(versions_data["versions"]) > 0, "Should have at least one version"

        # Step 3: Get backtest history for strategy
        stats_url = f"{api_config['base_url']}/backtests/{strategy_id}/stats"
        stats_response = requests.get(stats_url, headers=auth_headers, timeout=10)

        assert stats_response.status_code == 200, "Failed to get backtest stats"

        stats_data = stats_response.json()
        assert "backtests" in stats_data, "Should have backtests array"

        # Complete workflow successful
        print(f"\nWorkflow completed for strategy: {ai_strategies[0].get('strategy', {}).get('name')}")
        print(f"  - Versions: {len(versions_data['versions'])}")
        print(f"  - Backtests: {len(stats_data['backtests'])}")
