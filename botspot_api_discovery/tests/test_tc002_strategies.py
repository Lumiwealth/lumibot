"""
TC-002 Strategy Creation Tests
Tests for AI strategy generation and management endpoints
"""

import json

import pytest
import requests


class TestStrategyListing:
    """Tests for strategy listing and usage limits"""

    def test_list_strategies(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-strategies
        Lists all AI-generated strategies for the user
        """
        url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "aiStrategies" in data, "Response should contain aiStrategies array"

        ai_strategies = data["aiStrategies"]
        assert isinstance(ai_strategies, list), "aiStrategies should be a list"

        # If user has strategies, verify structure
        if len(ai_strategies) > 0:
            ai_strategy = ai_strategies[0]
            # Top level fields
            assert "id" in ai_strategy, "aiStrategy should have id (aiStrategyId)"
            assert "strategy" in ai_strategy, "aiStrategy should have nested strategy object"
            assert "revisionCount" in ai_strategy, "aiStrategy should have revisionCount"

            # Nested strategy fields
            strategy = ai_strategy["strategy"]
            required_fields = ["id", "name", "strategyType", "createdAt", "updatedAt"]
            for field in required_fields:
                assert field in strategy, f"Strategy missing required field: {field}"

    def test_get_usage_limits(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/usage-limits
        Checks user's prompt usage quota (X/500)
        """
        url = f"{api_config['base_url']}/ai-bot-builder/usage-limits"
        response = requests.get(url, headers=auth_headers, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.text, "Response should not be empty"

        # Verify response is valid JSON
        data = response.json()
        assert data is not None, "Response should contain valid JSON data"


class TestStrategyGeneration:
    """Tests for strategy generation via SSE"""

    @pytest.mark.slow
    def test_create_strategy_sse_stream(self, api_config, auth_headers):
        """
        Test POST /sse/stream (strategy generation)
        Uses Server-Sent Events for real-time generation
        WARNING: This test is slow (~2-3 minutes) and consumes a prompt quota
        """
        url = f"{api_config['base_url']}/sse/stream"

        # Prepare SSE headers
        sse_headers = auth_headers.copy()
        sse_headers["Accept"] = "text/event-stream"

        # Test payload - simple strategy to minimize generation time
        payload = {
            "type": "generate_strategy",
            "prompt": "Create a simple buy and hold strategy for SPY.",
            "aiStrategyId": "",  # Empty for new strategy
            "message": "Create a simple buy and hold strategy for SPY.",
            "files": [],
        }

        # Stream SSE response
        response = requests.post(url, headers=sse_headers, json=payload, stream=True, timeout=300)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert "text/event-stream" in response.headers.get("Content-Type", ""), "Should return SSE content type"

        # Parse SSE events
        events = []
        generated_code = None

        for line in response.iter_lines(decode_unicode=True):
            if not line:
                continue

            # SSE heartbeat
            if line == ":heartbeat":
                continue

            # Data event
            if line.startswith("data: "):
                event_data = line[6:]  # Remove "data: " prefix
                try:
                    event = json.loads(event_data)
                    events.append(event)

                    # Capture final generated code
                    if event.get("action") == "strategy_generated" and event.get("phase") == "complete":
                        generated_code = event.get("generatedCode")
                        break  # Stop after final event

                except json.JSONDecodeError:
                    pass  # Skip invalid JSON

        # Verify we received events
        assert len(events) > 0, "Should receive at least one SSE event"

        # Verify key events were received
        event_actions = [e.get("action") for e in events]
        expected_actions = ["prompt_to_ai", "code_generation_started", "strategy_generated"]

        for expected_action in expected_actions:
            assert expected_action in event_actions, f"Should receive '{expected_action}' event"

        # Verify generated code was returned
        assert generated_code is not None, "Final event should contain generated code"
        assert len(generated_code) > 100, "Generated code should be substantial"
        assert "class" in generated_code.lower(), "Generated code should contain a class definition"
        assert "strategy" in generated_code.lower(), "Generated code should contain 'Strategy'"

        # Verify token usage was tracked
        final_event = events[-1]
        assert "usage" in final_event, "Final event should contain token usage"
        usage = final_event["usage"]
        assert "model" in usage, "Usage should contain model name"
        assert usage.get("model") in ["gpt-5", "gpt-4"], "Should use GPT-5 or GPT-4"

    def test_create_strategy_requires_auth(self, api_config):
        """Test that strategy generation requires authentication"""
        url = f"{api_config['base_url']}/sse/stream"
        headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}

        payload = {
            "type": "generate_strategy",
            "prompt": "Test",
            "aiStrategyId": "",
            "message": "Test",
            "files": [],
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}"


class TestStrategyVersions:
    """Tests for strategy version management"""

    def test_get_strategy_versions_requires_id(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-versions?aiStrategyId={id}
        Should fail without aiStrategyId parameter
        """
        url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        response = requests.get(url, headers=auth_headers, timeout=10)

        # Should fail or return error without required param
        assert response.status_code in [400, 404, 500], "Should require aiStrategyId parameter"

    def test_get_strategy_versions_with_fake_id(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-versions with non-existent ID
        Should return 404 or empty result
        """
        url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        fake_id = "00000000-0000-0000-0000-000000000000"
        params = {"aiStrategyId": fake_id}

        response = requests.get(url, headers=auth_headers, params=params, timeout=10)

        # Should either 404 or return empty versions
        if response.status_code == 200:
            data = response.json()
            # If it returns data, versions should be empty
            if "versions" in data:
                assert len(data["versions"]) == 0, "Should have no versions for fake ID"
        else:
            assert response.status_code == 404, f"Expected 404 for non-existent ID, got {response.status_code}"


class TestDiagramGeneration:
    """Tests for Mermaid diagram generation"""

    def test_generate_diagram(self, api_config, auth_headers):
        """
        Test POST /ai-bot-builder/generate-diagram
        Generates Mermaid flowchart from strategy code

        Note: This endpoint may require a valid revisionId from an existing strategy
        """
        url = f"{api_config['base_url']}/ai-bot-builder/generate-diagram"

        # Simple test strategy code
        test_code = """
from lumibot.strategies.strategy import Strategy

class SimpleStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if self.get_cash() > 10000:
            self.create_order("SPY", 10, "buy")
"""

        payload = {
            "python": test_code,
            "revisionId": "00000000-0000-0000-0000-000000000000",  # Test ID
            "skipRender": True,
        }

        response = requests.post(url, headers=auth_headers, json=payload, timeout=30)

        # API may return 404 if revisionId validation is enforced
        if response.status_code == 404:
            pytest.skip("Diagram generation requires valid revisionId from existing strategy")

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()
        assert "mmd" in data, "Response should contain Mermaid markdown"

        mermaid = data["mmd"]
        assert isinstance(mermaid, str), "Mermaid diagram should be a string"
        assert len(mermaid) > 0, "Mermaid diagram should not be empty"
        assert "flowchart" in mermaid.lower() or "graph" in mermaid.lower(), "Should contain flowchart syntax"

    def test_generate_diagram_requires_auth(self, api_config):
        """Test that diagram generation requires authentication"""
        url = f"{api_config['base_url']}/ai-bot-builder/generate-diagram"
        headers = {"Content-Type": "application/json"}

        payload = {"python": "test", "revisionId": "test", "skipRender": True}

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        assert response.status_code in [401, 403], f"Expected 401/403 without auth, got {response.status_code}"


class TestMarketplace:
    """Tests for marketplace endpoints"""

    def test_check_published_with_fake_id(self, api_config, auth_headers):
        """
        Test GET /marketplace/check-published/{strategyId}
        Check if strategy is published to marketplace
        """
        fake_id = "00000000-0000-0000-0000-000000000000"
        url = f"{api_config['base_url']}/marketplace/check-published/{fake_id}"

        response = requests.get(url, headers=auth_headers, timeout=10)

        # Should return 200 with isPublished: false, or 404
        if response.status_code == 200:
            data = response.json()
            assert "isPublished" in data or response.text, "Should indicate publication status"
        else:
            assert response.status_code == 404, f"Expected 200 or 404, got {response.status_code}"


@pytest.mark.integration
class TestStrategyWorkflow:
    """Integration tests for complete strategy lifecycle"""

    def test_list_then_get_versions_workflow(self, api_config, auth_headers):
        """
        Test complete workflow: list strategies -> get specific strategy versions
        Only runs if user has existing strategies
        """
        # Step 1: List all strategies
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        assert list_response.status_code == 200, "Failed to list strategies"

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        # Step 2: Get versions for first strategy
        first_ai_strategy = ai_strategies[0]
        ai_strategy_id = first_ai_strategy.get("id")  # The top-level id IS the aiStrategyId

        if not ai_strategy_id:
            pytest.skip("Strategy missing id (aiStrategyId)")

        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        versions_response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        assert versions_response.status_code == 200, f"Failed to get versions for {ai_strategy_id}"

        versions_data = versions_response.json()
        assert "versions" in versions_data, "Response should contain versions array"
        assert len(versions_data["versions"]) > 0, "Strategy should have at least one version"

        # Verify version structure
        version = versions_data["versions"][0]
        assert "version" in version, "Version should have version number"
        assert "code_out" in version, "Version should have generated code"
        assert len(version["code_out"]) > 0, "Generated code should not be empty"
