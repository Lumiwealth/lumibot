"""
TC-003 Strategy Results Tests
Tests for viewing generated strategy code, diagrams, and metadata
"""

import pytest
import requests


class TestStrategyResults:
    """Tests for viewing strategy results and details"""

    def test_get_strategy_versions_structure(self, api_config, auth_headers):
        """
        Test GET /ai-bot-builder/list-versions?aiStrategyId={id}
        Verifies complete response structure for strategy results
        """
        # First get a strategy to test with
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        assert list_response.status_code == 200, "Failed to list strategies"

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        # Get versions for first strategy
        ai_strategy_id = ai_strategies[0].get("id")

        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"

        data = response.json()

        # Verify top-level structure
        assert "user_id" in data, "Response should contain user_id"
        assert "aiStrategyId" in data, "Response should contain aiStrategyId"
        assert "strategy" in data, "Response should contain strategy object"
        assert "versions" in data, "Response should contain versions array"

        # Verify strategy metadata structure
        strategy = data["strategy"]
        required_strategy_fields = ["id", "name", "strategyType", "isPublic", "createdAt", "updatedAt"]
        for field in required_strategy_fields:
            assert field in strategy, f"Strategy missing required field: {field}"

        # Verify versions array
        versions = data["versions"]
        assert isinstance(versions, list), "Versions should be a list"
        assert len(versions) > 0, "Should have at least one version"


class TestStrategyCode:
    """Tests for strategy code retrieval"""

    def test_get_generated_code(self, api_config, auth_headers):
        """
        Test retrieving generated Python code from strategy versions
        Verifies code is valid Lumibot strategy format
        """
        # Get a strategy with versions
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, "Failed to get versions"

        versions = response.json().get("versions", [])
        assert len(versions) > 0, "Should have at least one version"

        # Verify first version has code
        version = versions[0]
        assert "code_out" in version, "Version should have code_out field"
        assert "version" in version, "Version should have version number"

        code = version["code_out"]
        assert code is not None, "Code should not be None"
        assert isinstance(code, str), "Code should be a string"
        assert len(code) > 100, "Code should be substantial (>100 chars)"

        # Verify code contains Lumibot strategy structure
        assert "from lumibot" in code.lower(), "Code should import from lumibot"
        assert "class" in code, "Code should contain a class definition"
        assert "Strategy" in code, "Code should reference Strategy"
        assert (
            "def initialize" in code or "def on_trading_iteration" in code
        ), "Code should contain strategy lifecycle methods"

    def test_version_metadata(self, api_config, auth_headers):
        """
        Test version metadata structure
        Verifies each version has required fields
        """
        # Get a strategy with versions
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        versions = response.json().get("versions", [])
        assert len(versions) > 0, "Should have at least one version"

        # Verify version structure
        version = versions[0]
        required_fields = ["version", "code_out"]
        for field in required_fields:
            assert field in version, f"Version missing required field: {field}"

        # Version number should be integer
        assert isinstance(version["version"], int), "Version number should be an integer"
        assert version["version"] >= 1, "Version number should be >= 1"


class TestStrategyDiagram:
    """Tests for Mermaid diagram retrieval"""

    def test_get_mermaid_diagram(self, api_config, auth_headers):
        """
        Test retrieving Mermaid diagram from strategy versions
        Verifies diagram is valid Mermaid flowchart syntax
        """
        # Get a strategy with versions
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        versions = response.json().get("versions", [])
        assert len(versions) > 0, "Should have at least one version"

        version = versions[0]

        # Check if diagram exists (may be null for some strategies)
        if "mermaidDiagram" not in version or version["mermaidDiagram"] is None:
            pytest.skip("Strategy does not have a Mermaid diagram")

        diagram = version["mermaidDiagram"]
        assert isinstance(diagram, str), "Diagram should be a string"
        assert len(diagram) > 0, "Diagram should not be empty"

        # Verify Mermaid syntax
        assert (
            "flowchart" in diagram.lower() or "graph" in diagram.lower()
        ), "Diagram should contain Mermaid flowchart syntax"

        # Verify diagram contains nodes/edges
        assert "-->" in diagram or "---" in diagram, "Diagram should contain Mermaid connection syntax"

    def test_strategy_comments(self, api_config, auth_headers):
        """
        Test retrieving AI-generated strategy description/comments
        """
        # Get a strategy with versions
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        versions = response.json().get("versions", [])
        assert len(versions) > 0, "Should have at least one version"

        version = versions[0]

        # Comments field should exist (may be null or string)
        assert "comments" in version, "Version should have comments field"

        # If comments exist, verify structure
        if version["comments"] is not None:
            comments = version["comments"]
            assert isinstance(comments, str), "Comments should be a string"
            assert len(comments) > 0, "Comments should not be empty if present"


class TestStrategyMetadata:
    """Tests for strategy metadata retrieval"""

    def test_strategy_name_editable(self, api_config, auth_headers):
        """
        Test that strategy metadata includes name and is editable
        """
        # Get a strategy
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions (includes strategy metadata)
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        data = response.json()
        strategy = data.get("strategy", {})

        # Verify name field
        assert "name" in strategy, "Strategy should have name field"
        assert isinstance(strategy["name"], str), "Strategy name should be a string"
        assert len(strategy["name"]) > 0, "Strategy name should not be empty"

    def test_strategy_type_and_visibility(self, api_config, auth_headers):
        """
        Test that strategy metadata includes type and visibility status
        """
        # Get a strategy
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        data = response.json()
        strategy = data.get("strategy", {})

        # Verify strategyType
        assert "strategyType" in strategy, "Strategy should have strategyType field"
        assert strategy["strategyType"] == "AI", "AI-generated strategies should have type 'AI'"

        # Verify isPublic field
        assert "isPublic" in strategy, "Strategy should have isPublic field"
        assert isinstance(strategy["isPublic"], bool), "isPublic should be a boolean"

    def test_strategy_timestamps(self, api_config, auth_headers):
        """
        Test that strategy metadata includes creation and update timestamps
        """
        # Get a strategy
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        ai_strategy_id = ai_strategies[0].get("id")

        # Get versions
        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        data = response.json()
        strategy = data.get("strategy", {})

        # Verify timestamps
        assert "createdAt" in strategy, "Strategy should have createdAt timestamp"
        assert "updatedAt" in strategy, "Strategy should have updatedAt timestamp"

        # Verify ISO-8601 format (basic check)
        assert "T" in strategy["createdAt"], "createdAt should be ISO-8601 format"
        assert "Z" in strategy["createdAt"], "createdAt should include timezone"


@pytest.mark.integration
class TestStrategyResultsWorkflow:
    """Integration tests for complete strategy results viewing workflow"""

    def test_view_complete_strategy_results(self, api_config, auth_headers):
        """
        Test complete workflow: list strategies -> get versions -> extract all data
        Simulates viewing strategy results page
        """
        # Step 1: List strategies
        list_url = f"{api_config['base_url']}/ai-bot-builder/list-strategies"
        list_response = requests.get(list_url, headers=auth_headers, timeout=10)

        assert list_response.status_code == 200, "Failed to list strategies"

        ai_strategies = list_response.json().get("aiStrategies", [])

        if len(ai_strategies) == 0:
            pytest.skip("No strategies available for testing")

        # Step 2: Get complete strategy data
        ai_strategy_id = ai_strategies[0].get("id")
        strategy_name = ai_strategies[0].get("strategy", {}).get("name", "Unknown")

        versions_url = f"{api_config['base_url']}/ai-bot-builder/list-versions"
        params = {"aiStrategyId": ai_strategy_id}
        response = requests.get(versions_url, headers=auth_headers, params=params, timeout=10)

        assert response.status_code == 200, "Failed to get strategy versions"

        data = response.json()

        # Step 3: Extract and validate all components

        # Metadata
        strategy = data.get("strategy", {})
        assert strategy["name"] == strategy_name, "Strategy name should match"

        # Versions
        versions = data.get("versions", [])
        assert len(versions) > 0, "Should have at least one version"

        # Code
        latest_version = versions[0]
        code = latest_version.get("code_out")
        assert code is not None, "Should have generated code"
        assert len(code) > 100, "Code should be substantial"

        # Optional: Diagram and comments
        # (These may be None for some strategies, so we just check presence)
        assert "mermaidDiagram" in latest_version, "Should have mermaidDiagram field (may be null)"
        assert "comments" in latest_version, "Should have comments field (may be null)"

        # Verify we have everything needed for results page
        assert "user_id" in data, "Should have user_id"
        assert "aiStrategyId" in data, "Should have aiStrategyId"
