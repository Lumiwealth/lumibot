"""
BotSpot API Client - Strategies Resource

Methods for managing AI-generated trading strategies.
"""

from typing import Any, Dict, List, Optional

from ..base import BaseResource


class StrategiesResource(BaseResource):
    """
    Strategies API resource for AI-generated strategies.

    Provides methods for generating, listing, and managing AI trading strategies.
    Supports real-time strategy generation via Server-Sent Events (SSE).
    """

    def list(self) -> List[Dict[str, Any]]:
        """
        List all AI-generated strategies for the current user.

        Returns:
            List of AI strategy dictionaries with nested strategy objects

        Example:
            >>> client = BotSpot()
            >>> ai_strategies = client.strategies.list()
            >>> for ai_strategy in ai_strategies:
            ...     strategy = ai_strategy['strategy']
            ...     print(f"{strategy['name']}: {ai_strategy['revisionCount']} revisions")
            SMA Crossover: 1 revisions
            TSLA Trend Algo: 1 revisions
        """
        response = self._get("/ai-bot-builder/list-strategies")

        # Response format: {"user_id": "...", "aiStrategies": [...]}
        if isinstance(response, dict) and "aiStrategies" in response:
            return response["aiStrategies"]

        return []

    def get_usage_limits(self) -> Dict[str, Any]:
        """
        Get user's prompt usage limits (remaining prompt quota).

        Returns:
            Usage limits information (used/limit/remaining prompts)

        Example:
            >>> client = BotSpot()
            >>> limits = client.strategies.get_usage_limits()
            >>> print(f"Prompts used: {limits}")
        """
        return self._get("/ai-bot-builder/usage-limits")

    def get(self, strategy_id: str) -> Dict[str, Any]:
        """
        Get details for a specific strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            Strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.get("abc123")
            >>> print(strategy['name'])
        """
        response = self._get(f"/strategies/{strategy_id}")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def create(
        self,
        name: str,
        description: Optional[str] = None,
        code: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Create a new strategy.

        Args:
            name: Strategy name
            description: Optional strategy description
            code: Optional strategy code (Python)
            **kwargs: Additional strategy parameters

        Returns:
            Created strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.create(
            ...     name="My First Strategy",
            ...     description="A simple buy and hold strategy",
            ...     code="# Strategy code here"
            ... )
        """
        data = {
            "name": name,
            "description": description,
            "code": code,
            **kwargs,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._post("/strategies", data=data)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def update(self, strategy_id: str, **kwargs) -> Dict[str, Any]:
        """
        Update an existing strategy.

        Args:
            strategy_id: Strategy ID
            **kwargs: Strategy fields to update

        Returns:
            Updated strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.update(
            ...     "abc123",
            ...     name="Updated Strategy Name",
            ...     description="New description"
            ... )
        """
        response = self._put(f"/strategies/{strategy_id}", data=kwargs)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def delete(self, strategy_id: str) -> Dict[str, Any]:
        """
        Delete a strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            Deletion confirmation

        Example:
            >>> client = BotSpot()
            >>> result = client.strategies.delete("abc123")
        """
        return self._delete(f"/strategies/{strategy_id}")

    def get_versions(self, ai_strategy_id: str) -> Dict[str, Any]:
        """
        Get all versions/revisions of an AI strategy.

        Args:
            ai_strategy_id: AI Strategy ID (UUID)

        Returns:
            Dictionary with user_id, aiStrategyId, strategy info, and versions array

        Example:
            >>> client = BotSpot()
            >>> versions_data = client.strategies.get_versions("372cd38b-6d0d-43eb-8035-4701ab2a1692")
            >>> for version in versions_data['versions']:
            ...     print(f"Version {version['version']}: {len(version['code_out'])} chars")
            Version 1: 6789 chars
        """
        return self._get("/ai-bot-builder/list-versions", params={"aiStrategyId": ai_strategy_id})

    def generate_diagram(self, python_code: str, revision_id: str, skip_render: bool = True) -> Dict[str, Any]:
        """
        Generate Mermaid flowchart diagram from strategy code.

        Args:
            python_code: Full Python strategy code
            revision_id: Revision ID (UUID) from strategy version
            skip_render: Skip rendering (default True for API-only response)

        Returns:
            Dictionary with 'mmd' (Mermaid markdown), 'revisionId', 'skipRender'

        Example:
            >>> client = BotSpot()
            >>> code = "from lumibot.strategies.strategy import Strategy\\n..."
            >>> diagram = client.strategies.generate_diagram(code, "revision-uuid")
            >>> print(diagram['mmd'])
            flowchart TB...
        """
        data = {"python": python_code, "revisionId": revision_id, "skipRender": skip_render}

        return self._post("/ai-bot-builder/generate-diagram", data=data)

    def generate(self, prompt: str, files: Optional[List] = None) -> Dict[str, Any]:
        """
        Generate a new AI strategy from natural language prompt.

        NOTE: This method uses Server-Sent Events (SSE) for real-time generation.
        The full SSE implementation requires streaming support, which is not yet
        implemented in this SDK. For now, this returns a simple POST response.

        To use SSE streaming for real-time progress updates, use the SSE endpoint
        directly: POST /sse/stream with Accept: text/event-stream header.

        Args:
            prompt: Natural language strategy description
            files: Optional list of file attachments

        Returns:
            Strategy generation response (simplified, not streaming)

        Example:
            >>> client = BotSpot()
            >>> # Note: This will not stream progress in real-time yet
            >>> result = client.strategies.generate(
            ...     "Create a simple moving average crossover strategy for SPY"
            ... )

        For real-time streaming, use:
            POST https://api.botspot.trade/sse/stream
            Headers: Accept: text/event-stream
            Body: {"type": "generate_strategy", "prompt": "...", "aiStrategyId": "", "message": "...", "files": []}

        Generation takes approximately 2-3 minutes and uses GPT-5 (OpenAI).
        """
        # TODO: Implement SSE client for streaming support
        # For now, this provides a placeholder method

        raise NotImplementedError(
            "SSE streaming not yet implemented in SDK. "
            "Use POST /sse/stream directly with Accept: text/event-stream header for real-time generation."
        )
