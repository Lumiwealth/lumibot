"""
BotSpot API Client - Strategies Resource

Methods for managing trading strategies.
"""

from typing import Any, Dict, List, Optional

from ..base import BaseResource


class StrategiesResource(BaseResource):
    """
    Strategies API resource.

    Provides methods for creating, listing, updating, and managing trading strategies.
    """

    def list(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        List all strategies for the current user.

        Args:
            limit: Maximum number of strategies to return
            offset: Number of strategies to skip (for pagination)

        Returns:
            List of strategy dictionaries

        Example:
            >>> client = BotSpot()
            >>> strategies = client.strategies.list()
            >>> for strategy in strategies:
            ...     print(f"{strategy['name']}: {strategy['status']}")
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get("/strategies", params=params)

        # Handle response format (assuming {success: true, strategies: [...]})
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategies", [])
            return response.get("strategies", [])

        # If response is already a list
        if isinstance(response, list):
            return response

        return []

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
