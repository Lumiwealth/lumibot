"""
BotSpot API Client - Backtests Resource

Methods for running and managing backtests.
"""

from typing import Any, Dict, List, Optional

from ..base import BaseResource


class BacktestsResource(BaseResource):
    """
    Backtests API resource.

    Provides methods for running backtests and retrieving backtest results.
    """

    def list(
        self,
        strategy_id: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        List backtests.

        Args:
            strategy_id: Optional strategy ID to filter backtests
            limit: Maximum number of backtests to return
            offset: Number of backtests to skip (for pagination)

        Returns:
            List of backtest dictionaries

        Example:
            >>> client = BotSpot()
            >>> backtests = client.backtests.list(strategy_id="abc123")
            >>> for backtest in backtests:
            ...     print(f"{backtest['id']}: {backtest['status']}")
        """
        params = {}
        if strategy_id:
            params["strategyId"] = strategy_id
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get("/backtests", params=params)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("backtests", [])
            return response.get("backtests", [])

        if isinstance(response, list):
            return response

        return []

    def get(self, backtest_id: str) -> Dict[str, Any]:
        """
        Get details for a specific backtest.

        Args:
            backtest_id: Backtest ID

        Returns:
            Backtest dictionary with results

        Example:
            >>> client = BotSpot()
            >>> backtest = client.backtests.get("backtest123")
            >>> print(f"Return: {backtest['totalReturn']}")
        """
        response = self._get(f"/backtests/{backtest_id}")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("backtest", response)
            return response.get("backtest", response)

        return response

    def run(
        self,
        strategy_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_capital: Optional[float] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Run a backtest for a strategy.

        Args:
            strategy_id: Strategy ID to backtest
            start_date: Backtest start date (ISO format, e.g., "2023-01-01")
            end_date: Backtest end date (ISO format, e.g., "2023-12-31")
            initial_capital: Starting capital amount
            **kwargs: Additional backtest parameters

        Returns:
            Backtest result dictionary

        Example:
            >>> client = BotSpot()
            >>> backtest = client.backtests.run(
            ...     strategy_id="abc123",
            ...     start_date="2023-01-01",
            ...     end_date="2023-12-31",
            ...     initial_capital=10000
            ... )
            >>> print(f"Backtest ID: {backtest['id']}")
        """
        data = {
            "strategyId": strategy_id,
            "startDate": start_date,
            "endDate": end_date,
            "initialCapital": initial_capital,
            **kwargs,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._post("/backtests", data=data)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("backtest", response)
            return response.get("backtest", response)

        return response

    def get_results(self, backtest_id: str) -> Dict[str, Any]:
        """
        Get detailed results for a backtest.

        Args:
            backtest_id: Backtest ID

        Returns:
            Detailed backtest results including metrics, trades, equity curve, etc.

        Example:
            >>> client = BotSpot()
            >>> results = client.backtests.get_results("backtest123")
            >>> print(f"Sharpe Ratio: {results['sharpeRatio']}")
        """
        response = self._get(f"/backtests/{backtest_id}/results")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("results", response)
            return response.get("results", response)

        return response

    def delete(self, backtest_id: str) -> Dict[str, Any]:
        """
        Delete a backtest.

        Args:
            backtest_id: Backtest ID

        Returns:
            Deletion confirmation

        Example:
            >>> client = BotSpot()
            >>> result = client.backtests.delete("backtest123")
        """
        return self._delete(f"/backtests/{backtest_id}")
