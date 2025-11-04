"""
BotSpot API Client - Backtests Resource

Methods for running and managing backtests.
"""

from typing import Any, Callable, Dict, List, Optional

from ..base import BaseResource


class BacktestsResource(BaseResource):
    """
    Backtests API resource.

    Provides methods for running backtests and retrieving backtest results.
    Backtests are asynchronous operations that take 10-30+ minutes to complete.
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
        bot_id: str,
        code: str,
        start_date: str,
        end_date: str,
        revision_id: str,
        data_provider: str = "theta_data",
        requirements: str = "lumibot",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Submit a backtest for a strategy (asynchronous operation).

        **Returns immediately** with 202 status and backtestId. Use `get_status()` to poll
        for completion (backtests take 10-30+ minutes).

        Args:
            bot_id: AI strategy ID (UUID)
            code: Full Python strategy code (Lumibot format)
            start_date: ISO-8601 start date (e.g., "2024-01-01T00:00:00.000Z")
            end_date: ISO-8601 end date (e.g., "2024-12-31T00:00:00.000Z")
            revision_id: Strategy revision ID (UUID)
            data_provider: Data provider slug (default: "theta_data")
            requirements: Python dependencies (default: "lumibot")
            **kwargs: Additional parameters (env vars, etc.)

        Returns:
            Dictionary with:
            {
                "status": "initiated",
                "message": "Backtest initiated successfully...",
                "backtestId": "uuid",
                "manager_bot_id": "uuid"
            }

        Example:
            >>> client = BotSpot()
            >>> # Get strategy code
            >>> versions = client.strategies.get_versions("ai-strategy-id")
            >>> code = versions['versions'][0]['code_out']
            >>>
            >>> # Submit backtest
            >>> result = client.backtests.run(
            ...     bot_id="ai-strategy-id",
            ...     code=code,
            ...     start_date="2024-01-01T00:00:00.000Z",
            ...     end_date="2024-12-31T00:00:00.000Z",
            ...     revision_id="revision-uuid",
            ...     data_provider="theta_data"
            ... )
            >>> backtest_id = result['backtestId']
            >>> print(f"Backtest submitted: {backtest_id}")
            >>>
            >>> # Poll for completion
            >>> status = client.backtests.get_status(backtest_id)
            >>> print(f"Progress: {status['stage']}")
        """
        data = {
            "bot_id": bot_id,
            "main": code,
            "requirements": requirements,
            "start_date": start_date,
            "end_date": end_date,
            "revisionId": revision_id,
            "dataProvider": data_provider,
            **kwargs,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._post("/backtests", data=data)

        # Response is always a dict with status="initiated"
        return response

    def get_status(self, backtest_id: str) -> Dict[str, Any]:
        """
        Get current status of a running backtest (for polling).

        Poll this endpoint every 2-5 seconds while backtest is running.

        Args:
            backtest_id: Backtest ID (UUID) from run() response

        Returns:
            Dictionary with:
            {
                "running": bool,
                "manager_bot_id": "uuid",
                "stage": "backtesting" | "finalizing" | "completed",
                "backtestId": "uuid",
                "elapsed_ms": int,
                "status_description": str,
                "backtest_progress": []  // Progress events
            }

        Example:
            >>> client = BotSpot()
            >>> backtest_id = "3083d6a8-..."
            >>> status = client.backtests.get_status(backtest_id)
            >>> print(f"Running: {status['running']}, Stage: {status['stage']}")
            Running: True, Stage: backtesting
        """
        return self._get(f"/backtests/{backtest_id}/status")

    def wait_for_completion(
        self, backtest_id: str, poll_interval: int = 5, timeout: int = 3600, callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Poll backtest status until completion (blocking operation).

        Polls every `poll_interval` seconds until backtest completes or timeout is reached.

        Args:
            backtest_id: Backtest ID from run() response
            poll_interval: Seconds between status checks (default: 5)
            timeout: Maximum seconds to wait (default: 3600 = 1 hour)
            callback: Optional callback function called with status dict on each poll

        Returns:
            Final status dictionary when backtest completes

        Raises:
            TimeoutError: If backtest doesn't complete within timeout

        Example:
            >>> client = BotSpot()
            >>> backtest_id = "3083d6a8-..."
            >>>
            >>> def on_progress(status):
            ...     elapsed = status['elapsed_ms'] / 1000
            ...     print(f"Elapsed: {elapsed:.0f}s, Stage: {status['stage']}")
            >>>
            >>> final_status = client.backtests.wait_for_completion(
            ...     backtest_id,
            ...     poll_interval=5,
            ...     timeout=1800,  # 30 minutes
            ...     callback=on_progress
            ... )
            >>> print("Backtest completed!")
        """
        import time

        start_time = time.time()

        while True:
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(f"Backtest did not complete within {timeout} seconds")

            # Poll status
            status = self.get_status(backtest_id)

            # Call callback if provided
            if callback:
                callback(status)

            # Check if completed
            if not status.get("running", False):
                return status

            # Wait before next poll
            time.sleep(poll_interval)

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
