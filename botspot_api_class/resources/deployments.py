"""
BotSpot API Client - Deployments Resource

Methods for deploying and managing live trading bots.
"""

from typing import Any, Dict, List, Optional

from ..base import BaseResource


class DeploymentsResource(BaseResource):
    """
    Deployments API resource.

    Provides methods for deploying strategies to live trading and managing deployments.
    """

    def list(
        self,
        strategy_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        List deployments.

        Args:
            strategy_id: Optional strategy ID to filter deployments
            status: Optional status to filter (e.g., "running", "stopped", "error")
            limit: Maximum number of deployments to return
            offset: Number of deployments to skip (for pagination)

        Returns:
            List of deployment dictionaries

        Example:
            >>> client = BotSpot()
            >>> deployments = client.deployments.list(status="running")
            >>> for deployment in deployments:
            ...     print(f"{deployment['name']}: {deployment['status']}")
        """
        params = {}
        if strategy_id:
            params["strategyId"] = strategy_id
        if status:
            params["status"] = status
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        response = self._get("/deployments", params=params)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("deployments", [])
            return response.get("deployments", [])

        if isinstance(response, list):
            return response

        return []

    def get(self, deployment_id: str) -> Dict[str, Any]:
        """
        Get details for a specific deployment.

        Args:
            deployment_id: Deployment ID

        Returns:
            Deployment dictionary

        Example:
            >>> client = BotSpot()
            >>> deployment = client.deployments.get("deploy123")
            >>> print(f"Status: {deployment['status']}")
        """
        response = self._get(f"/deployments/{deployment_id}")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("deployment", response)
            return response.get("deployment", response)

        return response

    def create(
        self,
        strategy_id: str,
        name: Optional[str] = None,
        broker: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Create a new deployment (deploy a strategy to live trading).

        Args:
            strategy_id: Strategy ID to deploy
            name: Optional deployment name
            broker: Broker to use (e.g., "alpaca", "interactive_brokers")
            **kwargs: Additional deployment parameters (e.g., broker credentials, risk limits)

        Returns:
            Created deployment dictionary

        Example:
            >>> client = BotSpot()
            >>> deployment = client.deployments.create(
            ...     strategy_id="abc123",
            ...     name="My Live Bot",
            ...     broker="alpaca"
            ... )
        """
        data = {
            "strategyId": strategy_id,
            "name": name,
            "broker": broker,
            **kwargs,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._post("/deployments", data=data)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("deployment", response)
            return response.get("deployment", response)

        return response

    def start(self, deployment_id: str) -> Dict[str, Any]:
        """
        Start a deployment (begin live trading).

        Args:
            deployment_id: Deployment ID

        Returns:
            Updated deployment dictionary

        Example:
            >>> client = BotSpot()
            >>> deployment = client.deployments.start("deploy123")
            >>> print(f"Status: {deployment['status']}")
        """
        response = self._post(f"/deployments/{deployment_id}/start")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("deployment", response)
            return response.get("deployment", response)

        return response

    def stop(self, deployment_id: str) -> Dict[str, Any]:
        """
        Stop a deployment (pause live trading).

        Args:
            deployment_id: Deployment ID

        Returns:
            Updated deployment dictionary

        Example:
            >>> client = BotSpot()
            >>> deployment = client.deployments.stop("deploy123")
            >>> print(f"Status: {deployment['status']}")
        """
        response = self._post(f"/deployments/{deployment_id}/stop")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("deployment", response)
            return response.get("deployment", response)

        return response

    def get_logs(
        self,
        deployment_id: str,
        limit: Optional[int] = None,
        level: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get logs for a deployment.

        Args:
            deployment_id: Deployment ID
            limit: Maximum number of log entries to return
            level: Optional log level filter (e.g., "info", "warning", "error")

        Returns:
            List of log entry dictionaries

        Example:
            >>> client = BotSpot()
            >>> logs = client.deployments.get_logs("deploy123", level="error")
            >>> for log in logs:
            ...     print(f"{log['timestamp']}: {log['message']}")
        """
        params = {}
        if limit is not None:
            params["limit"] = limit
        if level:
            params["level"] = level

        response = self._get(f"/deployments/{deployment_id}/logs", params=params)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("logs", [])
            return response.get("logs", [])

        if isinstance(response, list):
            return response

        return []

    def delete(self, deployment_id: str) -> Dict[str, Any]:
        """
        Delete a deployment.

        Args:
            deployment_id: Deployment ID

        Returns:
            Deletion confirmation

        Example:
            >>> client = BotSpot()
            >>> result = client.deployments.delete("deploy123")
        """
        return self._delete(f"/deployments/{deployment_id}")
