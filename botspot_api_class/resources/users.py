"""
BotSpot API Client - Users Resource

Methods for accessing user profile and account information.
"""

from typing import Any, Dict

from ..base import BaseResource


class UsersResource(BaseResource):
    """
    Users API resource.

    Provides methods for accessing user profile and account information.
    """

    def get_profile(self) -> Dict[str, Any]:
        """
        Get the current user's profile.

        Returns:
            Dictionary containing profile information:
            - email: User's email address
            - firstName: User's first name
            - lastName: User's last name
            - nickname: User's nickname
            - role: User's role (e.g., "user", "admin")
            - phone: Phone number (if set)
            - location: Location (if set)
            - tradingExperience: Trading experience level
            - loginCount: Number of times user has logged in
            - lastLoginAt: Last login timestamp
            - activeProducts: List of active subscriptions
            - hasSetPassword: Boolean
            - hasCreatedStrategy: Boolean
            - hasRunBacktest: Boolean
            - hasDeployedBot: Boolean
            - hasRunningBot: Boolean

        Example:
            >>> client = BotSpot()
            >>> profile = client.users.get_profile()
            >>> print(f"Logged in as: {profile['firstName']} {profile['lastName']}")
        """
        response = self._get("/users/profile")

        # BotSpot API returns {success: true, profile: {...}}
        if not response.get("success"):
            # This shouldn't happen if status code was 200, but handle it
            raise Exception(f"Profile fetch returned success=false: {response}")

        return response.get("profile", {})

    def update_profile(self, **kwargs) -> Dict[str, Any]:
        """
        Update the current user's profile.

        Args:
            **kwargs: Profile fields to update (e.g., firstName, lastName, phone, location)

        Returns:
            Updated profile dictionary

        Example:
            >>> client = BotSpot()
            >>> profile = client.users.update_profile(
            ...     firstName="John",
            ...     lastName="Doe",
            ...     phone="+1234567890"
            ... )
        """
        response = self._put("/users/profile", data=kwargs)

        if not response.get("success"):
            raise Exception(f"Profile update returned success=false: {response}")

        return response.get("profile", {})
