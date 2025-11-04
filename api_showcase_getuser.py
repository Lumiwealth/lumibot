#!/usr/bin/env python3
"""
BotSpot API Client - Showcase Script

Demonstrates the elegance and simplicity of the BotSpot API client.
Shows automatic authentication, token caching, and user profile retrieval.

Usage:
    python api_showcase_getuser.py
"""

from botspot_api_class import BotSpot


def main():
    """Get and display user profile information."""

    # Initialize client - loads credentials from .env automatically
    # No need to manually authenticate - it happens automatically on first API call
    with BotSpot() as client:
        # Get user profile - triggers auto-login if needed, uses cached token if available
        profile = client.users.get_profile()

        # Display profile information
        print("\n" + "=" * 60)
        print(f"  👤 User: {profile['firstName']} {profile['lastName']}")
        print(f"  📧 Email: {profile['email']}")
        print(f"  🎭 Role: {profile['role']}")
        print(f"  📈 Experience: {profile.get('tradingExperience', 'N/A')}")
        print(f"  🔑 Login Count: {profile['loginCount']}")
        print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
