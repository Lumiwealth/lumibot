#!/usr/bin/env python3
"""
BotSpot API Client - Logout Showcase Script

Demonstrates how to logout by clearing cached tokens.
After logout, the next API call will require fresh authentication.

Usage:
    python api_showcase_logout.py
"""

from botspot_api_class import BotSpot


def main():
    """Clear cached tokens (logout)."""

    client = BotSpot()

    # Show current cache status before logout
    print("\n" + "=" * 60)
    print("  🔍 Checking current token cache status...")
    print("=" * 60)

    cache_info = client.get_cache_info()
    if cache_info:
        print("\n  ✓ Cached tokens found")
        print(f"  ⏳ Time remaining: {cache_info['time_remaining']}")
        print(f"  🕐 Expires: {cache_info['expires_at']}")
    else:
        print("\n  ℹ️  No cached tokens found (already logged out)")

    # Clear the cache (logout)
    print("\n" + "=" * 60)
    print("  🚪 Logging out (clearing token cache)...")
    print("=" * 60)

    client.clear_cache()

    print("\n  ✓ Token cache cleared successfully")
    print("  ℹ️  Next API call will require fresh authentication\n")

    # Verify logout
    cache_info_after = client.get_cache_info()
    if cache_info_after is None:
        print("=" * 60)
        print("  ✅ Logout confirmed - no cached tokens remain")
        print("=" * 60 + "\n")
    else:
        print("  ⚠️  Warning: Cache still exists (unexpected)")


if __name__ == "__main__":
    main()
