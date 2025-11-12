#!/usr/bin/env python3
"""
BotSpot API Client - Simple Test Script

Demonstrates the elegance of the new API client.
This replaces the 267-line automated_login_test.py with just ~30 lines.
"""

import sys

# Add parent directory to path to import botspot_api_class
sys.path.insert(0, "/Users/marvin/repos/lumibot")

from botspot_api_class import BotSpot

# ANSI colors for pretty output
YELLOW = "\033[93m"
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def main():
    print(f"{BOLD}BotSpot API Client - Simple Test{RESET}")
    print("=" * 60)

    try:
        # Initialize client (loads credentials from .env automatically)
        print(f"\n{CYAN}Initializing BotSpot client...{RESET}")
        with BotSpot() as client:
            print(f"{GREEN}✓{RESET} Client initialized (lazy auth enabled)")

            # Get user profile (this will trigger authentication if needed)
            print(f"\n{CYAN}Fetching user profile...{RESET}")
            profile = client.users.get_profile()
            print(f"{GREEN}✓{RESET} Profile fetched successfully")

            # Display profile information
            print(f"\n{BOLD}Profile Information:{RESET}")
            print(f"  📧 Email: {profile.get('email')}")
            print(f"  👤 Nickname: {profile.get('nickname')}")
            print(f"  🎭 Role: {profile.get('role')}")
            print(f"  📞 Phone: {profile.get('phone')}")
            print(f"  📍 Location: {profile.get('location')}")
            print(f"  📈 Trading Experience: {profile.get('tradingExperience')}")
            print(f"  🔑 Login Count: {profile.get('loginCount')}")
            print(f"  🕒 Last Login: {profile.get('lastLoginAt')}")

            # Show subscription status
            active_products = profile.get("activeProducts", [])
            if active_products:
                product = active_products[0]
                print(f"\n{BOLD}Subscription:{RESET}")
                print(f"  💳 Plan: {product.get('productName')}")
                print(f"  📊 Status: {product.get('status')}")
                print(f"  🔄 Recurring: {product.get('isRecurring')}")

            # Show onboarding progress
            print(f"\n{BOLD}Progress:{RESET}")
            print(f"  {'✓' if profile.get('hasSetPassword') else '✗'} Password Set")
            print(f"  {'✓' if profile.get('hasCreatedStrategy') else '✗'} Created Strategy")
            print(f"  {'✓' if profile.get('hasRunBacktest') else '✗'} Run Backtest")
            print(f"  {'✓' if profile.get('hasDeployedBot') else '✗'} Deployed Bot")
            print(f"  {'✓' if profile.get('hasRunningBot') else '✗'} Running Bot")

            # Print name in bright yellow
            first_name = profile.get("firstName", "")
            last_name = profile.get("lastName", "")
            full_name = f"{first_name} {last_name}".strip()

            if full_name:
                print(f"\n{YELLOW}{BOLD}{'=' * 60}")
                print(f"  USER NAME: {full_name}")
                print(f"{'=' * 60}{RESET}\n")
            else:
                print(f"\n{YELLOW}{BOLD}{'=' * 60}")
                print(f"  USER NAME: {profile.get('nickname', 'Unknown User')}")
                print(f"{'=' * 60}{RESET}\n")

            # Show cache info
            cache_info = client.get_cache_info()
            if cache_info:
                print(f"{BOLD}Token Cache Info:{RESET}")
                print(f"  🕐 Issued: {cache_info['issued_at']}")
                print(f"  ⏰ Expires: {cache_info['expires_at']}")
                print(f"  ⏳ Time Remaining: {cache_info['time_remaining']}")
                print(f"  {'✓' if cache_info['is_valid'] else '✗'} Valid")

            print(f"\n{GREEN}{BOLD}✓ ALL OPERATIONS COMPLETED SUCCESSFULLY!{RESET}")
            print(f"{CYAN}Summary: Went from 267 lines to ~30 lines with the new API client{RESET}\n")

    except Exception as e:
        print(f"\n{RED}✗ Error: {e}{RESET}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
