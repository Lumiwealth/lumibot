"""
List ProjectX TopStepX Accounts

This script connects to your TopStepX account and displays all available accounts
so you can choose which one to use in your .env file.

Requirements:
- ProjectX credentials set in .env file
- requests library (usually already installed)

Usage:
    python3 list_projectx_accounts.py

    or with virtual environment:

    /path/to/venv/bin/python list_projectx_accounts.py
"""

import os
import sys
import requests


def load_env_file():
    """Load environment variables from .env file"""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    value = value.strip().strip('"').strip("'")
                    os.environ[key.strip()] = value


def get_auth_token(base_url, username, api_key):
    """Get authentication token from ProjectX"""
    auth_url = f"{base_url}api/auth/loginkey"

    payload = {
        "userName": username,
        "apiKey": api_key,
    }

    try:
        response = requests.post(auth_url, json=payload)
        auth_resp = response.json()

        if not auth_resp.get("success"):
            error_message = auth_resp.get("errorMessage", "Authentication failed")
            print(f"❌ Authentication Error: {error_message}")
            return None

        return auth_resp.get("token")

    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None


def get_accounts(base_url, token):
    """Get list of accounts from ProjectX"""
    accounts_url = f"{base_url}api/account/search"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "onlyActiveAccounts": True
    }

    try:
        response = requests.post(accounts_url, json=payload, headers=headers)
        result = response.json()

        if result.get("success"):
            return result.get("accounts", [])
        else:
            error_message = result.get("errorMessage", "Failed to fetch accounts")
            print(f"❌ Error: {error_message}")
            return []

    except Exception as e:
        print(f"❌ Error fetching accounts: {e}")
        return []


def list_accounts():
    """Main function to list all ProjectX accounts"""

    print("=" * 70)
    print("ProjectX TopStepX Account Lister")
    print("=" * 70)
    print()

    # Load environment variables
    load_env_file()

    # Get credentials
    api_key = os.getenv('PROJECTX_TOPSTEPX_API_KEY')
    username = os.getenv('PROJECTX_TOPSTEPX_USERNAME')
    base_url = "https://api.topstepx.com/"

    if not api_key or not username:
        print("❌ Error: ProjectX credentials not found!")
        print()
        print("Please make sure your .env file contains:")
        print("  PROJECTX_TOPSTEPX_API_KEY=your_api_key")
        print("  PROJECTX_TOPSTEPX_USERNAME=your_username")
        return

    # Mask username for security
    if len(username) > 5:
        masked_username = username[:3] + "*" * (len(username) - 5) + username[-2:]
    else:
        masked_username = username[0] + "***"
    print(f"🔐 Connecting to TopStepX as: {masked_username}")
    print()

    # Authenticate
    print("🔄 Authenticating with TopStepX...")
    token = get_auth_token(base_url, username, api_key)

    if not token:
        print()
        print("💡 Troubleshooting:")
        print("  1. Verify your API key is correct")
        print("  2. Check your username matches your TopStepX account")
        print("  3. Ensure your API key hasn't expired")
        return

    # Get accounts
    print("📋 Fetching your accounts...")
    accounts = get_accounts(base_url, token)

    if not accounts:
        print("⚠️  No accounts found or unable to fetch accounts.")
        print()
        print("This could mean:")
        print("  - Your API credentials don't have access to any accounts")
        print("  - There's an issue with your TopStepX account")
        print("  - The account search returned no active accounts")
        return

    print()
    print("=" * 70)
    print(f"✅ Found {len(accounts)} account(s)")
    print("=" * 70)
    print()

    # Display account details
    for i, account in enumerate(accounts, 1):
        account_id = account.get('id', 'N/A')
        account_name = account.get('name', 'Unknown')
        balance = account.get('balance', 0)

        # Get the actual boolean flags from API
        simulated = account.get('simulated', False)
        can_trade = account.get('canTrade', False)
        is_visible = account.get('isVisible', False)

        print(f"Account #{i}:")
        print(f"  📌 Account Name:  {account_name}")
        print(f"  🔢 Account ID:    {account_id}")
        print(f"  💰 Balance:       ${balance:,.2f}")
        print(f"  🎮 Simulated:     {'Yes' if simulated else 'No'}")
        print(f"  ✅ Can Trade:     {'Yes' if can_trade else 'No'}")
        print(f"  👁️  Is Visible:    {'Yes' if is_visible else 'No'}")
        print()

    # Provide guidance
    print("=" * 70)
    print("📝 How to use this information:")
    print("=" * 70)
    print()
    print("Copy the 'Account Name' of your preferred account and add it to your .env file:")
    print()
    print("  PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME=<Account Name>")
    print()
    print("For example, if your account name is 'TopStep-12345', add:")
    print("  PROJECTX_TOPSTEPX_PREFERRED_ACCOUNT_NAME=TopStep-12345")
    print()
    print("Note: If you only have one account, this is optional.")
    print("      Lumibot will automatically use the account with the highest balance.")
    print()


if __name__ == "__main__":
    try:
        list_accounts()
    except KeyboardInterrupt:
        print("\n\n⚠️  Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
