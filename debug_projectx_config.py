#!/usr/bin/env python3
"""
Debug script to check ProjectX environment variables and broker initialization.
Run this to see what environment variables are actually set and debug broker creation.
"""

import os
import sys
import termcolor
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

print("=" * 60)
print("ProjectX Environment Variables Debug")
print("=" * 60)

# Check all ProjectX environment variables
env_vars = {
    "PROJECTX_FIRM": os.environ.get("PROJECTX_FIRM"),
    "PROJECTX_API_KEY": os.environ.get("PROJECTX_API_KEY"),
    "PROJECTX_USERNAME": os.environ.get("PROJECTX_USERNAME"),
    "PROJECTX_BASE_URL": os.environ.get("PROJECTX_BASE_URL"),
    "PROJECTX_PREFERRED_ACCOUNT_NAME": os.environ.get("PROJECTX_PREFERRED_ACCOUNT_NAME"),
}

print("\n1. Environment Variables:")
print("-" * 30)
for key, value in env_vars.items():
    if value:
        # Mask sensitive info
        if "API_KEY" in key:
            display_value = f"{value[:8]}..." if len(value) > 8 else value
        else:
            display_value = value
        print(f"✅ {key} = {display_value}")
    else:
        print(f"❌ {key} = {termcolor.colored('NOT SET', 'red')}")

# Check broker detection logic
print(f"\n2. Broker Detection Logic:")
print("-" * 30)
has_api_key = bool(env_vars["PROJECTX_API_KEY"])
has_username = bool(env_vars["PROJECTX_USERNAME"])
print(f"Has API Key: {termcolor.colored('✅' if has_api_key else '❌', 'green' if has_api_key else 'red')}")
print(f"Has Username: {termcolor.colored('✅' if has_username else '❌', 'green' if has_username else 'red')}")
print(f"Will auto-detect broker: {termcolor.colored('✅' if (has_api_key and has_username) else '❌', 'green' if (has_api_key and has_username) else 'red')}")

# Check other Lumibot settings
print(f"\n3. Other Lumibot Settings:")
print("-" * 30)
print(f"IS_BACKTESTING = {os.environ.get('IS_BACKTESTING', 'NOT SET')}")
print(f"TRADING_BROKER = {os.environ.get('TRADING_BROKER', 'NOT SET')}")
print(f"DATA_SOURCE = {os.environ.get('DATA_SOURCE', 'NOT SET')}")

# Try to create the config and broker
print(f"\n4. Testing Broker Creation:")
print("-" * 30)

try:
    # Add the lumibot directory to the Python path
    lumibot_path = os.path.join(os.path.dirname(__file__), 'lumibot')
    if os.path.exists(lumibot_path):
        sys.path.insert(0, lumibot_path)
    
    from lumibot.credentials import PROJECTX_CONFIG
    
    print("ProjectX Config:")
    for key, value in PROJECTX_CONFIG.items():
        if value:
            if "api_key" in key:
                display_value = f"{value[:8]}..." if len(value) > 8 else value
            else:
                display_value = value
            print(f"  {key}: {display_value}")
        else:
            print(f"  {key}: {termcolor.colored('None', 'red')}")
    
    print(f"\nTrying to create ProjectX broker...")
    
    if PROJECTX_CONFIG["api_key"] and PROJECTX_CONFIG["username"]:
        from lumibot.brokers import ProjectX
        
        broker = ProjectX(PROJECTX_CONFIG)
        print(f"✅ {termcolor.colored('Broker created successfully!', 'green')}")
        print(f"   Broker name: {broker.name}")
        print(f"   Firm: {broker.firm}")
        
        # Test connection
        print(f"\nTesting broker connection...")
        try:
            connected = broker.connect()
            if connected:
                print(f"✅ {termcolor.colored('Connection successful!', 'green')}")
                print(f"   Account ID: {broker.account_id}")
            else:
                print(f"❌ {termcolor.colored('Connection failed', 'red')}")
        except Exception as e:
            print(f"❌ {termcolor.colored(f'Connection error: {e}', 'red')}")
            
    else:
        missing = []
        if not PROJECTX_CONFIG["api_key"]:
            missing.append("PROJECTX_API_KEY")
        if not PROJECTX_CONFIG["username"]:
            missing.append("PROJECTX_USERNAME")
        
        print(f"❌ {termcolor.colored(f'Cannot create broker - missing: {', '.join(missing)}', 'red')}")
        
except Exception as e:
    print(f"❌ {termcolor.colored(f'Error: {e}', 'red')}")
    import traceback
    traceback.print_exc()

print(f"\n5. Recommendations:")
print("-" * 30)

if not has_api_key:
    print("• Set PROJECTX_API_KEY in your .env file")
if not has_username:
    print("• Set PROJECTX_USERNAME in your .env file")
if not env_vars["PROJECTX_FIRM"]:
    print("• Set PROJECTX_FIRM (e.g., 'TOPONE', 'TSX') in your .env file")
if not env_vars["PROJECTX_BASE_URL"]:
    print("• Set PROJECTX_BASE_URL (your broker's API URL) in your .env file")

print("\nExample .env file for TOPONE:")
print("-" * 30)
print("""PROJECTX_FIRM=TOPONE
PROJECTX_API_KEY=your_api_key_here
PROJECTX_USERNAME=your_username_here
PROJECTX_BASE_URL=https://api.topone.com
PROJECTX_PREFERRED_ACCOUNT_NAME=Practice-Account-1

IS_BACKTESTING=false
TRADING_BROKER=projectx
DATA_SOURCE=projectx""")

print("\n" + "=" * 60) 