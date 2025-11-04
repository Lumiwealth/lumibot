#!/usr/bin/env python3
"""
Test script to verify BotSpot API endpoint replication
This script attempts to call discovered API endpoints using the extracted access token
"""

import json
from datetime import datetime

import requests

# Extracted from localStorage during authentication
# fmt: off
ACCESS_TOKEN = (
    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IlctUlM0NGZhd2tHM0FfQy1kbDRwcCJ9."
    "eyJpc3MiOiJodHRwczovL2JvdHNwb3QudXMuYXV0aDAuY29tLyIsInN1YiI6ImF1dGgwfDY5MDk2MTg4N2E5ODIwZGZjYjVjMmFkZSIsImF1ZCI6WyJ1cm46Ym90c3BvdC1wcm9kLWFwaSIsImh0dHBzOi8vYm90c3BvdC51cy5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNzYyMjI3MjM1LCJleHAiOjE3NjIzMTM2MzUsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwiLCJhenAiOiJzeXM3Q09QZ1VSd21FVllGaTVXYzVVOXJYSkVzeDU1ZCJ9."
    "LZsaAjMYRbHlQH0aSqSSmRHkDjfyLc7SBmwDKuQoA9CveIMy_Tit_b4Gmood_aIBc7qpvzrlAihzbYUKbk-rK2pI1e3oe8x9XXXOYe-VO8IJz3Vrkw_UTGMwaTXjbcp0LN2PlkApND4arj2Tth1sPdSN9MWmZfvpCSLIe-CbVcnPBDt2SnQuxIeS8C1J717hI_ZZ0YfmeTPxzVYpmcHN-lzFUwDB-i3BskySJtLkEehiBy9fzjQ_birBDmdf0nAYhgPATCXYTvmi70D2RqHs5QRN484t85MvdgQAX7LaXE4mdktjAlgJx3EnxHlVBi1lPoicOLtVe3zlP5pHkz2KnA"
)
# fmt: on

BASE_URL = "https://api.botspot.trade"


def test_user_profile():
    """Test GET /users/user_profile endpoint"""
    url = f"{BASE_URL}/users/user_profile"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "BotSpot-API-Discovery/1.0",
    }

    print(f"\n{'='*60}")
    print(f"Testing: GET {url}")
    print(f"{'='*60}")

    try:
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        print(f"Headers: {dict(response.headers)}")

        if response.status_code == 200:
            data = response.json()
            print("\n✓ SUCCESS - Response:")
            print(json.dumps(data, indent=2))
            return True
        else:
            print("\n✗ FAILED - Response:")
            print(response.text)
            return False
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


def test_user_profile_alt():
    """Test GET /users/profile endpoint (alternative)"""
    url = f"{BASE_URL}/users/profile"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "BotSpot-API-Discovery/1.0",
    }

    print(f"\n{'='*60}")
    print(f"Testing: GET {url}")
    print(f"{'='*60}")

    try:
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print("\n✓ SUCCESS - Response:")
            print(json.dumps(data, indent=2))
            return True
        else:
            print("\n✗ FAILED")
            return False
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


def test_strategies_onboarding():
    """Test GET /strategies/onboarding endpoint"""
    url = f"{BASE_URL}/strategies/onboarding"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "BotSpot-API-Discovery/1.0",
    }

    print(f"\n{'='*60}")
    print(f"Testing: GET {url}")
    print(f"{'='*60}")

    try:
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            print(f"\n✓ SUCCESS - Found {len(data.get('strategies', []))} strategies")
            return True
        else:
            print("\n✗ FAILED")
            return False
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        return False


if __name__ == "__main__":
    print(f"\n{'#'*60}")
    print("BotSpot API Replication Test")
    print(f"Started: {datetime.now().isoformat()}")
    print(f"{'#'*60}")

    results = {
        "user_profile": test_user_profile(),
        "user_profile_alt": test_user_profile_alt(),
        "strategies_onboarding": test_strategies_onboarding(),
    }

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for endpoint, success in results.items():
        status = "✓ VERIFIED" if success else "✗ FAILED"
        print(f"{endpoint}: {status}")

    total = len(results)
    passed = sum(results.values())
    print(f"\nTotal: {passed}/{total} endpoints verified")
