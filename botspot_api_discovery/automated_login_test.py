#!/usr/bin/env python3
"""
BotSpot Authentication Flow - Hybrid Automated Test Script
Uses Selenium ONLY for login/token capture, then pure API calls for everything else
"""

import json
import os
import sys
import time

import requests
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ANSI color codes
YELLOW = "\033[93m"
GREEN = "\033[92m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_step(step_num, description):
    """Print a step header"""
    print(f"\n{CYAN}{BOLD}[STEP {step_num}]{RESET} {description}")


def print_success(message):
    """Print success message"""
    print(f"{GREEN}✓{RESET} {message}")


def print_error(message):
    """Print error message"""
    print(f"{RED}✗{RESET} {message}")


def print_name(name):
    """Print name in bright yellow"""
    print(f"\n{YELLOW}{BOLD}{'=' * 60}")
    print(f"  USER NAME: {name}")
    print(f"{'=' * 60}{RESET}\n")


def capture_tokens_with_browser(username, password):
    """Use Selenium to login and capture OAuth tokens from localStorage"""

    print_step(1, "Launch browser for authentication (Selenium)")

    # Setup Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in background
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        print_success("Browser launched (headless mode)")

        print_step(2, "Navigate to BotSpot login page")
        driver.get("https://botspot.trade/login")
        print_success("Login page loaded")

        # Wait for Auth0 login form
        print_step(3, "Wait for Auth0 form and submit credentials")
        wait = WebDriverWait(driver, 10)

        # Wait for email field
        email_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        password_field = driver.find_element(By.NAME, "password")
        submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")

        # Fill credentials
        email_field.send_keys(username)
        password_field.send_keys(password)
        print_success("Credentials filled")

        # Submit form
        submit_button.click()
        print_success("Login form submitted")

        # Wait for redirect to BotSpot (OAuth complete)
        print_step(4, "Wait for OAuth flow to complete")
        wait.until(lambda d: "botspot.trade" in d.current_url and "auth0" not in d.current_url)
        time.sleep(2)  # Extra wait for localStorage to populate
        print_success(f"Redirected to: {driver.current_url}")

        print_step(5, "Extract tokens from browser localStorage")

        # Extract Auth0 tokens from localStorage
        auth_keys = driver.execute_script(
            """
            const keys = Object.keys(localStorage);
            return keys.filter(k => k.includes('auth0') && k.includes('urn:botspot-prod-api'));
        """
        )

        if not auth_keys:
            print_error("No Auth0 token keys found in localStorage")
            return None, None

        auth_key = auth_keys[0]
        token_data_str = driver.execute_script(f"return localStorage.getItem('{auth_key}');")

        if not token_data_str:
            print_error("Token data is empty")
            return None, None

        token_data = json.loads(token_data_str)
        access_token = token_data.get("body", {}).get("access_token")
        expires_in = token_data.get("body", {}).get("expires_in")

        if not access_token:
            print_error("Access token not found in localStorage data")
            return None, None

        print_success(f"Access token extracted (expires in {expires_in}s)")
        print_success(f"Token preview: {access_token[:50]}...")

        # Also get user ID token
        user_key = driver.execute_script(
            """
            const keys = Object.keys(localStorage);
            return keys.find(k => k.includes('auth0') && k.includes('@@user@@'));
        """
        )

        id_token = None
        if user_key:
            user_data_str = driver.execute_script(f"return localStorage.getItem('{user_key}');")
            if user_data_str:
                user_data = json.loads(user_data_str)
                id_token = user_data.get("id_token")
                print_success("ID token also extracted")

        return access_token, id_token

    except Exception as e:
        print_error(f"Browser automation failed: {e}")
        return None, None

    finally:
        print_step(6, "Close browser")
        if driver:
            driver.quit()
            print_success("Browser closed")


def main():
    print(f"{BOLD}BotSpot Authentication Flow - Hybrid Test{RESET}")
    print("=" * 60)
    print(f"{CYAN}Selenium for token capture → Pure API for data fetching{RESET}\n")

    # Load credentials from .env
    load_dotenv("/Users/marvin/repos/lumibot/.env")
    username = os.getenv("BOTSPOT_USERNAME")
    password = os.getenv("BOTSPOT_PASSWORD")

    if not username or not password:
        print_error("BOTSPOT_USERNAME and BOTSPOT_PASSWORD must be set in .env")
        sys.exit(1)

    # Part 1: Browser automation to capture tokens
    access_token, id_token = capture_tokens_with_browser(username, password)

    if not access_token:
        print_error("Failed to capture access token")
        sys.exit(1)

    # Part 2: Pure API calls using captured token
    print(f"\n{CYAN}{'=' * 60}")
    print("BROWSER CLOSED - Now using pure API calls")
    print(f"{'=' * 60}{RESET}")

    API_BASE = "https://api.botspot.trade"

    # Create fresh session for API calls
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "User-Agent": "BotSpot-API-Test/1.0",
        }
    )

    print_step(7, "Fetch user profile via API (no browser)")

    profile_url = f"{API_BASE}/users/profile"
    response = session.get(profile_url)

    if response.status_code != 200:
        print_error(f"Failed to fetch profile: {response.status_code}")
        print(f"Response: {response.text}")
        sys.exit(1)

    profile_data = response.json()

    if not profile_data.get("success"):
        print_error("Profile fetch returned success=false")
        print(f"Response: {profile_data}")
        sys.exit(1)

    profile = profile_data.get("profile", {})

    print_success("User profile fetched successfully via pure API!")

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
        print_name(full_name)
    else:
        print_name(profile.get("nickname", "Unknown User"))

    # Logout (client-side only - no API call needed)
    print_step(8, "Logout (token-based session)")
    print(f"{CYAN}Note: Logout is client-side only. " f"Simply discard the token to end session.{RESET}")
    print_success("Token discarded - session ended")

    print(f"\n{GREEN}{BOLD}✓ ALL OPERATIONS COMPLETED SUCCESSFULLY!{RESET}")
    print(f"{CYAN}Summary: Browser used ONLY for token capture, all data via API{RESET}\n")


if __name__ == "__main__":
    main()
