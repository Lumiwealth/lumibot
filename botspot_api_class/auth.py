"""
BotSpot API Client - Authentication Manager

Handles OAuth authentication using Selenium to automate the Auth0 login flow.
Captures access tokens from browser localStorage for subsequent API calls.
"""

import json
import logging
import time
from typing import Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .exceptions import AuthenticationError

logger = logging.getLogger(__name__)


class AuthManager:
    """
    Manages authentication via Selenium browser automation.

    Uses headless Chrome to navigate through Auth0 OAuth flow,
    captures tokens from browser localStorage, then closes browser.
    """

    LOGIN_URL = "https://botspot.trade/login"
    AUTH0_LOCALSTORAGE_KEY_PATTERN = "auth0"
    API_AUDIENCE_PATTERN = "urn:botspot-prod-api"
    USER_KEY_PATTERN = "@@user@@"

    def __init__(self, headless: bool = True):
        """
        Initialize the authentication manager.

        Args:
            headless: If True, run browser in headless mode (no visible window).
                     Set to False for debugging.
        """
        self.headless = headless

    def authenticate(self, username: str, password: str) -> Tuple[str, Optional[str], int, Optional[str]]:
        """
        Authenticate with BotSpot and capture OAuth tokens.

        Args:
            username: BotSpot email address
            password: BotSpot password

        Returns:
            Tuple of (access_token, id_token, expires_in, refresh_token)

        Raises:
            AuthenticationError: If login fails or tokens cannot be extracted
        """
        logger.info("Starting authentication via Selenium")

        driver = None
        try:
            # Setup Chrome options
            options = webdriver.ChromeOptions()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")

            # Launch browser
            driver = webdriver.Chrome(options=options)
            logger.info(f"Browser launched ({'headless' if self.headless else 'visible'})")

            # Navigate to login page
            logger.info(f"Navigating to {self.LOGIN_URL}")
            driver.get(self.LOGIN_URL)
            time.sleep(1)

            # Wait for Auth0 login form
            wait = WebDriverWait(driver, 10)

            try:
                email_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
                password_field = driver.find_element(By.NAME, "password")
                submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            except Exception as e:
                raise AuthenticationError(f"Login form not found: {e}") from e

            # Fill credentials
            logger.info(f"Filling credentials for {username}")
            email_field.send_keys(username)
            time.sleep(0.3)
            password_field.send_keys(password)
            time.sleep(0.3)

            # Submit form
            logger.info("Submitting login form")
            submit_button.click()

            # Wait for redirect to BotSpot (OAuth complete)
            try:
                wait.until(lambda d: "botspot.trade" in d.current_url and "auth0" not in d.current_url)
                time.sleep(2)  # Extra wait for localStorage to populate
            except Exception as e:
                # Check if still on Auth0 page - likely means bad credentials
                if "auth0" in driver.current_url:
                    raise AuthenticationError(
                        "Login failed - check your credentials. "
                        "Ensure BOTSPOT_USERNAME and BOTSPOT_PASSWORD are correct."
                    ) from e
                raise AuthenticationError(f"OAuth flow did not complete: {e}") from e

            logger.info("OAuth flow completed, extracting tokens")

            # Extract access token and refresh token from localStorage
            access_token, expires_in, refresh_token = self._extract_access_token(driver)

            # Extract ID token from localStorage
            id_token = self._extract_id_token(driver)

            logger.info(f"Authentication successful (token expires in {expires_in}s)")

            return access_token, id_token, expires_in, refresh_token

        except AuthenticationError:
            raise
        except Exception as e:
            raise AuthenticationError(f"Unexpected error during authentication: {e}") from e
        finally:
            if driver:
                driver.quit()
                logger.info("Browser closed")

    def _extract_access_token(self, driver) -> Tuple[str, int, Optional[str]]:
        """
        Extract access token and refresh token from browser localStorage.

        Args:
            driver: Selenium WebDriver instance

        Returns:
            Tuple of (access_token, expires_in, refresh_token)

        Raises:
            AuthenticationError: If token cannot be extracted
        """
        try:
            # Find Auth0 localStorage key containing API audience
            auth_keys = driver.execute_script(
                f"""
                const keys = Object.keys(localStorage);
                return keys.filter(k =>
                    k.includes('{self.AUTH0_LOCALSTORAGE_KEY_PATTERN}') &&
                    k.includes('{self.API_AUDIENCE_PATTERN}')
                );
            """
            )

            if not auth_keys:
                raise AuthenticationError(
                    "No Auth0 token keys found in localStorage. "
                    "This may indicate a change in the authentication flow."
                )

            # Get token data from localStorage
            auth_key = auth_keys[0]
            token_data_str = driver.execute_script(f"return localStorage.getItem('{auth_key}');")

            if not token_data_str:
                raise AuthenticationError("Token data is empty in localStorage")

            # Parse token data
            token_data = json.loads(token_data_str)

            body = token_data.get("body", {})
            access_token = body.get("access_token")
            expires_in = body.get("expires_in")
            refresh_token = body.get("refresh_token")

            if not access_token:
                raise AuthenticationError("Access token not found in localStorage data")

            # Log refresh token availability
            if refresh_token:
                logger.info(f"✅ REFRESH TOKEN FOUND: {refresh_token[:50]}...")
                logger.info("Refresh tokens are ENABLED - can implement auto-refresh!")
            else:
                # Print bright yellow TODO reminder
                print("\n" + "\033[93m\033[1m" + "=" * 80)
                print("  ⚠️  TODO: Ask Rob if the OAuth timing can be extended longer than 24h?")
                print("  Currently: Tokens expire after 24 hours → requires daily re-login")
                print("  Ideal: Enable 'Refresh Token Rotation' in Auth0 for indefinite sessions")
                print("=" * 80 + "\033[0m\n")

                logger.warning("❌ NO REFRESH TOKEN - Rotation not enabled by BotSpot")
                logger.warning("Contact Rob to enable 'Refresh Token Rotation' for longer sessions")

            return access_token, expires_in or 86400, refresh_token

        except json.JSONDecodeError as e:
            raise AuthenticationError(f"Failed to parse token data: {e}") from e
        except Exception as e:
            raise AuthenticationError(f"Failed to extract access token: {e}") from e

    def _extract_id_token(self, driver) -> Optional[str]:
        """
        Extract ID token from browser localStorage.

        Args:
            driver: Selenium WebDriver instance

        Returns:
            ID token string, or None if not found
        """
        try:
            # Find Auth0 user key containing ID token
            user_key = driver.execute_script(
                f"""
                const keys = Object.keys(localStorage);
                return keys.find(k =>
                    k.includes('{self.AUTH0_LOCALSTORAGE_KEY_PATTERN}') &&
                    k.includes('{self.USER_KEY_PATTERN}')
                );
            """
            )

            if not user_key:
                logger.debug("No user key found in localStorage (ID token may not be available)")
                return None

            # Get user data from localStorage
            user_data_str = driver.execute_script(f"return localStorage.getItem('{user_key}');")

            if not user_data_str:
                logger.debug("User data is empty in localStorage")
                return None

            # Parse user data
            user_data = json.loads(user_data_str)
            id_token = user_data.get("id_token")

            if id_token:
                logger.info("ID token extracted successfully")

            return id_token

        except Exception as e:
            logger.debug(f"Failed to extract ID token (non-critical): {e}")
            return None
