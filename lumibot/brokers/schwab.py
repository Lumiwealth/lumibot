import logging
import os
import json
from typing import Union, List, Optional
import dotenv
import traceback
import re
from datetime import datetime, timedelta
from pytz import timezone
import threading
from threading import Thread
from flask import Flask
import termcolor

from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import SchwabData, YahooData # Import YahooData

# Import Schwab specific libraries
from schwab.auth import easy_client, client_from_login_flow
from schwab.client import Client
from schwab.streaming import StreamClient

# Import PollingStream class
from lumibot.trading_builtins import PollingStream
import time
from pathlib import Path

def _sanitize_at_padding(tok: dict):
    """Replace any '@' with '=' in access_token and refresh_token fields."""
    for k in ("access_token", "refresh_token"):
        if isinstance(tok.get(k), str):
            tok[k] = tok[k].replace('@', '=')

def _ensure_token_metadata(token_path: Path):
    """
    Ensure Schwab token file is in the format expected by schwab-py ≥ 1.5:
    {
        "creation_timestamp": ...,
        "token": { ... }
    }
    """
    import time, json
    if not token_path.exists():
        return

    try: # Add try-except around file operations
        with token_path.open("r+", encoding="utf-8") as fp:
            tok_raw = json.load(fp)

        # If already wrapped, just update the token part
        if "creation_timestamp" in tok_raw and "token" in tok_raw:
            creation_ts = tok_raw["creation_timestamp"]
            tok = tok_raw["token"]
        else:
            creation_ts = int(time.time())
            tok = tok_raw

        # 1) REMOVED @ → = padding replacement here.
        #    Tokens should be used as-is from Schwab.

        # 2) add/patch mandatory fields inside the token
        now_ms = int(time.time() * 1000)
        defaults = {
            "issued_at": now_ms,
            "refresh_token_issued_at": now_ms,
            "expires_in": 1800,
            "refresh_token_expires_in": 90 * 24 * 3600, # Changed from 7776000 for clarity (90 days)
            "token_type": "Bearer",
            "scope": "api",
        }
        tok.update({k: v for k, v in defaults.items() if k not in tok})

        # 3) strip legacy id_token (schwab-py rejects it)
        tok.pop("id_token", None)

        # 4) Write back as wrapped dict
        wrapped = {
            "creation_timestamp": creation_ts,
            "token": tok,
        }
        # Use 'w' mode to overwrite the file completely
        with token_path.open("w", encoding="utf-8") as fp:
            json.dump(wrapped, fp)
        logging.warning(f"[DEBUG] token.json written (wrapped by _ensure_token_metadata): {wrapped}")

    except Exception as e:
        logging.error(f"[DEBUG] Error in _ensure_token_metadata: {e}")
        logging.error(traceback.format_exc())
        # If error occurs, try to delete the potentially corrupted file
        try:
            token_path.unlink(missing_ok=True)
            logging.warning(f"[DEBUG] Deleted potentially corrupted token file due to error in _ensure_token_metadata: {token_path}")
        except Exception as unlink_e:
            logging.error(f"[DEBUG] Failed to delete token file after error in _ensure_token_metadata: {unlink_e}")


def _is_token_valid_for_schwab_py(token_path: Path):
    """
    Returns True if the token at token_path has both access_token and refresh_token.
    Accepts both wrapped and flat formats.
    """
    if not token_path.exists():
        return False
    import json
    try:
        with token_path.open("r", encoding="utf-8") as fp:
            tok = json.load(fp)
        # Accept both wrapped and flat token formats
        if "access_token" in tok and "refresh_token" in tok:
            return bool(tok.get("access_token") and tok.get("refresh_token"))
        if "token" in tok and isinstance(tok["token"], dict):
            t = tok["token"]
            return bool(t.get("access_token") and t.get("refresh_token"))
        return False
    except Exception as e:
        logging.error(f"[DEBUG] Exception in _is_token_valid_for_schwab_py: {e}")
        return False

# --- Helper Function ---
# Modify helper signature to accept app_secret
def _launch_botspot_helper(token_path: Path, api_key: str, app_secret: str, callback_url: str):
    import os, sys, webbrowser, base64, logging, traceback, urllib.parse # Import urllib
    from flask import Flask, request, redirect
    from pathlib import Path
    import termcolor # Make sure termcolor is imported if used here
    # Import the function needed for manual code exchange
    from schwab.auth import client_from_manual_flow

    # Use passed-in credentials
    APP_KEY = api_key
    APP_SECRET = app_secret # Store app_secret for token exchange later
    BOTSPOT = callback_url # Base callback URL

    # --- REVERTED CHANGE ---
    # Do NOT append appKey/appSecret to the redirect_uri sent to Schwab.
    # The redirect_uri should be the registered callback URL.
    # URL-encode the *base* callback URI for the redirect_uri parameter
    encoded_redirect_uri = urllib.parse.quote(BOTSPOT, safe='')

    # Construct the final Schwab authorization URL using the encoded base redirect_uri
    url = (
        "https://api.schwabapi.com/v1/oauth/authorize"
        f"?response_type=code&client_id={APP_KEY}&redirect_uri={encoded_redirect_uri}&state=lumibot"
    )
    # Log the URL being opened
    logging.info(f"[Schwab Helper] Opening Schwab authorization URL: {url}")
    # Log the base callback URI used for redirect_uri
    logging.info(f"[Schwab Helper] redirect_uri parameter sent to Schwab (encoded): {encoded_redirect_uri}")


    app = Flask("schwab-oauth")

    @app.route("/")
    def index():
        # Bootstrap 5, button, textarea, clear instructions
        return (
            """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1">
                <title>LumiBot - Schwab Login</title>
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
            </head>
            <body class="bg-light">
            <div class="container" style="max-width: 540px; margin-top: 48px;">
                <div class="card shadow-sm">
                    <div class="card-body">
                        <h3 class="card-title mb-3 text-primary">Connect Schwab to LumiBot</h3>
                        <ol class="mb-3">
                            <li>Click the button below to open Schwab's login page.</li>
                            <li>Authorize LumiBot and copy the <b>code</b> you see on BotSpot.</li>
                            <li>Paste the <b>code</b> below and click <b>Save Token</b>.</li>
                        </ol>
                        <div class="d-grid mb-3">
                            <a href='""" + url + """' target="_blank" class="btn btn-primary btn-lg">
                                <i class="bi bi-box-arrow-up-right"></i> Authorize Schwab Account
                            </a>
                        </div>
                        <form method="post" id="tokenForm">
                            <div class="mb-3">
                                <label for="auth_code" class="form-label">Paste your Schwab <b>code</b>:</label>
                                <textarea class="form-control" id="auth_code" name="t" rows="4" placeholder="Paste the authorization code here..." required></textarea>
                            </div>
                            <button type="submit" class="btn btn-success w-100">Save Token</button>
                        </form>
                        <div id="msg" class="mt-3"></div>
                    </div>
                </div>
                <div class="text-center text-muted mt-3" style="font-size:0.95em;">
                    Need help? See the <a href="https://lumibot.lumiwealth.com/brokers.schwab.html" target="_blank">docs</a>.
                </div>
            </div>
            <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
            <script>
            document.getElementById('tokenForm').onsubmit = async function(e) {
                e.preventDefault();
                var msg = document.getElementById('msg');
                msg.textContent = '';
                msg.className = '';
                var code = document.getElementById('auth_code').value.trim(); // Get value from textarea
                if (!code) {
                    msg.textContent = 'Please paste your authorization code.'; // Updated message
                    msg.className = 'alert alert-danger';
                    return;
                }
                msg.textContent = 'Exchanging code for token...'; // Updated message
                msg.className = 'alert alert-info';
                let resp = await fetch('/', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: 't=' + encodeURIComponent(code) // Send the code
                });
                let text = await resp.text();
                if (resp.ok && text.includes('token saved')) {
                    msg.textContent = '✅ Token saved! Please restart LumiBot.';
                    msg.className = 'alert alert-success';
                    document.getElementById('auth_code').value = ''; // Clear textarea
                } else {
                    msg.textContent = 'Failed to save token: ' + text;
                    msg.className = 'alert alert-danger';
                }
            };
            </script>
            </body>
            </html>
            """
        )

    @app.route("/", methods=["POST"])
    def save():
        # This now receives the authorization CODE
        code = request.form.get("t", "").strip()
        if not code:
             return "error: no code provided", 400
        try:
            # Always remove any existing token.json before writing new one
            if token_path.exists():
                token_path.unlink()

            logging.info(f"[Schwab Helper] Received code: {code[:10]}...") # Log received code (truncated)
            logging.info(f"[Schwab Helper] Exchanging code using client_from_manual_flow...")
            logging.info(f"[Schwab Helper]   api_key: {APP_KEY[:4]}...") # Use APP_KEY from outer scope
            logging.info(f"[Schwab Helper]   callback_url: {BOTSPOT}") # Use BOTSPOT from outer scope
            logging.info(f"[Schwab Helper]   token_path: {token_path}")

            # Use client_from_manual_flow to handle the token exchange and save to token_path
            # It requires api_key, app_secret, callback_url, token_path, and the code
            client_from_manual_flow(
                api_key=APP_KEY,        # Use APP_KEY from outer scope
                app_secret=APP_SECRET,  # Use APP_SECRET from outer scope
                callback_url=BOTSPOT,   # Use BOTSPOT (base callback URL) from outer scope
                token_path=token_path,
                code=code               # Pass the received code
            )

            # client_from_manual_flow creates token.json. Run _ensure_token_metadata for robustness.
            logging.warning(f"[DEBUG] token.json written by client_from_manual_flow: {token_path}")
            _ensure_token_metadata(token_path) # Ensure standard metadata format

            # LOG: Confirm token was saved and print contents
            try:
                with token_path.open("r", encoding="utf-8") as fp:
                    token_json = fp.read()
                logging.warning(f"[DEBUG] token.json contents after POST (FULL, sensitive!): {token_json}")
            except Exception as e:
                logging.warning(f"[DEBUG] Could not read token.json after POST: {e}")

            return "✅ token saved – you may close this tab."
        except Exception as e:
            logging.error(f"[DEBUG] Exception in / POST (token exchange): {e}")
            logging.error(traceback.format_exc()) # Log traceback for exchange/saving errors
            # Try to delete potentially bad token file if exchange failed
            try:
                token_path.unlink(missing_ok=True)
            except: pass
            return f"error exchanging code or saving token: {e}", 400

    import threading
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=8080, debug=False),
        daemon=True).start()

    webbrowser.open("http://localhost:8080")
    logging.info("[Schwab] waiting for token via http://localhost:8080 …")
    # Wait for token.json to appear, then return (do not exit process)
    while not token_path.exists():
        time.sleep(2)
    # LOG: Token file detected, continue
    logging.warning("[DEBUG] token.json detected after helper, continuing broker initialization.")

class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Check if the level is enabled to avoid formatting costs if not necessary
        if self.logger.isEnabledFor(kwargs.get('level', logging.INFO)):
            # Lazy formatting of the message
            return f'[{self.extra["strategy_name"]}] {msg}', kwargs
        else:
            return msg, kwargs

    def update_strategy_name(self, new_strategy_name):
        self.extra['strategy_name'] = new_strategy_name
        # Pre-format part of the log message that's static or changes infrequently
        self.formatted_prefix = f'[{new_strategy_name}]'

class Schwab(Broker):
    """
    Broker implementation for Schwab API.
    
    This class provides the integration with Schwab's trading platform,
    implementing all necessary methods required by the Lumibot framework
    to interact with the broker.

    Link to Schwab API documentation: https://developer.schwab.com/ and create an account to get API doc access.
    Link to the Python client library: https://schwab-py.readthedocs.io/en/latest/
    """

    NAME = "Schwab"
    POLL_EVENT = PollingStream.POLL_EVENT

    def __init__(
            self,
            config=None,
            data_source=None,
    ):
        # === Initialize error flag very early ===
        self.schwab_authorization_error = False
        # === End Initialize error flag ===

        # === Prepare Data Source ===
        # Determine if SchwabData is intended or if a specific one was passed
        is_schwab_data_intended = data_source is None or isinstance(data_source, SchwabData)
        final_data_source = data_source

        if is_schwab_data_intended:
            if data_source is None:
                # Create a SchwabData instance now, client will be set later
                logging.debug("[Schwab] Creating initial SchwabData instance (client will be set later).")
                final_data_source = SchwabData()
            # If a SchwabData instance was passed, use it directly
            else:
                logging.debug("[Schwab] Using provided SchwabData instance.")
                # final_data_source is already data_source
        else:
            # If a different, non-SchwabData source was explicitly passed, use it
            logging.debug(f"[Schwab] Using explicitly provided non-SchwabData source: {type(data_source).__name__}")
            # final_data_source is already data_source

        # Call super().__init__ with the determined data source
        super().__init__(
            name=self.NAME,
            data_source=final_data_source, # Pass the actual intended or created data source
            config=config,
        )
        # === End Prepare Data Source ===

        # Initialize Schwab specific attributes
        self._subscribers = []
        # Use standard logging module's logger
        self.logger = CustomLoggerAdapter(logging.getLogger(__name__), {'strategy_name': "unknown"})
        self.extended_trading_minutes = 0
        # self.schwab_authorization_error = False # Moved earlier
        self.client = None
        self.hash_value = None
        self.account_number = None
        self.stream_client = None
        self.stream = None
        # Store if SchwabData was the goal for later client assignment
        self._is_schwab_data_intended = is_schwab_data_intended

        # Load environment variables (still useful for fallback if config is missing keys)
        dotenv.load_dotenv()

        logging.warning("==== [DEBUG] Schwab Broker Initialization ====")
        logging.warning(f"config passed to __init__: {config}")

        config = config or {}

        # Account Number (Required) - Prioritize config, fallback to env
        account_number = config.get("SCHWAB_ACCOUNT_NUMBER") or os.environ.get("SCHWAB_ACCOUNT_NUMBER")
        if not account_number:
            # Set error flag before raising
            self.schwab_authorization_error = True
            raise ValueError("Schwab account number (SCHWAB_ACCOUNT_NUMBER) not found in config or environment variables.")
        self.account_number = str(account_number)

        # API Key (Required) - Prioritize config, fallback to env
        api_key = config.get("SCHWAB_APP_KEY") or os.environ.get("SCHWAB_APP_KEY")
        if not api_key:
            # Set error flag before raising
            self.schwab_authorization_error = True
            raise ValueError("Schwab App Key (SCHWAB_APP_KEY) not found in config or environment variables.")

        # === Load App Secret from config (preferred) or environment ===
        app_secret = config.get("SCHWAB_APP_SECRET") or os.environ.get("SCHWAB_APP_SECRET")
        if not app_secret:
            # Set error flag before raising
            self.schwab_authorization_error = True
            raise ValueError("Schwab App Secret (SCHWAB_APP_SECRET) not found in config or environment variables. Please ensure it is set correctly.")
        logging.warning(f"app_secret (final): {'<set>' if app_secret else '<not set>'}") # Log if set
        # === End Load App Secret ===

        # Callback URL (Required for helper) - Prioritize config, fallback to env, then default
        callback_url = config.get("SCHWAB_CALLBACK") or os.environ.get("SCHWAB_CALLBACK") or "https://api.botspot.trade/broker_oauth/schwab" # Use HTTPS and correct default

        # Token (Optional) - Prioritize config, fallback to env
        token_str_env = config.get("SCHWAB_TOKEN") or os.environ.get("SCHWAB_TOKEN")

        logging.warning(f"account_number (final): {self.account_number}")
        logging.warning(f"api_key (final): {'<set>' if api_key else '<not set>'}")
        logging.warning("==== [END DEBUG] ====")

        # Determine token path
        token_path = Path(__file__).parent / "token.json"

        # Check if token exists (file or env var)
        has_file_token = token_path.exists() and token_path.stat().st_size > 0
        has_env_token = bool(token_str_env) # Check if env var is set and not empty
        logging.info(f"[Schwab] SCHWAB_TOKEN env value: {'<set>' if has_env_token else '<not set>'}")
        logging.info(f"[Schwab] used_env_token: {has_env_token}") # Log if we intend to use env token
        logging.info(f"[Schwab] token.json exists: {has_file_token}")
        if has_file_token:
             logging.info(f"[Schwab] token.json size: {token_path.stat().st_size} bytes")
             try:
                 with token_path.open("r", encoding="utf-8") as f_check:
                     content_check = f_check.read()
                 logging.warning(f"[DEBUG] token.json contents (FULL, sensitive!): {content_check}")
             except Exception as read_err:
                 logging.warning(f"[DEBUG] Could not read token.json for logging: {read_err}")


        # --- Token Loading/Helper Logic ---
        # Uses api_key, app_secret, callback_url defined above
        if not has_file_token and not has_env_token:
            logging.error(termcolor.colored("[Schwab] No Schwab token file found and SCHWAB_TOKEN env var not set. Launching login helper.", "red"))
            logging.error(termcolor.colored(f"[Schwab] _launch_botspot_helper called: reason=no_token_file_and_no_env_token, api_key={api_key[:4]}..., callback={callback_url}", "red"))
            # Pass app_secret to helper
            _launch_botspot_helper(token_path, api_key, app_secret, callback_url)
            print(colored("✅ Token saved! Please restart LumiBot to continue.", "green"))
            os._exit(0) # Exit after helper runs

        elif has_file_token:
            logging.info(f"[Schwab] Attempting to load Schwab token from {token_path}")
            try:
                # Ensure metadata format BEFORE attempting to load
                _ensure_token_metadata(token_path)
                logging.warning(f"[DEBUG] token.json about to be read after ensure_metadata: {token_path.read_text()}")

                # === REMOVED SANITIZE STEP ===
                # The '@' character should NOT be replaced. easy_client expects the token as-is.

                if not _is_token_valid_for_schwab_py(token_path):
                     # Set error flag before raising
                     self.schwab_authorization_error = True
                     raise ValueError("Token file format invalid after metadata check.") # Updated error message

                logging.info("[Schwab] token.json passed format check, calling easy_client...")
                self.client = easy_client(
                    api_key=api_key,
                    app_secret=app_secret,
                    callback_url=callback_url, # Use corrected callback_url
                    token_path=str(token_path),
                )
                logging.info(f"[Schwab] Successfully loaded Schwab client from {token_path}")
                # used_env_token = False # This variable wasn't used later, removed

            except Exception as e:
                logging.error(colored(f"[Schwab] Error loading token from {token_path}: {e}", "red"))
                logging.error(traceback.format_exc())
                token_path.unlink(missing_ok=True)
                logging.error(termcolor.colored("[Schwab] Deleted potentially corrupt token file. Launching login helper.", "red"))
                logging.error(termcolor.colored(f"[Schwab] _launch_botspot_helper called: reason=token_load_error, api_key={api_key[:4]}..., callback={callback_url}", "red"))
                # Pass app_secret to helper
                _launch_botspot_helper(token_path, api_key, app_secret, callback_url)
                print(colored("✅ Token saved! Please restart LumiBot to continue.", "green"))
                os._exit(0)

        elif has_env_token:
             logging.info("[Schwab] Attempting to load Schwab token from SCHWAB_TOKEN env var")
             try:
                 # Decode the base64 string from env var
                 import base64, json, time
                 code = token_str_env.strip()
                 missing_padding = len(code) % 4
                 if missing_padding:
                     code += '=' * (4 - missing_padding)
                 clean = base64.urlsafe_b64decode(code.encode('ascii'))
                 tok = json.loads(clean)

                 # --- REMOVED @ replacement ---
                 # Tokens should be used as-is.

                 # Wrap and add metadata (similar to _ensure_token_metadata but in memory)
                 now_ms = int(time.time() * 1000)
                 defaults = {
                     "issued_at": now_ms, "refresh_token_issued_at": now_ms,
                     "expires_in": 1800, "refresh_token_expires_in": 90 * 24 * 3600,
                     "token_type": "Bearer", "scope": "api",
                 }
                 tok.update({k: v for k, v in defaults.items() if k not in tok})
                 tok.pop("id_token", None) # Remove id_token

                 # Write to temp file for easy_client
                 import tempfile
                 with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json", encoding="utf-8") as tmp_fp:
                     wrapped = {"creation_timestamp": int(time.time()), "token": tok}
                     json.dump(wrapped, tmp_fp)
                     temp_token_path = tmp_fp.name
                 logging.warning(f"[DEBUG] Wrote env token to temp file: {temp_token_path}")

                 self.client = easy_client(
                     api_key=api_key,
                     app_secret=app_secret,
                     callback_url=callback_url, # Use corrected callback_url
                     token_path=temp_token_path,
                 )
                 logging.info("[Schwab] Successfully loaded Schwab client from SCHWAB_TOKEN env var")
                 # Clean up temp file? Or let OS handle it? easy_client might keep handle. Let OS handle for now.
                 # os.unlink(temp_token_path)

             except Exception as e:
                 logging.error(colored(f"[Schwab] Error loading token from SCHWAB_TOKEN env var: {e}", "red"))
                 logging.error(traceback.format_exc())
                 logging.error(termcolor.colored("[Schwab] Falling back to login helper.", "red"))
                 logging.error(termcolor.colored(f"[Schwab] _launch_botspot_helper called: reason=env_token_load_error, api_key={api_key[:4]}..., callback={callback_url}", "red"))
                 # Pass app_secret to helper
                 _launch_botspot_helper(token_path, api_key, app_secret, callback_url)
                 print(colored("✅ Token saved! Please restart LumiBot to continue.", "green"))
                 os._exit(0)


        # --- Post Client Initialization ---
        # Get account numbers and find the hash value
        try:
            logging.warning("[DEBUG] About to call get_account_numbers() on Schwab client.")
            if not self.client:
                 # This should ideally not happen if token loading succeeded, but check just in case
                 self.schwab_authorization_error = True # Set flag
                 raise ConnectionError("Schwab client was not initialized before attempting to get account numbers.")

            response = self.client.get_account_numbers()

            if response.status_code == 200:
                account_numbers_data = response.json()
                found_hash = None
                for account_info in account_numbers_data:
                    if account_info.get("accountNumber") == self.account_number:
                        found_hash = account_info.get("hashValue")
                        break

                if found_hash:
                    # self.hash_value = found_hash # Set in _finish_initialization
                    logging.info(f"[Schwab] Successfully found hash value for account {self.account_number}")
                    # Call finish_initialization only after successful client load and hash retrieval
                    # Pass the already initialized data_source (self.data_source)
                    self._finish_initialization(config, self.data_source, self.account_number, found_hash)
                else:
                    # Set error flag before raising
                    self.schwab_authorization_error = True
                    raise ValueError(f"Account number {self.account_number} not found in linked accounts.")

            elif response.status_code == 401:
                 logging.error(colored("[Schwab] Authorization Error (401) when getting account numbers. Token might be invalid or expired.", "red"))
                 self.schwab_authorization_error = True # Set flag
                 token_path.unlink(missing_ok=True) # Delete potentially bad token
                 logging.error(termcolor.colored("[Schwab] Deleted potentially invalid token file. Launching login helper.", "red"))
                 logging.error(termcolor.colored(f"[Schwab] _launch_botspot_helper called: reason=get_account_401, api_key={api_key[:4]}..., callback={callback_url}", "red"))
                 # Pass app_secret to helper
                 _launch_botspot_helper(token_path, api_key, app_secret, callback_url)
                 print(colored("✅ Token saved! Please restart LumiBot to continue.", "green"))
                 os._exit(0) # Exit after helper runs
            else:
                # Set error flag before raising
                self.schwab_authorization_error = True
                raise ConnectionError(f"Failed to get account numbers: {response.status_code} - {response.text}")

        except Exception as e:
            logging.error(colored(f"Error during Schwab client initialization or getting account hash: {e}", "red"))
            logging.error(traceback.format_exc())
            self.schwab_authorization_error = True # Set flag on any error during init
            # It's crucial to raise here to prevent proceeding with a broken state
            raise ConnectionError(f"Schwab initialization failed: {e}") from e

        # No more code should run here if an exception occurred above

    # Account and balance methods
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Get the actual cash balance at the broker.
        
        Parameters
        ----------
        quote_asset : Asset
            The quote asset to get the balance of (e.g., USD, EUR).
        strategy : Strategy
            The strategy object that is requesting the balance.

        Returns
        -------
        tuple of float
            A tuple containing (cash, positions_value, total_liquidation_value).
            - Cash = cash in the account (whatever the quote asset is).
            - Positions value = the value of all the positions in the account.
            - Portfolio value = the total equity value of the account (aka. portfolio value).
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.warning(colored("Schwab authorization failed previously. Cannot get balances.", "yellow"))
            return 0.0, 0.0, 0.0

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot get balances.", "red"))
            return 0.0, 0.0, 0.0 # Return default values

        try:
            # Get account information using the hash_value stored during initialization
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])

            if response.status_code != 200:
                logging.error(colored(f"Error getting account information: {response.status_code}, {response.text}", "red"))
                # Modify the error message slightly for clarity
                raise ConnectionError(f"Failed to get account information for hash {self.hash_value}: {response.text}")

            account_data = response.json()

            # Try to use aggregated balance first if available
            if 'aggregatedBalance' in account_data:
                # Use aggregated balance data
                aggregated_balance = account_data['aggregatedBalance']
                portfolio_value = float(aggregated_balance.get('currentLiquidationValue', 0))

                # Get cash from securitiesAccount
                securities_account = account_data.get('securitiesAccount', {})
                balances = securities_account.get('currentBalances', {})
                cash = float(balances.get('cashBalance', 0))
            else:
                # Fall back to original implementation
                securities_account = account_data.get('securitiesAccount', {})
                account_type = securities_account.get('type', '')

                # Get balances based on account type
                balances = securities_account.get('currentBalances', {})
                if account_type.lower() == 'margin':
                    cash = float(balances.get('cashBalance', 0))
                    portfolio_value = float(balances.get('liquidationValue', 0))
                    if portfolio_value == 0:
                        portfolio_value = float(balances.get('equity', 0))
                else: # Assuming CASH account type or similar
                    cash = float(balances.get('cashBalance', 0))
                    portfolio_value = float(balances.get('accountValue', 0))

            # Calculate positions value (portfolio value minus cash)
            positions_value = portfolio_value - cash

            return cash, positions_value, portfolio_value

        except Exception as e:
            logging.error(colored(f"Error getting balances from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())

            # Return default values in case of error
            return 0.0, 0.0, 0.0

    # Position methods
    def _pull_positions(self, strategy: 'Strategy') -> List[Position]:
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.warning(colored("Schwab authorization failed previously. Cannot pull positions.", "yellow"))
            return []

        try:
            # Add check for valid client and hash_value
            if not self.client or not self.hash_value:
                logging.error(colored(f"Schwab client or account hash not initialized. Cannot pull positions.", "red"))
                return [] # Return empty list

            # Get account details with positions
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])

            if response.status_code != 200:
                logging.error(colored(f"Error fetching positions: {response.status_code}, {response.text}", "red"))
                return []
            
            account_data = response.json()

            # Extract positions
            securities_account = account_data.get('securitiesAccount', {})
            schwab_positions = securities_account.get('positions', [])

            pos_dict = {}  # key: (symbol, asset_type, expiration, strike, right)
            strategy_name = strategy.name if strategy is not None else "Unknown"
            
            for schwab_position in schwab_positions:
                # Extract instrument details
                instrument = schwab_position.get('instrument', {})
                asset_type = instrument.get('assetType', '')
                symbol = instrument.get('symbol', '')

                # Initialize Asset object based on asset type
                asset = None
                if asset_type == 'EQUITY':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type == 'OPTION':
                    # Parse option details
                    option_symbol = instrument.get('symbol')
                    option_parts = self._parse_option_symbol(option_symbol)

                    if option_parts is None:
                        logging.error(colored(f"Failed to parse option symbol: {option_symbol}", "red"))
                        continue

                    asset = Asset(
                        symbol=option_parts['underlying'],
                        asset_type=Asset.AssetType.OPTION,
                        expiration=option_parts['expiry_date'],
                        strike=option_parts['strike_price'],
                        right=option_parts['option_type'],
                    )
                elif asset_type == 'FUTURE':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.FUTURE,
                    )
                elif asset_type == 'BOND':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.BOND,
                    )
                elif asset_type == 'MUTUAL_FUND':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.MUTUAL_FUND,
                    )
                elif asset_type == 'COLLECTIVE_INVESTMENT':
                    # Handle ETFs like CQQQ, UPRO as stocks
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type == 'ETF':
                    # Handle ETFs as stocks
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type in ['CASH_EQUIVALENT', 'MONEY_MARKET_FUND', 'CASH']:
                    # Use FOREX as a representation for cash and cash equivalents
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.FOREX,
                    )
                else:
                    # Skip unknown asset types
                    logging.warning(colored(f"Skipping unknown asset type: {asset_type} for symbol: {symbol}", "yellow"))
                    continue

                # Calculate net quantity (long - short)
                long_quantity = schwab_position.get('longQuantity', 0)
                short_quantity = schwab_position.get('shortQuantity', 0)
                net_quantity = long_quantity - short_quantity
                
                # Skip positions with zero quantity
                if net_quantity == 0:
                    continue
                
                # Extract position-specific details
                average_price = schwab_position.get('averagePrice', 0.0)

                # Only create position object if we have a valid asset
                if asset is not None:
                    # Create a unique key for the asset to avoid duplicates
                    key = (asset.symbol, asset.asset_type,
                          getattr(asset, 'expiration', None),
                          getattr(asset, 'strike', None),
                          getattr(asset, 'right', None))
                    
                    # If we already have this asset in our dict, update the quantity
                    if key in pos_dict:
                        pos_dict[key].quantity += net_quantity
                    else:
                        # Create a new Position object
                        pos_dict[key] = Position(
                            strategy_name,
                            asset=asset,
                            quantity=net_quantity,
                            avg_fill_price=average_price,
                        )
            
            # Log the number of positions found
            logging.debug(f"Pulled {len(pos_dict)} unique positions from Schwab")
            
            return list(pos_dict.values())

        except Exception as e:
            logging.error(colored(f"Error pulling positions from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Optional[Position]:
        """
        Pull a single position from the broker that matches the asset and strategy.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull.
        asset: Asset
            The asset to pull the position for.

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None.
            
        Notes
        -----
        This method compares different attributes based on asset type:
        - For stocks and futures: Compares only the symbol
        - For options: Compares symbol, strike, right, and expiration
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.warning(colored("Schwab authorization failed previously. Cannot pull position.", "yellow"))
            return None

        positions = self._pull_positions(strategy)
        
        for position in positions:
            # For stocks, just compare the symbol
            if asset.asset_type == Asset.AssetType.STOCK and position.asset.symbol == asset.symbol:
                return position
            # For options, compare all option details
            elif asset.asset_type == Asset.AssetType.OPTION:
                if (position.asset.symbol == asset.symbol and 
                    position.asset.strike == asset.strike and 
                    position.asset.right == asset.right and 
                    position.asset.expiration == asset.expiration):
                    return position
            # For futures, compare symbol
            elif asset.asset_type == Asset.AssetType.FUTURE and position.asset.symbol == asset.symbol:
                return position
                
        return None

    # Symbol parsing methods
    def _parse_option_symbol(self, option_symbol):
        """
        Parse Schwab option symbol format (e.g., 'SPY   240801P00541000') into its components.
        
        Parameters
        ----------
        option_symbol : str
            The option symbol in Schwab format.
            
        Returns
        -------
        dict
            A dictionary containing the parsed components:
            - 'underlying': The underlying symbol (e.g., 'SPY')
            - 'expiry_date': The expiration date as a datetime.date object
            - 'option_type': The option type ('CALL' or 'PUT')
            - 'strike_price': The strike price as a float
            
        Returns None if parsing failed.
        """
        try:
            # Define the regex pattern for the option symbol
            # Format is: symbol(spaces)YYMMDD(C|P)strike(with padding zeros)
            pattern = r'^(?P<underlying>[A-Z]+)\s+(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})$'

            # Match the pattern with the option symbol
            match = re.match(pattern, option_symbol)
            if not match:
                logging.error(colored(f"Invalid option symbol format: {option_symbol}", "red"))
                return None

            # Extract the parts from the regex match groups
            underlying = match.group('underlying').strip()
            expiry = match.group('expiry')
            option_type = match.group('type')
            strike_raw = match.group('strike')

            # Convert expiry date string to a date object
            # Format is YYMMDD, convert to YYYY-MM-DD
            expiry_date = datetime.strptime(expiry, '%y%m%d').date()

            # Convert strike price to a float (divide by 1000 to get actual price)
            strike_price = int(strike_raw) / 1000

            # Map option type to CALL or PUT
            option_type_full = 'CALL' if option_type == 'C' else 'PUT'

            return {
                'underlying': underlying,
                'expiry_date': expiry_date,
                'option_type': option_type_full,
                'strike_price': strike_price
            }
            
        except Exception as e:
            logging.error(colored(f"Error parsing option symbol {option_symbol}: {str(e)}", "red"))
            return None

    # Order methods
    def _pull_broker_all_orders(self) -> list:
        """
        Get the broker's open orders.

        Returns
        -------
        list
            A list of order responses from the broker query. These will be passed to 
            _parse_broker_order() to be converted to Order objects.
            
        Notes
        -----
        This method retrieves orders from the past 7 days by default to limit the
        volume of data returned while still capturing relevant recent orders.
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.warning(colored("Schwab authorization failed previously. Cannot pull all orders.", "yellow"))
            return []

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot pull all orders.", "red"))
            return [] # Return empty list

        try:
            # Get orders from last 7 days
            seek_start = datetime.now(timezone('UTC')) - timedelta(days=7)
            
            response = self.client.get_orders_for_account(
                self.hash_value,
                from_entered_datetime=seek_start
            )

            if response.status_code != 200:
                logging.error(colored(f"Error fetching orders: {response.status_code}, {response.text}", "red"))
                return []
            
            schwab_orders = response.json()
            
            return schwab_orders
        
        except Exception as e:
            logging.error(colored(f"Error pulling orders from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    def _pull_broker_order(self, identifier: str) -> dict:
        """
        Get a broker order representation by its id.

        Parameters
        ----------
        identifier : str
            The identifier of the order to pull.

        Returns
        -------
        dict
            The order representation from the broker, or None if not found.
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.warning(colored(f"Schwab authorization failed previously. Cannot pull order {identifier}.", "yellow"))
            return None

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot pull order {identifier}.", "red"))
            return None # Return None

        try:
            response = self.client.get_order_by_id(
                self.hash_value,
                identifier
            )

            if response.status_code != 200:
                logging.error(colored(f"Error fetching order {identifier}: {response.status_code}, {response.text}", "red"))
                return None
            
            return response.json()
        
        except Exception as e:
            logging.error(colored(f"Error pulling order {identifier} from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        """
        Parse a broker order representation to an order object.

        Parameters
        ----------
        response : dict
            The broker order representation, typically from API response.
        strategy_name : str
            The name of the strategy that placed the order.
        strategy_object : Strategy, optional
            The strategy object that placed the order.

        Returns
        -------
        Order
            The order object created from the broker's response, or None if parsing fails.
            
        Notes
        -----
        This method handles complex order structures including:
        - Simple orders (direct conversion to Lumibot orders)
        - OCO (One-Cancels-Other) orders with child orders
        - Other order strategies with child orders
        """
        try:
            # Check if there are child order strategies
            child_order_strategies = response.get("childOrderStrategies", None)
            
            # If there are child order strategies, process them
            if child_order_strategies is not None:
                # Create a list to hold the child order objects

                child_order_objects = []

                # Loop through the childOrderStrategies
                for child_order_strategy in child_order_strategies:
                    child_orders = self._parse_simple_order(child_order_strategy, strategy_name)
                    if child_orders:
                        child_order_objects.extend(child_orders)

                # Check if the orderStrategyType is OCO
                order_strategy_type = response.get("orderStrategyType", None)
                if order_strategy_type == "OCO" and len(child_order_objects) > 0:
                    # Set the order type to OCO
                    oco_order_type = Order.OrderType.OCO

                    # Get the asset object from the child_order_objects
                    asset = child_order_objects[0].asset

                    # Make sure this is the same asset for all the child orders
                    same_asset = True
                    for child_order_object in child_order_objects:
                        if child_order_object.asset != asset:
                            logging.error(colored("ERROR: Asset for all child orders in OCO order is not the same", "red"))
                            same_asset = False
                            break

                    if same_asset:
                        # Create an OCO order (using order_type parameter instead of deprecated type)
                        order = Order(
                            strategy=strategy_name,
                            order_type=oco_order_type,  # Use order_type instead of type
                            asset=asset,  # Include asset parameter
                        )

                        # Set the child orders for the OCO order
                        order.child_orders = child_order_objects
                        return order
                
                # If we get here and have child orders, return the first one
                if child_order_objects:
                    return child_order_objects[0]
            else:
                # Process simple order
                simple_orders = self._parse_simple_order(response, strategy_name)
                if simple_orders:
                    return simple_orders[0]  # Return the first order

            # If we couldn't parse anything, return None
            logging.warning(colored(f"Could not parse any valid orders from response", "yellow"))
            return None

        except Exception as e:
            logging.error(colored(f"Error parsing broker order: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _parse_simple_order(self, schwab_order: dict, strategy_name: str) -> List[Order]:
        """
        Parse a simple Schwab order (non-OCO) into Lumibot Order objects.
        
        Parameters
        ----------
        schwab_order : dict
            The Schwab order data.
        strategy_name : str
            The name of the strategy for which to create the order.
            
        Returns
        -------
        List[Order]
            A list of parsed order objects, or an empty list if parsing fails.
            
        Notes
        -----
        This method handles conversion of:
        - Order types (LIMIT, MARKET, STOP, etc.)
        - Order statuses (NEW, FILLED, CANCELED, etc.)
        - Asset types (STOCK, OPTION, FUTURE, etc.)
        - Order sides (BUY, SELL, BUY_TO_OPEN, etc.)
        
        It also extracts important order details such as:
        - Timestamps (entry and close)
        - Prices (limit price, stop price)
        - Order legs for multi-leg orders
        """
        try:
            # Check order entry/close times
            entered_time = datetime.strptime(schwab_order["enteredTime"], "%Y-%m-%dT%H:%M:%S%z")
            close_time = None
            if "closeTime" in schwab_order:
                close_time = datetime.strptime(schwab_order["closeTime"], "%Y-%m-%dT%H:%M:%S%z")

            # Convert to Lumibot Order type
            order_type_map = {
                "LIMIT": Order.OrderType.LIMIT,
                "MARKET": Order.OrderType.MARKET,
                "STOP": Order.OrderType.STOP,
                "STOP_LIMIT": Order.OrderType.STOP_LIMIT,
                "TRAILING_STOP": Order.OrderType.TRAIL
            }
            
            schwab_order_type = schwab_order.get("orderType", None)
            order_type = order_type_map.get(schwab_order_type)
            
            if not order_type and schwab_order_type == "NET_CREDIT":
                logging.info(colored(f"NET_CREDIT order type not supported: {schwab_order.get('orderId', '')}", "yellow"))
                return []
            elif not order_type:
                logging.error(colored(f"Unknown order type: {schwab_order_type}", "red"))
                return []

            # Convert to Lumibot status
            status_map = {
                "ACCEPTED": Order.OrderStatus.NEW,
                "PENDING_ACTIVATION": Order.OrderStatus.NEW,
                "QUEUED": Order.OrderStatus.NEW,
                "WORKING": Order.OrderStatus.NEW,
                "NEW": Order.OrderStatus.NEW,
                "REJECTED": Order.OrderStatus.ERROR,
                "PENDING_CANCEL": Order.OrderStatus.CANCELED,
                "CANCELED": Order.OrderStatus.CANCELED,
                "PENDING_REPLACE": Order.OrderStatus.CANCELED,
                "REPLACED": Order.OrderStatus.CANCELED,
                "EXPIRED": Order.OrderStatus.CANCELED,
                "FILLED": Order.OrderStatus.FILLED
            }
            
            schwab_order_status = schwab_order.get("status", None)
            status = status_map.get(schwab_order_status)
            
            if not status:
                logging.error(colored(f"Unknown order status: {schwab_order_status}", "red"))
                return []

            # Get the order id
            order_id = schwab_order.get("orderId", None)

            # Get prices
            price = schwab_order.get("price", None)
            stop_price = schwab_order.get("stopPrice", None)

            # Get the schwab legs
            schwab_legs = schwab_order.get("orderLegCollection", [])
            if not schwab_legs:
                logging.error(colored(f"No order legs found for order ID: {order_id}", "red"))
                return []

            # Process each leg as a separate order
            order_objects = []
            for schwab_leg in schwab_legs:
                # Get the asset information
                instrument = schwab_leg.get("instrument", {})
                
                # Get the symbol - prefer underlyingSymbol for options if available
                if "underlyingSymbol" in instrument:
                    symbol = instrument["underlyingSymbol"]
                else:
                    symbol = instrument.get("symbol", "")
                
                if not symbol:
                    logging.error(colored(f"No symbol found for order leg in order ID: {order_id}", "red"))
                    continue

                # Get the quantity
                quantity = schwab_leg.get("quantity", 0)
                if quantity <= 0:
                    logging.error(colored(f"Invalid quantity ({quantity}) for order ID: {order_id}", "red"))
                    continue

                # Convert order side
                side_mapping = {
                    "BUY": Order.OrderSide.BUY,
                    "SELL": Order.OrderSide.SELL,
                    "BUY_TO_COVER": Order.OrderSide.BUY_TO_COVER,
                    "SELL_SHORT": Order.OrderSide.SELL_SHORT,
                    "BUY_TO_OPEN": Order.OrderSide.BUY_TO_OPEN,
                    "BUY_TO_CLOSE": Order.OrderSide.BUY_TO_CLOSE,
                    "SELL_TO_OPEN": Order.OrderSide.SELL_TO_OPEN,
                    "SELL_TO_CLOSE": Order.OrderSide.SELL_TO_CLOSE
                }
                
                instruction = schwab_leg.get("instruction", "")
                side = side_mapping.get(instruction)
                
                if not side:
                    logging.error(colored(f"Unknown instruction: {instruction} for order ID: {order_id}", "red"))
                    continue

                # Determine asset type and create appropriate Asset object
                asset_type_map = {
                    "EQUITY": Asset.AssetType.STOCK,
                    "OPTION": Asset.AssetType.OPTION,
                    "FUTURE": Asset.AssetType.FUTURE,
                    "FOREX": Asset.AssetType.FOREX,
                    "INDEX": Asset.AssetType.INDEX
                }
                
                asset_type_str = schwab_leg.get("orderLegType", "")
                asset_type = asset_type_map.get(asset_type_str)
                
                if not asset_type:
                    logging.error(colored(f"Unknown asset type: {asset_type_str} for order ID: {order_id}", "red"))
                    continue

                # Create appropriate Asset object based on type
                asset = None
                if asset_type == Asset.AssetType.STOCK:
                    asset = Asset(
                        symbol=symbol,
                        asset_type=asset_type,
                    )
                elif asset_type == Asset.AssetType.OPTION:
                    option_symbol = instrument.get("symbol", "")
                    option_parts = self._parse_option_symbol(option_symbol)
                    
                    if not option_parts:
                        logging.error(colored(f"Failed to parse option symbol: {option_symbol} for order ID: {order_id}", "red"))
                        continue
                        
                    asset = Asset(
                        symbol=option_parts["underlying"],
                        asset_type=asset_type,
                        expiration=option_parts["expiry_date"],
                        strike=option_parts["strike_price"],
                        right=option_parts["option_type"],
                    )
                elif asset_type == Asset.AssetType.FUTURE:
                    asset = Asset(
                        symbol=symbol,
                        asset_type=asset_type,
                    )
                else:
                    logging.warning(colored(f"Asset type {asset_type} not fully supported yet for order ID: {order_id}", "yellow"))
                    continue

                # Create order object - using order_type instead of type
                order = Order(
                    strategy=strategy_name,
                    asset=asset,
                    quantity=quantity,
                    side=side,
                    order_type=order_type,  # Changed from type to order_type
                    limit_price=price,
                    stop_price=stop_price,
                    identifier=order_id,
                )

                # Set the status and timestamps
                order.status = status
                order.created_at = entered_time
                order.updated_at = close_time if close_time else entered_time

                order_objects.append(order)

            return order_objects

        except Exception as e:
            logging.error(colored(f"Error parsing simple order: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    def _finish_initialization(self, config, data_source, account_number, hash_value):
        """
        Complete the essential initialization required to run a strategy.
        This must be called after the Schwab client and tokens are ready.
        Base class __init__ should already be called.
        """
        self.account_number = account_number
        self.hash_value = hash_value

        # Only create stream client if client exists
        if self.client:
            try:
                self.stream_client = StreamClient(self.client, account_id=account_number)
            except Exception as e:
                logging.error(colored(f"Failed to create Schwab StreamClient: {e}", "red"))
                self.stream_client = None
                self.schwab_authorization_error = True # Indicate potential issue

        # --- Removed schwab_token_status.json creation ---

        # === Configure Data Source Client ===
        # The data_source passed here is the one set in self.data_source by super().__init__
        # self.data_source should already be the correct instance (either passed in or created in __init__)
        if self._is_schwab_data_intended and isinstance(self.data_source, SchwabData):
            if self.client:
                # Set the client on the existing SchwabData instance
                if not hasattr(self.data_source, 'client') or self.data_source.client is None:
                    self.data_source.set_client(self.client)
                    logging.debug("[Schwab] Client set on the existing SchwabData instance.")
                else:
                    # This case might happen if SchwabData was passed in already configured
                    logging.debug("[Schwab] Client seems already set on the SchwabData instance.")
            else:
                # This case indicates a problem earlier in initialization
                logging.error(colored("[Schwab] Cannot assign client to SchwabData source because broker client is missing.", "red"))
                self.schwab_authorization_error = True # Indicate potential issue
        elif not self._is_schwab_data_intended:
            # If a non-SchwabData source was intended and provided, no client assignment is needed here.
            logging.debug(f"[Schwab] Using non-SchwabData source: {type(self.data_source).__name__}. No client assignment needed here.")
        else:
            # This case should ideally not happen if __init__ logic is correct
            logging.warning(f"[Schwab] SchwabData was intended, but self.data_source is type {type(self.data_source).__name__}. Cannot set client.")
            self.schwab_authorization_error = True # Indicate potential mismatch
        # === End Configure Data Source Client ===


        # Only launch stream if stream_client was created
        if self.stream_client:
            self.stream = self._get_stream_object()
            self._launch_stream()
        else:
            logging.warning(colored("[Schwab] Stream not launched because StreamClient failed to initialize.", "yellow"))
            self.stream = None # Ensure stream is None if not launched

    # Unimplemented methods with stubs
    def _get_stream_object(self):
        """Get the broker stream connection"""
        stream = PollingStream(5.0)  # 5 seconds polling interval
        return stream

    def _register_stream_events(self):
        """Register callbacks for broker stream events"""
        broker = self

        @broker.stream.add_action(broker.POLL_EVENT)
        def on_trade_event_poll():
            # Implement polling similar to tradier.py without referencing _orders
            try:
                # Track the last time we synced positions to avoid doing it too frequently
                current_time = datetime.now()
                if not hasattr(broker, '_last_position_sync_time') or (current_time - broker._last_position_sync_time).total_seconds() > 30:
                    # Only sync positions every 30 seconds to avoid duplication
                    broker.sync_positions(None)
                    broker._last_position_sync_time = current_time
                
                # Always check for new orders
                orders = broker._pull_broker_all_orders()
                for order_data in orders:
                    order = broker._parse_broker_order(order_data, broker._strategy_name)
                    if order:
                        # Process each new order without checking against a nonexistent _orders attribute
                        broker._process_new_order(order)
            except Exception as e:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
            logging.info(f"Processing action for filled order {order} | {price} | {filled_quantity}")
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
                return True
            except Exception:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event_cancel(order):
            logging.info(f"Processing action for cancelled order {order}")
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_trade_event_error(order, error_msg):
            logging.error(f"Processing action for error order {order} | {error_msg}")
            try:
                if order.is_active():
                    broker._process_trade_event(order, broker.CANCELED_ORDER)
                logging.error(error_msg)
                order.set_error(error_msg)
            except Exception:
                logging.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        logging.info(colored("Starting Schwab stream...", "green"))
        try:
            # Add check to ensure self.stream is initialized
            if self.stream:
                self.stream._run()
            else:
                # Log that the stream object wasn't created, likely due to init failure
                logging.error(colored("Schwab stream object not initialized, cannot run stream.", "red"))
        except Exception as e:
            logging.error(f"Error running Schwab stream: {e}")
            logging.error(traceback.format_exc())

    def _stream_established(self):
        """
        Called when the stream is established.
        This method is required by the broker framework to indicate the stream is ready.
        """
        logging.info(colored("Schwab stream connection established", "green"))
        # Clear only the _unprocessed_orders since _orders is not defined
        for item in self._unprocessed_orders.get_list():
            self._unprocessed_orders.remove(item)
        self._initialized = True
        
        # First time initialization - sync positions
        self.sync_positions(None)

    def _submit_order(self, order: Order) -> Order:
        """
        Submit an order to the broker after necessary checks and input sanitization.
        
        Parameters
        ----------
        order : Order
            The order to submit to the broker.

        Returns
        -------
        Order
            Updated order with broker identifier filled in, or None if submission failed.
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.error(colored(f"Schwab authorization failed previously. Cannot submit order {order}.", "red"))
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg="Schwab authorization failed previously")
            return None

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot submit order {order}.", "red"))
            # Dispatch error event if possible
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg="Schwab client/hash not initialized")
            return None

        try:
            # Create tag for the order (use strategy name if tag not provided)
            tag = order.tag if order.tag else order.strategy
            
            # Replace any characters that might cause issues
            tag = re.sub(r'[^a-zA-Z0-9-]', '-', tag)
            
            # Import Schwab order templates
            try:
                from schwab.orders.equities import (
                    equity_buy_market, equity_buy_limit,
                    equity_sell_market, equity_sell_limit,
                    equity_sell_short_market, equity_sell_short_limit,
                    equity_buy_to_cover_market, equity_buy_to_cover_limit
                )
                from schwab.orders.options import (
                    option_buy_to_open_market, option_buy_to_open_limit,
                    option_sell_to_open_market, option_sell_to_open_limit,
                    option_buy_to_close_market, option_buy_to_close_limit,
                    option_sell_to_close_market, option_sell_to_close_limit,
                    OptionSymbol
                )
                from schwab.orders.common import Duration, Session, OrderType
                from schwab.orders.generic import OrderBuilder
            except ImportError:
                logging.error(colored("Failed to import Schwab order templates. Make sure the schwab-py library is installed.", "red"))
                return None
            
            # Create the appropriate order builder based on asset type and order details
            order_builder = None
            
            # Handle different order types
            if order.is_advanced_order():
                logging.error(colored(f"Advanced orders (OCO/OTO/Bracket) are not yet implemented for Schwab broker.", "red"))
                return None
                
            elif order.asset.asset_type == Asset.AssetType.STOCK:
                order_builder = self._prepare_stock_order_builder(order, equity_buy_market, equity_buy_limit, 
                                                               equity_sell_market, equity_sell_limit,
                                                               equity_sell_short_market, equity_sell_short_limit,
                                                               equity_buy_to_cover_market, equity_buy_to_cover_limit)
                
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                order_builder = self._prepare_option_order_builder(order, option_buy_to_open_market, option_buy_to_open_limit,
                                                               option_sell_to_open_market, option_sell_to_open_limit,
                                                               option_buy_to_close_market, option_buy_to_close_limit,
                                                               option_sell_to_close_market, option_sell_to_close_limit,
                                                               OptionSymbol)
                
            elif order.asset.asset_type == Asset.AssetType.FUTURE:
                order_builder = self._prepare_futures_order_builder(order, OrderBuilder)
                
            else:
                logging.error(colored(f"Asset type {order.asset.asset_type} is not supported by Schwab broker.", "red"))
                return None
            
            if not order_builder:
                logging.error(colored(f"Failed to create order builder for {order}", "red"))
                return None
            
            # Set duration and session
            try:
                tif = order.time_in_force or "day"
                if tif == "day":
                    order_builder = order_builder.set_duration(Duration.DAY)
                elif tif == "gtc":
                    order_builder = order_builder.set_duration(Duration.GOOD_TILL_CANCEL)
                elif tif == "opg":
                    order_builder = order_builder.set_duration(Duration.ON_THE_OPEN)
                elif tif == "cls":
                    order_builder = order_builder.set_duration(Duration.ON_THE_CLOSE)
                
                # Set normal session
                order_builder = order_builder.set_session(Session.NORMAL)
                    
                # Build the order spec
                order_spec = order_builder.build()
            except Exception as e:
                logging.error(colored(f"Error building order specification: {e}", "red"))
                return None
            
            # IMPORTANT: Verify that we don't have a nested 'order_spec' inside the order_spec
            # This is the key fix for the validation error
            if "order_spec" in order_spec:
                # If order_spec contains another order_spec, use the inner one
                order_spec = order_spec["order_spec"]
                
            # Log the final order request - reduce verbosity
            logging.info(colored(f"Sending order to Schwab: {order.asset.symbol, order.quantity} @ {order.limit_price or 'market'}", "cyan"))
            
            # Submit the order to Schwab
            response = self.client.place_order(self.hash_value, order_spec)
            
            # Log the response - reduce verbosity
            logging.info(colored(f"Schwab order response status: {response.status_code}", "cyan"))
                
            # If we get an error response, extract details and return
            if response.status_code >= 400:
                error_msg = f"Error submitting order: HTTP {response.status_code}"
                if hasattr(response, 'text') and response.text:
                    try:
                        error_data = json.loads(response.text)
                        if 'message' in error_data:
                            error_msg += f" - {error_data['message']}"
                    except:
                        error_msg += f" - {response.text}"
                        
                logging.error(colored(error_msg, "red"))
                
                # Dispatch error event
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
                return None
            
            # Extract order ID from response
            order_id = None
            try:
                # Use the Schwab utility function to extract order ID if available
                try:
                    from schwab.utils import Utils
                    # Create a Utils instance with required client and account_hash parameters
                    utils_instance = Utils(self.client, self.hash_value)
                    order_id = utils_instance.extract_order_id(response)
                    if order_id:
                        logging.info(colored(f"Extracted order ID using Utils.extract_order_id: {order_id}", "green"))
                except (ImportError, Exception) as e:
                    logging.warning(colored(f"Could not use Utils.extract_order_id: {e}", "yellow"))
                
                # Fallback methods if Utils.extract_order_id fails
                if not order_id and hasattr(response, 'headers') and 'Location' in response.headers:
                    location = response.headers.get('Location', '')
                    order_id = location.split('/')[-1] if '/' in location else location.strip()
                    logging.info(colored(f"Extracted order ID from Location header: {order_id}", "green"))
                        
                # If still no order ID and we have text, try to use it directly
                if not order_id and hasattr(response, 'text') and response.text and response.text.strip():
                    order_id = response.text.strip()
                    logging.info(colored(f"Using response text as order ID: {order_id}", "green"))
            except Exception as e:
                logging.error(colored(f"Error extracting order ID: {e}", "red"))
            
            if not order_id:
                logging.error(colored(f"Failed to get order ID from response", "red"))
                return None
            
            # Update the order with the identifier
            order.identifier = order_id
            order.status = Order.OrderStatus.SUBMITTED
            
            # Store the raw response data
            order_data = {"id": order_id, "status": "SUBMITTED"}
            order.update_raw(order_data)
            
            # Add to unprocessed orders and dispatch to stream
            self._unprocessed_orders.append(order)
            self.stream.dispatch(self.NEW_ORDER, order=order)
                        
            return order
            
        except Exception as e:
            error_msg = f"Error submitting order {order}: {str(e)}"
            logging.error(colored(error_msg, "red"))
            logging.error(traceback.format_exc())
            
            # Dispatch error event
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
            
            return None

    def _prepare_stock_order_builder(self, order, equity_buy_market, equity_buy_limit, 
                                   equity_sell_market, equity_sell_limit,
                                   equity_sell_short_market, equity_sell_short_limit,
                                   equity_buy_to_cover_market, equity_buy_to_cover_limit):
        """
        Prepare the order builder for stock orders using Schwab order templates.
        
        Parameters
        ----------
        order : Order
            The order to prepare the builder for
        equity_buy_market, equity_buy_limit, etc. : function
            Schwab order template functions
            
        Returns
        -------
        OrderBuilder
            The order builder object for Schwab API
        """
        
        # Get order parameters
        symbol = order.asset.symbol
        quantity = int(order.quantity)
        limit_price = order.limit_price
        if order.order_type == Order.OrderType.STOP_LIMIT:
            limit_price = order.stop_limit_price
        
        # Create the appropriate order builder based on order side and type
        order_builder = None
        
        # Market orders
        if order.order_type == Order.OrderType.MARKET:
            if order.side == Order.OrderSide.BUY:
                order_builder = equity_buy_market(symbol, quantity)
            elif order.side == Order.OrderSide.SELL:
                order_builder = equity_sell_market(symbol, quantity)
            elif order.side == Order.OrderSide.SELL_SHORT:
                order_builder = equity_sell_short_market(symbol, quantity)
            elif order.side == Order.OrderSide.BUY_TO_COVER:
                order_builder = equity_buy_to_cover_market(symbol, quantity)
        
        # Limit orders
        elif order.order_type == Order.OrderType.LIMIT:
            if order.side == Order.OrderSide.BUY:
                order_builder = equity_buy_limit(symbol, quantity, limit_price)
            elif order.side == Order.OrderSide.SELL:
                order_builder = equity_sell_limit(symbol, quantity, limit_price)
            elif order.side == Order.OrderSide.SELL_SHORT:
                order_builder = equity_sell_short_limit(symbol, quantity, limit_price)
            elif order.side == Order.OrderSide.BUY_TO_COVER:
                order_builder = equity_buy_to_cover_limit(symbol, quantity, limit_price)
        
        # Stop and stop limit orders aren't directly supported by the templates, so we need a workaround
        elif order.order_type in [Order.OrderType.STOP, Order.OrderType.STOP_LIMIT]:
            logging.warning(colored(f"Using workaround for stop/stop-limit orders with Schwab templates", "yellow"))
            
            # Start with a market or limit order based on type
            if order.order_type == Order.OrderType.STOP:
                if order.side == Order.OrderSide.BUY:
                    order_builder = equity_buy_market(symbol, quantity)
                elif order.side == Order.OrderSide.SELL:
                    order_builder = equity_sell_market(symbol, quantity)
                elif order.side == Order.OrderSide.SELL_SHORT:
                    order_builder = equity_sell_short_market(symbol, quantity)
                elif order.side == Order.OrderSide.BUY_TO_COVER:
                    order_builder = equity_buy_to_cover_market(symbol, quantity)
            else:  # STOP_LIMIT
                if order.side == Order.OrderSide.BUY:
                    order_builder = equity_buy_limit(symbol, quantity, limit_price)
                elif order.side == Order.OrderSide.SELL:
                    order_builder = equity_sell_limit(symbol, quantity, limit_price)
                elif order.side == Order.OrderSide.SELL_SHORT:
                    order_builder = equity_sell_short_limit(symbol, quantity, limit_price)
                elif order.side == Order.OrderSide.BUY_TO_COVER:
                    order_builder = equity_buy_to_cover_limit(symbol, quantity, limit_price)
            
            # Then try to add stop price to the builder object through manual modification
            if order_builder:
                try:
                    # Add stop price to the order spec - this is a hack since the templates don't support it directly
                    order_spec = order_builder.order_spec
                    if order.order_type == Order.OrderType.STOP:
                        order_spec["orderType"] = "STOP"
                    else:
                        order_spec["orderType"] = "STOP_LIMIT"
                    
                    # Add stop price
                    order_spec["stopPrice"] = str(order.stop_price)
                    
                    # Reconstruct builder with modified spec
                    order_builder._order_spec = order_spec
                except Exception as e:
                    logging.error(colored(f"Failed to modify order builder for stop/stop-limit order: {e}", "red"))
                    return None
        else:
            logging.error(colored(f"Order type {order.order_type} not supported for stocks with Schwab templates.", "red"))
            return None
        
        if not order_builder:
            logging.error(colored(f"Failed to create order builder for side: {order.side}", "red"))
            return None
                
        return order_builder

    def _prepare_option_order_builder(self, order, option_buy_to_open_market, option_buy_to_open_limit,
                                   option_sell_to_open_market, option_sell_to_open_limit,
                                   option_buy_to_close_market, option_buy_to_close_limit,
                                   option_sell_to_close_market, option_sell_to_close_limit,
                                   OptionSymbol):
        """
        Prepare the order builder for option orders using Schwab order templates.
        
        Parameters
        ----------
        order : Order
            The order to prepare the builder for
        option_buy_to_open_market, option_buy_to_open_limit, etc. : function
            Schwab option order template functions
        OptionSymbol : class
            The Schwab OptionSymbol class for constructing option symbols
            
        Returns
        -------
        OrderBuilder
            The order builder object for Schwab API
        """
        try:
            # Get order parameters
            quantity = int(order.quantity)
            limit_price = order.limit_price
            
            # Construct the option symbol in Schwab format
            # Get option data from the order's asset
            underlying_symbol = order.asset.symbol
            expiration_date = order.asset.expiration
            strike_price = order.asset.strike
            option_type = 'C' if order.asset.right == 'CALL' else 'P'
            
            # Format strike price as string with proper decimal format
            strike_price_str = f"{strike_price:.2f}"
            
            # Create option symbol using Schwab's OptionSymbol builder
            option_symbol = OptionSymbol(
                underlying_symbol, 
                expiration_date, 
                option_type, 
                strike_price_str
            ).build();
            
            logging.info(colored(f"Created option symbol: {option_symbol}", "cyan"));
            
            # Create the order builder based on order side and type
            order_builder = None;
            
            # First determine if this is an opening or closing transaction
            is_opening = False;
            if order.side in [Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.SELL_TO_OPEN]:
                is_opening = True;
            elif order.side in [Order.OrderSide.BUY_TO_CLOSE, Order.OrderSide.SELL_TO_CLOSE]:
                is_opening = False;
            elif order.side == Order.OrderSide.BUY:
                # Default to opening transaction for BUY
                is_opening = True;
            elif order.side == Order.OrderSide.SELL:
                # Default to closing transaction for SELL
                is_opening = False;
            else:
                logging.error(colored(f"Unsupported order side for options: {order.side}", "red"))
                return None;
            
            # Second, determine if this is a buy or sell action
            is_buy = False;
            if order.side in [Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.BUY_TO_CLOSE]:
                is_buy = True;
            elif order.side in [Order.OrderSide.SELL, Order.OrderSide.SELL_TO_OPEN, Order.OrderSide.SELL_TO_CLOSE]:
                is_buy = False;
            else:
                logging.error(colored(f"Unsupported order side for options: {order.side}", "red"))
                return None;
            
            # Select the appropriate template function based on side, opening/closing, and order type
            if order.order_type == Order.OrderType.MARKET:
                if is_buy and is_opening:
                    order_builder = option_buy_to_open_market(option_symbol, quantity)
                elif is_buy and not is_opening:
                    order_builder = option_buy_to_close_market(option_symbol, quantity)
                elif not is_buy and is_opening:
                    order_builder = option_sell_to_open_market(option_symbol, quantity)
                elif not is_buy and not is_opening:
                    order_builder = option_sell_to_close_market(option_symbol, quantity)
            
            elif order.order_type == Order.OrderType.LIMIT:
                if limit_price is None:
                    logging.error(colored(f"Limit price is required for limit orders", "red"))
                    return None;
                    
                if is_buy and is_opening:
                    order_builder = option_buy_to_open_limit(option_symbol, quantity, limit_price)
                elif is_buy and not is_opening:
                    order_builder = option_buy_to_close_limit(option_symbol, quantity, limit_price)
                elif not is_buy and is_opening:
                    order_builder = option_sell_to_open_limit(option_symbol, quantity, limit_price)
                elif not is_buy and not is_opening:
                    order_builder = option_sell_to_close_limit(option_symbol, quantity, limit_price)
            
            # Handle stop and stop-limit orders
            elif order.order_type in [Order.OrderType.STOP, Order.OrderType.STOP_LIMIT]:
                               # # For stop orders, we start with a market or limit order template
                if order.order_type == Order.OrderType.STOP:
                    if is_buy and is_opening:
                        order_builder = option_buy_to_open_market(option_symbol, quantity)
                    elif is_buy and not is_opening:
                        order_builder = option_buy_to_close_market(option_symbol, quantity)
                    elif not is_buy and is_opening:
                        order_builder = option_sell_to_open_market(option_symbol, quantity)
                    elif not is_buy and not is_opening:
                        order_builder = option_sell_to_close_market(option_symbol, quantity)
                else:  # STOP_LIMIT
                    if limit_price is None:
                        logging.error(colored(f"Limit price is required for stop-limit orders", "red"))
                        return None;
                        
                    if is_buy and is_opening:
                        order_builder = option_buy_to_open_limit(option_symbol, quantity, limit_price)
                    elif is_buy and not is_opening:
                        order_builder = option_buy_to_close_limit(option_symbol, quantity, limit_price)
                    elif not is_buy and is_opening:
                        order_builder = option_sell_to_open_limit(option_symbol, quantity, limit_price)
                    elif not is_buy and not is_opening:
                        order_builder = option_sell_to_close_limit(option_symbol, quantity, limit_price)
                
                # Then modify the order spec to add stop price
                if order_builder and order.stop_price is not None:
                    try:
                        # Add stop price to the order spec
                        order_spec = order_builder.order_spec
                        if order.order_type == Order.OrderType.STOP:
                            order_spec["orderType"] = "STOP"
                        else:
                            order_spec["orderType"] = "STOP_LIMIT";
                        
                        # Add stop price
                        order_spec["stopPrice"] = str(order.stop_price);
                        
                        # Reconstruct builder with modified spec
                        order_builder._order_spec = order_spec;
                    except Exception as e:
                        logging.error(colored(f"Failed to modify order builder for stop/stop-limit option order: {e}", "red"))
                        return None;
            else:
                logging.error(colored(f"Order type {order.order_type} not supported for options with Schwab templates.", "red"))
                return None;

           
                
            if not order_builder:
                logging.error(colored(f"Failed to create option order builder for side: {order.side}", "red"))
                return None;
                    
            return order_builder;
            
        except Exception as e:
            logging.error(colored(f"Error creating option order builder: {e}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _prepare_futures_order_builder(self, order, OrderBuilder):
        """
        Prepare the order builder for futures orders using Schwab generic order builder.
        
        Parameters
        ----------
        order : Order
            The order to prepare the builder for
        OrderBuilder : class
            Schwab OrderBuilder class
            
        Returns
        -------
        OrderBuilder
            The order builder object for Schwab API
        """
        from schwab.orders.common import OrderType, EquityInstruction, OrderStrategyType, Session, Duration
        
        # Get order parameters
       
        symbol = order.asset.symbol
        quantity = int(order.quantity)
        
        # Futures symbols in Schwab sometimes need special formatting
        # Most common futures symbols include a slash, e.g., "/ES" for E-mini S&P 500
        if not symbol.startswith('/') and not ':' in symbol and not '.' in symbol:
            logging.info(colored(f"Converting futures symbol from {symbol} to /{symbol}", "cyan"))
            symbol = f"/{symbol}"
        
        try:
            # Create order spec directly (without using OrderBuilder methods)
            # This ensures we have the exact structure the API expects
            order_spec = {
                "session": "NORMAL",
                "duration": "GOOD_TILL_CANCEL",
                "orderStrategyType": "SINGLE",
                "orderLegCollection": [
                    {
                        "orderLegType": "FUTURE",
                        "instruction": "BUY" if order.side in [Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.BUY_TO_COVER] else "SELL",
                        "quantity": quantity,
                        "instrument": {
                            "assetType": "FUTURE",
                            "symbol": symbol
                        }
                    }
                ]
            }
            
            # Set order type
            if order.order_type == Order.OrderType.MARKET:
                order_spec["orderType"] = "MARKET"
            elif order.order_type == Order.OrderType.LIMIT:
                order_spec["orderType"] = "LIMIT"
                order_spec["price"] = float(order.limit_price)
            elif order.order_type == Order.OrderType.STOP:
                order_spec["orderType"] = "STOP"
                order_spec["stopPrice"] = float(order.stop_price)
            elif order.order_type == Order.OrderType.STOP_LIMIT:
                order_spec["orderType"] = "STOP_LIMIT"
                order_spec["price"] = float(order.stop_limit_price)
                order_spec["stopPrice"] = float(order.stop_price)
            else:
                logging.error(colored(f"Order type {order.order_type} not supported for futures with Schwab.", "red"))
                return None
            
            # Log the manually constructed order spec for debugging
            logging.info(colored(f"Manually constructed futures order spec: {json.dumps(order_spec, indent=2)}", "cyan"))
            
            # Create a new OrderBuilder with the direct spec
            # This bypasses all the OrderBuilder methods completely
            new_builder = OrderBuilder();
            # Important: We're directly setting the order spec as the final product
            # That will be returned by build() later, not creating a nested structure
            new_builder._order_spec = order_spec;
            
            # No need to call any setter methods since we've directly set the spec
            return new_builder;
            
        except Exception as e:
            logging.error(colored(f"Error creating futures order builder: {e}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        
        Parameters
        ----------
        order : Order
            The order to modify.
        limit_price : float, optional
            The new limit price for the order.
        stop_price : float, optional
            The new stop price for the order.
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.error(colored(f"Schwab authorization failed previously. Cannot modify order {order.identifier}.", "red"))
            return

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot modify order {order.identifier}.", "red"))
            return # Return early

        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            logging.error(colored("Order identifier is not set, unable to modify order. Did you remember to submit it?", "red"))
            return
            
        try:
            # Get the original order first to use as base for modification
            original_order_data = self._pull_broker_order(order.identifier) # Already checks hash_value

            if not original_order_data:
                logging.error(colored(f"Unable to fetch original order {order.identifier} for modification", "red"))
                return
                
            # Create a new order spec based on the original order
            new_order_spec = self._prepare_replacement_order_spec(order, original_order_data, limit_price, stop_price)
            
            if not new_order_spec:
                logging.error(colored(f"Failed to create replacement order specification for {order}", "red"))
                return
                
            # Replace the order
            response = self.client.replace_order(self.hash_value, order.identifier, new_order_spec)

            # Extract new order ID from response (the replaced order will have a new ID)
            new_order_id = None
            try:
                if hasattr(response, 'headers') and 'Location' in response.headers:
                    # Extract order ID from the Location header
                    location = response.headers['Location']
                    new_order_id = location.split('/')[-1]
                elif response.status_code == 200 or response.status_code == 201:
                    # Try to extract from text if possible
                    new_order_id = response.text.strip() if response.text else None
            except Exception as e:
                logging.error(colored(f"Error extracting new order ID: {e}", "red"))
                
            if not new_order_id:
                logging.error(colored(f"Failed to get new order ID after replacement", "red"))
                return
                            
            # Update the order with the new identifier
           
            order.previous_identifiers = order.previous_identifiers or []
            order.previous_identifiers.append(order.identifier)
            order.identifier = new_order_id            
            # Update price information
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price
                
            # No need to dispatch any events as the order is still considered the same from Lumibot's perspective
                
        except Exception as e:
            logging.error(colored(f"Error modifying order {order.identifier}: {str(e)}", "red"))
            logging.error(traceback.format_exc())

    def _prepare_replacement_order_spec(self, order, original_order_data, limit_price, stop_price):
        """
        Prepare a replacement order specification for order modification.
        
        Parameters
        ----------
        order : Order
            The order to modify
        original_order_data : dict
            The original order data from the broker
        limit_price : float or None
            The new limit price, or None to keep original
        stop_price : float or None
            The new stop price, or None to keep original
            
        Returns
        -------
        dict
            The replacement order specification
        """
        # This will need to be implemented based on the actual structure of Schwab's order specs
        # For now, let's create a basic implementation
        
        # Start with tag for the order
        tag = order.tag if order.tag else order.strategy
        tag = re.sub(r'[^a-zA-Z0-9-]', '-', tag)
        
        # Use original values for prices if new ones are not provided
        final_limit_price = limit_price if limit_price is not None else order.limit_price
        final_stop_price = stop_price if stop_price is not None else order.stop_price
        
        # Create the replacement order spec based on asset type
        if order.asset.asset_type == Asset.AssetType.STOCK:
            return self._prepare_stock_order_spec(order, final_limit_price, tag)
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            return self._prepare_option_order_spec(order, final_limit_price, tag)
        else:
            logging.error(colored(f"Asset type {order.asset.asset_type} is not supported for order modification", "red"))
            return None

    def get_historical_account_value(self) -> dict:
        """
        Get the historical account value.
        
        Returns
        -------
        dict
            A dictionary containing the historical account value with keys 'hourly' and 'daily'.
        """
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {"hourly": None, "daily": None}

    def cancel_order(self, order: Order) -> None:
        """
        Cancel an order at the broker. Nothing will be done for orders that are already cancelled or filled.
        
        Parameters
        ----------
        order : Order
            The order to cancel.

        Returns
        -------
        None
        """
        # Add check for authorization error first
        if self.schwab_authorization_error:
            logging.error(colored(f"Schwab authorization failed previously. Cannot cancel order {order.identifier}.", "red"))
            return

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logging.error(colored(f"Schwab client or account hash not initialized. Cannot cancel order {order.identifier}.", "red"))
            return # Return early

        logging.error(colored(f"Method 'cancel_order' for order {order} is not yet implemented.", "red"))
        # Implementation needed: call self.client.cancel_order(self.hash_value, order.identifier)
        # Handle response and potentially dispatch CANCELED_ORDER or ERROR_ORDER events
        return None

    def _launch_stream(self):
        """Set the asynchronous actions to be executed when events are sent via socket streams"""
        self._register_stream_events()
        t = Thread(target=self._run_stream, daemon=True, name=f"broker_{self.name}_thread")
        t.start()
        # Removed blocking wait for stream connection establishment
        return

    def sync_positions(self, strategy):
        """
        Override the default sync_positions method to prevent duplicate positions.

        This method ensures that positions are properly synchronized without creating duplicates.
        """
        # Add check for initialization status
        # This check should now work reliably as schwab_authorization_error always exists
        if not hasattr(self, '_filled_positions') or self.schwab_authorization_error:
             logging.warning(colored("[Schwab] Broker not fully initialized or in error state, skipping position sync.", "yellow"))
             return

        # Get current tracked positions for this strategy
        strategy_name = strategy.name if strategy else None
        tracked = self.get_tracked_positions(strategy_name)
        tracked_dict = {}

        # Create a dict of tracked positions keyed by their unique asset identifiers
        for position in tracked:
            asset = position.asset
            key = (asset.symbol, asset.asset_type,
                  getattr(asset, 'expiration', None),
                  getattr(asset, 'strike', None),
                  getattr(asset, 'right', None))
            tracked_dict[key] = position

        # Pull fresh positions from the broker
        new_positions = self._pull_positions(strategy)
        new_dict = {}
        for position in new_positions:
            asset = position.asset
            key = (asset.symbol, asset.asset_type,
                  getattr(asset, 'expiration', None),
                  getattr(asset, 'strike', None),
                  getattr(asset, 'right', None))
            new_dict[key] = position

        # Remove positions that no longer exist
        for key, position in tracked_dict.items():
            if key not in new_dict:
                # Use the proper method to remove positions from filled_positions list
                if position in self._filled_positions.get_list():
                    self._filled_positions.remove(position)

        # Update or add positions
        for key, position in new_dict.items():
            if key in tracked_dict:
                # Update existing position
                tracked_position = tracked_dict[key]
                tracked_position.quantity = position.quantity
            else:
                # Add new position
                self._filled_positions.append(position)

        logging.debug(f"Synchronized {len(new_positions)} positions for strategy {strategy_name if strategy_name else 'None'}")
    
    def _ensure_cloud_login(self, redirect_uri: str, token_path: str):
        """Spin up a one-time /schwab-login web route if token.json is missing."""
        if os.path.exists(token_path):
            return

        app = Flask("schwab-login")

        @app.route("/schwab-login")
        def schwab_login():
            client_from_login_flow(
                api_key      = self.client.api_key,
                app_secret   = self.client.app_secret,
                callback_url = redirect_uri,
                token_path   = token_path,
                interactive  = False
            )

            return "✅ Schwab token saved. You can close this tab."

        logging.info(
            colored(f"[Schwab] First-time setup: open {redirect_uri} "
                    "in your browser, complete login, then restart the bot.", "green")
        )
        threading.Thread(
            target=lambda: app.run(host="0.0.0.0", port=8080, debug=False),
            daemon=True
        ).start()
