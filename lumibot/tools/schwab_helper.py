from __future__ import annotations

import base64
import json
import logging
import time
import traceback
import urllib.parse
import webbrowser
from pathlib import Path
from termcolor import colored
import re
from datetime import datetime

class SchwabHelper:
    @staticmethod
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
            logging.info(f"[DEBUG] Token file successfully written and wrapped by _ensure_token_metadata to {token_path}")

        except Exception as e:
            logging.error(f"[DEBUG] Error in _ensure_token_metadata: {e}")
            logging.error(traceback.format_exc())
            # If error occurs, try to delete the potentially corrupted file
            try:
                token_path.unlink(missing_ok=True)
                logging.warning(f"[DEBUG] Deleted potentially corrupted token file due to error in _ensure_token_metadata: {token_path}")
            except Exception as unlink_e:
                logging.error(f"[DEBUG] Failed to delete token file after error in _ensure_token_metadata: {unlink_e}")

    @staticmethod
    def _initiate_schwab_auth_and_get_token_payload(api_key: str, backend_callback_url: str, token_path: Path) -> bool:
        """
        Initiates the Schwab OAuth flow by opening the browser and guides the user
        to paste the token payload obtained from the external backend/frontend.
        Saves the processed payload to token_path.
        Returns True if successful, False otherwise.
        """
        auth_url_base = "https://api.schwabapi.com/v1/oauth/authorize"
        params = {
            "response_type": "code",
            "client_id": api_key,
            "redirect_uri": backend_callback_url,
            "state": "lumibot_python_client_auth"
        }
        auth_url = f"{auth_url_base}?{urllib.parse.urlencode(params)}"
        
        print(colored("Schwab Authorization Needed:", "yellow"))
        print(colored("This script will attempt to guide you through Schwab's OAuth2 flow.", "cyan"))
        print(colored("If you already have a Schwab token payload string, you can skip the interactive steps", "cyan"))
        print(colored("by setting the SCHWAB_TOKEN environment variable before running this script.", "cyan"))
        print(colored("Otherwise, please follow the steps below:", "cyan"))

        logging.info(f"Opening Schwab authorization URL in your browser: {auth_url}")
        logging.info(f"Using redirect_uri for Schwab: {backend_callback_url}")
        
        try:
            webbrowser.open(auth_url)
        except Exception as e:
            logging.error(f"Could not open browser: {e}. Please manually open the URL above.")
        
        print(colored("1. Your browser should have opened to the Schwab authorization page.", "yellow"))
        print(colored(f"   If not, please manually navigate to: {auth_url}", "yellow"))
        print(colored(f"2. After authorizing Schwab, you will be redirected to a page (e.g., on your backend at '{backend_callback_url}').", "yellow"))
        print(colored("3. Your backend will automatically exchange that code for tokens and redirect you to a \"Schwab connected\" success page.", "yellow"))
        print(colored("4. On that page, click Copy to grab the displayed code.", "yellow"))
        print(colored("   Paste that payload below (or set it as SCHWAB_TOKEN in your environment).", "yellow"))
        
        payload_str = ""
        try:
            payload_str = input(colored("5. Paste the copied payload string here and press Enter: ", "green")).strip()
        except EOFError:
            logging.error("EOFError: Cannot read input for Schwab token payload. Running in a non-interactive environment?")
            print(colored("Cannot read input for Schwab token payload. If running non-interactively, ensure the SCHWAB_TOKEN environment variable is set with the token payload.", "red"))
            return False
        except KeyboardInterrupt:
            print(colored("\nSchwab authorization cancelled by user.", "yellow"))
            return False
        if not payload_str:
            logging.error("No payload pasted. Token acquisition failed.")
            return False
        try:
            SchwabHelper._save_payload_str_to_token_file(payload_str, token_path)
            logging.info(f"Schwab token payload processed and saved to {token_path}")
            return True
        except Exception as e:
            logging.error(f"Error processing pasted payload or saving token: {e}")
            logging.error(traceback.format_exc())
            if token_path.exists():
                try:
                    token_path.unlink(missing_ok=True)
                except OSError:
                    pass
            return False

    @staticmethod
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

    @staticmethod
    def _parse_option_symbol(option_symbol):
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

    @staticmethod
    def _save_payload_str_to_token_file(payload_str: str, token_path: Path):
        """Decodes a base64url token payload string and saves it to token_path."""
        if not payload_str:
            raise ValueError("Empty payload string provided.")
        missing_padding = len(payload_str) % 4
        if missing_padding:
            payload_str += '=' * (4 - missing_padding)
        try:
            decoded_bytes = base64.urlsafe_b64decode(payload_str)
            token_data_from_payload = json.loads(decoded_bytes.decode('utf-8'))
        except Exception as e:
            raise ValueError(f"Failed to decode or parse payload string: {e}") from e
        now_ms = int(time.time() * 1000)
        defaults = {
            "issued_at": now_ms,
            "refresh_token_issued_at": now_ms,
            "expires_in": token_data_from_payload.get("expires_in", 1800),
            "refresh_token_expires_in": token_data_from_payload.get("refresh_token_expires_in", 7776000),
            "token_type": token_data_from_payload.get("token_type", "Bearer"),
            "scope": token_data_from_payload.get("scope", "api"),
        }
        final_token_data = token_data_from_payload.copy()
        for key, value in defaults.items():
            if key not in final_token_data:
                final_token_data[key] = value
        final_token_data.pop("id_token", None)
        if "access_token" not in final_token_data or "refresh_token" not in final_token_data:
            raise ValueError("Decoded payload missing 'access_token' or 'refresh_token'.")
        wrapped_token = {
            "creation_timestamp": int(time.time()),
            "token": final_token_data,
        }
        with token_path.open("w", encoding="utf-8") as fp:
            json.dump(wrapped_token, fp)
        logging.info(f"Token payload processed and saved to {token_path}")

__all__ = [
    "SchwabHelper",
]