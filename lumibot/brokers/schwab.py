import base64
import json
import os
import re
import traceback
from datetime import datetime, timedelta
from threading import Thread
from typing import List, Optional, Union

import dotenv
from pytz import timezone

# Import Schwab specific libraries
from schwab.client import Client
from schwab.streaming import StreamClient
from termcolor import colored

from .broker import Broker, LumibotBrokerAPIError
from lumibot.data_sources import SchwabData  # Import YahooData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

# Import PollingStream class
import time
from pathlib import Path

from lumibot.tools import SchwabHelper
from lumibot.trading_builtins import PollingStream

# ---- Lumiwealth default Schwab app configuration ----
LUMI_DEFAULT_APP_KEY = "RfUVxotUc8p6CbeCwFmophgNZSat0TLv"
LUMI_DEFAULT_CALLBACK = "https://api.botspot.trade/broker_oauth/schwab"

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
    ACTIVE_SCHWAB_ORDER_STATUSES = {
        "AWAITING_PARENT_ORDER",
        "AWAITING_CONDITION",
        "AWAITING_STOP_CONDITION",
        "AWAITING_MANUAL_REVIEW",
        "ACCEPTED",
        "AWAITING_UR_OUT",
        "PENDING_ACTIVATION",
        "QUEUED",
        "WORKING",
        "NEW",
    }
    POLL_EVENT = PollingStream.POLL_EVENT

    @staticmethod
    def _mask_account_number(account_number):
        if account_number is None:
            return "<missing>"
        account_number = str(account_number)
        return f"****{account_number[-4:]}" if len(account_number) > 4 else "****"

    @staticmethod
    def _mask_hash_value(hash_value):
        if hash_value is None:
            return "<missing>"
        hash_value = str(hash_value)
        if len(hash_value) <= 10:
            return "<set>"
        return f"{hash_value[:6]}...{hash_value[-4:]}"

    @classmethod
    def _redact_schwab_payload(cls, payload):
        if isinstance(payload, dict):
            redacted = {}
            for key, value in payload.items():
                key_lower = str(key).lower()
                if key_lower in {"accountnumber", "account_number"}:
                    redacted[key] = cls._mask_account_number(value)
                elif key_lower in {"hashvalue", "hash_value", "accounthash", "account_hash"}:
                    redacted[key] = cls._mask_hash_value(value)
                else:
                    redacted[key] = cls._redact_schwab_payload(value)
            return redacted
        if isinstance(payload, list):
            return [cls._redact_schwab_payload(item) for item in payload]
        return payload

    def __init__(
            self,
            config=None,
            data_source=None,
    ):
        # === Initialize error flag very early ===
        self.schwab_authorization_error = False
        self._broker_fully_ready = False # Initialize new flag
        # === End Initialize error flag ===

        # === Prepare Data Source ===
        # Determine if SchwabData is intended or if a specific one was passed
        is_schwab_data_intended = data_source is None or isinstance(data_source, SchwabData)
        final_data_source = data_source

        if is_schwab_data_intended:
            if data_source is None:
                # Create a SchwabData instance now, client will be set later
                logger.debug("[Schwab] Creating initial SchwabData instance (client will be set later).")
                final_data_source = SchwabData()
            # If a SchwabData instance was passed, use it directly
            else:
                logger.debug("[Schwab] Using provided SchwabData instance.")
                # final_data_source is already data_source
        else:
            # If a different, non-SchwabData source was explicitly passed, use it
            logger.debug(f"[Schwab] Using explicitly provided non-SchwabData source: {type(data_source).__name__}")
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
        # self.logger = get_logger(__name__)
        self.extended_trading_minutes = 0
        # self.schwab_authorization_error = False # Moved earlier
        self.client = None
        self.hash_value = None
        self.account_number = None
        self.stream_client = None
        self.stream = None
        # Store if SchwabData was the goal for later client assignment
        self._is_schwab_data_intended = is_schwab_data_intended

        # --- Market calendar setting ---
        # StrategyExecutor relies on broker.market to decide whether trading is
        # 24/7 or should follow an exchange calendar.  Derive it from config or
        # env, else default to "NASDAQ" which is compatible with pandas-market-calendars.
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "NASDAQ"

        # Load environment variables (still useful for fallback if config is missing keys)
        dotenv.load_dotenv()
        logger.debug("==== Schwab Broker Initialization (New OAuth Flow) ====")
        config = config or {}

        # Account Number (Required) - Prioritize config, fallback to env
        account_number = config.get("SCHWAB_ACCOUNT_NUMBER") or os.environ.get("SCHWAB_ACCOUNT_NUMBER")
        if not account_number:
            # Set error flag before raising
            self.schwab_authorization_error = True
            raise ValueError("Schwab account number (SCHWAB_ACCOUNT_NUMBER) not found in config or environment variables.")
        self.account_number = str(account_number)

        # API Key (Required) - Prioritize config, fallback to env
        api_key = config.get("SCHWAB_APP_KEY") or os.environ.get("SCHWAB_APP_KEY") or LUMI_DEFAULT_APP_KEY
        if not api_key:
            self.schwab_authorization_error = True
            raise ValueError("Schwab App Key (SCHWAB_APP_KEY) not found in config or environment variables.")

        # Remove all app_secret handling
        logger.info("[Schwab] SCHWAB_APP_SECRET is no longer used by this Python client.")

        schwab_backend_redirect_uri = (
            config.get("SCHWAB_BACKEND_CALLBACK_URL")
            or os.environ.get("SCHWAB_BACKEND_CALLBACK_URL")
            or LUMI_DEFAULT_CALLBACK
        )
        if not schwab_backend_redirect_uri:
            self.schwab_authorization_error = True
            raise ValueError(
                "SCHWAB_BACKEND_CALLBACK_URL not found in config or environment variables. "
                "This URL is your backend endpoint that Schwab redirects to after user authorization "
                "(e.g., https://api.botspot.trade/broker_oauth/schwab) and is required for the new OAuth flow."
            )
        logger.info(f"[Schwab] Using SCHWAB_BACKEND_CALLBACK_URL: {schwab_backend_redirect_uri}")

        token_payload_env = config.get("SCHWAB_TOKEN") or os.environ.get("SCHWAB_TOKEN")
        logger.debug(f"account_number (final): {self._mask_account_number(self.account_number)}")
        logger.debug(f"api_key (final): {'<set>' if api_key else '<not set>'}")
        logger.debug(f"SCHWAB_BACKEND_CALLBACK_URL (final): {schwab_backend_redirect_uri}")
        logger.debug(f"SCHWAB_TOKEN (env/config): {'<set>' if token_payload_env else '<not set>'}")
        logger.debug("==== [END Schwab Broker Initialization] ====")

        # Determine where to store the Schwab token file.
        # Priority:
        #   1. Config value SCHWAB_TOKEN_PATH
        #   2. Env var  SCHWAB_TOKEN_PATH
        #   3. Fallback to the current working directory as "schwab_token.json"
        token_path_value = (
            config.get("SCHWAB_TOKEN_PATH") if config else None
        ) or os.environ.get("SCHWAB_TOKEN_PATH")

        if token_path_value:
            token_path = Path(token_path_value).expanduser().resolve()
        else:
            token_path = Path.cwd() / "schwab_token.json"

        # Ensure the directory exists (especially if a custom path was provided)
        try:
            token_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as _mkdir_e:
            logger.warning(f"[Schwab] Could not create token directory {token_path.parent}: {_mkdir_e}")

        token_available_and_valid = False

        if token_payload_env:
            logger.info("[Schwab] SCHWAB_TOKEN environment variable found. Processing it.")
            try:
                SchwabHelper._save_payload_str_to_token_file(token_payload_env, token_path)
                if SchwabHelper._is_token_valid_for_schwab_py(token_path):
                    SchwabHelper._ensure_token_metadata(token_path)
                    if SchwabHelper._is_token_valid_for_schwab_py(token_path):
                        token_available_and_valid = True
                        logger.info(f"[Schwab] Token from SCHWAB_TOKEN env var processed, validated, and saved to {token_path}")
                    else:
                        logger.error(f"[Schwab] Token from SCHWAB_TOKEN env var became invalid after SchwabHelper._ensure_token_metadata. Deleting {token_path}.")
                        token_path.unlink(missing_ok=True)
                else:
                    logger.error(f"[Schwab] Token from SCHWAB_TOKEN env var resulted in an invalid token file at {token_path}. Deleting.")
                    token_path.unlink(missing_ok=True)
            except Exception as e:
                logger.error(f"[Schwab] Error processing SCHWAB_TOKEN: {e}")
                if token_path.exists(): token_path.unlink(missing_ok=True)

        if not token_available_and_valid and token_path.exists() and token_path.stat().st_size > 0:
            logger.info(f"[Schwab] Existing token file found at {token_path}. Validating...")
            try:
                SchwabHelper._ensure_token_metadata(token_path)
                if SchwabHelper._is_token_valid_for_schwab_py(token_path):
                    token_available_and_valid = True
                    logger.info(f"[Schwab] Existing token file {token_path} is valid after metadata check.")
                else:
                    logger.warning(f"[Schwab] Existing token file {token_path} is invalid after checks. Deleting.")
                    token_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"[Schwab] Error validating/fixing existing token file {token_path}: {e}. Deleting.")
                if token_path.exists(): token_path.unlink(missing_ok=True)

        if not token_available_and_valid:
            logger.info("[Schwab] No valid token found. Initiating user authorization flow to obtain token payload.")
            auth_success = SchwabHelper._initiate_schwab_auth_and_get_token_payload(api_key, schwab_backend_redirect_uri, token_path)
            if not auth_success:
                self.schwab_authorization_error = True
                raise ConnectionError(
                    "Schwab token acquisition failed via user authorization flow. "
                    "Please check logs, ensure SCHWAB_APP_KEY and SCHWAB_BACKEND_CALLBACK_URL are correct, "
                    "and that the backend OAuth flow is functioning. Restart to try again."
                )
            if SchwabHelper._is_token_valid_for_schwab_py(token_path):
                SchwabHelper._ensure_token_metadata(token_path)
                if SchwabHelper._is_token_valid_for_schwab_py(token_path):
                    token_available_and_valid = True
                else:
                    logger.error(f"[Schwab] Token became invalid after SchwabHelper._ensure_token_metadata post-auth. Deleting {token_path}.")
                    if token_path.exists(): token_path.unlink(missing_ok=True)
            else:
                self.schwab_authorization_error = True
                raise ConnectionError("Schwab token file is missing or invalid even after successful authorization flow.")

        if not token_available_and_valid:
            self.schwab_authorization_error = True
            raise ConnectionError(f"Critical error: Schwab token could not be made available or validated at {token_path}.")

        try:
            logger.info(f"[Schwab] Loading token from {token_path} for manual client setup.")
            with open(token_path, encoding='utf-8') as f:
                wrapped_token_data = json.load(f)
            token_dict_for_session = wrapped_token_data.get('token')
            if not token_dict_for_session or 'access_token' not in token_dict_for_session:
                raise ValueError("Token file is missing the 'token' object or 'access_token' within it.")

            # Build an OAuth2Session that can automatically refresh the Schwab token.
            from requests_oauthlib import OAuth2Session as _OAS

            def _update_token(updated_token):
                """Write refreshed token back to token.json so it persists across restarts."""
                try:
                    wrapped = {
                        "creation_timestamp": int(time.time()),
                        "token": updated_token,
                    }
                    with open(token_path, "w", encoding="utf-8") as fp:
                        json.dump(wrapped, fp)
                    logger.info(f"[Schwab] Token automatically refreshed and written to {token_path}")
                except Exception as e_write:
                    logger.error(f"[Schwab] Failed to write refreshed token to file: {e_write}")

            # Build kwargs for token refresh – only include client_secret if it actually exists
            refresh_kwargs = {
                "client_id": api_key,
                # "grant_type": "refresh_token",  #breaks refresh in oauth2session as it creates a dupe param type error
            }
            client_secret_env = config.get("SCHWAB_APP_SECRET") or os.environ.get("SCHWAB_APP_SECRET")
            if client_secret_env:
                refresh_kwargs["client_secret"] = client_secret_env

            #add expires_at to token_dict_for_session. This is needed for the auto_refresh to work. Otherwise oauth2session always thinks it expires 30min from startup
            token_dict_for_session['expires_at'] = int(token_dict_for_session['issued_at']/1000 + (token_dict_for_session['expires_in']) - 30) #30 second buffer
            
            oauth_session = _OAS(
                client_id=api_key,
                token=token_dict_for_session,
                auto_refresh_url="https://api.schwabapi.com/v1/oauth/token",
                auto_refresh_kwargs=refresh_kwargs,
                token_updater=_update_token,
            )

            if api_key and client_secret_env:
                def _refresh_token_hook(token_url, headers, body):
                    headers['Authorization'] = f"Basic {base64.b64encode(f'{api_key}:{client_secret_env}'.encode()).decode()}"
                    logger.info(f"[Schwab] Refreshing token with auth headers")
                    return token_url, headers, body

                #create refresh hook. This is beacuse oa2session does not perform refreshes with auth headers, only with json bodies. 
                oauth_session.register_compliance_hook("refresh_token_request", _refresh_token_hook)

            # NOTE: schwab-py >=1.6 removed the app_secret parameter from the Client constructor.
            # Passing it raises: TypeError: BaseClient.__init__() got an unexpected keyword argument 'app_secret'.
            # The secret is only needed when REFRESHING a token via the auth helpers, not when we already
            # have a full token dict and build the OAuth2Session ourselves, so we can safely omit it here.
            self.client = Client(api_key=api_key, session=oauth_session)
            logger.info(f"[Schwab] Successfully initialized Schwab client from {token_path} (app_secret not used).")
            # Check if SCHWAB_APP_SECRET is available for auto-refresh warning
            app_secret_for_refresh = config.get("SCHWAB_APP_SECRET") or os.environ.get("SCHWAB_APP_SECRET")
            if not app_secret_for_refresh:
                logger.warning(
                    "[Schwab] Token auto-refresh by this client may not work as SCHWAB_APP_SECRET is not configured. "
                    "You may need to re-authenticate by providing a new SCHWAB_TOKEN or deleting token.json when the current token expires."
                )
        except Exception as e:
            logger.error(colored(f"[Schwab] Error initializing Schwab client from token file {token_path}: {e}", "red"))
            logger.error(traceback.format_exc())
            if token_path.exists():
                logger.warning(f"[Schwab] Deleting potentially corrupt token file: {token_path}")
                token_path.unlink(missing_ok=True)
            self.schwab_authorization_error = True
            raise ConnectionError(
                f"Failed to initialize Schwab client: {e}. "
                "Consider deleting token.json and clearing SCHWAB_TOKEN env var, then restarting."
            ) from e

        # -------------------------------------------------------------
        # Retrieve account hash (needed for all subsequent endpoints)
        # -------------------------------------------------------------
        try:
            resp_accounts = self.client.get_account_numbers()
            if hasattr(resp_accounts, 'status_code') and resp_accounts.status_code == 200:
                accounts_json = resp_accounts.json()
                # Find entry matching our account number; fall back to first
                target_acc = None
                for acc in accounts_json:
                    if str(acc.get('accountNumber')) == str(self.account_number):
                        target_acc = acc
                        break
                if not target_acc and accounts_json:
                    target_acc = accounts_json[0]
                    logger.warning(
                        "[Schwab] Could not match account number %s; using first account hash from API response.",
                        self._mask_account_number(self.account_number),
                    )

                if target_acc and 'hashValue' in target_acc:
                    hash_value = target_acc['hashValue']
                    logger.info(
                        "[Schwab] Retrieved account hash %s for account %s.",
                        self._mask_hash_value(hash_value),
                        self._mask_account_number(self.account_number),
                    )
                    # Complete remaining setup (stream, data source linking, etc.)
                    # Set hash_value on self before signaling readiness
                    self.hash_value = hash_value
                    self._broker_fully_ready = True # Signal readiness
                    self._finish_initialization(config, self.data_source, self.account_number, hash_value)
                else:
                    logger.error(
                        "[Schwab] Unable to locate account hash in response from get_account_numbers(). "
                        "API response: %s",
                        self._redact_schwab_payload(accounts_json),
                    )
                    self.schwab_authorization_error = True
            else:
                code = getattr(resp_accounts, 'status_code', 'n/a')
                logger.error(f"[Schwab] Failed to fetch account numbers. HTTP status {code}")
                if code == 401:
                    # Token is invalid, delete it so user will be prompted to re-authenticate
                    if token_path.exists(): token_path.unlink(missing_ok=True)
                    logger.warning(f"[Schwab] Deleted invalid token file {token_path} due to 401 error.")
                    raise ConnectionError("Schwab authentication failed (401 Unauthorized). Token deleted. Please restart to re-authenticate.")
                self.schwab_authorization_error = True
        except Exception as e_acc:
            logger.error(colored(f"[Schwab] Exception while fetching account numbers: {e_acc}", "red"))
            logger.error(traceback.format_exc())
            self.schwab_authorization_error = True

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
        if not self._broker_fully_ready:
            logger.warning(colored("[Schwab] Broker not fully ready. Cannot get balances.", "yellow"))
            return None
        if self.schwab_authorization_error:
            logger.warning(colored("Schwab authorization failed previously. Cannot get balances.", "yellow"))
            return None

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logger.error(colored("Schwab client or account hash not initialized. Cannot get balances.", "red"))
            return None

        try:
            # Get account information using the hash_value stored during initialization
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])

            if response.status_code != 200:
                logger.error(colored(f"Error getting account information: {response.status_code}", "red"))
                # Modify the error message slightly for clarity
                raise ConnectionError(
                    "Failed to get account information for hash "
                    f"{self._mask_hash_value(self.hash_value)}; HTTP status {response.status_code}"
                )

            account_data = response.json()

            # Try to use aggregated balance first if available
            if 'aggregatedBalance' in account_data:
                # Use aggregated balance data
                aggregated_balance = account_data['aggregatedBalance']
                portfolio_value = self._get_required_balance_float(aggregated_balance, 'currentLiquidationValue')

                # Get cash from securitiesAccount
                securities_account = account_data.get('securitiesAccount', {})
                balances = securities_account.get('currentBalances', {})
                cash = self._get_required_balance_float(balances, 'cashBalance')
            else:
                # Fall back to original implementation
                securities_account = account_data.get('securitiesAccount', {})
                account_type = securities_account.get('type', '')

                # Get balances based on account type
                balances = securities_account.get('currentBalances', {})
                if account_type.lower() == 'margin':
                    cash = self._get_required_balance_float(balances, 'cashBalance')
                    portfolio_value = self._get_required_balance_float(balances, 'liquidationValue', 'equity')
                else: # Assuming CASH account type or similar
                    cash = self._get_required_balance_float(balances, 'cashBalance')
                    portfolio_value = self._get_required_balance_float(balances, 'accountValue')

            if cash is None or portfolio_value is None:
                logger.warning(colored(
                    f"Schwab balance response missing required balance fields; cash={cash}, "
                    f"portfolio_value={portfolio_value}. Leaving cached balances unchanged.",
                    "yellow",
                ))
                return None

            # Calculate positions value (portfolio value minus cash)
            positions_value = portfolio_value - cash

            return cash, positions_value, portfolio_value

        except Exception as e:
            logger.error(colored(f"Error getting balances from Schwab: {str(e)}", "red"))
            logger.error(traceback.format_exc())

            return None

    @staticmethod
    def _attach_raw_broker_data(entity, raw_payload, **metadata):
        entity._raw = raw_payload
        entity.raw_broker_payload = raw_payload
        entity.broker_parse_warning = metadata.pop("broker_parse_warning", None)
        entity.broker_parse_degraded = metadata.pop("broker_parse_degraded", False)
        for key, value in metadata.items():
            setattr(entity, key, value)
        return entity

    @staticmethod
    def _coerce_float(value, *, field_name: str, context: str):
        try:
            return float(value or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid Schwab {field_name} for {context}: {value!r}") from exc

    @staticmethod
    def _get_required_balance_float(balances: dict, *keys):
        for key in keys:
            if key in balances and balances.get(key) is not None:
                return float(balances.get(key))
        return None

    def _asset_from_schwab_instrument(self, instrument: dict, raw_asset_type: str, raw_payload: dict, *, context: str):
        symbol = instrument.get("symbol") or instrument.get("underlyingSymbol") or instrument.get("cusip") or ""
        raw_asset_type = raw_asset_type or "UNKNOWN"

        if not symbol:
            return None

        if raw_asset_type == "EQUITY":
            return self._attach_raw_broker_data(
                Asset(
                    symbol=self._normalize_symbol_for_internal(symbol, asset_type=Asset.AssetType.STOCK),
                    asset_type=Asset.AssetType.STOCK,
                ),
                raw_payload,
                raw_asset_type=raw_asset_type,
            )

        if raw_asset_type == "OPTION":
            option_symbol = instrument.get("symbol")
            option_parts = SchwabHelper._parse_option_symbol(option_symbol)
            if option_parts:
                return self._attach_raw_broker_data(
                    Asset(
                        symbol=self._normalize_symbol_for_internal(
                            option_parts["underlying"], asset_type=Asset.AssetType.OPTION
                        ),
                        asset_type=Asset.AssetType.OPTION,
                        expiration=option_parts["expiry_date"],
                        strike=option_parts["strike_price"],
                        right=option_parts["option_type"],
                    ),
                    raw_payload,
                    raw_asset_type=raw_asset_type,
                )

            logger.warning(colored(
                f"Could not parse Schwab option symbol {option_symbol!r} for {context}; "
                "returning unknown asset instead.",
                "yellow",
            ))

        known_asset_types = {
            "FUTURE": Asset.AssetType.FUTURE,
            "FOREX": Asset.AssetType.FOREX,
            "INDEX": Asset.AssetType.INDEX,
            "COLLECTIVE_INVESTMENT": Asset.AssetType.STOCK,
            "ETF": Asset.AssetType.STOCK,
            "CASH_EQUIVALENT": Asset.AssetType.FOREX,
            "MONEY_MARKET_FUND": Asset.AssetType.FOREX,
            "CASH": Asset.AssetType.FOREX,
        }
        known_unsupported_asset_types = {"MUTUAL_FUND"}
        mapped_asset_type = known_asset_types.get(raw_asset_type, Asset.AssetType.UNKNOWN)
        is_known_unsupported = raw_asset_type in known_unsupported_asset_types
        if mapped_asset_type == Asset.AssetType.STOCK:
            symbol = self._normalize_symbol_for_internal(symbol, asset_type=Asset.AssetType.STOCK)

        if mapped_asset_type == Asset.AssetType.UNKNOWN and not is_known_unsupported:
            logger.warning(colored(
                f"Unknown Schwab asset type {raw_asset_type!r} for {context}; preserving broker row as unknown.",
                "yellow",
            ))

        return self._attach_raw_broker_data(
            Asset(symbol=symbol, asset_type=mapped_asset_type),
            raw_payload,
            raw_asset_type=raw_asset_type,
            broker_parse_warning=(
                f"Unknown Schwab asset type: {raw_asset_type}"
                if mapped_asset_type == Asset.AssetType.UNKNOWN and not is_known_unsupported
                else None
            ),
            broker_parse_degraded=(mapped_asset_type == Asset.AssetType.UNKNOWN and not is_known_unsupported),
        )

    # Position methods
    def _pull_positions(self, strategy: 'Strategy') -> List[Position]:
        self._last_positions_refresh_degraded = False
        # Add check for authorization error first
        if not self._broker_fully_ready:
            logger.warning(colored("[Schwab] Broker not fully ready. Cannot pull positions.", "yellow"))
            self._last_positions_refresh_degraded = True
            return []
        if self.schwab_authorization_error:
            logger.warning(colored("Schwab authorization failed previously. Cannot pull positions.", "yellow"))
            self._last_positions_refresh_degraded = True
            return []

        try:
            # Add check for valid client and hash_value
            if not self.client or not self.hash_value:
                logger.error(colored("Schwab client or account hash not initialized. Cannot pull positions.", "red"))
                self._last_positions_refresh_degraded = True
                return [] # Return empty list

            # Get account details with positions
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])

            if response.status_code != 200:
                logger.error(colored(f"Error fetching positions from Schwab: {response.status_code}", "red"))
                self._last_positions_refresh_degraded = True
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

                asset = self._asset_from_schwab_instrument(
                    instrument,
                    asset_type,
                    schwab_position,
                    context=f"position symbol {symbol or '<missing>'}",
                )
                if asset is None:
                    self._last_positions_refresh_degraded = True
                    logger.warning(colored(
                        f"Skipping Schwab position with no usable symbol/instrument identifier: {schwab_position}",
                        "yellow",
                    ))
                    continue

                # Calculate net quantity (long - short)
                long_quantity = schwab_position.get('longQuantity', 0) or 0
                short_quantity = schwab_position.get('shortQuantity', 0) or 0
                try:
                    net_quantity = (
                        self._coerce_float(long_quantity, field_name="longQuantity", context=f"position {symbol}")
                        - self._coerce_float(short_quantity, field_name="shortQuantity", context=f"position {symbol}")
                    )
                except ValueError as exc:
                    self._last_positions_refresh_degraded = True
                    logger.warning(colored(
                        f"Skipping Schwab position with invalid quantity for asset type: {asset_type}, "
                        f"symbol: {symbol}: {exc}",
                        "yellow",
                    ))
                    continue

                # Skip positions with zero quantity
                if net_quantity == 0:
                    continue

                # Extract position-specific details
                average_price = schwab_position.get('averagePrice', 0.0)
                pnl = schwab_position.get('longOpenProfitLoss') or schwab_position.get('shortOpenProfitLoss') or None
                market_value = schwab_position.get('marketValue', None)

                # Only create position object if we have a valid asset
                if asset is not None:
                    # Create a unique key for the asset to avoid duplicates
                    key = (asset.symbol, asset.asset_type,
                          getattr(asset, 'expiration', None),
                          getattr(asset, 'strike', None),
                          getattr(asset, 'right', None))

                    # If we already have this asset in our dict, update the quantity
                    if key in pos_dict:
                        existing_position = pos_dict[key]
                        existing_position.quantity += net_quantity
                        if pnl is not None:
                            existing_position.pnl = (existing_position.pnl or 0) + pnl
                        if market_value is not None:
                            existing_position.market_value += market_value
                    else:
                        # Create a new Position object
                        pos_dict[key] = Position(
                            strategy_name,
                            asset=asset,
                            quantity=net_quantity,
                            avg_fill_price=average_price,
                        )
                        self._attach_raw_broker_data(
                            pos_dict[key],
                            schwab_position,
                            raw_asset_type=asset_type,
                            broker_parse_warning=getattr(asset, "broker_parse_warning", None),
                            broker_parse_degraded=getattr(asset, "broker_parse_degraded", False),
                        )
                        
                        pos_dict[key].pnl = pnl
                        pos_dict[key].market_value = market_value

            # Log the number of positions found
            logger.debug(f"Pulled {len(pos_dict)} unique positions from Schwab")

            return list(pos_dict.values())

        except Exception as e:
            logger.error(colored(f"Error pulling positions from Schwab: {str(e)}", "red"))
            logger.error(traceback.format_exc())
            self._last_positions_refresh_degraded = True
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
            logger.warning(colored("Schwab authorization failed previously. Cannot pull position.", "yellow"))
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
        if not self._broker_fully_ready:
            logger.warning(colored("[Schwab] Broker not fully ready. Cannot pull all orders.", "yellow"))
            return []
        if self.schwab_authorization_error:
            logger.warning(colored("Schwab authorization failed previously. Cannot pull all orders.", "yellow"))
            return []

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logger.error(colored("Schwab client or account hash not initialized. Cannot pull all orders.", "red"))
            return [] # Return empty list

        try:
            # Get orders from last 7 days
            seek_start = datetime.now(timezone('UTC')) - timedelta(days=7)

            response = self.client.get_orders_for_account(
                self.hash_value,
                from_entered_datetime=seek_start
            )

            if response.status_code != 200:
                logger.error(colored(f"Error fetching orders from Schwab: {response.status_code}", "red"))
                return []

            schwab_orders = response.json()

            return schwab_orders

        except Exception as e:
            logger.error(colored(f"Error pulling orders from Schwab: {str(e)}", "red"))
            logger.error(traceback.format_exc())
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
            logger.warning(colored(f"Schwab authorization failed previously. Cannot pull order {identifier}.", "yellow"))
            return None

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logger.error(colored(f"Schwab client or account hash not initialized. Cannot pull order {identifier}.", "red"))
            return None # Return None

        try:
            response = self.client.get_order(
                identifier,
                self.hash_value,
            )

            if response.status_code != 200:
                logger.error(colored(f"Error fetching Schwab order {identifier}: {response.status_code}", "red"))
                return None

            return response.json()

        except Exception as e:
            logger.error(colored(f"Error pulling order {identifier} from Schwab: {str(e)}", "red"))
            logger.error(traceback.format_exc())
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
                skipped_child_orders = 0

                # Loop through the childOrderStrategies
                for child_order_strategy in child_order_strategies:
                    if child_order_strategy.get("childOrderStrategies") is not None:
                        child_order = self._parse_broker_order(child_order_strategy, strategy_name)
                        if child_order:
                            child_order_objects.append(child_order)
                        else:
                            skipped_child_orders += 1
                        continue

                    child_orders = self._parse_simple_order(child_order_strategy, strategy_name)
                    if child_orders:
                        child_order_objects.extend(child_orders)
                    else:
                        skipped_child_orders += 1

                # Check if the orderStrategyType is OCO
                order_strategy_type = response.get("orderStrategyType", None)
                if order_strategy_type == "OCO" and len(child_order_objects) > 0:
                    # Get the asset object from the child_order_objects
                    asset = child_order_objects[0].asset

                    # Make sure this is the same asset for all the child orders
                    same_asset = True
                    for child_order_object in child_order_objects:
                        if child_order_object.asset != asset:
                            logger.error(colored("ERROR: Asset for all child orders in OCO order is not the same", "red"))
                            same_asset = False
                            break

                    if same_asset:
                        # Create an OCO order (using order_type parameter instead of deprecated type)
                        order = Order(
                            strategy=strategy_name,
                            order_class=Order.OrderClass.OCO,
                            asset=asset,  # Include asset parameter
                        )

                        # Set the child orders for the OCO order
                        order.child_orders = child_order_objects
                        return order

                # If we get here and have child orders, return the first one
                if child_order_objects:
                    active_children = [child for child in child_order_objects if child.is_active()]
                    return active_children[0] if active_children else child_order_objects[0]
                if skipped_child_orders == len(child_order_strategies):
                    return None
            else:
                # Process simple order
                simple_orders = self._parse_simple_order(response, strategy_name)
                if simple_orders:
                    return simple_orders[0]  # Return the first order

            logger.warning(colored(
                f"Could not parse any valid Schwab orders from response for order ID: {response.get('orderId', '')}",
                "yellow",
            ))
            return None

        except Exception as e:
            logger.warning(colored(f"Skipping malformed Schwab broker order: {str(e)}", "yellow"))
            logger.debug(traceback.format_exc())
            return None

    def _schwab_order_type_to_lumibot(self, raw_order_type):
        order_type_map = {
            "LIMIT": Order.OrderType.LIMIT,
            "MARKET": Order.OrderType.MARKET,
            "STOP": Order.OrderType.STOP,
            "STOP_LIMIT": Order.OrderType.STOP_LIMIT,
            "TRAILING_STOP": Order.OrderType.TRAIL,
        }
        if raw_order_type in order_type_map:
            return order_type_map[raw_order_type]

        known_non_strategy_history_types = {"EXERCISE"}
        if raw_order_type in known_non_strategy_history_types:
            return Order.OrderType.UNKNOWN

        logger.warning(colored(
            f"Unknown Schwab order type {raw_order_type!r}; preserving broker order as unknown.",
            "yellow",
        ))
        return Order.OrderType.UNKNOWN

    def _schwab_status_to_lumibot(self, raw_status):
        status_map = {
            "ACCEPTED": Order.OrderStatus.NEW,
            "AWAITING_PARENT_ORDER": Order.OrderStatus.NEW,
            "AWAITING_CONDITION": Order.OrderStatus.NEW,
            "AWAITING_STOP_CONDITION": Order.OrderStatus.NEW,
            "AWAITING_MANUAL_REVIEW": Order.OrderStatus.NEW,
            "AWAITING_UR_OUT": Order.OrderStatus.NEW,
            "PENDING_ACTIVATION": Order.OrderStatus.NEW,
            "QUEUED": Order.OrderStatus.NEW,
            "WORKING": Order.OrderStatus.NEW,
            "NEW": Order.OrderStatus.NEW,
            "REJECTED": Order.OrderStatus.ERROR,
            "PENDING_CANCEL": Order.OrderStatus.CANCELLING,
            "CANCELED": Order.OrderStatus.CANCELED,
            "PENDING_REPLACE": Order.OrderStatus.CANCELLING,
            "REPLACED": Order.OrderStatus.CANCELED,
            "EXPIRED": Order.OrderStatus.EXPIRED,
            "FILLED": Order.OrderStatus.FILLED,
            "UNKNOWN": Order.OrderStatus.UNKNOWN,
        }
        if raw_status in status_map:
            return status_map[raw_status]

        logger.warning(colored(
            f"Unknown Schwab order status {raw_status!r}; preserving broker order with unknown status.",
            "yellow",
        ))
        return Order.OrderStatus.UNKNOWN

    def _schwab_instruction_to_side(self, instruction):
        side_mapping = {
            "BUY": Order.OrderSide.BUY,
            "SELL": Order.OrderSide.SELL,
            "BUY_TO_COVER": Order.OrderSide.BUY_TO_COVER,
            "SELL_SHORT": Order.OrderSide.SELL_SHORT,
            "BUY_TO_OPEN": Order.OrderSide.BUY_TO_OPEN,
            "BUY_TO_CLOSE": Order.OrderSide.BUY_TO_CLOSE,
            "SELL_TO_OPEN": Order.OrderSide.SELL_TO_OPEN,
            "SELL_TO_CLOSE": Order.OrderSide.SELL_TO_CLOSE,
        }
        if instruction in side_mapping:
            return side_mapping[instruction]

        logger.warning(colored(
            f"Unknown Schwab order instruction {instruction!r}; preserving broker order with unknown side.",
            "yellow",
        ))
        return Order.OrderSide.UNKNOWN

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
            schwab_order_type = schwab_order.get("orderType", None)
            order_type = self._schwab_order_type_to_lumibot(schwab_order_type)

            entered_time = None
            raw_entered_time = schwab_order.get("enteredTime")
            if raw_entered_time:
                try:
                    entered_time = datetime.strptime(raw_entered_time, "%Y-%m-%dT%H:%M:%S%z")
                except Exception as exc:
                    logger.warning(colored(
                        f"Could not parse Schwab enteredTime {raw_entered_time!r} "
                        f"for order ID: {schwab_order.get('orderId', '')}: {exc}",
                        "yellow",
                    ))

            close_time = None
            if "closeTime" in schwab_order:
                try:
                    close_time = datetime.strptime(schwab_order["closeTime"], "%Y-%m-%dT%H:%M:%S%z")
                except Exception as exc:
                    logger.warning(colored(
                        f"Could not parse Schwab closeTime {schwab_order.get('closeTime')!r} "
                        f"for order ID: {schwab_order.get('orderId', '')}: {exc}",
                        "yellow",
                    ))

            schwab_order_status = schwab_order.get("status", None)
            status = self._schwab_status_to_lumibot(schwab_order_status)

            # Get the order id
            order_id = schwab_order.get("orderId", None)
            if order_id is None:
                logger.warning(colored(f"Skipping Schwab order without orderId: {schwab_order}", "yellow"))
                return []

            # Get prices
            price = schwab_order.get("price", None)
            stop_price = schwab_order.get("stopPrice", None)

            # Get the schwab legs
            schwab_legs = schwab_order.get("orderLegCollection", [])
            if not schwab_legs:
                logger.warning(colored(f"No order legs found for Schwab order ID: {order_id}", "yellow"))
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
                    symbol = instrument.get("symbol", "") or instrument.get("cusip", "")

                if not symbol:
                    logger.warning(colored(f"No symbol found for Schwab order leg in order ID: {order_id}", "yellow"))
                    continue

                asset_type_str = schwab_leg.get("orderLegType", "")

                # Get the quantity
                quantity = schwab_leg.get("quantity", 0)
                try:
                    quantity = self._coerce_float(quantity, field_name="quantity", context=f"order {order_id}")
                except ValueError as exc:
                    logger.warning(colored(str(exc), "yellow"))
                    continue
                if quantity <= 0:
                    logger.warning(colored(f"Invalid quantity ({quantity}) for Schwab order ID: {order_id}", "yellow"))
                    continue

                instruction = schwab_leg.get("instruction", "")
                side = self._schwab_instruction_to_side(instruction)

                asset = self._asset_from_schwab_instrument(
                    instrument,
                    asset_type_str,
                    schwab_order,
                    context=f"order {order_id} leg {symbol}",
                )
                if asset is None:
                    logger.warning(colored(
                        f"No usable asset found for Schwab order leg in order ID: {order_id}",
                        "yellow",
                    ))
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
                order.broker_create_date = entered_time
                order.created_at = order.broker_create_date
                order.broker_update_date = close_time if close_time else entered_time
                self._attach_raw_broker_data(
                    order,
                    schwab_order,
                    raw_order_type=schwab_order_type,
                    raw_order_status=schwab_order_status,
                    raw_order_side=instruction,
                    raw_asset_type=asset_type_str,
                    broker_parse_warning=(
                        "Unknown Schwab order field(s)"
                        if (
                            order_type == Order.OrderType.UNKNOWN
                            or status == Order.OrderStatus.UNKNOWN
                            or side == Order.OrderSide.UNKNOWN
                            or asset.asset_type == Asset.AssetType.UNKNOWN
                        )
                        else getattr(asset, "broker_parse_warning", None)
                    ),
                    broker_parse_degraded=(
                        order_type == Order.OrderType.UNKNOWN
                        or status == Order.OrderStatus.UNKNOWN
                        or side == Order.OrderSide.UNKNOWN
                        or asset.asset_type == Asset.AssetType.UNKNOWN
                    ),
                )

                order_objects.append(order)

            return order_objects

        except Exception as e:
            logger.warning(colored(f"Skipping malformed Schwab simple order: {str(e)}", "yellow"))
            logger.debug(traceback.format_exc())
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
                logger.error(colored(f"Failed to create Schwab StreamClient: {e}", "red"))
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
                    logger.debug("[Schwab] Client set on the existing SchwabData instance.")
                else:
                    # This case might happen if SchwabData was passed in already configured
                    logger.debug("[Schwab] Client seems already set on the SchwabData instance.")
            else:
                # This case indicates a problem earlier in initialization
                logger.error(colored("[Schwab] Cannot assign client to SchwabData source because broker client is missing.", "red"))
                self.schwab_authorization_error = True # Indicate potential issue
        elif not self._is_schwab_data_intended:
            # If a non-SchwabData source was intended and provided, no client assignment is needed here.
            logger.debug(f"[Schwab] Using non-SchwabData source: {type(self.data_source).__name__}. No client assignment needed here.")
        else:
            # This case should ideally not happen if __init__ logic is correct
            logger.warning(f"[Schwab] SchwabData was intended, but self.data_source is type {type(self.data_source).__name__}. Cannot set client.")
            self.schwab_authorization_error = True # Indicate potential mismatch
        # === End Configure Data Source Client ===


        # Only launch stream if stream_client was created
        if self.stream_client:
            self.stream = self._get_stream_object()
            self._launch_stream()
        else:
            logger.warning(colored("[Schwab] Stream not launched because StreamClient failed to initialize.", "yellow"))
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
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
            logger.info(f"Processing action for filled order {order} | {price} | {filled_quantity}")
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
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event_cancel(order):
            logger.info(f"Processing action for cancelled order {order}")
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_trade_event_error(order, error_msg):
            logger.error(f"Processing action for error order {order} | {error_msg}")
            try:
                if order.is_active():
                    broker._process_trade_event(order, broker.CANCELED_ORDER)
                logger.error(error_msg)
                order.set_error(error_msg)
            except Exception:
                logger.error(traceback.format_exc())

    def _run_stream(self):
        stream = getattr(self, "stream", None)
        if not stream:
            logger.warning(colored("Schwab stream object not initialized, skipping stream runner.", "yellow"))
            return
        self._stream_established()
        try:
            logger.info(colored("Starting Schwab stream...", "green"))
            stream._run()
        except Exception as e:
            logger.error(f"Error running Schwab stream: {e}")
            logger.error(traceback.format_exc())

    def _stream_established(self):
        """
        Called when the stream is established.
        This method is required by the broker framework to indicate the stream is ready.
        """
        logger.info(colored("Schwab stream connection established", "green"))
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
            logger.error(colored(f"Schwab authorization failed previously. Cannot submit order {order}.", "red"))
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg="Schwab authorization failed previously")
            return None

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logger.error(colored(f"Schwab client or account hash not initialized. Cannot submit order {order}.", "red"))
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
                from schwab.orders.common import Duration, OrderType, Session
                from schwab.orders.equities import (
                    equity_buy_limit,
                    equity_buy_market,
                    equity_buy_to_cover_limit,
                    equity_buy_to_cover_market,
                    equity_sell_limit,
                    equity_sell_market,
                    equity_sell_short_limit,
                    equity_sell_short_market,
                )
                from schwab.orders.generic import OrderBuilder
                from schwab.orders.options import (
                    OptionSymbol,
                    option_buy_to_close_limit,
                    option_buy_to_close_market,
                    option_buy_to_open_limit,
                    option_buy_to_open_market,
                    option_sell_to_close_limit,
                    option_sell_to_close_market,
                    option_sell_to_open_limit,
                    option_sell_to_open_market,
                )
            except ImportError:
                logger.error(colored("Failed to import Schwab order templates. Make sure the schwab-py library is installed.", "red"))
                return None

            # Create the appropriate order builder based on asset type and order details
            order_builder = None

            # Handle different order types
            if order.is_advanced_order():
                logger.error(colored("Advanced orders (OCO/OTO/Bracket) are not yet implemented for Schwab broker.", "red"))
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
                logger.error(colored(f"Asset type {order.asset.asset_type} is not supported by Schwab broker.", "red"))
                return None

            if not order_builder:
                logger.error(colored(f"Failed to create order builder for {order}", "red"))
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
                logger.error(colored(f"Error building order specification: {e}", "red"))
                return None

            # IMPORTANT: Verify that we don't have a nested 'order_spec' inside the order_spec
            # This is the key fix for the validation error
            if "order_spec" in order_spec:
                # If order_spec contains another order_spec, use the inner one
                order_spec = order_spec["order_spec"]

            # Log the final order request - reduce verbosity
            logger.info(colored(f"Sending order to Schwab: {order.asset.symbol, order.quantity} @ {order.limit_price or 'market'}", "cyan"))

            # Submit the order to Schwab
            response = self.client.place_order(self.hash_value, order_spec)

            # Log the response - reduce verbosity
            logger.info(colored(f"Schwab order response status: {response.status_code}", "cyan"))

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

                logger.error(colored(error_msg, "red"))

                # Dispatch error event
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
                return None

            # Extract order ID from response
            order_id = None
            try:
                # Use the Schwab utility function to extract order ID if available
                #this fails because we are using Oauth2Session which uses Requests which uses "ok", whereas schwab-py uses HTTPX which has (undocumented) "is_error" to check the response. 
                # try:
                #     from schwab.utils import Utils
                #     # Create a Utils instance with required client and account_hash parameters
                #     utils_instance = Utils(self.client, self.hash_value)
                #     order_id = utils_instance.extract_order_id(response)
                #     if order_id:
                #         logger.info(colored(f"Extracted order ID using Utils.extract_order_id: {order_id}", "green"))
                # except (ImportError, Exception) as e:
                #     logger.warning(colored(f"Could not use Utils.extract_order_id: {e}", "yellow"))

                # Fallback methods if Utils.extract_order_id fails
                if not order_id and hasattr(response, 'headers') and 'Location' in response.headers:
                    location = response.headers.get('Location', '')
                    order_id = location.split('/')[-1] if '/' in location else location.strip()
                    logger.info(colored(f"Extracted order ID from Location header: {order_id}", "green"))

                # If still no order ID and we have text, try to use it directly
                if not order_id and hasattr(response, 'text') and response.text and response.text.strip():
                    order_id = response.text.strip()
                    logger.info(colored(f"Using response text as order ID: {order_id}", "green"))
            except Exception as e:
                logger.error(colored(f"Error extracting order ID: {e}", "red"))

            if not order_id:
                logger.error(colored("Failed to get order ID from response", "red"))
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
            logger.error(colored(error_msg, "red"))
            logger.error(traceback.format_exc())

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
        symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type)
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
            logger.warning(colored("Using workaround for stop/stop-limit orders with Schwab templates", "yellow"))

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
                    logger.error(colored(f"Failed to modify order builder for stop/stop-limit order: {e}", "red"))
                    return None
        else:
            logger.error(colored(f"Order type {order.order_type} not supported for stocks with Schwab templates.", "red"))
            return None

        if not order_builder:
            logger.error(colored(f"Failed to create order builder for side: {order.side}", "red"))
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
            underlying_symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type)
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
            ).build()

            logger.info(colored(f"Created option symbol: {option_symbol}", "cyan"))

            # Create the order builder based on order side and type
            order_builder = None

            # First determine if this is an opening or closing transaction
            is_opening = False
            if order.side in [Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.SELL_TO_OPEN]:
                is_opening = True
            elif order.side in [Order.OrderSide.BUY_TO_CLOSE, Order.OrderSide.SELL_TO_CLOSE]:
                is_opening = False
            elif order.side == Order.OrderSide.BUY:
                # Default to opening transaction for BUY
                is_opening = True
            elif order.side == Order.OrderSide.SELL:
                # Default to closing transaction for SELL
                is_opening = False
            else:
                logger.error(colored(f"Unsupported order side for options: {order.side}", "red"))
                return None

            # Second, determine if this is a buy or sell action
            is_buy = False
            if order.side in [Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.BUY_TO_CLOSE]:
                is_buy = True
            elif order.side in [Order.OrderSide.SELL, Order.OrderSide.SELL_TO_OPEN, Order.OrderSide.SELL_TO_CLOSE]:
                is_buy = False
            else:
                logger.error(colored(f"Unsupported order side for options: {order.side}", "red"))
                return None

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
                    logger.error(colored("Limit price is required for limit orders", "red"))
                    return None

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
                        logger.error(colored("Limit price is required for stop-limit orders", "red"))
                        return None

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
                            order_spec["orderType"] = "STOP_LIMIT"

                        # Add stop price
                        order_spec["stopPrice"] = str(order.stop_price)

                        # Reconstruct builder with modified spec
                        order_builder._order_spec = order_spec
                    except Exception as e:
                        logger.error(colored(f"Failed to modify order builder for stop/stop-limit option order: {e}", "red"))
                        return None
            else:
                logger.error(colored(f"Order type {order.order_type} not supported for options with Schwab templates.", "red"))
                return None



            if not order_builder:
                logger.error(colored(f"Failed to create option order builder for side: {order.side}", "red"))
                return None

            return order_builder

        except Exception as e:
            logger.error(colored(f"Error creating option order builder: {e}", "red"))
            logger.error(traceback.format_exc())
            return None

    def _build_order_spec_from_builder(self, order_builder, time_in_force=None):
        """Apply Schwab defaults and return the final API order spec."""
        if not order_builder:
            return None

        try:
            from schwab.orders.common import Duration, Session
        except ImportError:
            logger.error(colored("Failed to import Schwab order enums. Make sure the schwab-py library is installed.", "red"))
            return None

        try:
            tif = time_in_force or "day"
            if tif == "day":
                order_builder = order_builder.set_duration(Duration.DAY)
            elif tif == "gtc":
                order_builder = order_builder.set_duration(Duration.GOOD_TILL_CANCEL)
            elif tif == "opg":
                order_builder = order_builder.set_duration(Duration.ON_THE_OPEN)
            elif tif == "cls":
                order_builder = order_builder.set_duration(Duration.ON_THE_CLOSE)

            order_builder = order_builder.set_session(Session.NORMAL)
            order_spec = order_builder.build()

            if "order_spec" in order_spec:
                order_spec = order_spec["order_spec"]

            return order_spec
        except Exception as e:
            logger.error(colored(f"Error building Schwab order specification: {e}", "red"))
            logger.error(traceback.format_exc())
            return None

    def _prepare_stock_order_spec(self, order, limit_price=None, stop_price=None, tag=None):
        """
        Prepare a Schwab replacement order spec for a stock order.

        Schwab modifies orders by replacing them, so this reuses the same
        builder path used for new stock submissions and only temporarily applies
        the requested replacement prices while building the spec.
        """
        try:
            from schwab.orders.equities import (
                equity_buy_limit,
                equity_buy_market,
                equity_buy_to_cover_limit,
                equity_buy_to_cover_market,
                equity_sell_limit,
                equity_sell_market,
                equity_sell_short_limit,
                equity_sell_short_market,
            )
        except ImportError:
            logger.error(colored("Failed to import Schwab stock order templates. Make sure the schwab-py library is installed.", "red"))
            return None

        original_limit_price = order.limit_price
        original_stop_price = order.stop_price
        original_tag = order.tag

        try:
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price
            if tag is not None:
                order.tag = tag

            order_builder = self._prepare_stock_order_builder(
                order,
                equity_buy_market,
                equity_buy_limit,
                equity_sell_market,
                equity_sell_limit,
                equity_sell_short_market,
                equity_sell_short_limit,
                equity_buy_to_cover_market,
                equity_buy_to_cover_limit,
            )
            return self._build_order_spec_from_builder(order_builder, order.time_in_force)
        finally:
            order.limit_price = original_limit_price
            order.stop_price = original_stop_price
            order.tag = original_tag

    def _prepare_option_order_spec(self, order, limit_price=None, stop_price=None, tag=None):
        """
        Prepare a Schwab replacement order spec for an option order.

        This is the replacement-side equivalent of `_prepare_option_order_builder`.
        It intentionally uses the same Schwab option templates as new submits so
        option modify/replace does not drift from normal option order creation.
        """
        try:
            from schwab.orders.options import (
                OptionSymbol,
                option_buy_to_close_limit,
                option_buy_to_close_market,
                option_buy_to_open_limit,
                option_buy_to_open_market,
                option_sell_to_close_limit,
                option_sell_to_close_market,
                option_sell_to_open_limit,
                option_sell_to_open_market,
            )
        except ImportError:
            logger.error(colored("Failed to import Schwab option order templates. Make sure the schwab-py library is installed.", "red"))
            return None

        original_limit_price = order.limit_price
        original_stop_price = order.stop_price
        original_tag = order.tag

        try:
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price
            if tag is not None:
                order.tag = tag

            order_builder = self._prepare_option_order_builder(
                order,
                option_buy_to_open_market,
                option_buy_to_open_limit,
                option_sell_to_open_market,
                option_sell_to_open_limit,
                option_buy_to_close_market,
                option_buy_to_close_limit,
                option_sell_to_close_market,
                option_sell_to_close_limit,
                OptionSymbol,
            )
            return self._build_order_spec_from_builder(order_builder, order.time_in_force)
        finally:
            order.limit_price = original_limit_price
            order.stop_price = original_stop_price
            order.tag = original_tag

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

        # Get order parameters

        symbol = order.asset.symbol
        quantity = int(order.quantity)

        # Futures symbols in Schwab sometimes need special formatting
        # Most common futures symbols include a slash, e.g., "/ES" for E-mini S&P 500
        if not symbol.startswith('/') and ':' not in symbol and '.' not in symbol:
            logger.info(colored(f"Converting futures symbol from {symbol} to /{symbol}", "cyan"))
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
                logger.error(colored(f"Order type {order.order_type} not supported for futures with Schwab.", "red"))
                return None

            # Log the manually constructed order spec for debugging
            logger.info(colored(f"Manually constructed futures order spec: {json.dumps(order_spec, indent=2)}", "cyan"))

            # Create a new OrderBuilder with the direct spec
            # This bypasses all the OrderBuilder methods completely
            new_builder = OrderBuilder()
            # Important: We're directly setting the order spec as the final product
            # That will be returned by build() later, not creating a nested structure


            new_builder._order_spec = order_spec

            # No need to call any setter methods since we've directly set the spec
            return new_builder

        except Exception as e:
            logger.error(colored(f"Error creating futures order builder: {e}", "red"))
            logger.error(traceback.format_exc())
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
            logger.error(colored(f"Schwab authorization failed previously. Cannot modify order {order.identifier}.", "red"))
            return

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            logger.error(colored(f"Schwab client or account hash not initialized. Cannot modify order {order.identifier}.", "red"))
            return # Return early

        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            logger.error(colored("Order identifier is not set, unable to modify order. Did you remember to submit it?", "red"))
            return

        try:
            # Get the original order first to use as base for modification
            original_order_data = self._pull_broker_order(order.identifier) # Already checks hash_value

            if not original_order_data:
                logger.error(colored(f"Unable to fetch original order {order.identifier} for modification", "red"))
                return

            # Create a new order spec based on the original order
            new_order_spec = self._prepare_replacement_order_spec(order, original_order_data, limit_price, stop_price)

            if not new_order_spec:
                logger.error(colored(f"Failed to create replacement order specification for {order}", "red"))
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
                logger.error(colored(f"Error extracting new order ID: {e}", "red"))

            if not new_order_id:
                logger.error(colored("Failed to get new order ID after replacement", "red"))
                return

            # Update the order with the new identifier

            order.previous_identifiers = getattr(order, "previous_identifiers", None) or []
            order.previous_identifiers.append(order.identifier)
            order.identifier = new_order_id
            # Update price information
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price

            # No need to dispatch any events as the order is still considered the same from Lumibot's perspective

        except Exception as e:
            logger.error(colored(f"Error modifying order {order.identifier}: {str(e)}", "red"))
            logger.error(traceback.format_exc())

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
            return self._prepare_stock_order_spec(order, final_limit_price, final_stop_price, tag)
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            return self._prepare_option_order_spec(order, final_limit_price, final_stop_price, tag)
        else:
            logger.error(colored(f"Asset type {order.asset.asset_type} is not supported for order modification", "red"))
            return None

    def get_historical_account_value(self) -> dict:
        """
        Get the historical account value.
        
        Returns
        -------
        dict
            A dictionary containing the historical account value with keys 'hourly' and 'daily'.
        """
        logger.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
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
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            raise ValueError("Order identifier is not set, unable to cancel order. Did you remember to submit it?")

        # Add check for authorization error first
        if self.schwab_authorization_error:
            error_msg = f"Schwab authorization failed previously. Cannot cancel order {order.identifier}."
            logger.error(colored(error_msg, "red"))
            raise LumibotBrokerAPIError(error_msg)

        # Add check for valid client and hash_value
        if not self.client or not self.hash_value:
            error_msg = f"Schwab client or account hash not initialized. Cannot cancel order {order.identifier}."
            logger.error(colored(error_msg, "red"))
            raise LumibotBrokerAPIError(error_msg)

        try:
            # schwab-py expects (order_id, account_hash) and issues
            # DELETE /trader/v1/accounts/{account_hash}/orders/{order_id}.
            response = self.client.cancel_order(order.identifier, self.hash_value)
        except Exception as exc:
            error_msg = f"Error canceling Schwab order {order.identifier}: {exc}"
            logger.error(colored(error_msg, "red"))
            raise LumibotBrokerAPIError(error_msg) from exc

        status_code = getattr(response, "status_code", None)
        if status_code is None:
            error_msg = f"Error canceling Schwab order {order.identifier}: unexpected response {type(response).__name__}"
            logger.error(colored(error_msg, "red"))
            raise LumibotBrokerAPIError(error_msg)

        if not 200 <= int(status_code) < 300:
            response_text = getattr(response, "text", "")
            error_msg = f"Error canceling Schwab order {order.identifier}: HTTP {status_code}"
            if response_text:
                error_msg += f" - {response_text}"
            logger.error(colored(error_msg, "red"))
            raise LumibotBrokerAPIError(error_msg)

        logger.info(colored(f"Schwab cancel accepted for order {order.identifier}.", "green"))

        if getattr(self, "stream", None) and hasattr(self.stream, "dispatch"):
            self.stream.dispatch(self.CANCELED_ORDER, wait_until_complete=True, order=order)
        else:
            order.status = self.CANCELED_ORDER
            order.set_canceled()
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
        if not self._broker_fully_ready: # Check new flag first
            logger.debug(colored("[Schwab] Broker not fully ready, skipping position sync.", "yellow"))
            return
        if not hasattr(self, '_filled_positions') or self.schwab_authorization_error:
             logger.warning(colored("[Schwab] Broker not fully initialized or in error state, skipping position sync.", "yellow"))
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

        if getattr(self, "_last_positions_refresh_degraded", False):
            logger.warning(colored(
                "Schwab position refresh was degraded; updating parsed positions but not removing "
                "tracked positions that were missing from the partial broker parse.",
                "yellow",
            ))
        else:
            # Remove positions that no longer exist only after a complete broker parse.
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
                self._sync_position_fields_from_broker(tracked_position, position)
            else:
                # Add new position
                self._filled_positions.append(position)

        logger.debug(f"Synchronized {len(new_positions)} positions for strategy {strategy_name if strategy_name else 'None'}")
