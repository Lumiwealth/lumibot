import os
import re
import traceback
import base64
import json
import time
import threading
import datetime
from typing import Union

import pandas as pd
import requests
from lumiwealth_tradier import Tradier as _Tradier
from lumiwealth_tradier.base import TradierApiError
from lumiwealth_tradier.orders import OrderLeg
from termcolor import colored

from .broker import Broker, LumibotBrokerAPIError
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, CashEvent, Order, Position
from lumibot.tools.helpers import create_options_symbol
from lumibot.tools.lumibot_logger import get_logger
from lumibot.trading_builtins import PollingStream

logger = get_logger(__name__)


class Tradier(Broker):
    """
    Broker that connects to Tradier API to place orders and retrieve data. Tradier API only supports Order streaming
    for live accounts, paper trading accounts must use a 'polling' method to retrieve order updates. This class will
    still use a CustomStream object to process order updates (which can be confusing!), but this will more seamlessly
    match what other LumiBrokers are doing without requiring changes to the stategy_executor. This
    polling method will also work for Live accounts, so it will be used by default. However, future updates will be
    made to natively support websocket streaming for Live accounts.

    ***Note: Tradier does not support Trailing StopLoss orders.
    """

    POLL_EVENT = PollingStream.POLL_EVENT
    CASH_ACTIVITY_TYPES = (
        "ach",
        "wire",
        "dividend",
        "fee",
        "tax",
        "journal",
        "check",
        "transfer",
        "adjustment",
        "interest",
    )

    # OAuth refresh endpoint (only available for approved Tradier partner apps).
    _OAUTH_REFRESH_URL = "https://api.tradier.com/v1/oauth/refreshtoken"
    _OAUTH_REFRESH_SKEW_SECONDS = 60  # Refresh a bit early to avoid edge-of-expiry failures.

    @staticmethod
    def _decode_base64url_json(payload_str: str) -> dict:
        """Decode a base64url JSON payload (no padding required)."""
        if not payload_str:
            raise ValueError("Empty payload string provided.")
        missing_padding = len(payload_str) % 4
        if missing_padding:
            payload_str += "=" * (4 - missing_padding)
        decoded_bytes = base64.urlsafe_b64decode(payload_str)
        return json.loads(decoded_bytes.decode("utf-8"))

    @staticmethod
    def _is_auth_error(err: Exception) -> bool:
        msg = str(err or "")
        # lumiwealth_tradier raises: "Error: 401 - <body>"
        return "Error: 401" in msg or msg.strip().startswith("401")

    def _oauth_enabled(self) -> bool:
        return bool(getattr(self, "_oauth_token_payload_b64", None))

    def _oauth_token_needs_refresh(self) -> bool:
        expires_at = getattr(self, "_oauth_token_expires_at", None)
        if not expires_at:
            return False
        return time.time() >= float(expires_at) - self._OAUTH_REFRESH_SKEW_SECONDS

    def _apply_access_token(self, new_access_token: str) -> None:
        """Update access token across broker + data source Tradier clients (best-effort)."""
        if not new_access_token or not isinstance(new_access_token, str):
            return

        self._tradier_access_token = new_access_token

        def _update_client(client) -> None:
            if client is None:
                return
            try:
                client.AUTH_TOKEN = new_access_token
            except Exception:
                pass
            for attr in ("account", "orders", "market"):
                try:
                    part = getattr(client, attr, None)
                    if part is None:
                        continue
                    part.AUTH_TOKEN = new_access_token
                    headers = getattr(part, "REQUESTS_HEADERS", None)
                    if isinstance(headers, dict):
                        headers["Authorization"] = f"Bearer {new_access_token}"
                except Exception:
                    continue

        _update_client(getattr(self, "tradier", None))

        ds = getattr(self, "data_source", None)
        if ds is not None:
            try:
                ds.api_key = new_access_token
            except Exception:
                pass
            _update_client(getattr(ds, "tradier", None))

    def _refresh_oauth_token(self, *, force: bool = False) -> bool:
        """Refresh Tradier OAuth token if possible. Returns True on successful refresh."""
        if not self._oauth_enabled():
            return False
        if not force and not self._oauth_token_needs_refresh():
            return False

        lock = getattr(self, "_oauth_refresh_lock", None)
        if lock is None:
            self._oauth_refresh_lock = threading.Lock()
            lock = self._oauth_refresh_lock

        with lock:
            if not force and not self._oauth_token_needs_refresh():
                return False

            refresh_token = getattr(self, "_oauth_refresh_token", None)
            client_id = getattr(self, "_oauth_client_id", None)
            client_secret = getattr(self, "_oauth_client_secret", None)

            if not refresh_token:
                logger.warning("[Tradier] TRADIER_REFRESH_TOKEN not configured; OAuth access token may expire.")
                return False
            if not client_id or not client_secret:
                logger.warning("[Tradier] TRADIER_OAUTH_CLIENT_ID / TRADIER_OAUTH_CLIENT_SECRET not configured; cannot refresh OAuth token.")
                return False

            try:
                resp = requests.post(
                    self._OAUTH_REFRESH_URL,
                    auth=(client_id, client_secret),
                    data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                    headers={"Accept": "application/json"},
                    timeout=15,
                )
            except Exception as e:
                logger.warning(f"[Tradier] OAuth refresh request failed: {e}")
                return False

            if not resp.ok:
                logger.warning(f"[Tradier] OAuth refresh failed: {resp.status_code} - {resp.text}")
                return False

            try:
                token_json = resp.json()
            except Exception as e:
                logger.warning(f"[Tradier] OAuth refresh returned non-JSON response: {e}")
                return False

            now_ms = int(time.time() * 1000)
            if token_json.get("issued_at") is None:
                token_json["issued_at"] = now_ms

            new_access_token = token_json.get("access_token")
            if not new_access_token:
                logger.warning("[Tradier] OAuth refresh response missing access_token.")
                return False

            # Refresh token is typically stable for Tradier partner apps, but handle the case where it changes.
            new_refresh_token = token_json.get("refresh_token")
            if new_refresh_token and new_refresh_token != refresh_token:
                logger.warning("[Tradier] OAuth refresh rotated refresh_token; rotation is not persisted in env vars and may require re-linking later.")
                self._oauth_refresh_token = new_refresh_token

            expires_in = token_json.get("expires_in")
            try:
                issued_at_ms = int(token_json.get("issued_at"))
                expires_in_s = int(float(expires_in)) if expires_in is not None else None
                if expires_in_s:
                    self._oauth_token_expires_at = issued_at_ms / 1000.0 + expires_in_s
            except Exception:
                # If we can't parse expiry, keep existing best-effort expiry (or none).
                pass

            self._apply_access_token(new_access_token)
            return True

    def _install_oauth_refresh_hooks(self) -> None:
        """Wrap Tradier API calls to refresh on expiry / 401 (best-effort)."""
        if not self._oauth_enabled():
            return

        def _wrap_component(component) -> None:
            if component is None:
                return
            if getattr(component, "_lumibot_oauth_wrapped", False):
                return

            orig_request = getattr(component, "request", None)
            if not callable(orig_request):
                return

            def request_with_refresh(*args, **kwargs):
                # Proactively refresh if near expiry.
                self._refresh_oauth_token(force=False)
                try:
                    return orig_request(*args, **kwargs)
                except TradierApiError as e:
                    # Retry once on auth errors after forcing a refresh.
                    if self._is_auth_error(e) and self._refresh_oauth_token(force=True):
                        return orig_request(*args, **kwargs)
                    raise

            component.request = request_with_refresh
            component._lumibot_oauth_wrapped = True

        # Broker client
        client = getattr(self, "tradier", None)
        for attr in ("account", "orders", "market"):
            _wrap_component(getattr(client, attr, None))

        # Data source client
        ds = getattr(self, "data_source", None)
        ds_client = getattr(ds, "tradier", None) if ds is not None else None
        for attr in ("account", "orders", "market"):
            _wrap_component(getattr(ds_client, attr, None))

    def __init__(
            self,
            config=None,
            account_number=None,
            access_token=None,
            paper=None,
            connect_stream=True,
            data_source=None,
            polling_interval=5.0,

            # Need sequential order submission for Tradier becuase it is very strict that buy orders exist
            # before any stoploss/limit orders.
            max_workers=1,

            # Tradier allows SPY option trading for 15 additional min after market close
            # This will need to be set directly by the strategy
            extended_trading_minutes=0,
    ):
        # Check if the user provided both config file and keys
        if (access_token is not None or account_number is not None or paper is not None) and config is not None:
            raise Exception(
                "Please provide either a config file or access_token, account_number, and paper for Tradier. "
                "You have provided both a config file and keys so we don't know which to use."
            )

        # Check if the user has provided a config file
        if config is not None:
            # Check if the user provided all the necessary keys
            if "ACCESS_TOKEN" not in config:
                raise Exception("'ACCESS_TOKEN' not found in Tradier config")
            if "ACCOUNT_NUMBER" not in config:
                raise Exception("'ACCOUNT_NUMBER' not found in Tradier config")
            if "PAPER" not in config:
                raise Exception("'PAPER' not found in Tradier config")

            # Set the values from the config file
            access_token = config["ACCESS_TOKEN"]
            account_number = config["ACCOUNT_NUMBER"]
            paper = config["PAPER"]

        # === Optional OAuth payload support (BotSpot deploy integration) ===
        # When running in BotSpot, the runtime may receive:
        # - TRADIER_TOKEN: base64url JSON payload from the OAuth token exchange
        # - TRADIER_REFRESH_TOKEN: optional (partner apps only)
        # - TRADIER_OAUTH_CLIENT_ID / TRADIER_OAUTH_CLIENT_SECRET: required to refresh
        self._oauth_token_payload_b64 = None
        self._oauth_refresh_token = None
        self._oauth_client_id = None
        self._oauth_client_secret = None
        self._oauth_token_expires_at = None  # epoch seconds

        payload_b64 = None
        try:
            if isinstance(config, dict):
                payload_b64 = config.get("TRADIER_TOKEN") or config.get("OAUTH_PAYLOAD")
        except Exception:
            payload_b64 = None
        payload_b64 = payload_b64 or os.environ.get("TRADIER_TOKEN")

        token_json = None
        if payload_b64:
            self._oauth_token_payload_b64 = payload_b64
            try:
                token_json = self._decode_base64url_json(payload_b64)
            except Exception as e:
                logger.warning(f"[Tradier] Failed to decode TRADIER_TOKEN payload: {e}")
                token_json = None

        if token_json:
            # Prefer explicit access_token argument/config; fall back to decoded payload.
            if not access_token:
                access_token = token_json.get("access_token") or token_json.get("AUTH_TOKEN")

            self._oauth_refresh_token = os.environ.get("TRADIER_REFRESH_TOKEN") or token_json.get("refresh_token")
            self._oauth_client_id = os.environ.get("TRADIER_OAUTH_CLIENT_ID")
            self._oauth_client_secret = os.environ.get("TRADIER_OAUTH_CLIENT_SECRET")

            try:
                issued_at_ms = int(token_json.get("issued_at") or 0)
                expires_in_s = int(float(token_json.get("expires_in"))) if token_json.get("expires_in") is not None else None
                if issued_at_ms and expires_in_s:
                    self._oauth_token_expires_at = issued_at_ms / 1000.0 + expires_in_s
            except Exception:
                # No reliable expiry metadata; refresh-on-401 hook still applies.
                pass

        # Check if the user has provided the necessary keys (after OAuth extraction)
        if access_token is None or account_number is None or paper is None:
            raise Exception("Please provide a config file or access_token, account_number, and paper (or set TRADIER_TOKEN for OAuth)")

        # Set the values from the keys
        self._tradier_access_token = access_token
        self._tradier_account_number = account_number
        self._tradier_paper = paper
        self.polling_interval = polling_interval

        # If this is an OAuth token, refresh before building API clients (best-effort).
        self._refresh_oauth_token(force=False)

        # Create the Tradier object
        self.tradier = _Tradier(account_number, self._tradier_access_token, paper)

        # Check if the user has provided a data source, if not, create one
        if data_source is None:
            data_source = TradierData(
                account_number=account_number,
                access_token=self._tradier_access_token,
                paper=paper,
                max_workers=max_workers,
                delay=15 if paper else 0,
            )

        # Install request wrappers before Broker initializes streams/threads.
        self.data_source = data_source
        self._install_oauth_refresh_hooks()

        super().__init__(
            name="Tradier",
            data_source=data_source,
            config=config,
            max_workers=max_workers,
            connect_stream=connect_stream,
            extended_trading_minutes=extended_trading_minutes,
        )

        # Override default market setting for Tradier to be NYSE, but still respect config/env if set
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "NYSE"

        # Telemetry counters (best-effort; used by runtime telemetry snapshots).
        self._telemetry_polls_total = 0
        self._telemetry_events_dispatched_total = 0
        self._telemetry_orders_seen_max = 0

    def _safe_stream_dispatch(self, event, **kwargs):
        """Dispatch an event to the stream if it exists.

        Tradier can run in polling mode and/or with `connect_stream=False`. Order submission and polling must not
        crash purely because a stream is unavailable.
        """

        stream = getattr(self, "stream", None)
        if stream is None:
            return
        try:
            try:
                self._telemetry_events_dispatched_total += 1
            except Exception:
                pass
            stream.dispatch(event, **kwargs)
        except Exception:
            return

    def cancel_order(self, order: Order):
        """Cancels an order at the broker. Nothing will be done for orders that are already cancelled or filled."""
        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            raise ValueError("Order identifier is not set, unable to cancel order. Did you remember to submit it?")

        # Cancel the order
        self.tradier.orders.cancel(order.identifier)

    def _modify_order(self, order: Order,
                      limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one (Tradier limitation).
        """
        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            raise ValueError("Order identifier is not set, unable to modify order. Did you remember to submit it?")

        # Modify the order
        try:
            self.tradier.orders.modify(
                order.identifier,
                limit_price=limit_price,
                stop_price=stop_price,
            )
        except TradierApiError as e:
            raise LumibotBrokerAPIError(f"Unable to modify order at broker. {e}") from e

    def _submit_orders(self, orders, is_multileg=False, order_type=None, duration="day", price=None):
        """
        Submit multiple orders to the broker. This function will submit the orders in the order they are provided.
        If any order fails to submit, the function will stop submitting orders and return the last successful order.

        Parameters
        ----------
        orders: list[Order]
            List of orders to submit
        is_multileg: bool
            Whether the order is a multi-leg order. Default is False.
        order_type: str
            The type of multi-leg order to submit, if applicable. Valid values are ('market', 'debit', 'credit', 'even'). Default is 'market'.
        duration: str
            The duration of the order. Valid values are ('day', 'gtc', 'pre', 'post'). Default is 'day'.
        price: float
            The limit price for the order. Required for 'debit' and 'credit' order types.

        Returns
        -------
            Order
                The list of processed order objects.
        """

        # Check if order_type is set, if not, set it to 'market'
        if order_type is None:
            order_type = Order.OrderType.MARKET

        # Check if the orders are empty
        if not orders or len(orders) == 0:
            return

        # Check if it is a multi-leg order
        if is_multileg:
            tag = orders[0].tag if orders[0].tag else orders[0].strategy

            # Remove anything that's not a letter, number or "-" because Tradier doesn't accept other characters
            tag = "".join([c if c.isalnum() or c == "-" else "" for c in tag])

            # Submit the multi-leg order
            parent_order = self._submit_multileg_order(orders, order_type, duration, price, tag)
            return [parent_order]

        else:
            # Submit each order
            sub_orders = []
            for order in orders:
               sub_orders.append(self._submit_order(order))

            return sub_orders

    def _submit_multileg_order(self, orders, order_type="market", duration="day", price=None, tag=None) -> Order:
        """
        Submit a multi-leg order to Tradier. This function will submit the multi-leg order to Tradier.

        Parameters
        ----------
        orders: list[Order]
            List of orders to submit
        order_type: str
            The type of multi-leg order to submit. Valid values are ('market', 'debit', 'credit', 'even')
            Default is 'market'.
        duration: str
            The duration of the order. Valid values are ('day', 'gtc', 'pre', 'post'). Default is 'day'.
        price: float
            The limit price for the order. Required for 'debit' and 'credit' order types.
        tag: str
            The tag to associate with the order.

        Returns
        -------
            parent order of the multi-leg orders
        """

        # Check if the order type is valid
        if order_type not in ["market", "debit", "credit", "even"]:
            raise ValueError(f"Invalid order type '{order_type}' for multi-leg order.")

        # Check if the duration is valid
        if duration not in ["day", "gtc", "pre", "post"]:
            raise ValueError(f"Invalid duration {duration} for multi-leg order.")

        # Check if the price is required
        if order_type in ["debit", "credit"] and price is None:
            raise ValueError(f"Price is required for '{order_type}' order type.")

        # Check that all the order objects have the same symbol
        if len(set([order.asset.symbol for order in orders])) > 1:
            raise ValueError("All orders in a multi-leg order must have the same symbol.")

        # Use broker-native class-share notation for the underlying symbol.
        symbol = self._normalize_symbol_for_broker(
            orders[0].asset.symbol,
            asset_type=orders[0].asset.asset_type,
        )

        # Create the legs for the multi-leg order
        legs = []
        for order in orders:
            # Create the options symbol
            option_symbol = create_options_symbol(
                order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
            )

            # Example leg: leg1 = OrderLeg(option_symbol=option_symbol_1, quantity=1, side='buy_to_open')
            leg = OrderLeg(
                option_symbol=option_symbol,
                quantity=int(order.quantity), # Quantity for Tradier must be a positive integer
                side=self._lumi_side2tradier(order),
            )
            legs.append(leg)

        # Example assuming order_type and duration are required and correctly set
        order_response = self.tradier.orders.multileg_order(
            symbol=symbol,
            order_type=order_type,
            duration=duration,
            legs=legs,
            price=price,
            tag=tag,
        )

        # Each leg uses a different option asset, just use the base symbol. This matches later Tradier API response.
        parent_asset = Asset(
            symbol=self._normalize_symbol_for_internal(symbol, asset_type=Asset.AssetType.STOCK)
        )
        parent_order = Order(
            identifier=order_response["id"],
            asset=parent_asset,
            strategy=orders[0].strategy,
            order_class=Order.OrderClass.MULTILEG,
            side=orders[0].side,
            quantity=orders[0].quantity,
            order_type=orders[0].order_type,
            time_in_force=duration,
            limit_price=price,
            tag=tag,
            status=Order.OrderStatus.SUBMITTED
        )
        for o in orders:
            o.parent_identifier = parent_order.identifier

        parent_order.child_orders = orders
        parent_order.update_raw(order_response)  # This marks order as 'transmitted'
        self._unprocessed_orders.append(parent_order)
        self._safe_stream_dispatch(self.NEW_ORDER, order=parent_order)
        return parent_order

    def _submit_order(self, order: Order):
        """
        Do checking and input sanitization, then submit the order to the broker.
        Parameters
        ----------
        order: Order
            The order to submit to the broker

        Returns
        -------
            Updated order with broker identifier filled in
        """

        tag = order.tag if order.tag else order.strategy
        # Replace non-alphanumeric characters with '-', underscore "_" is not allowed by Tradier
        tag = re.sub(r'[^a-zA-Z0-9-]', '-', tag)

        order_limit_price = order.limit_price \
            if order.order_type != Order.OrderType.STOP_LIMIT else order.stop_limit_price

        try:
            # Check if the order is an OCO/OTO/Bracker order
            if order.is_advanced_order():
                # Create the legs for the Combo order. For OTO/Bracket orders, the parent (entry) order is the first
                # leg order and the children (exit) orders follow. For OCO orders, the parent is excluded from the
                # legs list because there is no entry order (i.e. it has been submitted previously).
                legs = []
                if order.order_class != Order.OrderClass.OCO:
                    # Create the stock/options symbol
                    parent_option_symbol = create_options_symbol(
                        order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
                    ) if order.asset.asset_type == Asset.AssetType.OPTION else None
                    parent_stock_symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type) \
                        if order.asset.asset_type != Asset.AssetType.OPTION else None

                    # Add the parent order to the legs list
                    legs.append(OrderLeg(
                        stock_symbol=parent_stock_symbol,  # None if option order
                        option_symbol=parent_option_symbol,  # None if stock order
                        quantity=int(order.quantity),
                        side=self._lumi_side2tradier(order),
                        price=order_limit_price,
                        stop=order.stop_price,
                        type=order.order_type,
                    ))

                for child_order in order.child_orders:
                    if child_order.asset is None:
                        logger.error(f"Asset {child_order.asset} not supported by Tradier.")
                        return None

                    # Check if the child order is a stop limit order
                    # Note: Tradier does not support Trailing Stop orders
                    child_limit_price = child_order.limit_price \
                        if child_order.order_type != Order.OrderType.STOP_LIMIT else child_order.stop_limit_price

                    # Create the stock/options symbol
                    child_option_symbol = create_options_symbol(
                        order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
                    ) if child_order.asset.asset_type == Asset.AssetType.OPTION else None
                    child_stock_symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type) \
                        if child_order.asset.asset_type != Asset.AssetType.OPTION else None

                    # Create the leg
                    leg = OrderLeg(
                        stock_symbol=child_stock_symbol,  # None if option order
                        option_symbol=child_option_symbol,  # None if stock order
                        quantity=int(child_order.quantity),
                        side=self._lumi_side2tradier(child_order),
                        price=round(child_limit_price, 2) if child_limit_price else child_limit_price,
                        stop=round(child_order.stop_price, 2) if child_order.stop_price else child_order.stop_price,
                        type=child_order.order_type,
                    )
                    legs.append(leg)

                # Place the Advanced order
                try:
                    # Tradier calls parent Bracket orders an OTOCO. OCO/OTO names still match
                    tradier_class = 'otoco' if order.order_class == Order.OrderClass.BRACKET else order.order_class
                    order_response = self.tradier.orders.advanced_order(
                        duration=order.time_in_force,
                        order_class=tradier_class,
                        legs=legs,
                        tag=tag,
                    )
                except TradierApiError as e:
                    msg = colored(f"Error submitting order {order}: {e}", color="red")
                    self._safe_stream_dispatch(self.ERROR_ORDER, order=order, error_msg=msg)
                    return None

            elif order.asset is not None and order.asset.asset_type == Asset.AssetType.STOCK:
                symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type)

                # Place the order
                order_response = self.tradier.orders.order(
                    symbol,
                    self._lumi_side2tradier(order),
                    order.quantity,
                    order_type=order.order_type,
                    duration=order.time_in_force,
                    limit_price=order_limit_price,
                    stop_price=order.stop_price,
                    tag=tag,
                )

            elif order.asset is not None and order.asset.asset_type == Asset.AssetType.OPTION:
                tradier_side = self._lumi_side2tradier(order)
                stock_symbol = self._normalize_symbol_for_broker(order.asset.symbol, asset_type=order.asset.asset_type)
                option_symbol = create_options_symbol(
                    order.asset.symbol, order.asset.expiration, order.asset.right, order.asset.strike
                )

                if not tradier_side or not option_symbol:
                    logger.error(f"Unable to parse order {order} for Tradier.")
                    return None

                order_response = self.tradier.orders.order_option(
                    stock_symbol,
                    option_symbol,
                    tradier_side,
                    order.quantity,
                    order_type=order.order_type,
                    duration=order.time_in_force,
                    limit_price=order_limit_price,
                    stop_price=order.stop_price,
                    tag=tag,
                )
            else:
                # Log the error and return None
                logger.error(f"Asset {order.asset} not supported by Tradier.")
                return None

            order.identifier = order_response["id"]
            order.status = Order.OrderStatus.SUBMITTED
            order.update_raw(order_response)  # This marks order as 'transmitted'
            self._unprocessed_orders.append(order)
            self._safe_stream_dispatch(self.NEW_ORDER, order=order)

        except TradierApiError as e:
            msg = colored(f"Error submitting order {order}: {e}", color="red")
            self._safe_stream_dispatch(self.ERROR_ORDER, order=order, error_msg=msg)

        return order

    def _get_balances_at_broker(self, quote_asset: Asset, strategy):
        try:
            df = self.tradier.account.get_account_balance()
        except TradierApiError as e:
            # Check if the error is a 401 or 403, if so, the access token is invalid
            error = str(e)
            if "401" in error or "403" in error:
                # Check if the access token or account number is invalid
                if (self._tradier_access_token is None or self._tradier_account_number is None or
                        len(self._tradier_access_token) == 0 or len(self._tradier_account_number) == 0):
                    colored_message = colored("Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are blank. "
                                              "Please check your keys.", color="red")
                    raise ValueError(colored_message) from e

                # Conceal the end of the access token
                access_token = self._tradier_access_token[:7] + "*" * 7
                colored_message = colored(f"Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are invalid. "
                                          f"Your account number is: {self._tradier_account_number} and your "
                                          f"access token is: {access_token}", color="red")
                raise ValueError(colored_message) from e
            raise e
        except Exception as e:
            logger.error(f"Error pulling balances from Tradier: {e}")
            # Add traceback to the error message
            logger.error(traceback.format_exc())
            return None

        # Get the portfolio value (total_equity) column
        portfolio_value = float(df["total_equity"].iloc[0])

        # Get the cash (total_cash) column
        cash = float(df["total_cash"].iloc[0])

        # Calculate the gross positions value
        positions_value = portfolio_value - cash

        return cash, positions_value, portfolio_value

    def get_historical_account_value(self):
        logger.error("The function get_historical_account_value is not implemented yet for Tradier.")
        return {"hourly": None, "daily": None}

    @staticmethod
    def _extract_history_amount(row: dict) -> float:
        for key in ("amount", "net_amount", "total", "cash", "value"):
            if key in row and row.get(key) not in (None, ""):
                return CashEvent.coerce_amount(row.get(key))
        return 0.0

    @staticmethod
    def _extract_history_field(row: dict, raw_type: str, field_name: str):
        nested_key = f"{raw_type}.{field_name}"
        value = row.get(nested_key)
        if value not in (None, ""):
            return value
        return row.get(field_name)

    @classmethod
    def _extract_history_description(cls, row: dict, raw_type: str) -> str | None:
        description = cls._extract_history_field(row, raw_type, "description")
        if description not in (None, ""):
            return str(description)
        symbol = cls._extract_history_field(row, raw_type, "symbol")
        if symbol not in (None, ""):
            return str(symbol)
        return None

    @classmethod
    def _extract_history_symbol(cls, row: dict, raw_type: str) -> str | None:
        symbol = cls._extract_history_field(row, raw_type, "symbol")
        if symbol in (None, ""):
            return None
        return str(symbol)

    @classmethod
    def _extract_history_quantity(cls, row: dict, raw_type: str) -> str | None:
        quantity = cls._extract_history_field(row, raw_type, "quantity")
        if quantity in (None, ""):
            return None
        return str(quantity)

    @staticmethod
    def _override_cash_event_type_from_description(description: str | None) -> str | None:
        normalized_description = str(description or "").strip().lower()
        if not normalized_description:
            return None
        if "tax" in normalized_description:
            return "tax"
        if "fee" in normalized_description:
            return "fee"
        return None

    @classmethod
    def _map_cash_event_type(cls, raw_type: str, amount: float, description: str | None = None) -> tuple[str, bool]:
        normalized_raw_type = str(raw_type or "").strip().lower()
        description_override = cls._override_cash_event_type_from_description(description)
        if description_override is not None:
            return description_override, False
        if normalized_raw_type in {"ach", "wire", "check", "transfer"}:
            return ("deposit" if amount >= 0 else "withdrawal"), True
        if normalized_raw_type == "dividend":
            return "dividend", False
        if normalized_raw_type == "interest":
            return "interest", False
        if normalized_raw_type == "fee":
            return "fee", False
        if normalized_raw_type == "tax":
            return "tax", False
        if normalized_raw_type == "journal":
            return "journal", False
        if normalized_raw_type == "adjustment":
            return "adjustment", False
        return "other_cash", False

    @classmethod
    def _normalize_history_row_to_cash_event(cls, row: dict) -> CashEvent | None:
        if not isinstance(row, dict):
            return None

        raw_type = str(row.get("type") or "").strip().lower()
        if not raw_type or raw_type in {"trade", "option"}:
            return None

        amount = cls._extract_history_amount(row)
        if raw_type in {"ach", "wire", "check", "transfer"} and amount == 0:
            return None
        description = cls._extract_history_description(row, raw_type) or raw_type
        symbol = cls._extract_history_symbol(row, raw_type)
        quantity = cls._extract_history_quantity(row, raw_type)
        event_type, is_external_cash_flow = cls._map_cash_event_type(raw_type, amount, description)
        occurred_at = (
            row.get("date")
            or row.get("created_at")
            or row.get("settlement_date")
            or row.get("transaction_date")
        )
        broker_event_id = row.get("id") or row.get("event_id") or row.get("transaction_id")

        return CashEvent(
            event_id=CashEvent.build_event_id(
                broker_name="tradier",
                broker_event_id=broker_event_id,
                raw_type=raw_type,
                raw_subtype=row.get("status"),
                occurred_at=occurred_at,
                amount=amount,
                description=description,
                extra_components=[symbol, quantity],
            ),
            broker_event_id=broker_event_id,
            broker_name="tradier",
            event_type=event_type,
            raw_type=raw_type,
            raw_subtype=row.get("status"),
            amount=amount,
            currency=row.get("currency") or "USD",
            occurred_at=occurred_at,
            description=description,
            direction=CashEvent._infer_direction(amount),
            is_external_cash_flow=is_external_cash_flow,
        )

    def get_cash_events(
        self,
        *,
        since: datetime.datetime | None = None,
        limit: int | None = 100,
    ) -> list[CashEvent]:
        start_date = CashEvent.coerce_datetime(since).date() if since is not None else None
        end_date = datetime.datetime.now(datetime.timezone.utc).date()
        per_type_limit = max(int(limit or 100), 1)
        per_page_limit = min(per_type_limit, 1000)
        max_pages = max((per_type_limit - 1) // per_page_limit + 1, 1)

        event_by_id: dict[str, CashEvent] = {}
        for activity_type in self.CASH_ACTIVITY_TYPES:
            for page in range(1, max_pages + 1):
                history_df = self.tradier.account.get_history(
                    start_date=start_date,
                    end_date=end_date,
                    limit=per_page_limit,
                    page=page,
                    activity_type=activity_type,
                )

                if history_df is None or history_df.empty:
                    break

                for row in history_df.to_dict(orient="records"):
                    event = self._normalize_history_row_to_cash_event(row)
                    if event is not None:
                        event_by_id[event.event_id] = event

                if len(history_df.index) < per_page_limit:
                    break

        normalized_events = sorted(
            event_by_id.values(),
            key=lambda event: (event.occurred_at, event.event_id),
        )
        logger.debug("Tradier returned %s normalized cash events", len(normalized_events))
        return normalized_events

    def _pull_positions(self, strategy):
        try:
            positions_df = self.tradier.account.get_positions()
        except TradierApiError as e:
            # Check if the error is a 401 or 403, if so, the access token is invalid
            error = str(e)
            if "401" in error or "403" in error:
                # Check if the access token or account number is invalid
                if self._tradier_access_token is None or self._tradier_account_number is None or len(self._tradier_access_token) == 0 or len(self._tradier_account_number) == 0:
                    colored_message = colored("Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are blank. Please check your keys.", color="red")
                    raise ValueError(colored_message) from e

                # Conceal the end of the access token
                access_token = self._tradier_access_token[:7] + "*" * 7
                colored_message = colored(f"Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are invalid. Your account number is: {self._tradier_account_number} and your access token is: {access_token}", color="red")
                raise ValueError(colored_message) from e
            raise e
        except Exception as e:
            logger.error(f"Error pulling positions from Tradier: {e}")
            return []

        positions_ret = []

        if strategy is None:
            strategy_name = "Unknown"
        elif isinstance(strategy, str):
            strategy_name = strategy
        else:
            strategy_name = getattr(strategy, "name", str(strategy))

        # Loop through each row in the dataframe
        for _, row in positions_df.iterrows():
            # Get the symbol/quantity and create the position asset
            symbol = self._normalize_symbol_for_internal(row["symbol"], asset_type=Asset.AssetType.STOCK)
            quantity = row["quantity"]
            asset = Asset.symbol2asset(symbol)  # Parse the symbol. Handles 'stock' and 'option' types

            # Create the position
            position = Position(
                strategy=strategy_name,
                asset=asset,
                quantity=quantity,
            )
            positions_ret.append(position)  # Add the position to the list

        return positions_ret

    def _pull_position(self, strategy, asset):
        """
        Pull a single position from the broker that matches the asset and strategy. If no position is found, None is
        returned.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull
        asset: Asset
            The asset to pull the position for

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None
        """
        all_positions = self._pull_positions(strategy)

        # Loop through each position and check if it matches the asset
        for position in all_positions:
            if position.asset == asset:
                # We found the position, return it
                return position

        return None

    def _parse_broker_order_dict(self, response: dict, strategy_name: str, strategy_object=None):
        """
        Parse a broker order representation to a Lumi order object or objects. Once the Lumi order has been created,
        it will be dispatched to our "stream" queue for processing until a time when Live Streaming can be implemented.

        Parameters
        ----------
        response: dict
            The output from TradierAPI call returned by pull_broker_order()
        strategy_name: str
            The name of the strategy that placed the order
        strategy_object: Strategy
            The strategy object that placed the order

        Returns
        -------
        Order
            The Lumibot order object created from the response. For multileg orders, the parent order will be returned
            with child orders internally attached.
        """
        # First try to parse the parent order
        parent_order = self._parse_broker_order(response, strategy_name, strategy_object)

        # Check if the order is a multileg order
        if "leg" in response and isinstance(response["leg"], list):
            # Reset child orders and replace them with the parsed child orders from broker
            parent_order.child_orders = []

            # Loop through each leg in the response
            for leg in response["leg"]:
                # Create the order object
                child_order = self._parse_broker_order(leg, strategy_name, strategy_object)
                child_order.parent_identifier = parent_order.identifier

                # Add the order to the list
                parent_order.add_child_order(child_order)

        return parent_order

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None):
        """
        Parse a broker order representation to a Lumi order object. Once the Lumi order has been created, it will
        be dispatched to our "stream" queue for processing until a time when Live Streaming can be implemented.

        Tradier API Documentation:
        https://documentation.tradier.com/brokerage-api/reference/response/orders

        :param response: The output from TradierAPI call returned _by pull_broker_order()
        :param strategy_name: The name of the strategy that placed the order
        :param strategy_object: The strategy object that placed the order
        """
        strategy_name = (
            strategy_name if strategy_name else strategy_object.name if strategy_object else None
        )

        # For OCO orders, tradier leaves lots of fields empty (float nan). Pull values from the children if needed
        legs = response["leg"] if "leg" in response and isinstance(response["leg"], list) else []
        limit_order = next((o for o in legs if o["type"] == "limit"), {})
        stop_order = next((o for o in legs if o["type"] == "stop"), {})

        # Parse the symbol & side
        symbol = self._extract_order_value(response, limit_order, "symbol")
        option_symbol = self._extract_order_value(response, limit_order, "option_symbol")
        side = self._extract_order_value(response, limit_order, "side")

        asset = (
            Asset.symbol2asset(option_symbol)
            if option_symbol and not pd.isna(option_symbol)
            else Asset.symbol2asset(self._normalize_symbol_for_internal(symbol, asset_type=Asset.AssetType.STOCK))
        )

        # Get the reason_description if it exists
        reason_description = response.get("reason_description", "")

        # Tradier sometimes returns None for avg_fill_price and sometimes $0.0. It mostly appears that:
        #    - 0.0 occurs during submission (mostly for OCO child orders it seems)
        #    - None while the order is active/cancelled
        #    - A value when the order is filled
        # Lumibot treats 0.0 as a valid fill amount, so need to convert to None when it is just a placeholder
        #    value for non-filled orders.
        avg_fill_price = response["avg_fill_price"] if "avg_fill_price" in response else None
        if avg_fill_price == 0.0 and not Order.is_equivalent_status(response["status"], Order.OrderStatus.FILLED):
            avg_fill_price = None

        # Map Tradier order types to Lumi order types
        lumi_order_type = self._tradier_type2lumi(self._extract_order_value(response, {}, "type"))

        # Create the order object
        order = Order(
            identifier=response["id"],
            strategy=strategy_name,
            status=response["status"],  # Status conversion happens automatically in Order
            asset=asset,
            side=self._tradier_side2lumi(side),
            quantity=self._extract_order_value(response, limit_order, "quantity"),
            order_type=lumi_order_type,
            time_in_force=self._extract_order_value(response, limit_order, "duration"),
            limit_price=self._extract_order_value(response, limit_order, "price"),
            stop_price=self._extract_order_value(response, stop_order, "stop_price"),
            tag=response["tag"] if "tag" in response and response["tag"] else None,
            date_created=response["create_date"],
            avg_fill_price=avg_fill_price,
            error_message=reason_description,
            order_class=self._tradier_class2lumi(response["class"] if "class" in response else None) or Order.OrderClass.SIMPLE,
        )
        # Example Tradier Date Value: '2024-10-04T15:46:14.946Z'
        order.broker_create_date = response["create_date"] if "create_date" in response else None
        order.broker_update_date = response["transaction_date"] if "transaction_date" in response else None
        order.update_raw(response)  # This marks order as 'transmitted'
        return order

    @staticmethod
    def _tradier_type2lumi(order_type):
        """
        Map Tradier order types to Lumi order types.
        Tradier may return 'debit', 'credit', or 'even' for multi-leg orders, which should be treated as 'limit'.
        """
        if order_type in ("debit", "credit", "even"):
            return "limit"
        return order_type

    @staticmethod
    def _extract_order_value(response, child_response, key):
        """
        OCO orders have empty values for many fields. This function will pull the value from the child order if
        the value is empty in the parent order.
        """
        is_oco = response["class"] == "oco"
        return response[key] if key in response and not is_oco else child_response.get(key, None)

    def _pull_broker_order(self, identifier):
        """
        This function pulls a single order from the broker by its identifier. Order is converted to a dictionary,
        and then returned. It is expected that the caller will convert the dictionary to an Order object by
        calling parse_broker_order() on the dictionary. Parsing the order will also dispatch it to the stream for
        processing.
        """
        orders = self._clean_order_records(self.tradier.orders.get_order(identifier))
        return orders[0] if len(orders) > 0 else None

    def _pull_broker_all_orders(self):
        """
        This function pulls all orders from the broker. Orders are converted to a list of dictionaries,
        and then returned. It is expected that the caller will convert each dictionary to an Order object by
        calling parse_broker_order() on the dictionary.
        """
        try:
            df = self.tradier.orders.get_orders()
        except Exception as e:
            logger.info(f"Error pulling orders from Tradier: {e}", exc_info=True)
            return []

        # Check if the dataframe is empty or None
        if df is None or df.empty:
            return []

        return self._clean_order_records(df)

    @staticmethod
    def _clean_order_records(df):
        """
        Cleans the order records DataFrame by rounding float values to 2 decimal places,
        replacing missing values with None, and converting the DataFrame to a list of dictionaries.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame containing order records.

        Returns
        -------
        list[dict]
            A list of dictionaries representing the cleaned order records.
        """
        # NOTE: This code path runs in a long-lived polling loop. Avoid full-DataFrame copies (apply/replace),
        # which can multiply peak memory when Tradier returns many rows.
        try:
            records = df.to_dict("records")
        except Exception:
            return []

        cleaned: list[dict] = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            out: dict = {}
            for k, v in rec.items():
                try:
                    if isinstance(v, float):
                        v = round(v, 2)
                    # Handle pandas missing sentinels (NA/NaT/nan) without materializing full copies.
                    if v is pd.NA or v is pd.NaT or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
                        v = None
                except Exception:
                    pass
                out[k] = v
            cleaned.append(out)
        return cleaned

    def _lumi_side2tradier(self, order: Order) -> str:
        # Make a copy of the side because we will modify it
        original_side = order.side

        # Set the side that we will return
        side = order.side

        if order.asset.asset_type == Asset.AssetType.STOCK:
            # Map extended side values to for Tradier API
            if side in ("buy_to_open"):
                side = "buy"
            elif side in ("sell_to_close"):
                side = "sell"
            elif side in ("buy_to_cover", "buy_to_close"):
                side = "buy_to_cover"
            elif side in ("sell_to_open", "sell_short"):
                side = "sell_short"
            return side

        # Convert the side to the Tradier side for options orders if necessary
        if side == Order.OrderSide.BUY or side == Order.OrderSide.SELL:
            # Check if we currently own the option
            position = self.get_tracked_position(order.strategy, order.asset)

            # Check if we own the option then we need to sell to close or buy to close
            if position is not None:
                if position.quantity > 0 and side == Order.OrderSide.SELL:
                    side = "sell_to_close"
                elif position.quantity >= 0 and side == Order.OrderSide.BUY:
                    side = "buy_to_open"
                elif position.quantity < 0 and side == Order.OrderSide.BUY:
                    side = "buy_to_close"
                elif position.quantity <= 0 and side == Order.OrderSide.SELL:
                    side = "sell_to_open"
                else:
                    logger.error(
                        f"Unable to determine the correct side for the order. " f"Position: {position}, Order: {order}"
                    )

            # Otherwise, we don't own the option so we need to buy to open or sell to open
            else:
                side = "buy_to_open" if side == Order.OrderSide.BUY else "sell_to_open"

        # Stoploss and limit orders are usually used to close positions, even if they are submitted "before" the
        # position is technically open (i.e. buy and stoploss order are submitted simultaneously)
        if (order.order_type in [Order.OrderType.STOP, Order.OrderType.TRAIL] and
                (original_side == Order.OrderSide.BUY or original_side == Order.OrderSide.SELL)):
            side = str(side).replace("to_open", "to_close")

        # Check if the side is a valid Tradier side
        if side not in ["buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"]:
            logger.error(f"Invalid option order side for Tradier: {order.side}")
            return ""

        return side

    @staticmethod
    def _tradier_class2lumi(order_class):
        """
        Converts a Tradier order class to a Lumi order class.
        Valid Tradier clases: One of: equity, option, combo, multileg
        Valid Lumi Order Classes: simple, bracket, oco, multileg, etc
        """
        if order_class is None or not isinstance(order_class, str):
            return None

        if order_class in ['equity', 'option']:
            return Order.OrderClass.SIMPLE

        # Check if the order class is a valid Lumi order class
        try:
            return Order.OrderClass(order_class)
        except ValueError:
            return None

    @staticmethod
    def _tradier_side2lumi(side):
        """
        Converts a Tradier side to a Lumi side.
        Valid Stock Sides: buy, buy_to_cover, sell, sell_short
        Valid Option Sides: buy_to_open, buy_to_close, sell_to_open, sell_to_close
        """
        # Check that the side is valid
        if not side or not isinstance(side, str):
            return None

        try:
            return Order.OrderSide(side)
        except ValueError:
            if "buy" in side:
                return Order.OrderSide.BUY
            elif "sell" in side:
                return Order.OrderSide.SELL
            else:
                raise ValueError(f"Invalid side {side} for Tradier.") from None

    # ==========Processing streams data=======================

    def do_polling(self):
        """
        This function is called every time the broker polls for new orders. It checks for new orders and
        dispatches them to the stream for processing.
        """
        # Pull the current Tradier positions and sync them with Lumibot's positions
        self.sync_positions(None)

        # Get current orders from Tradier and dispatch them to the stream for processing. Need to see all
        # lumi orders (not just active "tracked" ones) to catch any orders that might have changed final
        # status in Tradier.
        # df_orders = self.tradier.orders.get_orders()
        raw_orders = self._pull_broker_all_orders()
        try:
            self._telemetry_polls_total += 1
            self._telemetry_orders_seen_max = max(int(self._telemetry_orders_seen_max), len(raw_orders or []))
        except Exception:
            pass
        stored_orders = {x.identifier: x for x in self.get_all_orders()}
        for order_row in raw_orders:
            order = self._parse_broker_order_dict(order_row, strategy_name=self._strategy_name)
            # Process child orders first so they are tracked in the Lumi system before the parent order
            all_orders = [child for child in order.child_orders] + [order]

            # Process all parent and child orders
            for order in all_orders:
                # First time seeing this order, something weird has happened
                if order.identifier not in stored_orders:
                    # If it is the brokers first iteration then fully process the order because it is likely
                    # that the order was filled/canceled/etc before the strategy started.
                    if self._first_iteration:
                        # IMPORTANT: Avoid ingesting large historical order lists on startup.
                        # Tradier can return many closed orders; tracking them all in-memory can OOM long-running
                        # workers. On the first poll, we only need to reconcile currently-active orders.
                        if order.is_active() or order.status in {Order.OrderStatus.NEW}:
                            self._process_new_order(order)
                        else:
                            continue
                    else:
                        # Add to order in lumibot.
                        self._process_new_order(order)
                else:
                    # Always Update Quantity and Children. Children can change as they are assigned an identifier
                    # for the first time.
                    stored_order = stored_orders[order.identifier]
                    stored_order.quantity = order.quantity  # Update the quantity in case it has changed
                    stored_order.broker_create_date = order.broker_create_date
                    stored_order.broker_update_date = order.broker_update_date
                    if order.avg_fill_price:
                        stored_order.avg_fill_price = order.avg_fill_price
                    stored_children = [stored_orders[o.identifier] if o.identifier in stored_orders else o
                                       for o in order.child_orders]
                    stored_order.child_orders = stored_children

                    # Status has changed since last time we saw it, dispatch the new status.
                    #  - Polling methods are unable to track partial fills
                    #     - Partial fills often happen quickly and it is highly likely that polling will miss some of them
                    #     - Additionally, Lumi Order objects don't have a way to track quantity status changes and
                    #        adjusting the average sell price can be tricky
                    #     - Only dispatch filled orders if they are completely filled.
                    if not order.equivalent_status(stored_order):
                        match order.status.lower():
                            case "submitted" | "open":
                                self._safe_stream_dispatch(self.NEW_ORDER, order=stored_order)
                            case "partial_filled":
                                # Not handled for polling, only dispatch completely filled orders
                                pass
                            case "fill":
                                # Check if the order has an avg_fill_price, if not use the order_row price
                                if order.avg_fill_price is None:
                                    fill_price = order_row["avg_fill_price"]
                                else:
                                    fill_price = order.avg_fill_price

                                # Check if the order has a quantity
                                if order.quantity is None:
                                    fill_qty = order_row["exec_quantity"]
                                else:
                                    fill_qty = order.quantity

                                # For OCO orders - Parent order never gets filled values populated by Tradier API.
                                # Need to look at the child orders to get the necessary fill values.
                                if order.order_class == Order.OrderClass.OCO:
                                    filled_children = [o for o in order.child_orders if o.is_filled()]
                                    if filled_children:
                                        fill_price = filled_children[0].avg_fill_price
                                        fill_qty = filled_children[0].quantity

                                # There's race condition where Tradier API is marking status=filled but has not yet
                                # populated the avg_fill_price and other fill data. At some time in the future these
                                # values will be filled in by Tradier, so do not trigger a 'filled' event until
                                # all the needed data has been populated.
                                if fill_price is not None and fill_qty is not None:
                                    self._safe_stream_dispatch(
                                        self.FILLED_ORDER,
                                        order=stored_order,
                                        price=fill_price,
                                        filled_quantity=fill_qty,
                                    )
                            case "canceled":
                                self._safe_stream_dispatch(self.CANCELED_ORDER, order=stored_order)
                            case "error":
                                default_msg = f"{self.name} encountered an error with order {order.identifier} | {order}"
                                msg = order_row["reason_description"] if "reason_description" in order_row else default_msg
                                self._safe_stream_dispatch(self.ERROR_ORDER, order=stored_order, error_msg=msg)
                            case "cash_settled":
                                # Don't know how to detect this case in Tradier.
                                # Reference: https://documentation.tradier.com/brokerage-api/reference/response/orders
                                # Theory:
                                #  - Tradier will auto settle and create a new fill order for cash settled orders. Needs
                                #    testing to confirm.
                                pass
                    else:
                        # Status hasn't changed, but make sure we use the broker's status.
                        # I.e. 'submitted' becomes 'open'
                        stored_order.status = order.status

        # See if there are any tracked (aka active) orders that are no longer in the broker's list,
        # dispatch them as cancelled
        tracked_orders = {x.identifier: x for x in self.get_tracked_orders()}
        broker_ids = self._get_broker_id_from_raw_orders(raw_orders)
        for order_id, order in tracked_orders.items():
            if order_id not in broker_ids:
                logger.debug(
                    f"Poll Update: {self.name} no longer has order {order}, but Lumibot does. "
                    f"Dispatching as cancelled."
                )
                # Only dispatch orders that have not been filled or cancelled. Likely the broker has simply
                # stopped tracking them. This is particularly true with Paper Trading where orders are not tracked
                # overnight.
                if order.is_active():
                    self._safe_stream_dispatch(self.CANCELED_ORDER, order=order)

        if self._first_iteration:
            self._first_iteration = False

    def _get_broker_id_from_raw_orders(self, raw_orders):
        ids = []
        for o in raw_orders:
            if "id" in o:
                ids.append(o["id"])
            if "leg" in o and isinstance(o["leg"], list):
                for leg in o["leg"]:
                    if "id" in leg:
                        ids.append(leg["id"])

        return ids

    def _get_stream_object(self):
        """get the broker stream connection"""
        stream = PollingStream(self.polling_interval)
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        broker = self

        @broker.stream.add_action(broker.POLL_EVENT)
        def on_trade_event_poll():
            self.do_polling()

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_trade_event_new(order):
            # Log that the order was submitted
            logger.info(f"Processing action for new order {order}")

            try:
                broker._process_trade_event(
                    order,
                    broker.NEW_ORDER,
                )
                return True
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
            # Log that the order was filled
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
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event_cancel(order):
            # Log that the order was cancelled
            logger.info(f"Processing action for cancelled order {order}")

            try:
                broker._process_trade_event(
                    order,
                    broker.CANCELED_ORDER,
                )
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CASH_SETTLED)
        def on_trade_event_cash(order, price, filled_quantity):
            # Log that the order was cash settled
            logger.info(f"Processing action for cash settled order {order} | {price} | {filled_quantity}")

            try:
                broker._process_trade_event(
                    order,
                    broker.CASH_SETTLED,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
            except:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_trade_event_error(order, error_msg):
            # Log that the order had an error
            logger.error(f"Processing action for error order {order} | {error_msg}")
            try:
                if order.is_active():
                    # If the order has children, cancel them first upon error
                    if order.child_orders:
                        for child_order in order.child_orders:
                            child_order.set_error(error_msg)
                            broker._process_trade_event(
                                child_order,
                                broker.ERROR_ORDER,
                            )

                    # Then cancel the parent order
                    broker._process_trade_event(
                        order,
                        broker.ERROR_ORDER,
                    )
                logger.error(error_msg)
                order.set_error(error_msg)
            except:
                logger.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        # Try to run the stream
        try:
            self.stream._run()
        except TradierApiError as e:
            # Check if the error is a 401 or 403, if so, the access token is invalid
            error = str(e)
            if "401" in error or "403" in error:
                # Check if the access token or account number is invalid
                if self._tradier_access_token is None or self._tradier_account_number is None or len(self._tradier_access_token) == 0 or len(self._tradier_account_number) == 0:
                    colored_message = colored("Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are blank. Please check your keys.", color="red")
                    raise ValueError(colored_message)

                # Conceal the end of the access token
                access_token = self._tradier_access_token[:7] + "*" * 7
                colored_message = colored(f"Your TRADIER_ACCOUNT_NUMBER or TRADIER_ACCESS_TOKEN are invalid. Your account number is: {self._tradier_account_number} and your access token is: {access_token}", color="red")
                raise ValueError(colored_message)

    def _flatten_order(self, order):
        """Some submitted orders may trigger other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]

        # TODO: Need to implement this for Tradier

        return orders
