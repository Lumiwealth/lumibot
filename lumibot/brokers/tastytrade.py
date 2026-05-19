"""
Tastytrade broker for Lumibot.

Wraps the unofficial ``tastytrade`` Python SDK (https://github.com/tastyware/tastytrade)
which exposes a fully asynchronous API. Lumibot's :class:`Broker` abstract
methods are synchronous, so this adapter owns a dedicated asyncio event
loop running on a background daemon thread and dispatches every SDK call
through ``asyncio.run_coroutine_threadsafe``.

Functional surface:

- Authentication via OAuth (``provider_secret`` + ``refresh_token``)
- Account selection by account number
- Balances and positions (equity; option / future positions are skipped
  with a warning until full asset parsing lands)
- Order submission for equities, single-leg equity options, and multileg
  equity-option spreads (``order_type`` = market / limit / debit / credit /
  even, mapped onto Tastytrade's ``NewOrder``)
- Order modification via ``account.replace_order``
- Order cancellation
- Order parsing and read-back via ``account.get_order`` /
  ``account.get_live_orders`` / ``account.get_order_history``
- Polling stream dispatching NEW / FILLED / CANCELED / ERROR events to
  the strategy executor

Stubs (logged warnings, follow-ups still pending):

- Advanced orders (OCO / OTO / bracket → Tastytrade ``NewComplexOrder``)
- Native websocket streaming (``AlertStreamer`` + ``DXLinkStreamer``)
- Market data on ``TastytradeData`` (chains, quotes, historical bars)
"""

import asyncio
import datetime
import os
import re
import threading
import traceback
from decimal import Decimal
from typing import Any, Awaitable, List, Optional, TypeVar, Union

from termcolor import colored

from .broker import Broker
from lumibot.data_sources.tastytrade_data import TastytradeData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.lumibot_logger import get_logger
from lumibot.trading_builtins import PollingStream

logger = get_logger(__name__)

try:  # tastytrade is an optional runtime dep; surface a clear error if missing.
    from tastytrade import Account as _TTAccount
    from tastytrade import Session as _TTSession
    from tastytrade.order import (
        InstrumentType as _TTInstrumentType,
        Leg as _TTLeg,
        NewOrder as _TTNewOrder,
        OrderAction as _TTOrderAction,
        OrderStatus as _TTOrderStatus,
        OrderTimeInForce as _TTOrderTIF,
        OrderType as _TTOrderType,
    )
except Exception as _import_err:  # pragma: no cover - import-time guard
    _TTAccount = None
    _TTSession = None
    _TTInstrumentType = None
    _TTLeg = None
    _TTNewOrder = None
    _TTOrderAction = None
    _TTOrderStatus = None
    _TTOrderTIF = None
    _TTOrderType = None
    _TASTYTRADE_IMPORT_ERROR = _import_err
else:
    _TASTYTRADE_IMPORT_ERROR = None


T = TypeVar("T")


class _AsyncBridge:
    """Run a private asyncio loop on a daemon thread for sync callers."""

    def __init__(self, name: str = "tastytrade-asyncio"):
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._serve, name=name, daemon=True)
        self._thread.start()
        self._ready.wait()

    def _serve(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        try:
            self._loop.run_forever()
        finally:
            try:
                self._loop.close()
            except Exception:
                pass

    def run(self, coro: Awaitable[T], timeout: Optional[float] = 30.0) -> T:
        if not self._loop.is_running():
            raise RuntimeError("Tastytrade asyncio bridge is not running.")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result(timeout=timeout)
        except BaseException:
            # Includes TimeoutError, KeyboardInterrupt, and any exception the
            # coroutine raises after the caller has already moved on. Cancel
            # the underlying task so it doesn't keep running on the loop and
            # leak resources.
            future.cancel()
            raise

    def close(self) -> None:
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


class Tastytrade(Broker):
    """
    Tastytrade broker.

    Authentication is OAuth-only: provide ``client_secret`` + ``refresh_token``
    and (optionally) ``is_test=True`` for the certification (sandbox) environment.

    Configuration may be supplied via the ``config`` dict, kwargs, or
    environment variables (in this order of preference):

    - ``TASTYTRADE_CLIENT_SECRET``
    - ``TASTYTRADE_REFRESH_TOKEN``
    - ``TASTYTRADE_ACCOUNT_NUMBER``
    - ``TASTYTRADE_SANDBOX`` (``"true"`` / ``"1"`` / ``"yes"`` for cert env)
    """

    NAME = "Tastytrade"
    POLL_EVENT = PollingStream.POLL_EVENT

    def __init__(
        self,
        config: Optional[dict] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        account_number: Optional[str] = None,
        is_test: Optional[bool] = None,
        connect_stream: bool = True,
        data_source: Optional[TastytradeData] = None,
        max_workers: int = 1,
        polling_interval: float = 5.0,
    ):
        if _TTSession is None:
            raise ImportError(
                "The 'tastytrade' package is required to use the Tastytrade broker. "
                "Install it with `pip install tastytrade`."
            ) from _TASTYTRADE_IMPORT_ERROR

        # Resolve credentials: explicit kwargs > config dict > environment.
        if config:
            client_secret = client_secret or config.get("CLIENT_SECRET")
            refresh_token = refresh_token or config.get("REFRESH_TOKEN")
            account_number = account_number or config.get("ACCOUNT_NUMBER")
            if is_test is None and "SANDBOX" in config:
                is_test = self._parse_truthy(config.get("SANDBOX"))

        client_secret = client_secret or os.environ.get("TASTYTRADE_CLIENT_SECRET")
        refresh_token = refresh_token or os.environ.get("TASTYTRADE_REFRESH_TOKEN")
        account_number = account_number or os.environ.get("TASTYTRADE_ACCOUNT_NUMBER")
        if is_test is None:
            is_test = self._parse_truthy(os.environ.get("TASTYTRADE_SANDBOX"))

        missing = [
            n for n, v in (
                ("client_secret", client_secret),
                ("refresh_token", refresh_token),
                ("account_number", account_number),
            ) if not v
        ]
        if missing:
            raise ValueError(
                "Tastytrade broker missing required credentials: "
                + ", ".join(missing)
                + ". Provide via kwargs, the `config` dict, or env vars "
                  "(TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN / "
                  "TASTYTRADE_ACCOUNT_NUMBER)."
            )

        self._tt_account_number = account_number
        self._tt_is_test = bool(is_test)
        self.polling_interval = polling_interval
        self._async_bridge = _AsyncBridge()

        # Build the SDK Session (sync constructor) and resolve the Account.
        self._session = _TTSession(
            provider_secret=client_secret,
            refresh_token=refresh_token,
            is_test=self._tt_is_test,
        )
        self._account = self._async_bridge.run(
            _TTAccount.get(self._session, self._tt_account_number)
        )

        if data_source is None:
            data_source = TastytradeData(
                session=self._session,
                runner=self._async_bridge.run,
            )
        self.data_source = data_source

        super().__init__(
            name=self.NAME,
            data_source=data_source,
            config=config,
            max_workers=max_workers,
            connect_stream=connect_stream,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _run(self, coro: Awaitable[T], timeout: Optional[float] = 30.0) -> T:
        return self._async_bridge.run(coro, timeout=timeout)

    @staticmethod
    def _parse_truthy(value) -> bool:
        """Tolerant truthy parser for env vars / config dicts.

        Treats common stringy false-like values (``"false"``, ``"0"``,
        ``"no"``, ``"off"``, ``""``) as False so a config of
        ``{"SANDBOX": "false"}`` doesn't accidentally land on the cert
        environment via Python's ``bool("false") == True``.
        """
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        return s in ("1", "true", "yes", "y", "on")

    # ------------------------------------------------------------------
    # Account
    # ------------------------------------------------------------------
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        try:
            balances = self._run(self._account.get_balances(self._session))
        except Exception as e:
            logger.error(colored(f"[Tastytrade] Failed to fetch balances: {e}", "red"))
            return 0.0, 0.0, 0.0

        # Tastytrade balance object exposes ``cash_balance``, ``net_liquidating_value``,
        # and ``long_equity_value`` / ``short_equity_value`` (Decimal). Be defensive in
        # case the SDK shape shifts slightly between versions.
        cash = float(getattr(balances, "cash_balance", 0) or 0)
        nlv = float(getattr(balances, "net_liquidating_value", 0) or 0)
        long_eq = float(getattr(balances, "long_equity_value", 0) or 0)
        short_eq = float(getattr(balances, "short_equity_value", 0) or 0)
        positions_value = long_eq - short_eq
        return cash, positions_value, nlv

    def get_historical_account_value(self) -> dict:
        # The SDK exposes ``get_net_liquidating_value_history`` but mapping it to
        # Lumibot's expected hourly/daily shape requires non-trivial resampling;
        # leaving as a stub for a follow-up commit.
        logger.warning(colored(
            "Tastytrade.get_historical_account_value is not yet implemented.",
            "yellow",
        ))
        return {"hourly": None, "daily": None}

    # ------------------------------------------------------------------
    # Positions
    # ------------------------------------------------------------------
    def _pull_positions(self, strategy) -> List[Position]:
        try:
            tt_positions = self._run(self._account.get_positions(self._session))
        except Exception as e:
            logger.error(colored(f"[Tastytrade] Failed to fetch positions: {e}", "red"))
            return []

        strategy_name = self._strategy_name_from_input(strategy)
        positions: List[Position] = []
        for tp in tt_positions or []:
            asset = self._tt_position_to_asset(tp)
            if asset is None:
                continue
            qty = self._tt_position_quantity(tp)
            positions.append(Position(strategy=strategy_name, asset=asset, quantity=qty))
        return positions

    def _pull_position(self, strategy, asset: Asset) -> Optional[Position]:
        for p in self._pull_positions(strategy):
            if p.asset == asset:
                return p
        return None

    @staticmethod
    def _tt_position_quantity(tp) -> Decimal:
        qty = getattr(tp, "quantity", 0) or 0
        direction = (getattr(tp, "quantity_direction", "") or "").lower()
        try:
            qty = Decimal(str(qty))
        except Exception:
            qty = Decimal("0")
        if direction == "short":
            qty = -qty
        return qty

    @classmethod
    def _tt_position_to_asset(cls, tp) -> Optional[Asset]:
        """Convert a Tastytrade position to a Lumibot Asset.

        Handles ``Equity`` (stock) and ``Equity Option`` (OCC-symbol options).
        Futures, future options, crypto, and other instrument types log a
        warning and return None — they'll land when broader asset support
        does.
        """
        instrument = (getattr(tp, "instrument_type", "") or "").lower()
        symbol = getattr(tp, "symbol", None)
        if not symbol:
            return None
        if instrument == "equity":
            return Asset(symbol=symbol, asset_type=Asset.AssetType.STOCK)
        if instrument == "equity option":
            asset = cls._occ_to_asset(symbol)
            if asset is None:
                logger.warning(colored(
                    f"[Tastytrade] Could not parse OCC symbol on equity-option "
                    f"position: {symbol!r}. Skipping.",
                    "yellow",
                ))
            return asset
        logger.warning(colored(
            f"[Tastytrade] Skipping position with unhandled instrument_type "
            f"'{instrument}' for symbol {symbol}. (Futures / future options / "
            f"crypto support is a follow-up.)",
            "yellow",
        ))
        return None

    # ------------------------------------------------------------------
    # Mapping helpers (Lumibot ↔ Tastytrade)
    # ------------------------------------------------------------------
    @staticmethod
    def _to_occ_symbol(asset: Asset) -> str:
        """Build a 21-char OCC option symbol from a Lumibot Asset.

        Format: ``ROOT (left-padded to 6) + YYMMDD + C/P + strike*1000 (8 digits)``.
        Example: ``AAPL  260717C00230000``.
        """
        if asset.expiration is None or asset.right is None or asset.strike is None:
            raise ValueError(
                f"Option asset is missing expiration/right/strike: {asset!r}"
            )
        root = (asset.symbol or "").upper().ljust(6)
        exp = asset.expiration
        if isinstance(exp, datetime.datetime):
            exp = exp.date()
        yymmdd = exp.strftime("%y%m%d")
        right_letter = "C" if str(asset.right).upper().startswith("C") else "P"
        strike_int = int(round(float(asset.strike) * 1000))
        strike_str = f"{strike_int:08d}"
        return f"{root}{yymmdd}{right_letter}{strike_str}"

    @staticmethod
    def _lumi_side_to_tt_action(side: str, is_option: bool) -> "_TTOrderAction":
        """Map a Lumibot order side to a Tastytrade OrderAction.

        Tastytrade's API does NOT accept plain ``Buy``/``Sell`` on equity
        order legs — it wants the explicit open/close form even for stocks.
        ``BUY``/``SELL`` plain values exist in the SDK enum for special
        cases (e.g. notional market orders) but get rejected on the
        standard order endpoint with ``order_legs.action: is invalid``.
        """
        s = (side or "").lower()
        if is_option:
            mapping = {
                "buy_to_open": _TTOrderAction.BUY_TO_OPEN,
                "sell_to_open": _TTOrderAction.SELL_TO_OPEN,
                "buy_to_close": _TTOrderAction.BUY_TO_CLOSE,
                "sell_to_close": _TTOrderAction.SELL_TO_CLOSE,
                # Plain buy/sell on options default to opening; callers
                # should use the explicit *_to_open / *_to_close sides.
                "buy": _TTOrderAction.BUY_TO_OPEN,
                "sell": _TTOrderAction.SELL_TO_OPEN,
            }
        else:
            # Equities: open long = BUY_TO_OPEN, close long = SELL_TO_CLOSE,
            # short = SELL_TO_OPEN, cover = BUY_TO_CLOSE.
            mapping = {
                "buy": _TTOrderAction.BUY_TO_OPEN,
                "sell": _TTOrderAction.SELL_TO_CLOSE,
                "buy_to_open": _TTOrderAction.BUY_TO_OPEN,
                "sell_to_close": _TTOrderAction.SELL_TO_CLOSE,
                "sell_short": _TTOrderAction.SELL_TO_OPEN,
                "sell_to_open": _TTOrderAction.SELL_TO_OPEN,
                "buy_to_cover": _TTOrderAction.BUY_TO_CLOSE,
                "buy_to_close": _TTOrderAction.BUY_TO_CLOSE,
            }
        if s not in mapping:
            raise ValueError(f"Unsupported order side {side!r} for Tastytrade.")
        return mapping[s]

    @staticmethod
    def _lumi_order_type_to_tt(order_type: str) -> "_TTOrderType":
        s = (order_type or "").lower()
        mapping = {
            "market": _TTOrderType.MARKET,
            "limit": _TTOrderType.LIMIT,
            "smart_limit": _TTOrderType.LIMIT,
            "stop": _TTOrderType.STOP,
            "stop_limit": _TTOrderType.STOP_LIMIT,
            # debit/credit/even are multileg pricing modes — they all map to
            # LIMIT on the wire, with the leg actions and ``price`` carrying
            # the credit/debit semantics.
            "debit": _TTOrderType.LIMIT,
            "credit": _TTOrderType.LIMIT,
            "even": _TTOrderType.LIMIT,
        }
        if s not in mapping:
            raise ValueError(f"Unsupported order_type {order_type!r} for Tastytrade.")
        return mapping[s]

    @staticmethod
    def _lumi_tif_to_tt(tif: Optional[str]) -> "_TTOrderTIF":
        s = (tif or "day").lower()
        mapping = {
            "day": _TTOrderTIF.DAY,
            "gtc": _TTOrderTIF.GTC,
            "ioc": _TTOrderTIF.IOC,
            "ext": _TTOrderTIF.EXT,
            "pre": _TTOrderTIF.EXT,
            "post": _TTOrderTIF.EXT,
        }
        return mapping.get(s, _TTOrderTIF.DAY)

    def _build_leg(self, order: Order) -> "_TTLeg":
        """Build a Tastytrade ``Leg`` from a Lumibot child/single Order."""
        asset = order.asset
        if asset is None:
            raise ValueError(f"Order has no asset: {order!r}")

        if asset.asset_type == Asset.AssetType.STOCK:
            symbol = (asset.symbol or "").upper()
            instrument_type = _TTInstrumentType.EQUITY
            is_option = False
        elif asset.asset_type == Asset.AssetType.OPTION:
            symbol = self._to_occ_symbol(asset)
            instrument_type = _TTInstrumentType.EQUITY_OPTION
            is_option = True
        else:
            raise ValueError(
                f"Tastytrade broker does not yet support asset_type "
                f"{asset.asset_type!r} (symbol={asset.symbol!r})."
            )

        action = self._lumi_side_to_tt_action(order.side, is_option=is_option)
        qty = Decimal(str(order.quantity))
        return _TTLeg(
            instrument_type=instrument_type,
            symbol=symbol,
            action=action,
            quantity=qty,
        )

    @staticmethod
    def _format_price(price: Optional[Union[float, Decimal]]) -> Optional[Decimal]:
        if price is None:
            return None
        # Preserve sign — Tastytrade encodes price-effect in the sign of price
        # (negative = debit, positive = credit). The SDK's serializer strips
        # abs() before sending and pairs it with a "price-effect" field.
        sign = -1 if Decimal(str(price)) < 0 else 1
        return (sign * abs(Decimal(str(price))).quantize(Decimal("0.01")))

    @staticmethod
    def _is_debit_action(side: str, is_option: bool) -> bool:
        """Return True if a BUY-side action (debit). False for SELL-side (credit)."""
        s = (side or "").lower()
        if is_option:
            return s in ("buy", "buy_to_open", "buy_to_close")
        return s in ("buy", "buy_to_cover")

    def _sign_single_leg_price(
        self,
        order: Order,
        price: Optional[Union[float, Decimal]],
    ) -> Optional[Union[float, Decimal]]:
        """Apply sign convention for a single-leg order based on its side."""
        if price is None:
            return None
        is_option = order.asset and order.asset.asset_type == Asset.AssetType.OPTION
        is_debit = self._is_debit_action(order.side, is_option=is_option)
        magnitude = abs(Decimal(str(price)))
        return -magnitude if is_debit else magnitude

    def _build_new_order(
        self,
        legs: List["_TTLeg"],
        order_type: str,
        time_in_force: str,
        price: Optional[Union[float, Decimal]] = None,
        stop_trigger: Optional[Union[float, Decimal]] = None,
    ) -> "_TTNewOrder":
        tt_type = self._lumi_order_type_to_tt(order_type)
        tt_tif = self._lumi_tif_to_tt(time_in_force)

        kwargs: dict = {
            "time_in_force": tt_tif,
            "order_type": tt_type,
            "legs": legs,
        }
        if tt_type in (_TTOrderType.LIMIT, _TTOrderType.STOP_LIMIT):
            if price is None:
                raise ValueError(
                    f"Limit/Stop-Limit orders require a price (order_type={order_type!r})."
                )
            kwargs["price"] = self._format_price(price)
        if tt_type in (_TTOrderType.STOP, _TTOrderType.STOP_LIMIT):
            if stop_trigger is None:
                raise ValueError(
                    f"Stop / Stop-Limit orders require a stop_trigger "
                    f"(order_type={order_type!r})."
                )
            kwargs["stop_trigger"] = self._format_price(stop_trigger)
        return _TTNewOrder(**kwargs)

    # ------------------------------------------------------------------
    # Order submission
    # ------------------------------------------------------------------
    def _submit_order(self, order: Order) -> Optional[Order]:
        # Advanced orders (OCO/OTO/bracket) need NewComplexOrder — defer.
        if order.is_advanced_order():
            logger.error(colored(
                "[Tastytrade] Advanced (OCO/OTO/bracket) orders are not yet "
                "supported. Submit child orders individually for now.",
                "red",
            ))
            self._safe_stream_dispatch(self.ERROR_ORDER, order=order,
                                       error_msg="advanced orders unsupported")
            return None

        try:
            leg = self._build_leg(order)
        except Exception as e:
            logger.error(colored(f"[Tastytrade] Cannot build leg for {order!r}: {e}", "red"))
            self._safe_stream_dispatch(self.ERROR_ORDER, order=order, error_msg=str(e))
            return None

        # For STOP_LIMIT, Lumibot stores the limit price in stop_limit_price.
        order_type_str = (order.order_type or "limit")
        limit = order.limit_price
        if order_type_str == Order.OrderType.STOP_LIMIT:
            limit = order.stop_limit_price
        # Tastytrade encodes credit/debit in the price sign — BUY -> negative,
        # SELL -> positive. The SDK serializer strips abs() and emits price-effect.
        signed_limit = self._sign_single_leg_price(order, limit)

        try:
            new_order = self._build_new_order(
                legs=[leg],
                order_type=order_type_str,
                time_in_force=order.time_in_force,
                price=signed_limit,
                stop_trigger=order.stop_price,
            )
        except Exception as e:
            logger.error(colored(f"[Tastytrade] Cannot build NewOrder for {order!r}: {e}", "red"))
            self._safe_stream_dispatch(self.ERROR_ORDER, order=order, error_msg=str(e))
            return None

        try:
            response = self._run(self._account.place_order(self._session, new_order, dry_run=False))
        except Exception as e:
            logger.error(colored(f"[Tastytrade] place_order failed for {order!r}: {e}", "red"))
            self._safe_stream_dispatch(self.ERROR_ORDER, order=order, error_msg=str(e))
            return None

        return self._finalize_submitted_order(order, response)

    def _submit_orders(
        self,
        orders,
        is_multileg: bool = False,
        order_type: Optional[str] = None,
        duration: str = "day",
        price: Optional[Union[float, Decimal]] = None,
    ):
        if not orders:
            return []

        if not is_multileg:
            return [self._submit_order(o) for o in orders]

        # Multileg: build one NewOrder with all legs.
        if order_type is None:
            order_type = "market"
        order_type_norm = (order_type or "market").lower()
        if order_type_norm not in ("market", "limit", "debit", "credit", "even"):
            raise ValueError(
                f"Invalid multileg order_type {order_type!r}. Expected one of "
                f"market/limit/debit/credit/even."
            )

        # Lumibot multileg convention: all legs share the same underlying.
        underlyings = {o.asset.symbol for o in orders if o.asset and o.asset.symbol}
        if len(underlyings) > 1:
            raise ValueError(
                f"All legs of a multileg order must share an underlying; got {underlyings}."
            )

        legs = [self._build_leg(o) for o in orders]

        # Sign the price per Tastytrade's convention: positive = credit,
        # negative = debit. The serializer sends abs(price) + price-effect.
        if order_type_norm == "credit":
            if price is None:
                raise ValueError("price is required for 'credit' multileg.")
            tt_price: Optional[Decimal] = abs(Decimal(str(price)))
        elif order_type_norm == "debit":
            if price is None:
                raise ValueError("price is required for 'debit' multileg.")
            tt_price = -abs(Decimal(str(price)))
        elif order_type_norm == "even":
            tt_price = Decimal("0.00")
        elif order_type_norm == "limit":
            if price is None:
                raise ValueError("price is required for 'limit' multileg.")
            # Infer sign from leg net direction. If callers want explicit
            # credit/debit semantics they should use those keywords directly.
            buys = sum(1 for leg in legs if "buy" in str(leg.action.value).lower())
            sells = sum(1 for leg in legs if "sell" in str(leg.action.value).lower())
            if buys > 0 and sells == 0:
                tt_price = -abs(Decimal(str(price)))   # all buys -> debit
            elif sells > 0 and buys == 0:
                tt_price = abs(Decimal(str(price)))    # all sells -> credit
            else:
                raise ValueError(
                    "Mixed-action multileg 'limit' is ambiguous. Use "
                    "order_type='credit' or 'debit' to disambiguate."
                )
        else:  # market
            tt_price = None

        new_order = self._build_new_order(
            legs=legs,
            order_type="limit" if order_type_norm != "market" else "market",
            time_in_force=duration,
            price=tt_price,
        )

        try:
            response = self._run(self._account.place_order(self._session, new_order, dry_run=False))
        except Exception as e:
            logger.error(colored(f"[Tastytrade] Multileg place_order failed: {e}", "red"))
            for o in orders:
                self._safe_stream_dispatch(self.ERROR_ORDER, order=o, error_msg=str(e))
            return None

        # Build a parent Order representing the multileg group. Lumibot's
        # ``Order.OrderType`` doesn't have credit/debit/even — those are
        # wire-level pricing modes for the broker, not Lumibot order types.
        # Store the broker-level mapping (limit for non-market, market for
        # market) on the parent Order.
        parent_order_type = (
            Order.OrderType.MARKET if order_type_norm == "market"
            else Order.OrderType.LIMIT
        )
        parent_asset = Asset(
            symbol=orders[0].asset.symbol,
            asset_type=Asset.AssetType.STOCK,
        )
        parent = Order(
            identifier=str(getattr(getattr(response, "order", None), "id", "") or ""),
            asset=parent_asset,
            strategy=orders[0].strategy,
            order_class=Order.OrderClass.MULTILEG,
            side=orders[0].side,
            quantity=orders[0].quantity,
            order_type=parent_order_type,
            time_in_force=duration,
            limit_price=tt_price,
            status=Order.OrderStatus.SUBMITTED,
        )
        for child in orders:
            child.parent_identifier = parent.identifier
        parent.child_orders = list(orders)
        try:
            parent.update_raw(response)
        except Exception:
            pass
        self._unprocessed_orders.append(parent)
        self._safe_stream_dispatch(self.NEW_ORDER, order=parent)
        return [parent]

    def _finalize_submitted_order(self, order: Order, response: Any) -> Order:
        """Stamp identifier + SUBMITTED status onto a single-leg order."""
        placed = getattr(response, "order", None) or response
        identifier = getattr(placed, "id", None)
        if identifier is None and isinstance(placed, dict):
            identifier = placed.get("id")
        order.identifier = str(identifier) if identifier is not None else None
        order.status = Order.OrderStatus.SUBMITTED
        try:
            order.update_raw(response)
        except Exception:
            pass
        self._unprocessed_orders.append(order)
        self._safe_stream_dispatch(self.NEW_ORDER, order=order)
        return order

    def _safe_stream_dispatch(self, event, **kwargs):
        """Dispatch to stream if one is wired; no-op otherwise.

        Mirrors Tradier's helper so the broker doesn't crash when ``stream``
        is None (which is the case until streaming lands in a follow-up).
        """
        stream = getattr(self, "stream", None)
        if stream is None:
            return
        try:
            stream.dispatch(event, **kwargs)
        except Exception:
            return

    def cancel_order(self, order: Order) -> None:
        if order.is_filled() or order.is_canceled():
            return
        if not order.identifier:
            raise ValueError(
                "Order identifier is not set; cannot cancel. Did you submit it?"
            )
        try:
            self._run(self._account.delete_order(self._session, order.identifier))
        except Exception as e:
            logger.error(colored(
                f"[Tastytrade] Failed to cancel order {order.identifier}: {e}",
                "red",
            ))

    def _modify_order(self, order: Order,
                      limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """Replace an order's limit and/or stop price.

        Tastytrade implements modification as a *replace*: build a new
        ``NewOrder`` with the same legs and an updated price, then call
        ``account.replace_order(session, order_id, new_order)``.

        Multileg orders are rejected explicitly — ``_build_leg`` only knows
        how to construct a single leg from the parent ``Order``, which would
        silently submit a one-legged replacement and break a spread. Cancel
        and resubmit is the workaround until multileg replace is wired up.
        """
        if not order.identifier:
            raise ValueError(
                "Order identifier is not set; cannot modify. Did you submit it?"
            )
        if order.is_filled() or order.is_canceled():
            return

        if (order.order_class == Order.OrderClass.MULTILEG
                or len(getattr(order, "child_orders", []) or []) > 1):
            logger.error(colored(
                f"[Tastytrade] _modify_order does not support multileg orders "
                f"(order_class={order.order_class}, child_orders="
                f"{len(getattr(order, 'child_orders', []) or [])}). _build_leg "
                f"only constructs a single leg, which would silently break the "
                f"spread. Cancel and resubmit instead.",
                "red",
            ))
            return None

        try:
            leg = self._build_leg(order)
        except Exception as e:
            logger.error(colored(f"[Tastytrade] _modify_order build_leg failed: {e}", "red"))
            return None

        new_limit = limit_price if limit_price is not None else order.limit_price
        new_stop = stop_price if stop_price is not None else order.stop_price
        order_type_str = (order.order_type or "limit")
        signed_limit = self._sign_single_leg_price(order, new_limit)
        try:
            new_order = self._build_new_order(
                legs=[leg],
                order_type=order_type_str,
                time_in_force=order.time_in_force,
                price=signed_limit,
                stop_trigger=new_stop,
            )
        except Exception as e:
            logger.error(colored(f"[Tastytrade] _modify_order build NewOrder failed: {e}", "red"))
            return None

        try:
            response = self._run(self._account.replace_order(
                self._session, order.identifier, new_order,
            ))
        except Exception as e:
            logger.error(colored(
                f"[Tastytrade] replace_order({order.identifier}) failed: {e}",
                "red",
            ))
            return None

        # Replace returns a new PlacedOrder with a new id; update local order.
        placed = getattr(response, "order", None) or response
        new_id = getattr(placed, "id", None)
        if new_id is not None:
            order.identifier = str(new_id)
        if limit_price is not None:
            order.limit_price = limit_price
        if stop_price is not None:
            order.stop_price = stop_price
        try:
            order.update_raw(response)
        except Exception:
            pass
        return order

    # ------------------------------------------------------------------
    # Order parsing + read-back
    # ------------------------------------------------------------------
    _TT_STATUS_TO_LUMI = {
        # Tastytrade OrderStatus → Lumibot Order.OrderStatus
        "Received": Order.OrderStatus.SUBMITTED,
        "Routed": Order.OrderStatus.SUBMITTED,
        "In Flight": Order.OrderStatus.SUBMITTED,
        "Live": Order.OrderStatus.OPEN,
        "Contingent": Order.OrderStatus.OPEN,
        "Cancel Requested": Order.OrderStatus.CANCELLING,
        "Replace Requested": Order.OrderStatus.OPEN,
        "Cancelled": Order.OrderStatus.CANCELED,
        "Filled": Order.OrderStatus.FILLED,
        "Expired": Order.OrderStatus.EXPIRED,
        "Rejected": Order.OrderStatus.ERROR,
        "Removed": Order.OrderStatus.CANCELED,
        "Partially Removed": Order.OrderStatus.PARTIALLY_FILLED,
    }

    @classmethod
    def _tt_status_to_lumi(cls, status: Any) -> str:
        # status may be an OrderStatus enum or its string value.
        key = getattr(status, "value", status)
        return cls._TT_STATUS_TO_LUMI.get(str(key), Order.OrderStatus.NEW)

    @staticmethod
    def _occ_to_asset(symbol: str) -> Optional[Asset]:
        """Parse an OCC option symbol back into a Lumibot Asset."""
        m = re.match(r"^\s*([A-Z][A-Z0-9.\- ]{0,5}?)\s*(\d{6})([CP])(\d{8})\s*$", symbol or "")
        if not m:
            return None
        root, yymmdd, cp, strike_str = m.groups()
        try:
            expiration = datetime.datetime.strptime(yymmdd, "%y%m%d").date()
            strike = Decimal(strike_str) / Decimal(1000)
        except Exception:
            return None
        return Asset(
            symbol=root.strip(),
            asset_type=Asset.AssetType.OPTION,
            expiration=expiration,
            strike=float(strike),
            right=Asset.OptionRight.CALL if cp == "C" else Asset.OptionRight.PUT,
        )

    @classmethod
    def _leg_to_asset(cls, leg) -> Optional[Asset]:
        instrument = getattr(leg, "instrument_type", None)
        instrument_value = getattr(instrument, "value", instrument)
        symbol = getattr(leg, "symbol", "") or ""
        if instrument_value == "Equity":
            return Asset(symbol=symbol.strip().upper(), asset_type=Asset.AssetType.STOCK)
        if instrument_value == "Equity Option":
            return cls._occ_to_asset(symbol)
        return None

    @classmethod
    def _leg_to_lumi_side(cls, leg, is_option: bool) -> str:
        action = getattr(leg, "action", None)
        action_value = getattr(action, "value", action)
        action_str = str(action_value or "").lower()
        if is_option:
            return {
                "buy to open": Order.OrderSide.BUY_TO_OPEN,
                "sell to open": Order.OrderSide.SELL_TO_OPEN,
                "buy to close": Order.OrderSide.BUY_TO_CLOSE,
                "sell to close": Order.OrderSide.SELL_TO_CLOSE,
                "buy": Order.OrderSide.BUY_TO_OPEN,
                "sell": Order.OrderSide.SELL_TO_OPEN,
            }.get(action_str, Order.OrderSide.BUY)
        # Equity legs read back from Tastytrade use the explicit open/close
        # form ("Buy to Open", "Sell to Close", ...) because that's what we
        # had to send on the wire — Tastytrade rejects plain Buy/Sell on
        # equity legs. Preserve that detail when parsing.
        return {
            "buy": Order.OrderSide.BUY,
            "sell": Order.OrderSide.SELL,
            "buy to open": Order.OrderSide.BUY_TO_OPEN,
            "sell to open": Order.OrderSide.SELL_TO_OPEN,
            "buy to close": Order.OrderSide.BUY_TO_CLOSE,
            "sell to close": Order.OrderSide.SELL_TO_CLOSE,
        }.get(action_str, Order.OrderSide.BUY)

    def _parse_broker_order(self, response: Any, strategy_name: str,
                            strategy_object=None) -> Optional[Order]:
        """Convert a Tastytrade ``PlacedOrder`` into a Lumibot ``Order``.

        Multileg orders return a parent ``Order`` with one child per leg
        attached via ``add_child_order``. Single-leg orders return a single
        ``Order``.
        """
        if response is None:
            return None

        legs = list(getattr(response, "legs", []) or [])
        if not legs:
            return None

        identifier = getattr(response, "id", None)
        identifier = str(identifier) if identifier is not None else None
        status = self._tt_status_to_lumi(getattr(response, "status", None))
        order_type = getattr(getattr(response, "order_type", None), "value", None)
        order_type = str(order_type).lower() if order_type else Order.OrderType.LIMIT
        # Tastytrade enums use 'Stop Limit' / 'Marketable Limit' — normalize.
        order_type = order_type.replace(" ", "_")
        if order_type == "marketable_limit":
            order_type = "limit"
        tif = getattr(getattr(response, "time_in_force", None), "value", None)
        tif = str(tif).lower() if tif else "day"
        price = getattr(response, "price", None)
        stop = getattr(response, "stop_trigger", None)

        if len(legs) == 1:
            asset = self._leg_to_asset(legs[0])
            if asset is None:
                logger.warning(colored(
                    f"[Tastytrade] Unhandled leg for order {identifier}: {legs[0]!r}",
                    "yellow",
                ))
                return None
            qty = getattr(legs[0], "quantity", None) or 0
            side = self._leg_to_lumi_side(legs[0], is_option=(asset.asset_type == Asset.AssetType.OPTION))
            order = Order(
                identifier=identifier,
                asset=asset,
                strategy=strategy_name,
                quantity=Decimal(str(qty)),
                side=side,
                order_type=order_type,
                limit_price=price,
                stop_price=stop,
                time_in_force=tif,
                status=status,
            )
            try:
                order.update_raw(response)
            except Exception:
                pass
            return order

        # Multileg: parent Order + one child per leg.
        underlying = getattr(response, "underlying_symbol", None) or (
            getattr(self._leg_to_asset(legs[0]), "symbol", "") or ""
        )
        parent_asset = Asset(symbol=underlying, asset_type=Asset.AssetType.STOCK)
        parent = Order(
            identifier=identifier,
            asset=parent_asset,
            strategy=strategy_name,
            order_class=Order.OrderClass.MULTILEG,
            order_type=order_type,
            limit_price=price,
            time_in_force=tif,
            status=status,
        )
        for leg in legs:
            asset = self._leg_to_asset(leg)
            if asset is None:
                continue
            qty = getattr(leg, "quantity", None) or 0
            side = self._leg_to_lumi_side(leg, is_option=(asset.asset_type == Asset.AssetType.OPTION))
            child = Order(
                identifier=identifier,  # Tastytrade leg has no separate id
                asset=asset,
                strategy=strategy_name,
                quantity=Decimal(str(qty)),
                side=side,
                order_type=order_type,
                status=status,
            )
            child.parent_identifier = identifier
            parent.add_child_order(child)
        try:
            parent.update_raw(response)
        except Exception:
            pass
        return parent

    def _pull_broker_order(self, identifier: str) -> Optional[Any]:
        if not identifier:
            return None
        try:
            return self._run(self._account.get_order(self._session, identifier))
        except Exception as e:
            logger.error(colored(
                f"[Tastytrade] get_order({identifier}) failed: {e}", "red",
            ))
            return None

    def _pull_broker_all_orders(self) -> list:
        """Return all live orders. Filled/cancelled history can be fetched
        separately via ``get_order_history`` if a strategy needs it."""
        try:
            return list(self._run(self._account.get_live_orders(self._session)) or [])
        except Exception as e:
            logger.error(colored(
                f"[Tastytrade] get_live_orders failed: {e}", "red",
            ))
            return []

    # ------------------------------------------------------------------
    # Stream / polling
    # ------------------------------------------------------------------
    # Tastytrade *does* expose a websocket (``AlertStreamer`` for account
    # events, ``DXLinkStreamer`` for quotes). For this milestone we use
    # polling, matching what Tradier does — it's simpler, hits the same
    # SDK methods we already exercise, and avoids holding a long-lived
    # async websocket from a sync-shaped broker. Native streaming is a
    # follow-up.
    def _get_stream_object(self):
        return PollingStream(self.polling_interval)

    def _register_stream_events(self):
        broker = self

        @broker.stream.add_action(broker.POLL_EVENT)
        def on_poll():
            try:
                broker.do_polling()
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_new(order):
            try:
                broker._process_trade_event(order, broker.NEW_ORDER)
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_fill(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=getattr(order.asset, "multiplier", 1),
                )
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_cancel(order):
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_error(order, error_msg):
            try:
                if order.is_active() and order.child_orders:
                    for child in order.child_orders:
                        child.set_error(error_msg)
                        broker._process_trade_event(child, broker.ERROR_ORDER)
                broker._process_trade_event(order, broker.ERROR_ORDER)
                order.set_error(error_msg)
            except Exception:
                logger.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        try:
            self.stream._run()
        except Exception as e:
            logger.error(colored(
                f"[Tastytrade] polling stream crashed: {e}", "red",
            ))

    # ------------------------------------------------------------------
    # Polling implementation
    # ------------------------------------------------------------------
    @staticmethod
    def _avg_fill_from_legs(placed) -> Optional[Decimal]:
        """Compute size-weighted average fill price from a PlacedOrder's legs."""
        legs = list(getattr(placed, "legs", []) or [])
        total_qty = Decimal(0)
        total_value = Decimal(0)
        for leg in legs:
            for fill in (getattr(leg, "fills", None) or []):
                qty = Decimal(str(getattr(fill, "quantity", 0) or 0))
                price = Decimal(str(getattr(fill, "fill_price", 0) or 0))
                total_qty += qty
                total_value += qty * price
        if total_qty <= 0:
            return None
        return total_value / total_qty

    @staticmethod
    def _filled_qty_from_legs(placed) -> Optional[Decimal]:
        legs = list(getattr(placed, "legs", []) or [])
        total = Decimal(0)
        any_fill = False
        for leg in legs:
            for fill in (getattr(leg, "fills", None) or []):
                total += Decimal(str(getattr(fill, "quantity", 0) or 0))
                any_fill = True
        if not any_fill:
            return None
        # For multileg, this sums leg fills. For single-leg, it's the leg's
        # filled quantity directly.
        return total if len(legs) == 1 else total / Decimal(len(legs))

    def do_polling(self):
        """Poll Tastytrade for live orders, dispatch transitions to the stream.

        Mirrors Tradier's polling shape: pull live orders, parse, compare
        against tracked Lumibot orders, dispatch NEW / FILLED / CANCELED /
        ERROR events as the broker-side status moves.
        """
        # Sync positions so the strategy sees fresh holdings.
        try:
            self.sync_positions(None)
        except Exception:
            logger.error(traceback.format_exc())

        raw_orders = self._pull_broker_all_orders()
        stored_orders = {x.identifier: x for x in self.get_all_orders()}

        strategy_name = self._strategy_name
        if not strategy_name and len(self._subscribers) == 1:
            strategy_name = self._subscribers[0].name

        broker_ids = set()
        for placed in raw_orders or []:
            parsed = self._parse_broker_order(placed, strategy_name=strategy_name)
            if parsed is None:
                continue
            if parsed.identifier:
                broker_ids.add(parsed.identifier)

            for order in [*parsed.child_orders, parsed]:
                if not order.identifier:
                    continue

                if order.identifier not in stored_orders:
                    # First time we see this order. On startup, only ingest
                    # active orders to avoid OOM on long broker histories.
                    if self._first_iteration and not (
                        order.is_active() or order.status == Order.OrderStatus.NEW
                    ):
                        continue
                    self._process_new_order(order)
                    continue

                stored = stored_orders[order.identifier]
                stored.quantity = order.quantity or stored.quantity

                if order.equivalent_status(stored):
                    stored.status = order.status
                    continue

                status = (order.status or "").lower()
                # Dispatch the transition AND update stored.status synchronously
                # for terminal states. The dispatch enqueues an event that
                # the stream worker processes asynchronously, so without the
                # synchronous update the next poll's "missing from broker_ids"
                # check would still see is_active() == True and re-dispatch
                # CANCELED on top of an already-FILLED order.
                if status in ("submitted", "open"):
                    self._safe_stream_dispatch(self.NEW_ORDER, order=stored)
                elif status == "fill":
                    fill_price = self._avg_fill_from_legs(placed)
                    fill_qty = self._filled_qty_from_legs(placed) or order.quantity
                    if fill_price is not None and fill_qty is not None:
                        self._safe_stream_dispatch(
                            self.FILLED_ORDER,
                            order=stored,
                            price=fill_price,
                            filled_quantity=fill_qty,
                        )
                        stored.status = Order.OrderStatus.FILLED
                elif status == "canceled":
                    self._safe_stream_dispatch(self.CANCELED_ORDER, order=stored)
                    stored.status = Order.OrderStatus.CANCELED
                elif status == "error":
                    msg = getattr(placed, "reject_reason", None) or (
                        f"Tastytrade rejected order {order.identifier}"
                    )
                    self._safe_stream_dispatch(
                        self.ERROR_ORDER, order=stored, error_msg=msg,
                    )
                    stored.status = Order.OrderStatus.ERROR
                # 'partial_fill' deliberately not dispatched: polling can
                # easily miss partials; only complete fills are reliable.

        # Tracked locally but no longer reported by broker → likely cancelled.
        tracked = {x.identifier: x for x in self.get_tracked_orders()}
        for oid, order in tracked.items():
            if oid and oid not in broker_ids and order.is_active():
                logger.debug(
                    f"[Tastytrade] order {oid} no longer at broker; "
                    f"dispatching as cancelled."
                )
                self._safe_stream_dispatch(self.CANCELED_ORDER, order=order)

        if self._first_iteration:
            self._first_iteration = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __del__(self):
        bridge = getattr(self, "_async_bridge", None)
        if bridge is not None:
            try:
                bridge.close()
            except Exception:
                pass
