"""
Tastytrade broker for Lumibot.

Wraps the unofficial ``tastytrade`` Python SDK (https://github.com/tastyware/tastytrade)
which exposes a fully asynchronous API. Lumibot's :class:`Broker` abstract
methods are synchronous, so this adapter owns a dedicated asyncio event
loop running on a background daemon thread and dispatches every SDK call
through ``asyncio.run_coroutine_threadsafe``.

Initial commit scope (intentionally narrow, follow-ups will fill in the rest):

- Authentication via OAuth (``provider_secret`` + ``refresh_token``)
- Account selection by account number
- Real implementations: ``_get_balances_at_broker``, ``_pull_positions``,
  ``_pull_position``, ``cancel_order``
- Logged-stub implementations: ``_submit_order``, ``_submit_orders``
  (multileg), ``_modify_order``, ``_parse_broker_order``,
  ``_pull_broker_order``, ``_pull_broker_all_orders``
- Streaming: returns ``None`` from ``_get_stream_object`` and no-ops the
  register / run methods. A follow-up will plug in either polling (similar
  to Tradier) or the SDK's ``AlertStreamer`` / ``DXLinkStreamer``.
"""

import asyncio
import os
import threading
from decimal import Decimal
from typing import Any, Awaitable, List, Optional, TypeVar, Union

from termcolor import colored

from .broker import Broker
from lumibot.data_sources.tastytrade_data import TastytradeData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

try:  # tastytrade is an optional runtime dep; surface a clear error if missing.
    from tastytrade import Account as _TTAccount
    from tastytrade import Session as _TTSession
except Exception as _import_err:  # pragma: no cover - import-time guard
    _TTAccount = None
    _TTSession = None
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
        return future.result(timeout=timeout)

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
                is_test = bool(config.get("SANDBOX"))

        client_secret = client_secret or os.environ.get("TASTYTRADE_CLIENT_SECRET")
        refresh_token = refresh_token or os.environ.get("TASTYTRADE_REFRESH_TOKEN")
        account_number = account_number or os.environ.get("TASTYTRADE_ACCOUNT_NUMBER")
        if is_test is None:
            env_sandbox = os.environ.get("TASTYTRADE_SANDBOX", "")
            is_test = env_sandbox.strip().lower() in ("1", "true", "yes", "y")

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
    def _strategy_name(strategy) -> str:
        if strategy is None:
            return "Unknown"
        if isinstance(strategy, str):
            return strategy
        return getattr(strategy, "name", str(strategy))

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

        strategy_name = self._strategy_name(strategy)
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

    @staticmethod
    def _tt_position_to_asset(tp) -> Optional[Asset]:
        """Convert a Tastytrade position to a Lumibot Asset.

        Tastytrade exposes ``instrument_type`` (Equity, Equity Option, Future,
        Future Option, Cryptocurrency, ...) and ``symbol`` / ``underlying_symbol``.
        The full mapping (especially OCC option symbol parsing) will be done
        in the parser follow-up; for now we handle equities explicitly and
        log a warning for option/future/crypto positions so the user can see
        what's being skipped.
        """
        instrument = (getattr(tp, "instrument_type", "") or "").lower()
        symbol = getattr(tp, "symbol", None) or getattr(tp, "underlying_symbol", None)
        if not symbol:
            return None
        if instrument == "equity":
            return Asset(symbol=symbol, asset_type=Asset.AssetType.STOCK)
        logger.warning(colored(
            f"[Tastytrade] Skipping position with unhandled instrument_type "
            f"'{instrument}' for symbol {symbol}. Asset parsing will be "
            f"completed in a follow-up commit.",
            "yellow",
        ))
        return None

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    def _submit_order(self, order: Order) -> Optional[Order]:
        logger.error(colored(
            f"[Tastytrade] _submit_order is not yet implemented (order={order}).",
            "red",
        ))
        return None

    def _submit_orders(self, orders, is_multileg=False, order_type=None,
                       duration="day", price=None):
        logger.error(colored(
            "[Tastytrade] _submit_orders is not yet implemented "
            "(multileg path also pending).",
            "red",
        ))
        return None

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
        logger.error(colored(
            f"[Tastytrade] _modify_order is not yet implemented (order={order}).",
            "red",
        ))
        return None

    def _parse_broker_order(self, response: Any, strategy_name: str,
                            strategy_object=None) -> Optional[Order]:
        logger.error(colored(
            "[Tastytrade] _parse_broker_order is not yet implemented.",
            "red",
        ))
        return None

    def _pull_broker_order(self, identifier: str) -> Optional[dict]:
        logger.error(colored(
            f"[Tastytrade] _pull_broker_order({identifier}) is not yet implemented.",
            "red",
        ))
        return None

    def _pull_broker_all_orders(self) -> list:
        logger.error(colored(
            "[Tastytrade] _pull_broker_all_orders is not yet implemented.",
            "red",
        ))
        return []

    # ------------------------------------------------------------------
    # Streaming (deferred)
    # ------------------------------------------------------------------
    def _get_stream_object(self):
        logger.warning(colored(
            "[Tastytrade] _get_stream_object is not yet implemented; "
            "order events will not stream until a follow-up commit lands.",
            "yellow",
        ))
        return None

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

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
