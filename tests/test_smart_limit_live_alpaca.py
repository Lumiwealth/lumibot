import time
from datetime import datetime, timedelta

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import ALPACA_CONFIG, ALPACA_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


class _HarnessStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"
        self.options_helper = OptionsHelper(self)

    def on_trading_iteration(self):
        return


def _alpaca() -> Alpaca:
    # Prefer the dedicated test keys, but fall back to the default Alpaca config if the test keys are invalid.
    # This keeps apitests usable even if the paper test key pair was rotated.
    configs = [
        ("ALPACA_TEST_CONFIG", ALPACA_TEST_CONFIG),
        ("ALPACA_CONFIG", ALPACA_CONFIG),
    ]

    for label, cfg in configs:
        api_key = cfg.get("API_KEY")
        api_secret = cfg.get("API_SECRET")
        if not api_key or not api_secret or api_key == "<your key here>" or api_secret == "<your key here>":
            continue

        broker = Alpaca(cfg, connect_stream=False)
        try:
            broker.api.get_all_positions()
            return broker
        except Exception as exc:
            msg = str(exc).lower()
            if "unauthorized" in msg or "401" in msg:
                continue
            raise RuntimeError(f"Alpaca API failed for {label}: {exc}") from exc

    pytest.skip("Missing/invalid Alpaca credentials in .env (ALPACA_TEST_API_KEY/SECRET or ALPACA_API_KEY/SECRET)")


def _poll_alpaca_order(broker: Alpaca, order: Order):
    return broker.api.get_order_by_id(order.identifier)


def _cancel_alpaca_open_orders_for_symbol(broker: Alpaca, symbol: str) -> None:
    """Best-effort cleanup for Alpaca to avoid crypto wash-trade rejections in paper."""
    try:
        from alpaca.trading.enums import QueryOrderStatus
        from alpaca.trading.requests import GetOrdersRequest
    except Exception:
        return

    target = str(symbol).upper().replace("/", "")
    try:
        request = GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500)
        open_orders = broker.api.get_orders(filter=request) or []
    except Exception:
        return

    for raw in open_orders:
        raw_symbol = getattr(raw, "symbol", None)
        if raw_symbol is None and hasattr(raw, "_raw") and isinstance(raw._raw, dict):
            raw_symbol = raw._raw.get("symbol")
        if not raw_symbol:
            continue
        raw_norm = str(raw_symbol).upper().replace("/", "")
        if raw_norm != target:
            continue
        try:
            broker.api.cancel_order_by_id(getattr(raw, "id", None) or raw._raw.get("id"))  # noqa: SLF001
        except Exception:
            pass


def _wait_fill(
    strategy: _HarnessStrategy, order: Order, *, timeout: int, drive_smart_limit: bool
) -> tuple[bool, int, float]:
    start = time.time()
    last_price = None
    reprices = 0
    while time.time() - start < timeout:
        if drive_smart_limit:
            strategy._executor._process_smart_limit_orders()

        raw = _poll_alpaca_order(strategy.broker, order)
        raw_status = getattr(raw, "status", "")
        if hasattr(raw_status, "value"):
            raw_status = raw_status.value
        status = str(raw_status).lower()
        order.status = status

        price = getattr(raw, "limit_price", None)
        if price is not None:
            try:
                price = float(price)
                order.limit_price = price
                if last_price is None:
                    last_price = price
                elif abs(price - last_price) > 1e-9:
                    reprices += 1
                    last_price = price
            except Exception:
                pass

        if status in {"filled", "fill"}:
            elapsed = time.time() - start
            return True, reprices, elapsed
        if status in {"canceled", "cancelled", "rejected", "expired", "error"}:
            elapsed = time.time() - start
            return False, reprices, elapsed

        time.sleep(1.0)

    return False, reprices, time.time() - start


def _strike_step(symbol: str) -> float:
    return 1.0 if symbol.upper() == "SPY" else 5.0


def _pick_expiry(strategy: _HarnessStrategy, underlying: Asset, days_out: int):
    chains = strategy.get_chains(underlying)
    if not chains:
        raise RuntimeError(f"No chains for {underlying.symbol}")
    target_date = datetime.now().astimezone().date() + timedelta(days=days_out)
    expiry = strategy.options_helper.get_expiration_on_or_after_date(
        target_date, chains, "call", underlying_asset=underlying
    )
    if expiry is None:
        raise RuntimeError("No expiry found")
    return expiry


def _pick_atm(strategy: _HarnessStrategy, underlying: Asset) -> float:
    price = strategy.get_last_price(underlying)
    if price and float(price) > 0:
        return float(price)
    raise RuntimeError("Underlying price unavailable")


def _build_spy_iron_condor(
    strategy: _HarnessStrategy, *, days_out: int, short_distance: float, wing_width: float, cfg: SmartLimitConfig
):
    underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    expiry = _pick_expiry(strategy, underlying, days_out)
    atm = _pick_atm(strategy, underlying)
    step = _strike_step("SPY")
    atm_rounded = round(atm / step) * step

    put_short = atm_rounded - short_distance
    put_long = put_short - wing_width
    call_short = atm_rounded + short_distance
    call_long = call_short + wing_width

    put_short_asset = strategy.options_helper.find_next_valid_option(underlying, put_short, expiry, put_or_call="put")
    put_long_asset = strategy.options_helper.find_next_valid_option(underlying, put_long, expiry, put_or_call="put")
    call_short_asset = strategy.options_helper.find_next_valid_option(
        underlying, call_short, expiry, put_or_call="call"
    )
    call_long_asset = strategy.options_helper.find_next_valid_option(underlying, call_long, expiry, put_or_call="call")

    assert put_short_asset and put_long_asset and call_short_asset and call_long_asset

    open_legs = [
        strategy.create_order(
            put_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            put_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            call_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            call_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
    ]
    close_legs = [
        strategy.create_order(
            put_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            put_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            call_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
        strategy.create_order(
            call_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        ),
    ]
    return open_legs, close_legs


def test_alpaca_spy_multileg_smart_limit_fills_and_reprices():
    broker = _alpaca()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    open_legs, close_legs = _build_spy_iron_condor(strategy, days_out=7, short_distance=5.0, wing_width=5.0, cfg=cfg)

    submitted = strategy.submit_order(open_legs)
    parent = submitted[0] if isinstance(submitted, list) else submitted
    assert parent is not None

    ok_open, reprices_open, open_elapsed = _wait_fill(strategy, parent, timeout=240, drive_smart_limit=True)
    assert ok_open, f"Open did not fill (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)"

    submitted_close = strategy.submit_order(close_legs)
    parent_close = submitted_close[0] if isinstance(submitted_close, list) else submitted_close
    ok_close, reprices_close, close_elapsed = _wait_fill(strategy, parent_close, timeout=240, drive_smart_limit=True)
    assert ok_close, f"Close did not fill (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)"

    if open_elapsed > cfg.get_step_seconds():
        assert reprices_open >= 1


def test_alpaca_spy_single_leg_smart_limit_fills():
    broker = _alpaca()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    expiry = _pick_expiry(strategy, underlying, days_out=30)
    atm = _pick_atm(strategy, underlying)
    step = _strike_step("SPY")
    atm_rounded = round(atm / step) * step
    strike = atm_rounded + (10 * step)

    call_asset = strategy.options_helper.find_next_valid_option(underlying, strike, expiry, put_or_call="call")
    assert call_asset is not None

    open_order = strategy.create_order(
        call_asset,
        1,
        Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    submitted_open = strategy.submit_order(open_order)
    assert submitted_open is not None
    ok_open, reprices_open, open_elapsed = _wait_fill(strategy, submitted_open, timeout=180, drive_smart_limit=True)
    assert ok_open, f"Single-leg open did not fill (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)"

    close_order = strategy.create_order(
        call_asset,
        1,
        Order.OrderSide.SELL_TO_CLOSE,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    submitted_close = strategy.submit_order(close_order)
    ok_close, reprices_close, close_elapsed = _wait_fill(strategy, submitted_close, timeout=180, drive_smart_limit=True)
    assert ok_close, f"Single-leg close did not fill (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)"


def test_alpaca_spy_stock_smart_limit_fills():
    broker = _alpaca()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

    open_order = strategy.create_order(
        asset,
        1,
        Order.OrderSide.BUY,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    submitted_open = strategy.submit_order(open_order)
    assert submitted_open is not None
    ok_open, _, open_elapsed = _wait_fill(strategy, submitted_open, timeout=120, drive_smart_limit=True)
    assert ok_open, f"Stock buy did not fill (elapsed={open_elapsed:.1f}s)"

    close_order = strategy.create_order(
        asset,
        1,
        Order.OrderSide.SELL,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    submitted_close = strategy.submit_order(close_order)
    ok_close, _, close_elapsed = _wait_fill(strategy, submitted_close, timeout=120, drive_smart_limit=True)
    assert ok_close, f"Stock sell did not fill (elapsed={close_elapsed:.1f}s)"


def test_alpaca_crypto_btcusd_smart_limit_fills_24_7():
    broker = _alpaca()

    strategy = _HarnessStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, step_seconds=1, final_hold_seconds=180)
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    qty = 0.001

    try:
        _cancel_alpaca_open_orders_for_symbol(broker, "BTC/USD")

        open_order = strategy.create_order(
            base,
            qty,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=cfg,
            quote=quote,
        )
        submitted_open = strategy.submit_order(open_order)
        assert submitted_open is not None
        ok_open, reprices_open, open_elapsed = _wait_fill(strategy, submitted_open, timeout=240, drive_smart_limit=True)
        assert ok_open, f"BTCUSD buy did not fill (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)"

        # Alpaca crypto can apply fees/rounding such that the filled base quantity is slightly less than requested.
        # Close whatever we actually have available to avoid "insufficient balance" errors.
        close_qty = qty
        try:
            positions = broker.api.get_all_positions() or []
            for p in positions:
                sym = str(getattr(p, "symbol", "")).upper().replace("/", "")
                if sym.startswith(base.symbol.upper()):
                    close_qty = float(getattr(p, "qty", close_qty))
                    break
        except Exception:
            pass
        if close_qty <= 0:
            close_qty = qty

        close_order = strategy.create_order(
            base,
            close_qty,
            Order.OrderSide.SELL,
            order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=cfg,
            quote=quote,
        )
        submitted_close = strategy.submit_order(close_order)
        ok_close, reprices_close, close_elapsed = _wait_fill(
            strategy, submitted_close, timeout=240, drive_smart_limit=True
        )
        if not ok_close:
            market_close = strategy.create_order(
                base, qty, Order.OrderSide.SELL, order_type=Order.OrderType.MARKET, quote=quote
            )
            submitted_market = strategy.submit_order(market_close)
            ok_mkt, _, _ = _wait_fill(strategy, submitted_market, timeout=60, drive_smart_limit=False)
            assert ok_mkt, "BTCUSD market close fallback did not fill"
            pytest.fail(f"BTCUSD smart close did not fill (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)")

        if open_elapsed > cfg.get_step_seconds():
            assert reprices_open >= 1
        if close_elapsed > cfg.get_step_seconds():
            assert reprices_close >= 1
    finally:
        # Best-effort cleanup to avoid leaving open orders behind if the test fails mid-way.
        try:
            strategy.cancel_open_orders()
        except Exception:
            pass
