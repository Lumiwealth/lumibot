import time
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy


pytestmark = [pytest.mark.apitest, pytest.mark.alpaca]


class _HarnessStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"
        self.options_helper = OptionsHelper(self)

    def on_trading_iteration(self):
        return


def _alpaca() -> Alpaca:
    # Never turn an invalid isolated test key into trades on the default account.
    cfg = dict(ALPACA_TEST_CONFIG)
    if not cfg.get("API_KEY") or not cfg.get("API_SECRET"):
        pytest.skip("Missing dedicated ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET")
    cfg["PAPER"] = True
    # Bind quotes to the same isolated connection, not ambient DATA_SOURCE.
    broker = Alpaca(cfg, data_source=AlpacaData(cfg), connect_stream=False)
    account = broker.api.get_account()
    if getattr(account, "trading_blocked", False):
        pytest.fail("The dedicated paper test account cannot trade")
    from alpaca.trading.requests import GetOrdersRequest
    if broker.api.get_all_positions() or broker.api.get_orders(filter=GetOrdersRequest(status="open")):
        pytest.skip("SmartLimit API qualification requires an isolated initially flat test account")
    return broker


def _poll_alpaca_order(broker: Alpaca, order: Order):
    return broker.api.get_order_by_id(order.identifier)


def _confirmed_crypto_remainder(broker: Alpaca, order: Order, maximum: Decimal) -> Decimal:
    """Test-only continuation after a confirmed, partially filled cancellation.

    A priced order need not fill in a moving market. Never overlap its recovery
    with a still-working order or sell more than this invocation's remainder.
    """
    from alpaca.trading.requests import GetOrdersRequest

    confirmed = broker.api.get_order_by_id(order.identifier)
    assert str(confirmed.id) == str(order.identifier), "Cancellation identity changed"
    status = str(getattr(confirmed.status, "value", confirmed.status)).lower()
    assert status == "canceled", "Close recovery requires confirmed cancellation"
    assert not broker.api.get_orders(filter=GetOrdersRequest(status="open")), "Close recovery has active orders"
    requested = Decimal(str(confirmed.qty))
    filled = Decimal(str(confirmed.filled_qty))
    assert 0 <= filled <= requested <= maximum, "Unexpected close execution quantity"
    positions = broker.api.get_all_positions() or []
    position = next((p for p in positions if str(p.symbol).upper().replace("/", "") == "BTCUSD"), None)
    if position is None:
        return Decimal("0")
    available = Decimal(str(position.qty_available))
    quantity = Decimal(str(position.qty))
    assert 0 <= available <= quantity, "Unexpected remaining crypto position"
    return min(requested - filled, quantity, available)


def _wait_fill(strategy: _HarnessStrategy, order: Order, *, timeout: int, drive_smart_limit: bool) -> tuple[bool, int, float]:
    start = time.time()
    last_price = None
    reprices = 0
    while time.time() - start < timeout:
        if drive_smart_limit:
            strategy._executor._process_smart_limit_orders()

        # Use the real public accessor/reconciliation path. Copying raw status
        # here used to let this test pass with broken lifecycle callbacks.
        observed = strategy.get_order(order.identifier, broker_refresh=True, broker_refresh_ttl_seconds=1)
        assert observed is not None
        status = str(observed.status).lower()
        price = observed.limit_price
        if price is not None:
            try:
                price = float(price)
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
    expiry = strategy.options_helper.get_expiration_on_or_after_date(target_date, chains, "call", underlying_asset=underlying)
    if expiry is None:
        raise RuntimeError("No expiry found")
    return expiry


def _pick_atm(strategy: _HarnessStrategy, underlying: Asset) -> float:
    price = strategy.get_last_price(underlying)
    if price and float(price) > 0:
        return float(price)
    raise RuntimeError("Underlying price unavailable")


def _build_spy_iron_condor(strategy: _HarnessStrategy, *, days_out: int, short_distance: float, wing_width: float, cfg: SmartLimitConfig):
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
    call_short_asset = strategy.options_helper.find_next_valid_option(underlying, call_short, expiry, put_or_call="call")
    call_long_asset = strategy.options_helper.find_next_valid_option(underlying, call_long, expiry, put_or_call="call")

    assert put_short_asset and put_long_asset and call_short_asset and call_long_asset

    open_legs = [
        strategy.create_order(put_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(put_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(call_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(call_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
    ]
    close_legs = [
        strategy.create_order(put_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(put_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(call_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
        strategy.create_order(call_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg),
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
    qty = Decimal("0.001")
    owned_orders = []

    try:
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
        owned_orders.append(submitted_open)
        ok_open, reprices_open, open_elapsed = _wait_fill(strategy, submitted_open, timeout=240, drive_smart_limit=True)
        assert ok_open, f"BTCUSD buy did not fill (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)"

        # Alpaca crypto can apply fees/rounding such that the filled base quantity is slightly less than requested.
        # The initially flat account and this test's maximum bought quantity
        # bound cleanup. Never sell an unrelated account-wide BTC balance.
        positions = broker.api.get_all_positions() or []
        position = next((p for p in positions if str(p.symbol).upper().replace("/", "") == "BTCUSD"), None)
        assert position is not None, "Filled test buy has no independently visible broker position"
        close_qty = min(qty, Decimal(str(position.qty)))
        assert close_qty > 0

        close_order = strategy.create_order(
            base,
            close_qty,
            Order.OrderSide.SELL,
            order_type=Order.OrderType.SMART_LIMIT,
            smart_limit=cfg,
            quote=quote,
        )
        submitted_close = strategy.submit_order(close_order)
        owned_orders.append(submitted_close)
        ok_close, reprices_close, close_elapsed = _wait_fill(strategy, submitted_close, timeout=240, drive_smart_limit=True)
        for _ in range(2):
            if ok_close:
                break
            # Normal limit execution can cancel an unfilled remainder. Prove a
            # bounded next-iteration recovery, never a blind overlapping sell.
            close_qty = _confirmed_crypto_remainder(broker, submitted_close, close_qty)
            if close_qty == 0:
                ok_close = True
                break
            recovery = strategy.create_order(
                base, close_qty, Order.OrderSide.SELL,
                order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg, quote=quote,
            )
            submitted_close = strategy.submit_order(recovery)
            owned_orders.append(submitted_close)
            ok_close, reprices, elapsed = _wait_fill(strategy, submitted_close, timeout=240, drive_smart_limit=True)
            reprices_close += reprices
            close_elapsed += elapsed
        assert ok_close, f"BTCUSD smart close did not finish its bounded recovery (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)"
        assert not broker.api.get_all_positions(), "Test-owned crypto remained after the close"

        if open_elapsed > cfg.get_step_seconds():
            assert reprices_open >= 1
        if close_elapsed > cfg.get_step_seconds():
            assert reprices_close >= 1
    finally:
        # Only orders created by this invocation may be canceled in cleanup.
        for owned in owned_orders:
            if owned is not None and owned.is_active():
                strategy.cancel_order(owned)
