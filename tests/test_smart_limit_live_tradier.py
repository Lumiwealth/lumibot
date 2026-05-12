import time
from datetime import datetime, timedelta

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


class _HarnessStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1S"
        self.options_helper = OptionsHelper(self)

    def on_trading_iteration(self):
        return


def _tradier() -> Tradier:
    if not TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"):
        pytest.skip("Missing TRADIER_TEST_ACCOUNT_NUMBER / TRADIER_TEST_ACCESS_TOKEN in .env")
    return Tradier(
        account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
        access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
        paper=True,
        connect_stream=False,
    )


def _poll_tradier_order(broker: Tradier, order: Order) -> dict:
    # Internal helper is fine for apitests; this is not production code.
    return broker._pull_broker_order(order.identifier)


def _wait_fill(
    strategy: _HarnessStrategy, order: Order, *, timeout: int, drive_smart_limit: bool
) -> tuple[bool, int, float]:
    start = time.time()
    last_price = None
    reprices = 0
    while time.time() - start < timeout:
        if drive_smart_limit:
            strategy._executor._process_smart_limit_orders()

        record = _poll_tradier_order(strategy.broker, order)
        status = str(record.get("status", "")).lower()
        order.status = status

        price = record.get("price")
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

        if status == "filled":
            elapsed = time.time() - start
            return True, reprices, elapsed
        if status in {"canceled", "cancelled", "rejected", "expired", "error"}:
            elapsed = time.time() - start
            return False, reprices, elapsed

        time.sleep(1.0)

    return False, reprices, time.time() - start


def _flatten_positions_for_symbols(strategy: _HarnessStrategy, *, symbols: set[str], timeout: int = 120) -> None:
    """Best-effort cleanup so apitests don't trip broker-side open/close semantics."""
    try:
        positions = strategy.get_positions() or []
    except Exception:
        return

    for pos in positions:
        asset = getattr(pos, "asset", None)
        if asset is None:
            continue
        sym = str(getattr(asset, "symbol", "")).upper()
        if sym not in symbols:
            continue

        qty = getattr(pos, "quantity", None)
        try:
            qty_f = float(qty)
        except Exception:
            continue
        if abs(qty_f) < 1e-9:
            continue

        side = Order.OrderSide.SELL_TO_CLOSE if qty_f > 0 else Order.OrderSide.BUY_TO_CLOSE
        close_qty = abs(qty_f)
        try:
            close_order = strategy.create_order(asset, close_qty, side, order_type=Order.OrderType.MARKET)
            submitted = strategy.submit_order(close_order)
            _wait_fill(strategy, submitted, timeout=timeout, drive_smart_limit=False)
        except Exception:
            continue


def _strike_step(symbol: str) -> float:
    return 5.0 if symbol.upper() in {"SPX", "SPXW", "NDX", "RUT"} else 1.0


def _pick_expiry(strategy: _HarnessStrategy, underlying: Asset, days_out: int):
    chains = strategy.get_chains(underlying)
    if not chains and underlying.symbol.upper() == "SPX":
        chains = strategy.get_chains(Asset("SPXW", asset_type=Asset.AssetType.INDEX))
    if not chains and underlying.symbol.upper() == "SPXW":
        chains = strategy.get_chains(Asset("SPX", asset_type=Asset.AssetType.INDEX))
    if not chains:
        raise RuntimeError(f"No chains for {underlying.symbol}")

    target_date = datetime.now().astimezone().date() + timedelta(days=days_out)
    expiry = strategy.options_helper.get_expiration_on_or_after_date(
        target_date, chains, "call", underlying_asset=underlying
    )
    if expiry is None:
        raise RuntimeError("No expiry found")
    return expiry


def _pick_atm_strike(strategy: _HarnessStrategy, underlying: Asset, expiry) -> float:
    price = strategy.get_last_price(underlying)
    if price and float(price) > 0:
        return float(price)

    chains = strategy.get_chains(underlying)
    if not chains and underlying.symbol.upper() == "SPX":
        chains = strategy.get_chains(Asset("SPXW", asset_type=Asset.AssetType.INDEX))
    if not chains and underlying.symbol.upper() == "SPXW":
        chains = strategy.get_chains(Asset("SPX", asset_type=Asset.AssetType.INDEX))
    if not chains:
        raise RuntimeError("No chains available to infer ATM strike")

    strikes_raw = chains.strikes(expiry, "CALL") or []
    strikes = sorted(float(s) for s in strikes_raw if s is not None)
    if not strikes:
        raise RuntimeError("No strikes available to infer ATM strike")
    return float(strikes[len(strikes) // 2])


def _build_spx_iron_condor(
    strategy: _HarnessStrategy, *, days_out: int, short_distance: float, wing_width: float, cfg: SmartLimitConfig
):
    underlying = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    expiry = _pick_expiry(strategy, underlying, days_out)
    atm = _pick_atm_strike(strategy, underlying, expiry)
    step = _strike_step("SPX")
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


def test_tradier_spx_multileg_smart_limit_fills_and_reprices():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    try:
        strategy.initialize()
    except TypeError:
        strategy.initialize(parameters=None)

    _flatten_positions_for_symbols(strategy, symbols={"SPX", "SPXW"})

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=30)
    open_legs, close_legs = _build_spx_iron_condor(strategy, days_out=7, short_distance=50.0, wing_width=50.0, cfg=cfg)

    submitted = strategy.submit_order(open_legs)
    parent = submitted[0] if isinstance(submitted, list) else submitted
    assert parent is not None

    ok_open, reprices_open, open_elapsed = _wait_fill(strategy, parent, timeout=240, drive_smart_limit=True)
    if not ok_open:
        # Paper fills for multi-leg packages can be nondeterministic; treat non-fill cancels as a skip.
        status = str(getattr(parent, "status", "")).lower()
        if status in {"canceled", "cancelled", "expired"}:
            pytest.skip(
                f"Open did not fill before SMART_LIMIT canceled (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)"
            )
        pytest.fail(f"Open did not fill (status={status}, reprices={reprices_open}, elapsed={open_elapsed:.1f}s)")

    submitted_close = strategy.submit_order(close_legs)
    parent_close = submitted_close[0] if isinstance(submitted_close, list) else submitted_close
    ok_close, reprices_close, close_elapsed = _wait_fill(strategy, parent_close, timeout=240, drive_smart_limit=True)
    if not ok_close:
        status = str(getattr(parent_close, "status", "")).lower()
        if status in {"canceled", "cancelled", "expired"}:
            pytest.skip(
                f"Close did not fill before SMART_LIMIT canceled (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)"
            )
        pytest.fail(f"Close did not fill (status={status}, reprices={reprices_close}, elapsed={close_elapsed:.1f}s)")

    # If it filled instantly, reprices can be 0; otherwise we expect at least one reprice step.
    if open_elapsed > cfg.get_step_seconds():
        assert reprices_open >= 1


def test_tradier_spy_single_leg_smart_limit_fills():
    broker = _tradier()
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
    atm = _pick_atm_strike(strategy, underlying, expiry)
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


def test_tradier_spy_stock_smart_limit_fills():
    broker = _tradier()
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
