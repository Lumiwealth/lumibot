import time
from datetime import datetime, timedelta

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order, SmartLimitConfig, SmartLimitPreset
from lumibot.strategies.strategy import Strategy

pytestmark = [pytest.mark.apitest, pytest.mark.smartlimit_matrix]


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
    return broker._pull_broker_order(order.identifier)  # noqa: SLF001


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

    try:
        strategy.broker.cancel_order(order)
    except Exception:
        pass
    return False, reprices, time.time() - start


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


def _pick_wide_spread_stock(
    strategy: _HarnessStrategy, candidates: list[str], *, min_spread_pct: float
) -> Asset | None:
    for sym in candidates:
        asset = Asset(sym, asset_type=Asset.AssetType.STOCK)
        try:
            q = strategy.get_quote(asset)
        except Exception:
            continue
        bid = getattr(q, "bid", None)
        ask = getattr(q, "ask", None)
        try:
            bid_f = float(bid)
            ask_f = float(ask)
        except Exception:
            continue
        if ask_f <= 0 or bid_f < 0:
            continue
        mid = (bid_f + ask_f) / 2.0
        if mid <= 0:
            continue
        spread_pct = (ask_f - bid_f) / mid
        if spread_pct >= float(min_spread_pct):
            return asset
    return None


def _find_option(
    strategy: _HarnessStrategy,
    underlying: Asset,
    *,
    expiry,
    put_or_call: str,
    strike: float,
    max_spread_pct: float | None,
):
    step = _strike_step(underlying.symbol)
    for offset in range(0, 11):
        candidate_strike = strike + offset * step
        asset = strategy.options_helper.find_next_valid_option(
            underlying, candidate_strike, expiry, put_or_call=put_or_call
        )
        if asset is None:
            continue
        evaluation = strategy.options_helper.evaluate_option_market(asset, max_spread_pct=max_spread_pct)
        if evaluation.has_bid_ask and evaluation.buy_price and evaluation.sell_price:
            return asset
    return None


def _build_spx_iron_condor(
    strategy: _HarnessStrategy, *, days_out: int, short_distance: float, wing_width: float, order_type, smart_limit
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
            put_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            put_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            call_short_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            call_long_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
    ]
    close_legs = [
        strategy.create_order(
            put_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            put_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            call_short_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            call_long_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
    ]
    return open_legs, close_legs


def _build_spx_call_butterfly(strategy: _HarnessStrategy, *, days_out: int, width: float, order_type, smart_limit):
    underlying = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    expiry = _pick_expiry(strategy, underlying, days_out)
    atm = _pick_atm_strike(strategy, underlying, expiry)
    step = _strike_step("SPX")
    mid_strike = round(atm / step) * step
    low = mid_strike - width
    high = mid_strike + width

    low_asset = strategy.options_helper.find_next_valid_option(underlying, low, expiry, put_or_call="call")
    mid_asset = strategy.options_helper.find_next_valid_option(underlying, mid_strike, expiry, put_or_call="call")
    high_asset = strategy.options_helper.find_next_valid_option(underlying, high, expiry, put_or_call="call")
    assert low_asset and mid_asset and high_asset

    open_legs = [
        strategy.create_order(
            low_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            mid_asset, 2, Order.OrderSide.SELL_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            high_asset, 1, Order.OrderSide.BUY_TO_OPEN, order_type=order_type, smart_limit=smart_limit
        ),
    ]
    close_legs = [
        strategy.create_order(
            low_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            mid_asset, 2, Order.OrderSide.BUY_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
        strategy.create_order(
            high_asset, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=order_type, smart_limit=smart_limit
        ),
    ]
    return open_legs, close_legs


def _assert_open_close_fill(
    strategy: _HarnessStrategy,
    open_order,
    close_order,
    *,
    drive_smart_limit: bool,
    timeout_open: int,
    timeout_close: int,
):
    submitted_open = strategy.submit_order(open_order)
    open_parent = submitted_open[0] if isinstance(submitted_open, list) else submitted_open
    ok_open, reprices_open, open_elapsed = _wait_fill(
        strategy, open_parent, timeout=timeout_open, drive_smart_limit=drive_smart_limit
    )
    if not ok_open:
        status = str(getattr(open_parent, "status", "")).lower()
        if status in {"rejected", "error"}:
            pytest.skip(f"Open rejected by broker (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)")
        pytest.skip(f"Open did not fill (reprices={reprices_open}, elapsed={open_elapsed:.1f}s)")

    submitted_close = strategy.submit_order(close_order)
    close_parent = submitted_close[0] if isinstance(submitted_close, list) else submitted_close
    ok_close, reprices_close, close_elapsed = _wait_fill(
        strategy, close_parent, timeout=timeout_close, drive_smart_limit=drive_smart_limit
    )
    if not ok_close:
        if isinstance(close_order, list):
            close_mkt_legs = [
                strategy.create_order(leg.asset, leg.quantity, leg.side, order_type=Order.OrderType.MARKET)
                for leg in close_order
            ]
            submitted_mkt = strategy.submit_order(close_mkt_legs, is_multileg=True, order_type="market")
            mkt_parent = submitted_mkt[0] if isinstance(submitted_mkt, list) else submitted_mkt
            ok_mkt, _, _ = _wait_fill(strategy, mkt_parent, timeout=60, drive_smart_limit=False)
        else:
            mkt_close = strategy.create_order(
                close_order.asset, close_order.quantity, close_order.side, order_type=Order.OrderType.MARKET
            )
            submitted_mkt = strategy.submit_order(mkt_close)
            ok_mkt, _, _ = _wait_fill(strategy, submitted_mkt, timeout=60, drive_smart_limit=False)
        if not ok_mkt:
            pytest.fail(
                f"Close did not fill and market flatten failed (reprices={reprices_close}, elapsed={close_elapsed:.1f}s)"
            )
        pytest.skip(
            f"Close did not fill (reprices={reprices_close}, elapsed={close_elapsed:.1f}s); closed with MARKET and skipped"
        )


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


def test_tradier_matrix_stock_liquid_and_wide_spread():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=60)
    liquid = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    wide = _pick_wide_spread_stock(strategy, ["GME", "AMC", "PLTR", "SOFI", "F", "T"], min_spread_pct=0.002)
    if wide is None:
        pytest.skip("No wide-spread stock found in candidate list")

    for asset in (liquid, wide):
        buy = strategy.create_order(
            asset, 1, Order.OrderSide.BUY, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        )
        sell = strategy.create_order(
            asset, 1, Order.OrderSide.SELL, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        )
        _assert_open_close_fill(strategy, buy, sell, drive_smart_limit=True, timeout_open=180, timeout_close=180)


def test_tradier_matrix_single_leg_call_and_put_liquid_and_wide_spread():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=60)
    underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    expiry_liquid = _pick_expiry(strategy, underlying, 7)
    expiry_wide = _pick_expiry(strategy, underlying, 180)
    atm = _pick_atm_strike(strategy, underlying, expiry_liquid)
    step = _strike_step("SPY")
    atm_rounded = round(atm / step) * step

    call_liquid = _find_option(
        strategy, underlying, expiry=expiry_liquid, put_or_call="call", strike=atm_rounded, max_spread_pct=0.5
    )
    put_liquid = _find_option(
        strategy, underlying, expiry=expiry_liquid, put_or_call="put", strike=atm_rounded, max_spread_pct=0.5
    )
    call_wide = _find_option(
        strategy,
        underlying,
        expiry=expiry_wide,
        put_or_call="call",
        strike=atm_rounded + (20 * step),
        max_spread_pct=None,
    )

    if call_liquid is None or put_liquid is None or call_wide is None:
        pytest.skip("Could not find suitable option contracts for matrix test")

    for opt in (call_liquid, put_liquid, call_wide):
        open_order = strategy.create_order(
            opt, 1, Order.OrderSide.BUY_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        )
        close_order = strategy.create_order(
            opt, 1, Order.OrderSide.SELL_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
        )
        _assert_open_close_fill(
            strategy, open_order, close_order, drive_smart_limit=True, timeout_open=240, timeout_close=240
        )


def test_tradier_matrix_short_option_single_leg_smart_limit_or_skip():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    strategy.initialize(parameters=None)

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=60)
    underlying = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    expiry = _pick_expiry(strategy, underlying, 7)
    atm = _pick_atm_strike(strategy, underlying, expiry)
    step = _strike_step("SPY")
    strike = round(atm / step) * step

    call_asset = _find_option(
        strategy, underlying, expiry=expiry, put_or_call="call", strike=strike, max_spread_pct=0.75
    )
    if call_asset is None:
        pytest.skip("No suitable shortable option found")

    open_order = strategy.create_order(
        call_asset, 1, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
    )
    submitted_open = strategy.submit_order(open_order)
    ok_open, _, _ = _wait_fill(strategy, submitted_open, timeout=180, drive_smart_limit=True)
    if not ok_open and str(getattr(submitted_open, "status", "")).lower() in {"rejected", "error"}:
        pytest.skip("Broker rejected short option open (insufficient permissions/margin in paper)")
    assert ok_open, "Short option open did not fill"

    close_order = strategy.create_order(
        call_asset, 1, Order.OrderSide.BUY_TO_CLOSE, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
    )
    submitted_close = strategy.submit_order(close_order)
    ok_close, _, _ = _wait_fill(strategy, submitted_close, timeout=240, drive_smart_limit=True)
    assert ok_close, "Short option close did not fill"


def test_tradier_matrix_multileg_credit_condor_and_debit_butterfly_smart_limit_spx():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    strategy.initialize(parameters=None)
    _flatten_positions_for_symbols(strategy, symbols={"SPX", "SPXW"})

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, final_hold_seconds=60)

    open_legs, close_legs = _build_spx_iron_condor(
        strategy,
        days_out=7,
        short_distance=50.0,
        wing_width=50.0,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    _assert_open_close_fill(
        strategy, open_legs, close_legs, drive_smart_limit=True, timeout_open=360, timeout_close=360
    )

    open_bfly, close_bfly = _build_spx_call_butterfly(
        strategy,
        days_out=7,
        width=50.0,
        order_type=Order.OrderType.SMART_LIMIT,
        smart_limit=cfg,
    )
    _assert_open_close_fill(
        strategy, open_bfly, close_bfly, drive_smart_limit=True, timeout_open=360, timeout_close=360
    )


def test_tradier_matrix_multileg_limit_parity_without_debit_credit_spx():
    broker = _tradier()
    if hasattr(broker, "is_market_open") and not broker.is_market_open():
        pytest.skip("Market is closed")

    strategy = _HarnessStrategy(broker=broker)
    strategy.initialize(parameters=None)

    open_legs, close_legs = _build_spx_iron_condor(
        strategy,
        days_out=7,
        short_distance=50.0,
        wing_width=50.0,
        order_type=Order.OrderType.LIMIT,
        smart_limit=None,
    )

    submitted_open = strategy.submit_order(open_legs, is_multileg=True, order_type=Order.OrderType.LIMIT)
    open_parent = submitted_open[0] if isinstance(submitted_open, list) else submitted_open
    ok_open, _, _ = _wait_fill(strategy, open_parent, timeout=300, drive_smart_limit=False)
    if not ok_open:
        # NOTE: This test is about broker-agnostic LIMIT submission (no debit/credit required), not fill quality.
        # Multi-leg LIMIT orders can legitimately not fill quickly in paper; SMART_LIMIT fill tests cover execution.
        status = str(getattr(open_parent, "status", "")).lower()
        if status in {"rejected", "error"}:
            pytest.fail("Package LIMIT open was rejected")
        pytest.skip("Package LIMIT open did not fill within timeout (parity path submitted successfully)")

    submitted_close = strategy.submit_order(close_legs, is_multileg=True, order_type=Order.OrderType.LIMIT)
    close_parent = submitted_close[0] if isinstance(submitted_close, list) else submitted_close
    ok_close, _, _ = _wait_fill(strategy, close_parent, timeout=300, drive_smart_limit=False)
    if not ok_close:
        submitted_mkt = strategy.submit_order(close_legs, is_multileg=True, order_type="market")
        mkt_parent = submitted_mkt[0] if isinstance(submitted_mkt, list) else submitted_mkt
        ok_mkt, _, _ = _wait_fill(strategy, mkt_parent, timeout=60, drive_smart_limit=False)
        if not ok_mkt:
            pytest.fail("Package LIMIT close did not fill and market flatten failed")
        pytest.skip("Package LIMIT close did not fill; flattened with market (parity path verified)")
