"""Directional trailing-stop state and its native backtest consumers."""

from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies.strategy import Strategy


@pytest.mark.parametrize("rate", [0.02, Decimal("0.02")])
@pytest.mark.parametrize("side", [side for side in Order.OrderSide if side != Order.OrderSide.UNKNOWN])
def test_percentage_trail_tracks_favorable_extreme(side, rate):
    """Exercise initialization, favorable moves and reversals for every side alias."""
    order = Order("ratchet", Asset("SPY"), 2, side, trail_percent=rate)
    buy_sides = {"buy", "buy_to_open", "buy_to_close", "buy_to_cover"}
    if str(side) in buy_sides:
        prices, expected = [100, 90, 95, 85, 86], [102, 91.8, 91.8, 86.7, 86.7]
    else:
        prices, expected = [100, 110, 109, 115, 114], [98, 107.8, 107.8, 112.7, 112.7]

    # These thresholds come from running price extrema and a 2% offset, rather
    # than reproducing the production side predicate under test.
    actual = []
    for price in prices:
        order.update_trail_stop_price(price)
        actual.append(order.get_current_trail_stop_price())
    assert actual == pytest.approx(expected)
    assert order.trail_percent is rate


@pytest.mark.parametrize("rate", [-2.0, -0.1, 0.0, 1.0, 2.0, "0.02"])
@pytest.mark.parametrize("side", ["buy", "sell"])
def test_percentage_trail_preserves_admitted_magnitudes(side, rate):
    """Retain native conversion and admission without adding a percentage guard."""
    order = Order("ratchet", Asset("SPY"), 2, side, trail_percent=rate)
    prices = [100, 90, 110, 95]
    multiplier = 1 + float(rate) if side == "buy" else 1 - float(rate)
    candidates = [price * multiplier for price in prices]
    # Out-of-range offsets can invert the relation to the market price. The
    # promised invariant is directional thresholds, not validation or rescaling.
    expected = [min(candidates[:i]) if side == "buy" else max(candidates[:i]) for i in range(1, len(candidates) + 1)]
    actual = []
    for price in prices:
        order.update_trail_stop_price(price)
        actual.append(order.get_current_trail_stop_price())
    assert actual == pytest.approx(expected)
    assert order.trail_percent == float(rate)


@pytest.mark.parametrize(
    "side,prices,expected",
    [("buy", [100, 90, 95], [102, 92, 92]), ("sell", [100, 110, 109], [98, 108, 108])],
)
def test_fixed_dollar_trail_keeps_its_existing_direction(side, prices, expected):
    """The neighboring dollar branch is a preserved native control."""
    order = Order("ratchet", Asset("SPY"), 2, side, trail_price=Decimal("2"))
    actual = []
    for price in prices:
        order.update_trail_stop_price(price)
        actual.append(order.get_current_trail_stop_price())
    assert actual == pytest.approx(expected)


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("invalid_price", [None, "not-a-price"])
def test_rejected_price_preserves_initialized_stop(side, invalid_price):
    """Arithmetic rejection must occur before the first stored-stop mutation."""
    order = Order("ratchet", Asset("SPY"), 2, side, trail_percent=0.02)
    order.update_trail_stop_price(100)
    before = order.get_current_trail_stop_price()
    with pytest.raises(TypeError):
        order.update_trail_stop_price(invalid_price)
    assert order.get_current_trail_stop_price() == before


def test_non_trailing_order_remains_a_noop():
    """Other order types return before attempting trailing arithmetic."""
    order = Order("ratchet", Asset("SPY"), 2, "buy")
    order.update_trail_stop_price(object())
    assert order.get_current_trail_stop_price() is None


def test_unknown_side_preserves_native_initialization_only():
    """UNKNOWN is admitted but belongs to neither directional side family."""
    order = Order("ratchet", Asset("SPY"), 2, Order.OrderSide.UNKNOWN, trail_percent=0.02)
    for price in [100, 90, 110]:
        order.update_trail_stop_price(price)
        assert order.get_current_trail_stop_price() == pytest.approx(98)


class _TrailingStrategy(Strategy):
    """Use the public order factory with a fully in-memory market feed."""

    def initialize(self):
        """Select the same minute workflow as the existing broker regressions."""
        self.sleeptime = "1M"


def _make_strategy(prices, *, side, spread=None, ohlc=None):
    """Prepare real Pandas data with only the native simulated broker stream."""
    asset, quote = Asset("SPY"), Asset("USD", asset_type="forex")
    index = pd.date_range("2025-01-13 09:30", periods=len(prices), freq="min", tz="America/New_York")
    frame = pd.DataFrame(
        ohlc if ohlc is not None else {name: prices for name in ("open", "high", "low", "close")}, index=index
    )
    frame["volume"] = 1000
    if spread is not None:
        # Buy trails advance from bid and execute at ask; sell trails advance
        # from ask and execute at bid. Include a nonzero-spread control.
        frame["bid"] = prices if side == "buy" else [price - spread for price in prices]
        frame["ask"] = [price + spread for price in prices] if side == "buy" else prices
    data = Data(asset, frame.tz_localize(None), quote=quote, timestep="minute")
    source = PandasDataBacktesting(
        pandas_data={(asset, quote): data},
        datetime_start=index[0],
        datetime_end=index[-1] + pd.Timedelta(minutes=1),
        show_progress_bar=False,
        market="24/7",
    )
    source.load_data()
    broker = BacktestingBroker(data_source=source)
    broker.initialize_market_calendars(source.get_trading_days_pandas())
    strategy = _TrailingStrategy(broker=broker, budget=100000, analyze_backtest=False)
    broker._first_iteration = strategy._first_iteration = False
    return strategy, broker, asset, index


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("spread", [0.0, 0.5])
def test_quote_consumer_triggers_at_retained_percentage_stop(side, spread):
    """A reversal must cross the retained stop in the native quote consumer."""
    prices = [100, 90, 95] if side == "buy" else [100, 110, 109, 107]
    strategy, broker, asset, index = _make_strategy(prices, side=side, spread=spread)
    order = strategy.create_order(asset, 2, side, trail_percent=0.02)
    actual = []
    for dt in index:
        # Repeating the current broker timestamp advances it by one minute.
        # Observe the initial row without accidentally skipping it.
        if dt != broker.datetime:
            broker._update_datetime(dt)
        actual.append(broker._try_fill_with_quote(order, strategy))
    assert actual[:-1] == [None] * (len(prices) - 1)
    assert actual[-1] == pytest.approx(95 + spread if side == "buy" else 107 - spread)
    assert order.get_current_trail_stop_price() == pytest.approx(91.8 if side == "buy" else 107.8)


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_ohlc_consumer_fills_and_accounts_for_percentage_reversal(side):
    """Keep prior-stop timing and verify fills, quantity, position and cash."""
    prices = [100, 90, 91, 95] if side == "buy" else [100, 110, 109, 107]
    strategy, broker, asset, index = _make_strategy(prices, side=side)
    if side == "sell":
        entry = strategy.create_order(asset, 2, "buy")
        strategy.submit_order(entry)
        broker.process_pending_orders(strategy)
        strategy._executor.process_queue()
        assert entry.get_fill_price() == pytest.approx(100)
    order = strategy.create_order(asset, 2, side, trail_percent=0.02)
    strategy.submit_order(order)
    for dt in index:
        if dt != broker.datetime:
            broker._update_datetime(dt)
        broker.process_pending_orders(strategy)
        strategy._executor.process_queue()
        if dt != index[-1]:
            assert not order.is_filled()
    assert order.is_filled()
    assert order.get_fill_price() == pytest.approx(prices[-1])
    assert order.quantity == 2
    position = broker.get_tracked_position(strategy.name, asset)
    assert (float(position.quantity) if position else 0) == (2 if side == "buy" else 0)
    assert strategy.cash == pytest.approx(99810 if side == "buy" else 100014)


def test_ohlc_wide_bar_checks_stored_stop_before_ratcheting():
    """A bar must not trigger the stop that its own high has just established."""
    strategy, broker, asset, index = _make_strategy(
        [100, 109],
        side="sell",
        ohlc={"open": [100, 109], "high": [100, 110], "low": [100, 107], "close": [100, 109]},
    )
    entry = strategy.create_order(asset, 2, "buy")
    strategy.submit_order(entry)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert entry.get_fill_price() == pytest.approx(100)

    order = strategy.create_order(asset, 2, "sell", trail_percent=0.02)
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert not order.is_filled()
    assert order.get_current_trail_stop_price() == pytest.approx(98)

    broker._update_datetime(index[1])
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    # The low of 107 crosses the new 110 * 0.98 = 107.8 threshold, but not
    # the previously stored 98. Updating before checking would fill too early.
    assert not order.is_filled()
    assert order.get_current_trail_stop_price() == pytest.approx(107.8)
    assert broker.get_tracked_position(strategy.name, asset).quantity == 2
    assert strategy.cash == pytest.approx(99800)
