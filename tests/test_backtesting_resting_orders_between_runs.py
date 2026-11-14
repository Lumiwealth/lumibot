"""Resting orders are checked on every loaded bar between two strategy runs.

A stop or limit rests at the broker, so it can fill at any moment, not only when the
strategy happens to run. A strategy that runs every 15 minutes (or once a day) on minute
bars used to check its resting orders only at its own run times: a stop crossed between
two runs was missed unless the price was still past the stop at the next run.

Reproduced scenario this file starts from: a position with a stop at 90, the price dips to
85 between two runs and recovers before the next run. The stop never filled.

See docsrc/backtesting.how_orders_fill.rst ("Resting orders are checked on every bar").
"""

from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data, Order
from lumibot.strategies import Strategy
from tests.backtest.test_backtesting_broker_processing import (
    build_strategy,
    make_ohlcv,
    position_quantity,
    setup_strategy_with_prices,
    submit_and_fill,
)

STOCK = Asset("SYNTH", asset_type=Asset.AssetType.STOCK)
USD = Asset("USD", asset_type=Asset.AssetType.FOREX)
START = pd.Timestamp("2025-01-13 09:30", tz="America/New_York")

FLAT = (100.0, 100.5, 99.5, 100.0)


def _bars(overrides, length=16):
    """Flat bars at 100 with the given ``{minute: (open, high, low, close)}`` overrides."""
    return [overrides.get(i, FLAT) for i in range(length)]


def _setup(bars):
    strategy, broker, _ = setup_strategy_with_prices(STOCK, USD, bars)
    fills = []
    original = broker._execute_filled_order

    def recording(order, price, filled_quantity, strategy, *args, **kwargs):
        fills.append((order, float(price), broker.datetime))
        return original(order, price, filled_quantity, strategy, *args, **kwargs)

    broker._execute_filled_order = recording
    return strategy, broker, fills


def _long_position(strategy, broker, quantity=10):
    order = strategy.create_order(STOCK, quantity, Order.OrderSide.BUY)
    submit_and_fill(strategy, broker, order)
    assert order.is_filled()
    return order


def _rest(strategy, broker, order):
    """Submit a resting order and let the broker see it on the current (flat) bar."""
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert order.is_active(), "the flat run-time bar must not fill the resting order"
    return order


def _sleep_minutes(strategy, minutes):
    """What the backtest loop does between two runs of the strategy."""
    strategy._executor.safe_sleep(minutes * 60)
    strategy._executor.process_queue()


def _minute(fill_time):
    return int((fill_time - START).total_seconds() // 60)


def _fill_of(fills, order):
    matches = [(price, when) for filled, price, when in fills if filled is order]
    assert len(matches) == 1, f"expected exactly one fill, got {matches}"
    return matches[0]


# ----------------------------------------------------------------------------- stops


def test_stop_crossed_between_runs_fills_even_when_price_recovers():
    # The reproduced scenario: stop at 90, a dip to 85 on minute 5, back to 100 by minute 8.
    bars = _bars({4: (98.0, 98.0, 92.0, 93.0), 5: (93.0, 93.0, 85.0, 86.0), 6: (88.0, 96.0, 88.0, 95.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0))

    _sleep_minutes(strategy, 10)  # the strategy runs again at minute 10, price is 100 again

    assert stop.is_filled()
    price, when = _fill_of(fills, stop)
    assert price == pytest.approx(90.0)
    assert _minute(when) == 5
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(0.0)
    assert _minute(broker.datetime) == 10  # the clock still lands on the next run time


def test_stop_gapped_through_between_runs_fills_at_the_open():
    bars = _bars({5: (88.0, 89.0, 84.0, 86.0), 6: (95.0, 100.0, 95.0, 100.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, stop)
    assert price == pytest.approx(88.0)  # the open, not the stop price
    assert _minute(when) == 5


def test_buy_stop_crossed_between_runs_fills_at_the_stop():
    bars = _bars({3: (101.0, 106.0, 101.0, 102.0)})
    strategy, broker, fills = _setup(bars)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "buy", stop_price=105.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, stop)
    assert price == pytest.approx(105.0)
    assert _minute(when) == 3


def test_stop_never_touched_between_runs_stays_open():
    strategy, broker, fills = _setup(_bars({5: (99.0, 99.0, 91.0, 95.0)}))
    _long_position(strategy, broker)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0))

    _sleep_minutes(strategy, 10)

    assert stop.is_active()
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(10.0)


def test_stop_limit_triggered_between_runs_fills_no_worse_than_its_limit():
    # Stop 90 triggers on minute 5 (low 89). The sale happens at the stop price, which is
    # better than the 89.5 limit: the same rule as on a bar the strategy runs on.
    bars = _bars({5: (93.0, 93.0, 89.0, 92.0), 6: (95.0, 100.0, 95.0, 100.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    order = _rest(
        strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0, stop_limit_price=89.5)
    )

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, order)
    assert price == pytest.approx(90.0)
    assert _minute(when) == 5


# ----------------------------------------------------------------------------- limits


def test_limit_touched_between_runs_fills_at_the_limit():
    bars = _bars({4: (101.0, 105.5, 101.0, 102.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    limit = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", limit_price=105.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, limit)
    assert price == pytest.approx(105.0)
    assert _minute(when) == 4


def test_limit_gapped_through_between_runs_fills_at_the_better_open():
    bars = _bars({4: (107.0, 108.0, 106.0, 107.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    limit = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", limit_price=105.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, limit)
    assert price == pytest.approx(107.0)  # better than the limit
    assert _minute(when) == 4


def test_buy_limit_touched_between_runs_fills_at_the_limit():
    bars = _bars({7: (99.0, 99.0, 94.0, 98.0)})
    strategy, broker, fills = _setup(bars)
    limit = _rest(strategy, broker, strategy.create_order(STOCK, 10, "buy", limit_price=95.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, limit)
    assert price == pytest.approx(95.0)
    assert _minute(when) == 7
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(10.0)


# ----------------------------------------------------------------------------- trailing stops


def test_trailing_stop_follows_highs_between_runs_and_fills_on_the_pullback():
    # Trail 5 below the high. Highs reach 110 on minute 4, so the stop is 105 when
    # minute 6 trades down to 104. Minute 8 is back at 110.
    bars = _bars(
        {
            2: (101.0, 104.0, 101.0, 104.0),
            3: (104.0, 108.0, 104.0, 108.0),
            4: (108.0, 110.0, 108.0, 109.0),
            5: (109.0, 109.5, 106.0, 107.0),
            6: (107.0, 107.0, 104.0, 104.5),
            7: (105.0, 110.0, 105.0, 110.0),
        }
    )
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    trail = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", trail_price=5.0))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, trail)
    assert price == pytest.approx(105.0)
    assert _minute(when) == 6


def test_trailing_stop_not_hit_between_runs_still_ratchets_to_the_highs_it_saw():
    calm_at_110 = (110.0, 110.5, 109.5, 110.0)
    bars = [FLAT, FLAT, FLAT, (100.0, 112.0, 100.0, 111.0), (111.0, 111.0, 108.0, 110.0)] + [calm_at_110] * 11
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    trail = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", trail_price=5.0))

    _sleep_minutes(strategy, 10)

    assert trail.is_active()
    assert trail.get_current_trail_stop_price() == pytest.approx(107.0)  # 112 - 5


@pytest.mark.parametrize(
    "side, prices, expected",
    [
        # A sell trailing stop only moves up; a buy trailing stop only moves down.
        ("sell", [100.0, 110.0, 104.0], [90.0, 99.0, 99.0]),
        ("buy", [100.0, 90.0, 96.0], [110.0, 99.0, 99.0]),
    ],
)
def test_percent_trailing_stop_only_moves_in_the_favorable_direction(side, prices, expected):
    order = Order("s", STOCK, 10, side, trail_percent=0.10)
    seen = []
    for price in prices:
        order.update_trail_stop_price(price)
        seen.append(order.get_current_trail_stop_price())
    assert seen == pytest.approx(expected)


# ----------------------------------------------------------------------------- brackets and OCO


def _bracket(strategy, target, stop):
    return strategy.create_order(
        STOCK,
        10,
        "buy",
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=target,
        secondary_stop_price=stop,
    )


def _exit_fill(order):
    filled = [child for child in order.child_orders if child.is_filled()]
    assert len(filled) == 1
    return filled[0]


def test_bracket_bar_touching_both_between_runs_fills_the_stop():
    bars = _bars({6: (100.0, 106.0, 94.0, 100.0)})
    strategy, broker, fills = _setup(bars)
    bracket = _bracket(strategy, 105.0, 95.0)
    submit_and_fill(strategy, broker, bracket)

    _sleep_minutes(strategy, 10)

    exit_order = _exit_fill(bracket)
    assert exit_order.is_stop_order()
    price, when = _fill_of(fills, exit_order)
    assert price == pytest.approx(95.0)
    assert _minute(when) == 6
    assert all(not child.is_active() for child in bracket.child_orders)
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(0.0)


def test_bracket_first_exit_touched_between_runs_wins():
    # Target touched on minute 3; the stop is only reached on minute 7, after the exit.
    bars = _bars({3: (101.0, 105.5, 101.0, 105.0), 7: (99.0, 99.0, 93.0, 94.0)})
    strategy, broker, fills = _setup(bars)
    bracket = _bracket(strategy, 105.0, 95.0)
    submit_and_fill(strategy, broker, bracket)

    _sleep_minutes(strategy, 10)

    exit_order = _exit_fill(bracket)
    assert exit_order.order_type == Order.OrderType.LIMIT
    price, when = _fill_of(fills, exit_order)
    assert price == pytest.approx(105.0)
    assert _minute(when) == 3
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(0.0)


def test_limit_entry_bracket_fills_and_exits_between_two_runs():
    # Entry limit 97 fills on minute 2; its stop (93) starts working on the next bar and
    # is crossed on minute 5. Both happen between two runs.
    bars = _bars({2: (99.0, 99.0, 96.0, 97.5), 5: (97.0, 97.0, 92.0, 93.5)})
    strategy, broker, fills = _setup(bars)
    bracket = strategy.create_order(
        STOCK,
        10,
        "buy",
        limit_price=97.0,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=110.0,
        secondary_stop_price=93.0,
    )
    _rest(strategy, broker, bracket)

    _sleep_minutes(strategy, 10)

    assert bracket.is_filled()
    exit_order = _exit_fill(bracket)
    assert exit_order.is_stop_order()
    assert [(_minute(when), price) for _, price, when in fills] == [(2, 97.0), (5, 93.0)]
    assert position_quantity(broker, strategy, STOCK) == pytest.approx(0.0)


def test_oco_between_runs_first_touched_wins_and_both_touched_fills_the_stop():
    for bars, expect_stop, minute, price in [
        (_bars({3: (101.0, 105.5, 101.0, 105.0), 7: (99.0, 99.0, 93.0, 94.0)}), False, 3, 105.0),
        (_bars({6: (100.0, 106.0, 94.0, 100.0)}), True, 6, 95.0),
    ]:
        strategy, broker, fills = _setup(bars)
        _long_position(strategy, broker)
        oco = strategy.create_order(
            STOCK, 10, "sell", order_class=Order.OrderClass.OCO, limit_price=105.0, stop_price=95.0
        )
        strategy.submit_order(oco)
        broker.process_pending_orders(strategy)
        strategy._executor.process_queue()

        _sleep_minutes(strategy, 10)

        exit_order = _exit_fill(oco)
        assert exit_order.is_stop_order() is expect_stop
        got_price, when = _fill_of(fills, exit_order)
        assert got_price == pytest.approx(price)
        assert _minute(when) == minute
        assert all(not child.is_active() for child in oco.child_orders)


# ----------------------------------------------------------------------------- strategy callbacks


def test_order_placed_from_the_fill_callback_between_runs_fills_on_the_next_bar():
    bars = _bars({5: (93.0, 93.0, 85.0, 86.0), 6: (88.0, 96.0, 88.0, 95.0)})
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0))
    reentries = []

    def on_filled_order(position, order, price, quantity, multiplier):
        if order is stop:
            reentry = strategy.create_order(STOCK, 5, "buy")
            reentries.append(reentry)
            strategy.submit_order(reentry)

    strategy.on_filled_order = on_filled_order

    _sleep_minutes(strategy, 10)

    assert stop.is_filled()
    price, when = _fill_of(fills, reentries[0])
    assert _minute(when) == 6  # the bar after the stop fill, as with a one-minute strategy
    assert price == pytest.approx(88.0)


# ----------------------------------------------------------------------------- every-minute strategies


def _minute_by_minute(bars, check_between_runs):
    strategy, broker, fills = _setup(bars)
    broker._check_resting_orders_between_runs = check_between_runs
    _long_position(strategy, broker)
    trail = strategy.create_order(STOCK, 5, "sell", trail_price=4.0)
    bracket_exit = strategy.create_order(
        STOCK, 5, "sell", order_class=Order.OrderClass.OCO, limit_price=106.0, stop_price=93.0
    )
    strategy.submit_order(trail)
    strategy.submit_order(bracket_exit)
    calls = []
    original = broker.process_pending_orders

    def counting(strategy=strategy):
        calls.append(broker.datetime)
        return original(strategy=strategy)

    broker.process_pending_orders = counting
    for _ in range(len(bars) - 1):
        broker.process_pending_orders(strategy=strategy)
        strategy._executor.process_queue()
        strategy._executor.safe_sleep(60)
    return [(str(order.order_type), price, when) for order, price, when in fills], calls


def test_strategy_that_runs_every_minute_is_unchanged():
    rng = np.random.default_rng(11)
    closes = 100 + np.cumsum(rng.normal(0, 0.9, 120))
    bars = [(c - 0.2, c + 0.8, c - 0.8, c) for c in closes]

    with_checks, calls_with = _minute_by_minute(bars, True)
    without_checks, calls_without = _minute_by_minute(bars, False)

    assert len(with_checks) >= 3  # entry, trailing stop, one OCO exit
    assert with_checks == without_checks
    # No bar lies between two one-minute runs: the broker is never asked an extra time.
    assert calls_with == calls_without


def test_no_resting_orders_means_no_extra_broker_work():
    strategy, broker, fills = _setup(_bars({}))
    _long_position(strategy, broker)
    calls = []
    original = broker.process_pending_orders
    broker.process_pending_orders = lambda strategy=strategy: (calls.append(1), original(strategy=strategy))[1]

    _sleep_minutes(strategy, 10)

    assert calls == []
    assert _minute(broker.datetime) == 10


def test_between_run_checks_stop_only_on_bars_that_can_fill():
    # 600 quiet bars, one bar that crosses the stop: one extra broker pass, not 600.
    bars = _bars({431: (93.0, 93.0, 85.0, 86.0)}, length=601)
    strategy, broker, fills = _setup(bars)
    _long_position(strategy, broker)
    stop = _rest(strategy, broker, strategy.create_order(STOCK, 10, "sell", stop_price=90.0))
    calls = []
    original = broker.process_pending_orders
    broker.process_pending_orders = lambda strategy=strategy: (calls.append(broker.datetime), original(strategy=strategy))[1]

    _sleep_minutes(strategy, 600)

    assert stop.is_filled()
    assert [_minute(when) for when in calls] == [431]


# ----------------------------------------------------------------------------- options


OPTION = Asset("SYNTH", asset_type=Asset.AssetType.OPTION, expiration=date(2025, 1, 17), strike=100.0, right="CALL")


def _option_setup(option_bars, stock_bars=None):
    """A stock and one of its calls, both on minute trade bars (no quotes)."""
    stock_bars = stock_bars or _bars({}, length=len(option_bars))
    frames = {STOCK: make_ohlcv(stock_bars), OPTION: make_ohlcv(option_bars)}
    pandas_data = {}
    for asset, frame in frames.items():
        local = frame.tz_convert("America/New_York").tz_localize(None)
        pandas_data[(asset, USD)] = Data(asset=asset, df=local, quote=USD, timestep="minute", timezone="America/New_York")
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=START,
        datetime_end=START + pd.Timedelta(minutes=len(option_bars)),
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    strategy = build_strategy(broker)
    strategy._first_iteration = False
    fills = []
    original = broker._execute_filled_order

    def recording(order, price, filled_quantity, strategy, *args, **kwargs):
        fills.append((order, float(price), broker.datetime))
        return original(order, price, filled_quantity, strategy, *args, **kwargs)

    broker._execute_filled_order = recording
    return strategy, broker, fills


OPTION_FLAT = (2.0, 2.05, 1.95, 2.0)


def test_option_stop_crossed_between_runs_fills_on_the_bar_that_crossed_it():
    option_bars = [OPTION_FLAT] * 16
    option_bars[5] = (1.8, 1.8, 1.2, 1.3)
    option_bars[6] = (1.6, 2.0, 1.6, 2.0)
    strategy, broker, fills = _option_setup(option_bars)
    entry = strategy.create_order(OPTION, 1, "buy")
    submit_and_fill(strategy, broker, entry)
    assert entry.is_filled()
    stop = _rest(strategy, broker, strategy.create_order(OPTION, 1, "sell", stop_price=1.5))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, stop)
    assert price == pytest.approx(1.5)
    assert _minute(when) == 5


def test_option_limit_touched_between_runs_fills_at_the_limit():
    option_bars = [OPTION_FLAT] * 16
    option_bars[4] = (2.1, 2.6, 2.1, 2.4)
    strategy, broker, fills = _option_setup(option_bars)
    entry = strategy.create_order(OPTION, 1, "buy")
    submit_and_fill(strategy, broker, entry)
    limit = _rest(strategy, broker, strategy.create_order(OPTION, 1, "sell", limit_price=2.5))

    _sleep_minutes(strategy, 10)

    price, when = _fill_of(fills, limit)
    assert price == pytest.approx(2.5)
    assert _minute(when) == 4


def test_option_market_order_keeps_its_fill_bar_price_rule():
    # A market option order fills on the bar it is processed on, at that bar's own price
    # (4.6.60). Between-run checks are for resting orders; they do not move this fill.
    option_bars = [OPTION_FLAT] * 16
    option_bars[0] = (2.0, 2.05, 1.95, 2.0)
    option_bars[5] = (3.0, 3.1, 2.9, 3.0)
    strategy, broker, fills = _option_setup(option_bars)
    entry = strategy.create_order(OPTION, 1, "buy")
    submit_and_fill(strategy, broker, entry)

    price, when = _fill_of(fills, entry)
    assert _minute(when) == 0
    assert price == pytest.approx(2.0)
    assert broker._option_fill_price_counts == {"trade": 1}

    _sleep_minutes(strategy, 10)
    assert len(fills) == 1


# ----------------------------------------------------------------------------- a whole backtest


class _DailyStopStrategy(Strategy):
    """Runs once a day on minute data: buys, then rests a stop at 90."""

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.done = False
        self.vars.fills = []

    def on_trading_iteration(self):
        if self.vars.done:
            return
        self.vars.done = True
        self.submit_order(
            self.create_order(
                STOCK,
                10,
                "buy",
                order_class=Order.OrderClass.OTO,
                secondary_stop_price=90.0,
            )
        )

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.vars.fills.append((order.is_sell_order(), float(price), self.get_datetime()))


@pytest.mark.usefixtures("disable_datasource_override")
def test_daily_strategy_on_minute_bars_fills_a_stop_crossed_during_the_day(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    sessions = pd.bdate_range("2025-01-13", "2025-01-16")
    index = pd.DatetimeIndex(
        np.concatenate(
            [pd.date_range(f"{day.date()} 09:30", periods=390, freq="1min", tz="America/New_York").values for day in sessions]
        ),
        tz="UTC",
    ).tz_convert("America/New_York")
    frame = pd.DataFrame(
        {"open": 100.0, "high": 100.5, "low": 99.5, "close": 100.0, "volume": 1000.0}, index=index
    )
    # Tuesday 11:00 to 11:05: a dip to 85 that is back at 100 well before the close.
    dip = pd.date_range("2025-01-14 11:00", periods=6, freq="1min", tz="America/New_York")
    frame.loc[dip, ["open", "high", "low", "close"]] = [
        [98.0, 98.0, 92.0, 93.0],
        [93.0, 93.0, 85.0, 86.0],
        [86.0, 90.0, 86.0, 89.0],
        [89.0, 95.0, 89.0, 95.0],
        [95.0, 99.0, 95.0, 99.0],
        [99.0, 100.5, 99.0, 100.0],
    ]
    data = Data(asset=STOCK, df=frame, quote=USD, timestep="minute")

    _, strategy = _DailyStopStrategy.run_backtest(
        PandasDataBacktesting,
        datetime(2025, 1, 13),
        datetime(2025, 1, 17),
        pandas_data={(STOCK, USD): data},
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
        show_progress_bar=False,
        quiet_logs=True,
    )

    sells = [(price, when) for is_sell, price, when in strategy.vars.fills if is_sell]
    assert len(sells) == 1
    price, when = sells[0]
    assert price == pytest.approx(90.0)
    assert when.strftime("%Y-%m-%d %H:%M") == "2025-01-14 11:01"
    assert strategy.get_position(STOCK) is None
