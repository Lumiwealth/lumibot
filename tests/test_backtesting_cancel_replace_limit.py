"""A limit order rests through the whole bar, also when the data carries bid/ask quotes.

Reproduced scenario: a scalper runs every minute on minute bars whose data has a bid/ask
quote per bar (futures, crypto, options). Each run it cancels its old limit entry and places
a fresh one a few points below the price. Live, each order rests at the broker for the full
minute and fills when the price trades down through it. In the backtest a quoted bar only
compared the limit with the bar's bid/ask snapshot and never with the bar's traded high and
low, so the bot never filled (even a limit left resting never filled while the quote stayed
above it). The between-run checker already used "traded range OR quote"; the run-time check
now uses the same rule.

See docsrc/backtesting.how_orders_fill.rst.
"""

import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset, Order
from tests.backtest.test_backtesting_broker_processing import build_data_source, build_strategy, make_ohlcv
from tests.test_backtesting_resting_orders_between_runs import START, _bars

FUTURE = Asset("SYNTHF", asset_type=Asset.AssetType.STOCK)
USD = Asset("USD", asset_type=Asset.AssetType.FOREX)


def _setup_quoted(bars):
    df = make_ohlcv(bars)
    df["bid"] = df["close"] - 0.25
    df["ask"] = df["close"] + 0.25
    data_source = build_data_source(FUTURE, USD, df)
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


def _minute(when):
    return int((when - START).total_seconds() // 60)


def _run(strategy, broker, limit_price, minutes, refresh):
    """Each minute: optionally cancel the working entry, place a buy limit if none works, wait."""
    placed = []
    for _ in range(minutes):
        if any(o.is_filled() for o in placed):
            break
        working = [o for o in placed if o.is_active()]
        if refresh:
            for order in working:
                strategy.cancel_order(order)
            strategy._executor.process_queue()
            working = []
        if not working:
            order = strategy.create_order(FUTURE, 1, Order.OrderSide.BUY, limit_price=limit_price)
            strategy.submit_order(order)
            placed.append(order)
        broker.process_pending_orders(strategy)
        strategy._executor.process_queue()
        strategy._executor.safe_sleep(60)
        strategy._executor.process_queue()
    return placed


@pytest.mark.parametrize("refresh", [True, False], ids=["cancel_replace_every_bar", "left_resting"])
def test_quoted_bar_that_trades_through_the_limit_fills_it(refresh):
    # Flat at 100 (quote 99.75 / 100.25); minute 3 trades down to 95 and closes back at 100.
    strategy, broker, fills = _setup_quoted(_bars({3: (100.0, 100.0, 95.0, 100.0)}))
    placed = _run(strategy, broker, 96.0, 8, refresh)

    filled = [order for order in placed if order.is_filled()]
    assert len(filled) == 1, [(o.status, o.limit_price) for o in placed]
    order, price, when = fills[0]
    assert order is filled[0]
    assert price == pytest.approx(96.0)
    assert _minute(when) == 3


@pytest.mark.parametrize("refresh", [True, False], ids=["cancel_replace_every_bar", "left_resting"])
def test_quoted_bar_that_stays_above_the_limit_does_not_fill(refresh):
    strategy, broker, fills = _setup_quoted(_bars({3: (100.0, 100.0, 96.5, 100.0)}))
    placed = _run(strategy, broker, 96.0, 8, refresh)
    assert not any(order.is_filled() for order in placed)
    assert fills == []


def test_marketable_limit_still_fills_at_the_quote():
    strategy, broker, fills = _setup_quoted(_bars({}))
    placed = _run(strategy, broker, 101.0, 2, refresh=False)
    assert placed[0].is_filled()
    assert fills[0][1] == pytest.approx(100.25)  # the ask, not the limit
