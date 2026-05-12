from __future__ import annotations

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset, SmartLimitConfig, SmartLimitPreset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


class _DummyIbkrFuturesStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True

    def on_trading_iteration(self):
        return


class _CallbackIbkrFuturesStrategy(_DummyIbkrFuturesStrategy):
    def initialize(self, parameters=None):
        super().initialize(parameters=parameters)
        self.filled_events = []

    def on_filled_order(self, position, order, price, quantity, multiplier):
        filled_events = getattr(self, "filled_events", None)
        if filled_events is None:
            filled_events = []
            self.filled_events = filled_events
        filled_events.append(
            {
                "symbol": order.asset.symbol,
                "side": order.side,
                "price": price,
                "quantity": quantity,
                "multiplier": multiplier,
            }
        )


def _make_df(*, bid0: float, ask0: float, bid1: float, ask1: float) -> pd.DataFrame:
    idx = pd.date_range("2025-12-08 09:31", periods=2, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [ask0, ask1],
            "high": [ask0, ask1],
            "low": [bid0, bid1],
            "close": [(bid0 + ask0) / 2, (bid1 + ask1) / 2],
            "bid": [bid0, bid1],
            "ask": [ask0, ask1],
            "volume": [1000, 1000],
        },
        index=idx,
    )
    return df


def _make_quote_only_df(*, bids: list[float], asks: list[float]) -> pd.DataFrame:
    idx = pd.date_range("2025-12-08 09:31", periods=len(bids), freq="1min", tz="America/New_York")
    mids = [(b + a) / 2 for b, a in zip(bids, asks, strict=False)]
    df = pd.DataFrame(
        {
            # Force quote-based fill model by making OHLC incomplete.
            "open": [pd.NA] * len(bids),
            "high": [pd.NA] * len(bids),
            "low": [pd.NA] * len(bids),
            "close": mids,
            "bid": bids,
            "ask": asks,
            "volume": [1000] * len(bids),
        },
        index=idx,
    )
    return df


def test_ibkr_rest_backtesting_futures_market_roundtrip_uses_bid_ask_and_multiplier(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    df = _make_quote_only_df(bids=[100.00, 100.75], asks=[100.25, 101.00])

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=df.index[0].to_pydatetime(),
        datetime_end=(df.index[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=df.index[0].to_pydatetime(),
        end_date=df.index[-1].to_pydatetime(),
    )

    q0 = broker.get_quote(fut, quote=None)
    assert float(q0.ask) == pytest.approx(100.25)
    assert float(q0.bid) == pytest.approx(100.00)

    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()
    assert buy.get_fill_price() == pytest.approx(100.25, rel=1e-12)

    cash_after_open = 10_000.0 - 1300.0
    assert strategy.cash == pytest.approx(cash_after_open, rel=1e-9)

    broker._update_datetime(df.index[1].to_pydatetime())
    q1 = broker.get_quote(fut, quote=None)
    assert float(q1.bid) == pytest.approx(100.75)

    sell = strategy.create_order(fut, Decimal("1"), Order.OrderSide.SELL, order_type=Order.OrderType.MARKET)
    strategy.submit_order(sell)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert sell.is_filled()
    assert sell.get_fill_price() == pytest.approx(100.75, rel=1e-12)

    expected_pnl = (100.75 - 100.25) * 1.0 * 5.0
    expected_cash = 10_000.0 + expected_pnl
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)


def test_ibkr_rest_backtesting_futures_market_fill_preserves_custom_callback(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    df = _make_quote_only_df(bids=[100.00, 100.75], asks=[100.25, 101.00])

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=df.index[0].to_pydatetime(),
        datetime_end=(df.index[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _CallbackIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=df.index[0].to_pydatetime(),
        end_date=df.index[-1].to_pydatetime(),
    )

    order = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert order.is_filled()
    assert strategy.filled_events == [
        {
            "symbol": "MES",
            "side": Order.OrderSide.BUY,
            "price": pytest.approx(100.25),
            "quantity": pytest.approx(1.0),
            "multiplier": 5,
        }
    ]


def test_ibkr_rest_backtesting_futures_smart_limit_uses_asset_min_tick(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # With bid=100.00 and ask=100.25, mid=100.125. Futures tick=0.25 should round BUY fill up to 100.25.
    df = _make_df(bid0=100.00, ask0=100.25, bid1=100.00, ask1=100.25)

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=df.index[0].to_pydatetime(),
        datetime_end=(df.index[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=df.index[0].to_pydatetime(),
        end_date=df.index[-1].to_pydatetime(),
    )

    cfg = SmartLimitConfig(preset=SmartLimitPreset.FAST, final_price_pct=1.0, slippage=0.0)
    order = strategy.create_order(
        fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.SMART_LIMIT, smart_limit=cfg
    )
    strategy.submit_order(order)

    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert order.is_filled()
    assert order.get_fill_price() == pytest.approx(100.25, rel=1e-12)


def test_ibkr_rest_backtesting_futures_stop_and_stop_limit_orders_fill(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.date_range("2025-12-08 09:31", periods=2, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [pd.NA, pd.NA],
            "high": [pd.NA, pd.NA],
            "low": [pd.NA, pd.NA],
            "close": [100.00, 100.00],
            "bid": [99.75, 99.75],
            "ask": [100.25, 100.25],
            "volume": [1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()

    stop_exit = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.STOP,
        stop_price=Decimal("999999"),
    )
    strategy.submit_order(stop_exit)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert stop_exit.is_filled()

    stop_limit_entry = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.STOP_LIMIT,
        stop_price=Decimal("0.25"),
        limit_price=Decimal("999999"),
    )
    strategy.submit_order(stop_limit_entry)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert stop_limit_entry.is_filled()


def test_ibkr_rest_backtesting_futures_market_order_does_not_fill_inside_large_session_gap(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Model a CME Globex equity futures early close:
    # - session ends at 13:00 ET (no 13:00..17:59 minute bars)
    # - next session opens at 18:00 ET (large timestamp gap)
    #
    # When the backtest clock is at 13:00 (inside the gap) and a market order is submitted, the
    # order should remain working (accepted) but must not fill until the clock reaches a timestamp
    # where there is actionable data again (18:00 reopen bar).
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-09-01 12:59:00", tz="America/New_York"),
            pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York"),
        ]
    )
    df = pd.DataFrame(
        {
            "open": [6482.25, 6480.00],
            "high": [6483.00, 6483.00],
            "low": [6481.75, 6479.75],
            "close": [6483.00, 6480.25],
            "volume": [1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE, multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    broker._update_datetime(pd.Timestamp("2025-09-01 13:00:00", tz="America/New_York").to_pydatetime())

    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert not buy.is_filled()

    broker._update_datetime(pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York").to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert buy.is_filled()
    assert buy.get_fill_price() == pytest.approx(6480.00, rel=1e-12)


def test_ibkr_rest_backtesting_futures_stop_does_not_trigger_inside_intraday_gap(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Same early-close shape as the market-fill test, but validate stop fills too.
    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-09-01 12:59:00", tz="America/New_York"),
            pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York"),
        ]
    )
    df = pd.DataFrame(
        {
            "open": [6482.25, 6480.00],
            "high": [6483.00, 6483.00],
            "low": [6481.75, 6479.75],
            "close": [6483.00, 6480.25],
            "volume": [1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE, multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    # Create an open long position so we have a sell stop to trigger.
    # NOTE: the broker clock is already at `datetime_start` on init. Avoid calling `_update_datetime`
    # with the same timestamp because it will advance by +1 minute to guarantee monotonic time.
    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()

    # Evaluate in the gap (no bar at 17:55). Stop should not fill without actionable data.
    broker._update_datetime(pd.Timestamp("2025-09-01 17:55:00", tz="America/New_York").to_pydatetime())
    stop_exit = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.STOP,
        stop_price=Decimal("999999"),
    )
    strategy.submit_order(stop_exit)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert not stop_exit.is_filled()

    # Once the session reopens and data resumes, the stop can trigger and fill.
    broker._update_datetime(pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York").to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert stop_exit.is_filled()
    assert stop_exit.get_fill_price() == pytest.approx(6480.00, rel=1e-12)


def test_ibkr_rest_backtesting_futures_gap_one_bar_before_open_waits_for_reopen(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-09-01 12:59:00", tz="America/New_York"),
            pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York"),
        ]
    )
    df = pd.DataFrame(
        {
            "open": [6482.25, 6480.00],
            "high": [6483.00, 6483.00],
            "low": [6481.75, 6479.75],
            "close": [6483.00, 6480.25],
            "volume": [1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE, multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()

    # One minute before the next session open (18:00) still has no bar. The order must remain
    # pending until the clock reaches 18:00.
    broker._update_datetime(pd.Timestamp("2025-09-01 17:59:00", tz="America/New_York").to_pydatetime())
    stop_exit = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.STOP,
        stop_price=Decimal("999999"),
    )
    strategy.submit_order(stop_exit)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert not stop_exit.is_filled()

    broker._update_datetime(pd.Timestamp("2025-09-01 18:00:00", tz="America/New_York").to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert stop_exit.is_filled()
    assert stop_exit.get_fill_price() == pytest.approx(6480.00, rel=1e-12)


def test_ibkr_rest_backtesting_futures_trailing_stop_triggers(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.date_range("2025-12-08 09:31", periods=5, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [pd.NA] * 5,
            "high": [pd.NA] * 5,
            "low": [pd.NA] * 5,
            "close": [100.75, 102.75, 103.75, 104.75, 101.75],
            "bid": [100.00, 102.00, 103.00, 104.00, 101.00],
            "ask": [100.25, 102.25, 103.25, 104.25, 101.25],
            "volume": [1000, 1000, 1000, 1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    buy = strategy.create_order(fut, Decimal("1"), Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    strategy.submit_order(buy)
    broker._update_datetime(idx[1].to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()

    trail = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.TRAIL,
        trail_price=Decimal("1.0"),
    )
    strategy.submit_order(trail)

    for ts in idx[2:]:
        broker._update_datetime(ts.to_pydatetime())
        broker.process_pending_orders(strategy)
        strategy._executor.process_queue()
        if trail.is_filled():
            break

    assert trail.is_filled()


def test_ibkr_rest_backtesting_futures_oco_and_oto_orders_execute(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.date_range("2025-12-08 09:31", periods=3, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [100.25, 100.25, 100.25],
            "high": [100.25, 100.25, 100.25],
            "low": [100.25, 100.25, 100.25],
            "close": [100.25, 100.25, 100.25],
            "bid": [100.00, 100.00, 100.00],
            "ask": [100.25, 100.25, 100.25],
            "volume": [1000, 1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    broker._update_datetime(idx[0].to_pydatetime())

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    oto = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("999999"),
        order_class=Order.OrderClass.OTO,
        secondary_limit_price=Decimal("0"),
    )
    strategy.submit_order(oto)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert oto.is_filled()
    assert oto.child_orders and len(oto.child_orders) == 1

    broker._update_datetime(idx[1].to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert oto.child_orders[0].is_filled()

    oco = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_class=Order.OrderClass.OCO,
        limit_price=Decimal("999999"),
        stop_price=Decimal("0.25"),
    )
    strategy.submit_order(oco)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert oco.child_orders and len(oco.child_orders) == 2
    assert any(child.is_filled() for child in oco.child_orders)
    assert any(child.is_canceled() for child in oco.child_orders)


def test_ibkr_rest_backtesting_futures_bracket_order_executes_children(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    idx = pd.date_range("2025-12-08 09:31", periods=3, freq="1min", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [100.25, 100.25, 100.25],
            "high": [100.25, 100.25, 100.25],
            "low": [100.25, 100.25, 100.25],
            "close": [100.25, 100.25, 100.25],
            "bid": [100.00, 100.00, 100.00],
            "ask": [100.25, 100.25, 100.25],
            "volume": [1000, 1000, 1000],
        },
        index=idx,
    )

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=idx[0].to_pydatetime(),
        datetime_end=(idx[-1] + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    broker._update_datetime(idx[0].to_pydatetime())

    strategy = _DummyIbkrFuturesStrategy(
        broker=broker,
        budget=10_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut.min_tick = 0.25

    data_source.get_historical_prices_between_dates(
        (fut, Asset("USD", asset_type=Asset.AssetType.FOREX)),
        timestep="minute",
        quote=None,
        start_date=idx[0].to_pydatetime(),
        end_date=idx[-1].to_pydatetime(),
    )

    bracket = strategy.create_order(
        fut,
        Decimal("1"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("999999"),
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=Decimal("0"),
        secondary_stop_price=Decimal("999999"),
    )
    strategy.submit_order(bracket)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert bracket.is_filled()
    assert bracket.child_orders

    broker._update_datetime(idx[1].to_pydatetime())
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert any(child.is_filled() for child in bracket.child_orders)
