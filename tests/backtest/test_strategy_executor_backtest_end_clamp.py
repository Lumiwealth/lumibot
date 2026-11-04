from __future__ import annotations

from datetime import datetime, timedelta

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.strategies.strategy_executor import StrategyExecutor


class _DummyDataSource:
    def __init__(self, datetime_end):
        self.datetime_end = datetime_end


class _DummyBroker:
    IS_BACKTESTING_BROKER = True

    def __init__(self, *, datetime_end):
        self.market = "24/7"
        self.data_source = _DummyDataSource(datetime_end=datetime_end)
        self.datetime = datetime_end

    def is_market_open(self):
        return True


class _DummyStrategy:
    is_backtesting = True
    minutes_before_closing = 0
    name = "dummy"

    def __init__(self, broker):
        self.broker = broker

    def _update_cash_with_dividends(self):
        return

    def on_trading_iteration(self):
        return

    def await_market_to_close(self, timedelta: int | None = None):
        raise AssertionError("await_market_to_close() should not be called after backtest end")


def test_strategy_executor_does_not_await_market_close_past_backtest_end(monkeypatch):
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 0, 0))
    broker = _DummyBroker(datetime_end=end)
    strategy = _DummyStrategy(broker)
    executor = StrategyExecutor(strategy)

    monkeypatch.setattr(executor, "_before_market_closes", lambda: None)
    monkeypatch.setattr(executor, "_after_market_closes", lambda: None)

    def fake_run_backtesting_loop(is_continuous_market, time_to_close):
        # Simulate the common end-of-backtest state: we overslept by one bar past the
        # exclusive end bound (e.g., from 23:59 -> 00:00 next day).
        broker.datetime = end + timedelta(minutes=1)

    monkeypatch.setattr(executor, "_run_backtesting_loop", fake_run_backtesting_loop)

    # Should return early without calling await_market_to_close().
    executor._run_trading_session()


def test_trading_loop_does_not_execute_an_iteration_at_terminal_bound(monkeypatch):
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 9, 35))
    broker = _DummyBroker(datetime_end=end)
    executor = StrategyExecutor(_DummyStrategy(broker))

    def unexpected_iteration():
        raise AssertionError("No new trading iteration is inside the exclusive end bound")

    monkeypatch.setattr(executor, "_on_trading_iteration", unexpected_iteration)
    executor._run_backtesting_loop(is_continuous_market=True, time_to_close=None)


def test_preopen_buffer_does_not_start_trading_when_actual_open_is_outside_window(monkeypatch):
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 9, 15))
    broker = _DummyBroker(datetime_end=end)
    broker.datetime = end - timedelta(minutes=45)
    broker._end_of_trading_days_reached = False
    broker.should_continue = lambda: not broker._end_of_trading_days_reached
    broker.is_market_open = lambda: False
    strategy = _DummyStrategy(broker)
    strategy.get_datetime = lambda: broker.datetime
    waits = []
    def await_open(timedelta=None):
        waits.append(timedelta)
        if timedelta == 0:
            broker._end_of_trading_days_reached = True
    strategy.await_market_to_open = await_open
    executor = StrategyExecutor(strategy)
    monkeypatch.setattr(executor, "_is_pandas_daily_data_source", lambda: False)
    monkeypatch.setattr(executor, "_before_market_opens", lambda: None)
    def unexpected_start():
        raise AssertionError("The actual session opening is outside the requested window")
    monkeypatch.setattr(executor, "_before_starting_trading", unexpected_start)
    assert executor._setup_market_session(has_data_source=True) is False
    assert waits == [None, 0]
