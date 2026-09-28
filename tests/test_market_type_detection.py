from datetime import datetime, timedelta, timezone

import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


class _NoopStrategy(Strategy):
    """Minimal strategy stub for StrategyExecutor tests."""

    def initialize(self):
        self.set_market("us_futures")

    def on_trading_iteration(self):
        pass


class _DailyNoopStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.set_market("NYSE")

    def on_trading_iteration(self):
        pass


class _MinuteNoopStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"
        self.set_market("NYSE")

    def on_trading_iteration(self):
        pass


@pytest.fixture
def strategy_executor():
    broker = PandasDataBacktesting(
        datetime_start=datetime(2025, 10, 28),
        datetime_end=datetime(2025, 11, 6),
    )
    backtesting_broker = BacktestingBroker(data_source=broker)
    strat = _NoopStrategy(broker=backtesting_broker)
    return StrategyExecutor(strategy=strat)


def test_us_futures_treated_as_non_continuous(strategy_executor):
    """us_futures closes over the weekend; it must not be flagged as continuous."""
    assert strategy_executor._is_continuous_market("us_futures") is False


def test_true_continuous_markets_remain_continuous(strategy_executor):
    """24/7 markets should still be recognised as continuous."""
    assert strategy_executor._is_continuous_market("24/7") is True


def test_24_5_market_is_not_treated_as_continuous(strategy_executor):
    """24/5 has a weekend gap, so live startup still needs a current calendar."""
    assert strategy_executor._is_continuous_market("24/5") is False


def test_live_calendar_initialization_bounds_include_current_session(strategy_executor, mocker):
    captured_kwargs = {}

    def fake_get_trading_days(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return object()

    initialize_spy = mocker.patch.object(strategy_executor.broker, "initialize_market_calendars")
    mocker.patch(
        "lumibot.strategies.strategy_executor.get_trading_days",
        side_effect=fake_get_trading_days,
    )

    now = datetime(2026, 7, 8, 14, 7, 54, tzinfo=timezone.utc)
    strategy_executor._initialize_live_market_calendars("NASDAQ", now_utc=now)

    initialize_spy.assert_called_once()
    assert captured_kwargs["market"] == "NASDAQ"
    assert captured_kwargs["start_date"] <= now - timedelta(days=14)
    # get_trading_days treats end_date as exclusive, so live startup must push beyond today.
    assert captured_kwargs["end_date"] >= now + timedelta(days=15)
    assert captured_kwargs["tzinfo"] is not None


def test_ensure_progress_inside_open_session(strategy_executor, mocker):
    """When time_to_close stalls during open market, executor should advance clock."""
    broker = strategy_executor.broker
    mocker.patch.object(broker, "is_market_open", return_value=True)
    update_spy = mocker.patch.object(broker, "_update_datetime")
    mocker.patch.object(broker, "get_time_to_close", return_value=15)

    result = strategy_executor._ensure_progress_inside_open_session(0)

    update_spy.assert_called_once_with(1)
    assert result == 15


def test_ensure_progress_noop_when_market_closed(strategy_executor, mocker):
    broker = strategy_executor.broker
    mocker.patch.object(broker, "is_market_open", return_value=False)
    update_spy = mocker.patch.object(broker, "_update_datetime")

    result = strategy_executor._ensure_progress_inside_open_session(0)

    update_spy.assert_not_called()
    assert result == 0


def test_initialize_seeds_day_cadence_for_daily_backtests():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 10, 28),
        datetime_end=datetime(2025, 11, 6),
    )
    backtesting_broker = BacktestingBroker(data_source=data_source)
    strat = _DailyNoopStrategy(broker=backtesting_broker)
    executor = StrategyExecutor(strategy=strat)

    assert data_source._timestep == "minute"
    executor._initialize()
    assert data_source._timestep == "day"


def test_initialize_keeps_minute_cadence_for_intraday_backtests():
    data_source = PandasDataBacktesting(
        datetime_start=datetime(2025, 10, 28),
        datetime_end=datetime(2025, 11, 6),
    )
    backtesting_broker = BacktestingBroker(data_source=data_source)
    strat = _MinuteNoopStrategy(broker=backtesting_broker)
    executor = StrategyExecutor(strategy=strat)

    assert data_source._timestep == "minute"
    executor._initialize()
    assert data_source._timestep == "minute"
