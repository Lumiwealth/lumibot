import datetime
from unittest.mock import patch

import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.strategies import Strategy
from lumibot.strategies._strategy import _Strategy


class MinimalStrategy(Strategy):
    """No-op strategy used for backtest scaffolding."""

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        pass


class DummyDataSource:
    """Lightweight datasource stub capturing the start/end datetimes."""

    SOURCE = "dummy"

    def __init__(self, datetime_start=None, datetime_end=None, **kwargs):
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self._data_store = {}


class DummyTrader:
    """Trader stub that records strategies and returns canned results."""

    def __init__(self, *args, **kwargs):
        self._strategies = []

    def add_strategy(self, strategy):
        self._strategies.append(strategy)

    def run_all(self, **_kwargs):
        return {strategy.name: {"dummy": True} for strategy in self._strategies}


class _EarlyExit(Exception):
    """Signal to stop run_backtest after the datasource is constructed."""


def test_verify_backtest_inputs_accepts_mixed_timezones():
    """Regression: verify_backtest_inputs must not crash on naive vs aware inputs."""
    naive_start = datetime.datetime(2025, 1, 1)
    aware_end = datetime.datetime(2025, 9, 30, tzinfo=datetime.timezone.utc)

    # Should not raise
    _Strategy.verify_backtest_inputs(naive_start, aware_end)


def test_run_backtest_normalizes_mixed_timezones():
    """Strategy.run_backtest should normalize naive/aware datetimes before validation."""
    naive_start = datetime.datetime(2025, 1, 1)
    aware_end = datetime.datetime(2025, 9, 30, tzinfo=datetime.timezone.utc)

    captured = {}

    class CapturingDataSource(DummyDataSource):
        def __init__(self, datetime_start=None, datetime_end=None, **kwargs):
            super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
            captured["start"] = self.datetime_start
            captured["end"] = self.datetime_end

    def broker_factory(data_source, *args, **kwargs):
        captured["data_source"] = data_source
        raise _EarlyExit

    with patch("lumibot.strategies._strategy.BacktestingBroker", side_effect=broker_factory), \
         patch("lumibot.strategies._strategy.Trader", DummyTrader):
        with pytest.raises(_EarlyExit):
            MinimalStrategy.run_backtest(
                CapturingDataSource,
                backtesting_start=naive_start,
                backtesting_end=aware_end,
                show_plot=False,
                show_tearsheet=False,
                show_indicators=False,
                show_progress_bar=False,
                save_logfile=False,
                save_stats_file=False,
            )

    assert "start" in captured and captured["start"].tzinfo is not None
    assert "end" in captured and captured["end"].tzinfo is not None
    assert captured["start"].tzinfo.zone == LUMIBOT_DEFAULT_PYTZ.zone
    assert captured["end"].tzinfo.zone == LUMIBOT_DEFAULT_PYTZ.zone
    assert captured["start"].tzinfo.zone == captured["end"].tzinfo.zone
