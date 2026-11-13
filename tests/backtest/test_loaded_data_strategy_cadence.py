"""Loaded bar frequency must not be confused with strategy invocation cadence."""
from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy
from lumibot.traders import Trader


@pytest.mark.parametrize("cadence,expected", [("1D", 2), ("1H", 14), ("30M", 26)])
def test_strategy_cadence_over_loaded_minute_bars(cadence, expected, tmp_path):
    index = pd.date_range("2026-10-26 09:30", "2026-10-26 16:00", freq="min", tz="America/New_York")
    index = index.append(index + pd.Timedelta(days=1))
    frame = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1000}, index=index)
    asset = Asset("XYZ")
    source = PandasDataBacktesting(
        datetime_start=datetime(2026, 10, 26), datetime_end=datetime(2026, 10, 27, 16, 1),
        pandas_data={asset: Data(asset, frame, timestep="minute")}, show_progress_bar=False,
    )

    class CadenceControl(Strategy):
        def initialize(self):
            self.sleeptime = cadence
            self.vars.iterations = []

        def on_trading_iteration(self):
            self.vars.iterations.append(self.get_datetime())

    broker = BacktestingBroker(source)
    strategy = CadenceControl(broker=broker, budget=10000, benchmark_asset=None,
        risk_free_rate=0, analyze_backtest=False, stats_file=str(tmp_path / "stats.csv"))
    trader = Trader(backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(trades_file=str(tmp_path / "trades.csv"), show_plot=False,
        save_tearsheet=False, show_tearsheet=False, show_indicators=False)
    assert len(strategy.vars.iterations) == expected
    assert source._timestep == "minute"
    assert {dt.date() for dt in strategy.vars.iterations} == {
        datetime(2026, 10, 26).date(), datetime(2026, 10, 27).date(),
    }


def test_loaded_data_does_not_override_provider_explicit_cadence_policy():
    class RoutedSource(PandasDataBacktesting):
        def __init__(self, **kwargs):
            self._timestep_explicit = False
            super().__init__(**kwargs)

    frame = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100., "volume": 1000},
        index=pd.date_range("2026-10-26", periods=2, freq="D", tz="America/New_York"))
    asset = Asset("XYZ")
    kwargs = dict(datetime_start=datetime(2026, 10, 26), datetime_end=datetime(2026, 10, 27),
        pandas_data={asset: Data(asset, frame, timestep="day")}, show_progress_bar=False)
    assert PandasDataBacktesting(**kwargs)._timestep_explicit is True
    assert RoutedSource(**kwargs)._timestep_explicit is False
