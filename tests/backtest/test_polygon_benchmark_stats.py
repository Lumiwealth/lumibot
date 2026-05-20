from __future__ import annotations

from datetime import datetime

from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.strategies import Strategy


class _BenchmarkSmokeStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        return


def test_polygon_benchmark_missing_bars_do_not_crash(monkeypatch):
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 0, 0))

    data_source = PolygonDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        api_key="dummy",
    )
    monkeypatch.setattr(data_source, "get_historical_prices_between_dates", lambda *args, **kwargs: None)

    broker = BacktestingBroker(data_source=data_source)
    strategy = _BenchmarkSmokeStrategy(
        broker=broker,
        budget=100_000,
        backtesting_start=start,
        backtesting_end=end,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        risk_free_rate=0,
        analyze_backtest=False,
    )
    strategy.is_backtesting = True

    strategy._dump_benchmark_stats()

    assert strategy._benchmark_returns_df is None
