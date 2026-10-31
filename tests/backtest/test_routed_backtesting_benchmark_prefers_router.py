from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.strategies import Strategy


class _BenchmarkSmokeStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        return


def test_router_benchmark_uses_router_datasource_instead_of_yahoo(monkeypatch):
    # Avoid any local ThetaTerminal side effects during datasource init.
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 0, 0))

    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": {"default": "thetadata", "cont_future": "ibkr"}},
    )

    benchmark_calls = {"router": 0, "yahoo": 0}

    class _Bars:
        def __init__(self, df: pd.DataFrame):
            self.df = df

    bench_index = pd.date_range(start, end, freq="1D", inclusive="left")
    bench_df = pd.DataFrame(
        {
            "open": [100.0] * len(bench_index),
            "high": [101.0] * len(bench_index),
            "low": [99.0] * len(bench_index),
            "close": [100.0] * len(bench_index),
            "volume": [1000] * len(bench_index),
        },
        index=bench_index,
    )

    def fake_between_dates(*args, **kwargs):
        benchmark_calls["router"] += 1
        return _Bars(bench_df)

    monkeypatch.setattr(router, "get_historical_prices_between_dates", fake_between_dates)

    import lumibot.tools.indicators as indicators

    def boom(*args, **kwargs):
        benchmark_calls["yahoo"] += 1
        raise AssertionError("Yahoo benchmark fetch should not be used for router backtests")

    monkeypatch.setattr(indicators, "get_symbol_returns", boom)

    broker = BacktestingBroker(data_source=router)

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

    assert benchmark_calls["router"] == 1
    assert benchmark_calls["yahoo"] == 0
    assert strategy._benchmark_returns_df is not None
    assert not strategy._benchmark_returns_df.empty


@pytest.mark.parametrize("bad_open,missing_session", [(None, False), (float("nan"), False), (0., False), (100., True)])
@pytest.mark.parametrize("fallback_available", [False, True])
def test_invalid_router_benchmark_uses_existing_fallback(monkeypatch, bad_open, missing_session, fallback_available):
    from types import SimpleNamespace

    from lumibot.strategies import _strategy

    index = pd.date_range("2025-01-06", periods=2, tz="America/New_York")
    if missing_session:
        index = pd.date_range("2025-01-06", periods=3, tz="America/New_York")[[0, 2]]
    bars = pd.DataFrame({"open": [bad_open, 101.], "close": [101., 102.]}, index=index)
    router = object.__new__(RoutedBacktestingPandas)
    monkeypatch.setattr(router, "get_historical_prices_between_dates",
                        lambda *args, **kwargs: SimpleNamespace(df=bars))
    fallback_index = pd.date_range(index[0], index[-1], freq="D")
    fallback = pd.DataFrame({"symbol_cumprod": [1. + .01 * n for n in range(len(fallback_index))]},
                            index=fallback_index)
    if not fallback_available:
        fallback = None
    calls = []

    def read_fallback(*args):
        calls.append(args)
        return fallback

    monkeypatch.setattr(_strategy, "get_symbol_returns", read_fallback)
    strategy = SimpleNamespace(is_backtesting=True, _benchmark_asset=Asset("ALPHA"),
        _backtesting_start=index[0], _backtesting_end=index[-1],
        _quote_asset=Asset("USD", Asset.AssetType.FOREX),
        broker=SimpleNamespace(data_source=router, market="NYSE"),
        logger=SimpleNamespace(error=lambda *args: None, warning=lambda *args: None))
    _strategy._Strategy._dump_benchmark_stats(strategy)
    assert len(calls) == 1
    assert strategy._benchmark_returns_df is fallback


@pytest.mark.parametrize("asset_type,strategy_market,expected_calendar,dates", [
    ("stock", "LSE", "LSE", ["2025-04-17", "2025-04-22"]),
    ("stock", "24/7", "NYSE", ["2025-01-17", "2025-01-21"]),
    ("crypto", "NYSE", "24/7", ["2025-01-04", "2025-01-05"]),
])
def test_router_benchmark_preserves_equity_and_continuous_calendars(
    monkeypatch, asset_type, strategy_market, expected_calendar, dates,
):
    from types import SimpleNamespace

    from lumibot.strategies import _strategy

    index = pd.to_datetime(dates).tz_localize("UTC")
    bars = pd.DataFrame({"open": [100., 101.], "close": [101., 102.]}, index=index)
    router = object.__new__(RoutedBacktestingPandas)
    monkeypatch.setattr(router, "get_historical_prices_between_dates",
                        lambda *args, **kwargs: SimpleNamespace(df=bars))

    def unexpected_fallback(*args):
        pytest.fail("Complete benchmark sessions should use the routed observations")

    monkeypatch.setattr(_strategy, "get_symbol_returns", unexpected_fallback)
    strategy = SimpleNamespace(is_backtesting=True, _benchmark_asset=Asset("ALPHA", asset_type),
        _backtesting_start=index[0], _backtesting_end=index[-1] + pd.Timedelta(hours=16),
        _quote_asset=Asset("USD", Asset.AssetType.FOREX),
        broker=SimpleNamespace(data_source=router, market=strategy_market),
        logger=SimpleNamespace(error=lambda *args: None, warning=lambda *args: None))
    _strategy._Strategy._dump_benchmark_stats(strategy)
    assert strategy._benchmark_returns_df.attrs["benchmark_calendar"] == expected_calendar
    assert strategy._benchmark_returns_df.symbol_cumprod.iloc[-1] == pytest.approx(1.02)


@pytest.mark.parametrize("missing_boundary", [None, "first", "last"])
def test_router_daily_benchmark_covers_requested_sessions_from_intraday_start(monkeypatch, missing_boundary):
    from types import SimpleNamespace

    from lumibot.entities import Data
    from lumibot.strategies import _strategy

    index = pd.date_range("2025-01-06", periods=3, tz="America/New_York")
    frame = pd.DataFrame({"open": [100., 104., 108.], "high": [105., 109., 111.],
        "low": [99., 103., 107.], "close": [105., 109., 110.], "volume": [100.] * 3}, index=index)
    if missing_boundary == "first":
        frame = frame.iloc[1:]
    elif missing_boundary == "last":
        frame = frame.iloc[:-1]
    data = Data(Asset("ALPHA"), frame, timestep="day")
    router = object.__new__(RoutedBacktestingPandas)
    requests = []

    def between_dates(asset, timestep, **kwargs):
        requests.append(kwargs)
        return SimpleNamespace(df=data.get_bars_between_dates(timestep=timestep,
            start_date=kwargs["start_date"], end_date=kwargs["end_date"]))

    monkeypatch.setattr(router, "get_historical_prices_between_dates", between_dates)
    fallback_calls = []
    monkeypatch.setattr(_strategy, "get_symbol_returns", lambda *args: fallback_calls.append(args))
    start = index[0] + pd.Timedelta(hours=9, minutes=30)
    end = index[-1] + pd.Timedelta(hours=16)
    strategy = SimpleNamespace(is_backtesting=True, _benchmark_asset=Asset("ALPHA"),
        _backtesting_start=start, _backtesting_end=end, _quote_asset=Asset("USD", Asset.AssetType.FOREX),
        broker=SimpleNamespace(data_source=router, market="NYSE"),
        logger=SimpleNamespace(error=lambda *args: None, warning=lambda *args: None))
    _strategy._Strategy._dump_benchmark_stats(strategy)
    assert requests[0]["start_date"] == index[0]
    if missing_boundary:
        assert len(fallback_calls) == 1
        assert strategy._benchmark_returns_df is None
    else:
        assert not fallback_calls
        assert strategy._benchmark_returns_df.symbol_cumprod.iloc[-1] == pytest.approx(1.10)
