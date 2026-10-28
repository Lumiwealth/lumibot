"""Routed providers retain daily session labels through cache construction/refresh."""
import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import (
    ProviderSpec,
    RoutedBacktestingPandas,
    _DataFrameRoutingAdapter,
    _IbkrRoutingAdapter,
)
from lumibot.entities import Asset
from lumibot.tools.indicators import benchmark_returns_from_bars


def _frame(index, prices):
    return pd.DataFrame({"open": prices, "high": prices, "low": prices,
                         "close": prices, "volume": [100.] * len(prices)}, index=index)


def _load(monkeypatch, frames, timestep="day", adapter_type=_DataFrameRoutingAdapter):
    router = object.__new__(RoutedBacktestingPandas)
    router._data_store = {}
    router.datetime_start = pd.Timestamp("2025-01-06", tz="America/New_York")
    # Keep the fixture outside the provider's existing prefetch tolerance so
    # the second request exercises an overlapping refresh, not the loaded cache.
    router.datetime_end = pd.Timestamp("2025-01-20", tz="America/New_York")
    adapter = adapter_type(router)
    responses = iter(frames)
    fetch_method = "_get_price_data" if adapter_type is _IbkrRoutingAdapter else "_fetch_df"
    monkeypatch.setattr(adapter, fetch_method, lambda **kwargs: next(responses).copy())
    asset, quote = Asset("ALPHA"), Asset("USD", "forex")

    def update(end):
        adapter.update_pandas_data(asset=asset, quote_asset=quote, length=3,
            timestep=timestep, start_dt=pd.Timestamp(end, tz="America/New_York"),
            require_quote_data=False, require_ohlc_data=True, snapshot_only=False,
            provider_spec=ProviderSpec("synthetic"))
        return router._data_store[(asset, quote, timestep)]

    return update


@pytest.mark.parametrize("timezone", [None, "UTC", "America/New_York", "America/Los_Angeles", "Asia/Tokyo"])
@pytest.mark.parametrize("adapter_type", [_DataFrameRoutingAdapter, _IbkrRoutingAdapter])
def test_daily_source_labels_survive_routed_load_and_overlapping_refresh(monkeypatch, timezone, adapter_type):
    frames = [
        _frame(pd.date_range("2025-01-06", periods=3, tz=timezone), [10., 11., 12.]),
        _frame(pd.date_range("2025-01-08", periods=3, tz=timezone), [12., 13., 14.]),
    ]
    update = _load(monkeypatch, frames, adapter_type=adapter_type)
    first = update("2025-01-09")
    begin = pd.Timestamp("2025-01-06", tz="America/New_York")
    end = pd.Timestamp("2025-01-09", tz="America/New_York")
    assert first.get_bars_between_dates(timestep="day", start_date=begin, end_date=end).close.tolist() == [10., 11., 12.]
    bars = first.get_bars_between_dates(timestep="day", start_date=begin, end_date=end)
    benchmark = benchmark_returns_from_bars(bars, market="NYSE", start_date=begin, end_date=end)
    assert benchmark.symbol_cumprod.iloc[-1] == pytest.approx(1.2)
    assert list(bars.index.date) == list(pd.date_range("2025-01-06", periods=3).date)
    refreshed = update("2025-01-11")
    assert refreshed.get_bars_between_dates(timestep="day", start_date=begin,
        end_date=pd.Timestamp("2025-01-11", tz="America/New_York")).close.tolist() == [10., 11., 12., 13., 14.]
    assert refreshed.df.index.is_unique
    assert str(refreshed.df.index.tz) == "America/New_York"


@pytest.mark.parametrize("timestep,index,begin,end,expected", [
    ("day", pd.date_range("2025-01-06T21:00", periods=3, tz="UTC"),
     "2025-01-06T12:00", "2025-01-07T12:00", [10.]),
    ("minute", pd.date_range("2025-01-06T14:30", periods=3, freq="min", tz="UTC"),
     "2025-01-06T09:30:30", "2025-01-06T09:33", [11., 12.]),
])
def test_routed_intraday_and_non_midnight_daily_values_remain_instants(monkeypatch, timestep, index, begin, end, expected):
    data = _load(monkeypatch, [_frame(index, [10., 11., 12.])], timestep)("2025-01-09")
    result = data.get_bars_between_dates(timestep=timestep,
        start_date=pd.Timestamp(begin, tz="America/New_York"),
        end_date=pd.Timestamp(end, tz="America/New_York"))
    assert result.close.tolist() == expected
    assert data._day_label_timezone is None
