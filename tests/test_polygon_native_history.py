"""Exercise native provider storage while only the external OHLCV response is synthetic."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper


@pytest.fixture
def market(monkeypatch):
    ny = ZoneInfo("America/New_York")
    daily_index = pd.bdate_range(end="2025-01-17", periods=400, tz=ny)
    minute_index = pd.date_range("2025-01-13 09:00", "2025-01-13 10:00", freq="min", tz=ny)

    def frame(index, price):
        return pd.DataFrame(
            {"open": price, "high": price, "low": price, "close": price, "volume": 1000}, index=index
        )

    daily, minute = frame(daily_index, 99.0), frame(minute_index, 120.0)
    minute.loc[minute.index > datetime(2025, 1, 13, 9, 30, tzinfo=ny), ["open", "high", "low", "close"]] = 900
    requests = []

    def provider(*args, **kwargs):
        requests.append(kwargs["timespan"])
        return (daily if kwargs["timespan"] == "day" else minute).copy()

    monkeypatch.setattr(polygon_helper, "get_price_data_from_polygon", provider)
    source = PolygonDataBacktesting(
        datetime(2025, 1, 13, 9, 30, tzinfo=ny), datetime(2025, 1, 17, tzinfo=ny), api_key="synthetic"
    )
    return source, Asset("SYNTH"), Asset("USD", asset_type="forex"), requests, daily, minute


@pytest.mark.parametrize("minute_first", [False, True])
def test_daily_research_and_minute_price_coexist(market, minute_first):
    source, asset, usd, requests, _, _ = market
    if minute_first:
        assert source.get_last_price(asset, quote=usd) == 120
    initial = source.get_historical_prices(asset, 200, timestep="day", quote=usd)
    assert initial is not None and len(initial.df) == 200
    assert initial.df.iloc[-1]["close"] == 99
    assert initial.df.index.max() < source.get_datetime()  # No future research bars.
    assert source.get_last_price(asset, quote=usd) == 120
    assert requests.count("day") == 1
    assert requests.count("minute") == 1
    requests.clear()
    repeated = source.get_historical_prices(asset, 200, timestep="day", quote=usd)
    assert repeated is not None
    pd.testing.assert_frame_equal(repeated.df, initial.df)
    assert source.get_last_price(asset, quote=usd) == 120
    assert requests == []


def test_daily_only_reuses_daily_without_minute_download(market):
    source, asset, usd, requests, _, _ = market
    first = source.get_historical_prices(asset, 200, timestep="day", quote=usd)
    second = source.get_historical_prices(asset, 200, timestep="day", quote=usd)
    pd.testing.assert_frame_equal(first.df, second.df)
    assert requests == ["day"]


def test_intervals_remain_separate_for_each_quote_currency(market, monkeypatch):
    source, asset, usd, requests, _, _ = market
    eur = Asset("EUR", asset_type="forex")
    original = polygon_helper.get_price_data_from_polygon

    def provider(*args, **kwargs):
        result = original(*args, **kwargs)
        if kwargs["quote_asset"] == eur:
            result[["open", "high", "low", "close"]] *= 2
        return result

    monkeypatch.setattr(polygon_helper, "get_price_data_from_polygon", provider)
    for quote, scale in [(usd, 1), (eur, 2)]:
        bars = source.get_historical_prices(asset, 200, timestep="day", quote=quote)
        assert bars.df.iloc[-1]["close"] == 99 * scale
        assert source.get_last_price(asset, quote=quote) == 120 * scale
    requests.clear()
    for quote, scale in [(usd, 1), (eur, 2)]:
        assert source.get_historical_prices(asset, 200, timestep="day", quote=quote).df.iloc[-1]["close"] == 99 * scale
        assert source.get_last_price(asset, quote=quote) == 120 * scale
    assert requests == []


def test_preloaded_intervals_keep_daily_history_and_finest_legacy_price(market):
    source, asset, usd, _, daily, minute = market
    stored = source._set_pandas_data_keys(
        [Data(asset, minute, timestep="minute", quote=usd), Data(asset, daily, timestep="day", quote=usd)]
    )
    assert stored[(asset, usd, "day")].timestep == "day"
    assert stored[(asset, usd, "minute")].timestep == "minute"
    assert stored[(asset, usd)].timestep == "minute"


def test_storage_limit_counts_aliases_once_and_evicts_all_aliases(market):
    source, asset, usd, _, _, minute = market
    first = Data(asset, minute.copy(), timestep="minute", quote=usd)
    other = Asset("OTHER")
    second = Data(other, minute.copy(), timestep="minute", quote=usd)
    source.pandas_data.clear()
    source.pandas_data.update(source._set_pandas_data_keys([first, second]))
    source.MAX_STORAGE_BYTES = int(second.df.memory_usage().sum())
    source._enforce_storage_limit(source.pandas_data)
    assert set(source.pandas_data) == {(other, usd), (other, usd, "minute")}
    assert source.pandas_data[(other, usd)] is source.pandas_data[(other, usd, "minute")]
