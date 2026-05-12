from __future__ import annotations

from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
import pytz

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_intraday_cache_end_validation_does_not_reuse_stale_prior_day_data(monkeypatch):
    """Regression: minute caches must not treat prior-day coverage as valid for a new trading day.

    The ThetaData backtesting engine uses a cache-coverage heuristic to decide whether to refetch
    intraday datasets. A prior implementation compared only *dates* and allowed a multi-day tolerance,
    which meant a dataset that ended at the prior day's close could be reused for several subsequent
    days. That caused `get_last_price()` to return the prior-day close for intraday timestamps and
    broke determinism (SPX Copy2/Copy3 cold-cache runs selected different strikes/trades vs warm runs).

    This test asserts that when the existing cache ends on 2025-01-21 but the simulation time is
    2025-01-22, `_update_pandas_data()` attempts a refetch (and thus calls `thetadata_helper.get_price_data`).
    """

    tz = pytz.timezone("America/New_York")
    dt = tz.localize(datetime(2025, 1, 22, 10, 15))

    source = ThetaDataBacktestingPandas(
        datetime_start=tz.localize(datetime(2025, 1, 21, 9, 30)),
        datetime_end=tz.localize(datetime(2025, 1, 28, 16, 0)),
        username="test",
        password="test",
        tzinfo=tz,
    )

    monkeypatch.setattr(source, "get_datetime", lambda: dt)

    asset = Asset("SPXW", asset_type=Asset.AssetType.INDEX)
    quote_asset = Asset("USD", asset_type="forex")
    canonical_key = (asset, quote_asset, "minute")

    # Existing cached data: ends at prior-day close (2025-01-21 16:00 ET).
    existing_end = tz.localize(datetime(2025, 1, 21, 16, 0))
    idx = pd.date_range(existing_end - timedelta(minutes=4), existing_end, freq="min", tz=tz)
    df = pd.DataFrame({"close": [6049.25, 6049.26, 6049.27, 6049.28, 6049.29]}, index=idx)
    source.pandas_data[canonical_key] = SimpleNamespace(df=df, timestep="minute")
    source._dataset_metadata[canonical_key] = {
        "timestep": "minute",
        "start": idx[0].to_pydatetime(),
        "end": idx[-1].to_pydatetime(),
        "rows": len(df),
        "has_quotes": False,
        "has_ohlc": True,
        "prefetch_complete": True,
    }

    def _fetch_called(*_args, **_kwargs):
        raise RuntimeError("fetch_called")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fetch_called)

    with pytest.raises(RuntimeError, match="fetch_called"):
        source._update_pandas_data(
            asset, quote_asset, 5, "minute", dt, require_quote_data=False, require_ohlc_data=True
        )


def test_intraday_index_minute_ohlc_clamps_end_requirement_to_session_close(monkeypatch, caplog):
    """Regression: index minute OHLC should treat session close as "complete" coverage.

    Theta index minute OHLC backtests are regular-session bounded (RTH). If the backtest end bound
    is represented as midnight (common for date-only UI inputs), the backtesting datasource may
    derive an intraday `end_requirement` at 23:59 (or 18:59 for UTC-midnight). Requiring coverage
    beyond session close (16:00 ET) makes the cache impossible to satisfy and causes a perpetual
    STALE→REFRESH loop.

    This test seeds an index minute OHLC cache that ends exactly at session close on the last
    trading day and asserts `_update_pandas_data()` does not attempt to refetch.
    """

    tz = pytz.timezone("America/New_York")
    dt = tz.localize(datetime(2025, 2, 6, 10, 15))

    # Backtest end is at midnight (date-only semantics); DataSourceBacktesting will subtract 1 min,
    # making the internal end_anchor 23:59 on 2025-02-06.
    source = ThetaDataBacktestingPandas(
        datetime_start=tz.localize(datetime(2025, 2, 3, 0, 0)),
        datetime_end=tz.localize(datetime(2025, 2, 7, 0, 0)),
        username="test",
        password="test",
        tzinfo=tz,
    )
    monkeypatch.setattr(source, "get_datetime", lambda: dt)
    monkeypatch.setenv("BACKTESTING_MARKET", "NYSE")

    asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote_asset = Asset("USD", asset_type="forex")
    canonical_key = (asset, quote_asset, "minute")

    existing_end = tz.localize(datetime(2025, 2, 6, 16, 0))
    idx = pd.date_range(existing_end - timedelta(minutes=4), existing_end, freq="min", tz=tz)
    df = pd.DataFrame({"close": [6075.0, 6075.1, 6075.2, 6075.3, 6075.4]}, index=idx)
    source.pandas_data[canonical_key] = SimpleNamespace(df=df, timestep="minute")
    source._dataset_metadata[canonical_key] = {
        "timestep": "minute",
        "start": idx[0].to_pydatetime(),
        "end": idx[-1].to_pydatetime(),
        "rows": len(df),
        "has_quotes": False,
        "has_ohlc": True,
        "prefetch_complete": True,
    }

    def _fetch_called(*_args, **_kwargs):
        raise RuntimeError("fetch_called")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fetch_called)

    with caplog.at_level("INFO"):
        source._update_pandas_data(
            asset, quote_asset, 5, "minute", dt, require_quote_data=False, require_ohlc_data=True
        )

    assert "[THETA][CACHE][STALE]" not in caplog.text


def test_intraday_index_minute_quote_clamps_end_requirement_to_session_close(monkeypatch, caplog):
    """Regression: index intraday coverage clamping must apply even for quote/last-price probes.

    Some strategies (and OptionsHelper internals) request intraday index data via "quote"/last price
    paths rather than explicit OHLC requests. The end-coverage heuristic still must treat the session
    close as complete, otherwise backtests can enter the same STALE→REFRESH loop.
    """

    tz = pytz.timezone("America/New_York")
    dt = tz.localize(datetime(2025, 2, 6, 10, 15))

    source = ThetaDataBacktestingPandas(
        datetime_start=tz.localize(datetime(2025, 2, 3, 0, 0)),
        datetime_end=tz.localize(datetime(2025, 2, 7, 0, 0)),
        username="test",
        password="test",
        tzinfo=tz,
    )
    monkeypatch.setattr(source, "get_datetime", lambda: dt)
    monkeypatch.setenv("BACKTESTING_MARKET", "NYSE")

    asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote_asset = Asset("USD", asset_type="forex")
    canonical_key = (asset, quote_asset, "minute")

    existing_end = tz.localize(datetime(2025, 2, 6, 16, 0))
    idx = pd.date_range(existing_end - timedelta(minutes=4), existing_end, freq="min", tz=tz)
    df = pd.DataFrame({"close": [6075.0, 6075.1, 6075.2, 6075.3, 6075.4]}, index=idx)
    source.pandas_data[canonical_key] = SimpleNamespace(df=df, timestep="minute")
    source._dataset_metadata[canonical_key] = {
        "timestep": "minute",
        "start": idx[0].to_pydatetime(),
        "end": idx[-1].to_pydatetime(),
        "rows": len(df),
        "has_quotes": True,
        "has_ohlc": True,
        "prefetch_complete": True,
    }

    def _fetch_called(*_args, **_kwargs):
        raise RuntimeError("fetch_called")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fetch_called)

    with caplog.at_level("INFO"):
        source._update_pandas_data(
            asset, quote_asset, 5, "minute", dt, require_quote_data=True, require_ohlc_data=False
        )

    assert "[THETA][CACHE][STALE]" not in caplog.text


def test_intraday_index_minute_clamps_end_requirement_to_last_trading_session_close(monkeypatch, caplog):
    """Regression: index intraday coverage must align weekends/holidays to the last trading close.

    Example failure: backtest end date lands on a market holiday, cached data ends at the previous
    session close (or early close), and the datasource repeatedly refetches because it demands
    coverage through the non-trading end date.
    """

    tz = pytz.timezone("America/New_York")
    dt = tz.localize(datetime(2025, 12, 24, 12, 0))

    # End is exclusive midnight 2025-12-26 -> internal end_requirement is 2025-12-25 23:59 (holiday).
    source = ThetaDataBacktestingPandas(
        datetime_start=tz.localize(datetime(2025, 12, 1, 0, 0)),
        datetime_end=tz.localize(datetime(2025, 12, 26, 0, 0)),
        username="test",
        password="test",
        tzinfo=tz,
    )
    monkeypatch.setattr(source, "get_datetime", lambda: dt)
    monkeypatch.setenv("BACKTESTING_MARKET", "NYSE")

    asset = Asset("SPXW", asset_type=Asset.AssetType.INDEX)
    quote_asset = Asset("USD", asset_type="forex")
    canonical_key = (asset, quote_asset, "minute")

    # 2025-12-24 is an early close (13:00 ET). Seed cache through that close.
    existing_end = tz.localize(datetime(2025, 12, 24, 13, 0))
    idx = pd.date_range(existing_end - timedelta(minutes=4), existing_end, freq="min", tz=tz)
    df = pd.DataFrame({"close": [5950.0, 5950.1, 5950.2, 5950.3, 5950.4]}, index=idx)
    source.pandas_data[canonical_key] = SimpleNamespace(df=df, timestep="minute")
    source._dataset_metadata[canonical_key] = {
        "timestep": "minute",
        "start": idx[0].to_pydatetime(),
        "end": idx[-1].to_pydatetime(),
        "rows": len(df),
        "has_quotes": True,
        "has_ohlc": True,
        "prefetch_complete": True,
    }

    def _fetch_called(*_args, **_kwargs):
        raise RuntimeError("fetch_called")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fetch_called)

    with caplog.at_level("INFO"):
        source._update_pandas_data(
            asset, quote_asset, 5, "minute", dt, require_quote_data=False, require_ohlc_data=True
        )

    assert "[THETA][CACHE][STALE]" not in caplog.text


def test_index_minute_fetch_bounds_end_to_dt_not_backtest_end(monkeypatch):
    """Regression: index minute fetches must be bounded to the simulation timestamp.

    For intraday backtests we fetch and validate coverage incrementally as `dt` advances.
    If the request end is forced to the full backtest end up-front, early iterations can fail
    with large coverage gaps even when the data path is otherwise healthy.
    """

    tz = pytz.timezone("America/New_York")
    dt = tz.localize(datetime(2025, 1, 2, 10, 15))

    source = ThetaDataBacktestingPandas(
        datetime_start=tz.localize(datetime(2025, 1, 1, 0, 0)),
        datetime_end=tz.localize(datetime(2025, 12, 1, 0, 0)),
        username="test",
        password="test",
        tzinfo=tz,
    )
    monkeypatch.setattr(source, "get_datetime", lambda: dt)
    asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote_asset = Asset("USD", asset_type="forex")

    calls = []

    def _fake_get_price_data(_fetch_asset, _start, _end, **_kwargs):
        calls.append({"start": _start, "end": _end})
        idx = pd.DatetimeIndex([dt])
        df = pd.DataFrame(
            {"open": [6000.0], "high": [6000.0], "low": [6000.0], "close": [6000.0], "volume": [0.0]},
            index=idx,
        )
        return df

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    source._update_pandas_data(asset, quote_asset, 60, "minute", dt, require_quote_data=False, require_ohlc_data=True)

    assert calls, "Expected _update_pandas_data() to fetch OHLC via thetadata_helper.get_price_data()"
    assert calls[0]["end"] == dt
    assert calls[0]["end"] != source.datetime_end
