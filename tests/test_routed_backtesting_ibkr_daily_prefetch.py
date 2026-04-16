from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import MethodType

import pandas as pd

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.entities import Asset


def test_routed_backtesting_prefetches_ibkr_crypto_daily_window_once(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, end=end_dt, freq="D")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
                "bid": 100.0,
                "ask": 100.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)

    # Avoid invoking ThetaDataBacktestingPandas.__init__() (kills local ThetaTerminal processes).
    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "crypto": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2025, 12, 1, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "day"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(days=int(length)), "day"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # First call should prefetch against the full backtest window (end_dt=backtest_end).
    routed._update_pandas_data(base, quote, 205, "day", start_dt=backtest_start)
    assert calls, "Expected ibkr_helper.get_price_data to be called"
    assert calls[0][1] == backtest_end

    # Second call (same series) should not re-fetch.
    routed._update_pandas_data(base, quote, 205, "day", start_dt=backtest_start + timedelta(days=1))
    assert len(calls) == 1


def test_routed_backtesting_prefetches_ibkr_stock_daily_with_full_lookback(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, end=end_dt, freq="D")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
                "bid": 100.0,
                "ask": 100.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2022, 12, 31, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "day"

    # For 200+ day SMA lookbacks, the start window should be substantially deeper than
    # "backtest start - ~225 calendar days".
    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(days=352), "day"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    routed._update_pandas_data(base, quote, 220, "day", start_dt=backtest_start)
    assert calls, "Expected ibkr_helper.get_price_data to be called"
    assert calls[0][0] == backtest_start - timedelta(days=352)
    assert calls[0][1] == backtest_end

    routed._update_pandas_data(base, quote, 220, "day", start_dt=backtest_start + timedelta(days=1))
    assert len(calls) == 1


def test_routed_backtesting_stock_daily_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, periods=5, freq="D")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
                "bid": 100.0,
                "ask": 100.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2021, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2022, 12, 31, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "day"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(days=352), "day"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("TSLA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    routed._update_pandas_data(base, quote, 220, "day", start_dt=backtest_start)
    assert calls
    assert (base, quote, "day") not in routed._fully_loaded_series


def test_routed_backtesting_stock_minute_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, periods=5, freq="1min")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "stock": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2025, 1, 2, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "minute"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(minutes=int(length)), "minute"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("TSLA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    routed._update_pandas_data(base, quote, 30, "minute", start_dt=backtest_start)
    assert calls
    assert (base, quote, "minute") not in routed._fully_loaded_series


def test_routed_backtesting_future_minute_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, periods=5, freq="1min")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "cont_future": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2025, 1, 2, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "minute"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(minutes=int(length)), "minute"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    routed._update_pandas_data(base, quote, 30, "minute", start_dt=backtest_start)
    assert calls
    assert (base, quote, "minute") not in routed._fully_loaded_series


def test_routed_backtesting_crypto_daily_prefetch_stays_unloaded_when_coverage_fails(monkeypatch):
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        idx = pd.date_range(start=start_dt, periods=5, freq="D")
        df = pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1.0,
            },
            index=idx,
        )
        df.index.name = "timestamp"
        return df

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "crypto": "ibkr"}
    routed._ibkr_fully_loaded_series = set()
    routed._fully_loaded_series = set()
    routed._data_store = {}

    backtest_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    backtest_end = datetime(2025, 1, 10, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "day"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(days=int(length)), "day"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    routed._update_pandas_data(base, quote, 30, "day", start_dt=backtest_start)
    assert calls
    assert (base, quote, "day") not in routed._fully_loaded_series
