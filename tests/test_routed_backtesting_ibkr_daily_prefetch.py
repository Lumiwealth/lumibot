from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import MethodType

import pandas as pd
import pytest

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


def test_routed_backtesting_crypto_daily_tuple_lookup_uses_prefetched_canonical_key(monkeypatch):
    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
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

    source = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 4, 23, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 5, 5, tzinfo=timezone.utc),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    source.load_data()

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    source._update_datetime(source.datetime_start)
    bars = source.get_historical_prices((base, quote), length=5, timestep="day", quote=quote)

    assert bars is not None
    assert getattr(bars, "df", None) is not None
    assert not bars.df.empty


def test_routed_ibkr_crypto_intraday_rejects_stale_underfilled_last_price_and_quote(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    ny = "America/New_York"
    stale_ts = pd.Timestamp("2026-03-24 00:00", tz=ny)
    sim_ts = pd.Timestamp("2026-04-17 10:00", tz=ny)
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        df = pd.DataFrame(
            {
                "open": [70511.75],
                "high": [70511.75],
                "low": [70511.75],
                "close": [70511.75],
                "bid": [70510.75],
                "ask": [70512.75],
                "volume": [1.0],
            },
            index=pd.DatetimeIndex([stale_ts]),
        )
        df.index.name = "timestamp"
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    source = RoutedBacktestingPandas(
        datetime_start=pd.Timestamp("2026-03-09 00:00", tz=ny).to_pydatetime(),
        datetime_end=pd.Timestamp("2026-06-05 00:00", tz=ny).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    source.load_data()
    source._update_datetime(sim_ts.to_pydatetime())

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    assert source.get_last_price(base, timestep="minute", quote=quote) is None

    quote_obj = source.get_quote(base, quote=quote, timestep="minute")
    assert quote_obj.price is None
    assert quote_obj.bid is None
    assert quote_obj.ask is None

    key = source.find_asset_in_data_store(base, quote, "minute")
    assert key is not None
    data = source._data_store[key]
    assert data.strict_end_check is True
    assert float(data.df["close"].iloc[-1]) == 70511.75
    assert calls


def test_routed_ibkr_crypto_intraday_rejects_future_underfilled_last_price_and_quote(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    ny = "America/New_York"
    future_ts = pd.Timestamp("2026-03-24 00:00", tz=ny)
    sim_ts = pd.Timestamp("2026-03-12 07:00", tz=ny)
    calls: list[tuple[datetime, datetime, str]] = []

    def _fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append((start_dt, end_dt, str(timestep)))
        df = pd.DataFrame(
            {
                "open": [70511.75],
                "high": [70511.75],
                "low": [70511.75],
                "close": [70511.75],
                "bid": [70510.75],
                "ask": [70512.75],
                "volume": [1.0],
            },
            index=pd.DatetimeIndex([future_ts]),
        )
        df.index.name = "timestamp"
        return df

    monkeypatch.setattr(ibkr_helper, "get_price_data", _fake_get_price_data)
    monkeypatch.setattr(ibkr_helper, "frame_covers_requested_window", lambda *args, **kwargs: False)

    source = RoutedBacktestingPandas(
        datetime_start=pd.Timestamp("2026-03-09 00:00", tz=ny).to_pydatetime(),
        datetime_end=pd.Timestamp("2026-06-05 00:00", tz=ny).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    source.load_data()
    source._update_datetime(sim_ts.to_pydatetime())

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    assert source.get_last_price(base, timestep="minute", quote=quote) is None

    quote_obj = source.get_quote(base, quote=quote, timestep="minute")
    assert quote_obj.price is None
    assert quote_obj.bid is None
    assert quote_obj.ask is None

    key = source.find_asset_in_data_store(base, quote, "minute")
    assert key is not None
    data = source._data_store[key]
    assert data.strict_end_check is True
    assert float(data.df["close"].iloc[0]) == 70511.75
    assert calls


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


@pytest.mark.parametrize(
    "timestep,ts_unit,expected_timeframe",
    [
        ("5m", "minute", "1m"),
        ("1h", "hour", "1h"),
        ("day", "day", "1d"),
    ],
)
def test_routed_ccxt_crypto_prefetches_full_backtest_window_once(monkeypatch, timestep, ts_unit, expected_timeframe):
    import lumibot.tools.ccxt_data_store as ccxt_data_store

    calls: list[dict[str, object]] = []

    class FakeCcxtCacheDB:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start, end):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start,
                    "end": end,
                }
            )
            freq = {"minute": "1min", "hour": "1h", "day": "1D"}[ts_unit]
            idx = pd.date_range(start=start, end=end, freq=freq)
            if idx.empty:
                idx = pd.DatetimeIndex([pd.Timestamp(start)])
            df = pd.DataFrame(
                {
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 10.0,
                },
                index=idx,
            )
            df.index.name = "timestamp"
            return df

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", FakeCcxtCacheDB)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "crypto": "coinbase"}
    routed._data_store = {}

    backtest_start = datetime(2026, 6, 17, tzinfo=timezone.utc)
    backtest_end = datetime(2026, 6, 24, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return timestep

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        if ts_unit == "day":
            return end_dt - timedelta(days=int(length)), "day"
        if ts_unit == "hour":
            return end_dt - timedelta(hours=int(length)), "hour"
        return end_dt - timedelta(minutes=int(length)), "minute"

    def _build_dataset_keys(self, asset, quote, resolved_ts_unit):
        canonical = (asset, quote, resolved_ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)

    routed._update_pandas_data(base, quote, 1300, timestep, start_dt=backtest_start)

    assert len(calls) == 1
    assert calls[0]["exchange_id"] == "coinbase"
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == expected_timeframe
    assert calls[0]["end"] == backtest_end

    canonical_key = (base, quote, ts_unit)
    assert canonical_key in routed._data_store
    assert canonical_key in routed._registry.adapter_for_spec(routed._provider_spec_for_asset(base))._fully_loaded_series

    routed._update_pandas_data(base, quote, 1300, timestep, start_dt=backtest_start + timedelta(hours=1))
    assert len(calls) == 1


def test_routed_ccxt_crypto_expands_prefetch_when_later_lookback_is_deeper(monkeypatch):
    import lumibot.tools.ccxt_data_store as ccxt_data_store

    calls: list[dict[str, object]] = []

    class FakeCcxtCacheDB:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start, end):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start": start,
                    "end": end,
                }
            )
            idx = pd.date_range(start=start, end=end, freq="1min")
            df = pd.DataFrame(
                {
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 10.0,
                },
                index=idx,
            )
            df.index.name = "timestamp"
            return df

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", FakeCcxtCacheDB)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "crypto": "coinbase"}
    routed._data_store = {}

    backtest_start = datetime(2026, 6, 17, tzinfo=timezone.utc)
    backtest_end = datetime(2026, 6, 24, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "5m"

    def _get_start_datetime_and_ts_unit(self, length, ts, start_dt=None, start_buffer=timedelta(0)):
        end_dt = start_dt if isinstance(start_dt, datetime) else backtest_start
        return end_dt - timedelta(minutes=int(length) * 5), "minute"

    def _build_dataset_keys(self, asset, quote, ts_unit):
        canonical = (asset, quote, ts_unit)
        legacy = (asset, quote)
        return canonical, legacy

    routed.get_datetime = MethodType(_get_datetime, routed)
    routed.get_timestep = MethodType(_get_timestep, routed)
    routed.get_start_datetime_and_ts_unit = MethodType(_get_start_datetime_and_ts_unit, routed)
    routed._build_dataset_keys = MethodType(_build_dataset_keys, routed)

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)

    routed._update_pandas_data(base, quote, 400, "5m", start_dt=backtest_start)
    routed._update_pandas_data(base, quote, 1300, "5m", start_dt=backtest_start)

    assert len(calls) == 2
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == "1m"
    assert calls[1]["symbol"] == "BTC/USDT"
    assert calls[1]["timeframe"] == "1m"
    assert calls[1]["start"] < calls[0]["start"]
    assert calls[1]["end"] == backtest_end


def test_routed_ccxt_crypto_empty_full_window_is_not_retried_each_step(monkeypatch):
    import lumibot.tools.ccxt_data_store as ccxt_data_store

    calls: list[dict[str, object]] = []

    class FakeCcxtCacheDB:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start, end):
            calls.append({"symbol": symbol, "timeframe": timeframe, "start": start, "end": end})
            return pd.DataFrame()

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", FakeCcxtCacheDB)

    routed = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    routed._routing = {"default": "thetadata", "crypto": "coinbase"}
    routed._data_store = {}

    backtest_start = datetime(2026, 6, 17, tzinfo=timezone.utc)
    backtest_end = datetime(2026, 6, 24, tzinfo=timezone.utc)
    routed.datetime_start = backtest_start
    routed.datetime_end = backtest_end

    def _get_datetime(self):
        return backtest_start

    def _get_timestep(self):
        return "5m"

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

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)

    routed._update_pandas_data(base, quote, 1300, "5m", start_dt=backtest_start)
    routed._update_pandas_data(base, quote, 1300, "5m", start_dt=backtest_start + timedelta(hours=1))

    assert len(calls) == 1
    assert calls[0]["symbol"] == "BTC/USDT"
    assert routed._data_store == {}
