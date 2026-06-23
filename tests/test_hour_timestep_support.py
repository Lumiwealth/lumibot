from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytz

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.data_sources.ccxt_backtesting_data import CcxtBacktestingData
from lumibot.entities import Asset, Data


def test_data_supports_hour_timestep_and_get_bars():
    asset = Asset(symbol="SPY", asset_type="stock")
    quote = Asset(symbol="USD", asset_type="forex")

    idx = pd.DatetimeIndex(
        [
            datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 13, 0, tzinfo=timezone.utc),
        ]
    ).tz_convert("America/New_York")
    df = pd.DataFrame(
        {"open": [1.0, 2.0, 3.0, 4.0], "high": [1.1, 2.1, 3.1, 4.1], "low": [0.9, 1.9, 2.9, 3.9], "close": [1.0, 2.0, 3.0, 4.0], "volume": [10, 11, 12, 13]},
        index=idx,
    )

    data = Data(asset, df, timestep="hour", quote=quote)
    bars = data.get_bars(dt=idx[-1].to_pydatetime(), length=2, timestep="hour")

    assert bars is not None
    assert len(bars) == 2
    assert list(bars.columns)[:4] == ["open", "high", "low", "close"]


def test_data_native_hour_timeshift_minus_one_includes_current_bar():
    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

    idx = pd.DatetimeIndex(
        [
            datetime(2026, 3, 15, 2, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 15, 3, 0, tzinfo=timezone.utc),
            datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
        ]
    )
    df = pd.DataFrame(
        {
            "open": [90.0, 95.0, 100.0],
            "high": [91.0, 96.0, 101.0],
            "low": [89.0, 94.0, 99.0],
            "close": [90.5, 95.5, 100.5],
            "volume": [9.0, 9.5, 10.0],
        },
        index=idx,
    )

    data = Data(asset, df, timestep="hour", quote=quote)
    bars = data.get_bars(
        dt=datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc),
        length=1,
        timestep="hour",
        timeshift=-1,
    )

    assert bars is not None
    assert len(bars) == 1
    assert bars.index[-1].tz_convert("UTC") == pd.Timestamp("2026-03-15 04:00:00+00:00")
    assert bars["close"].iloc[-1] == 100.5
    assert getattr(data, "_get_bars_slice_cache_key")[0] == "native_1"


def test_routed_backtesting_allows_hour_history_for_futures(monkeypatch):
    import lumibot.tools.thetadata_helper as thetadata_helper
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = {"ibkr": 0}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls["ibkr"] += 1
        idx = pd.date_range(start=start_dt, end=end_dt, freq="h")
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        idx = idx.tz_convert("America/New_York")
        return pd.DataFrame(
            {"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 10.0},
            index=idx,
        )

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"future": "ibkr", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    asset = Asset(symbol="MES", asset_type="future")
    quote = Asset(symbol="USD", asset_type="forex")
    bars = ds.get_historical_prices(asset, length=3, timestep="hour", quote=quote)

    assert calls["ibkr"] >= 1
    assert bars is not None
    assert getattr(bars, "df", None) is not None
    assert not bars.df.empty


def test_routed_backtesting_allows_hour_history_for_ccxt_crypto(monkeypatch):
    import lumibot.tools.ccxt_data_store as ccxt_data_store
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = []

    class _FakeCcxtCache:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start_datetime, end_dt):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_datetime": start_datetime,
                    "end_dt": end_dt,
                }
            )
            idx = pd.DatetimeIndex(
                [
                    datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 11, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
                ]
            )
            return pd.DataFrame(
                {"open": [1.0, 2.0, 3.0], "high": [1.1, 2.1, 3.1], "low": [0.9, 1.9, 2.9], "close": [1.0, 2.0, 3.0], "volume": [10, 11, 12]},
                index=idx.tz_localize(None),
            )

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", _FakeCcxtCache)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 2, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto": "coinbase", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
    bars = ds.get_historical_prices(asset, length=2, timestep="hour", quote=quote)

    assert calls
    assert calls[0]["exchange_id"] == "coinbase"
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == "1h"
    assert bars is not None
    assert getattr(bars, "df", None) is not None
    assert not bars.df.empty


def test_ccxt_between_dates_populates_cold_cache(monkeypatch):
    import lumibot.data_sources.ccxt_backtesting_data as ccxt_backtesting_data

    calls = []

    class _FakeCcxtCache:
        def __init__(self, exchange_id, max_download_limit=None):
            self.exchange_id = exchange_id
            self.max_download_limit = max_download_limit

        def download_ohlcv(self, symbol, timeframe, start_datetime, end_dt):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_datetime": start_datetime,
                    "end_dt": end_dt,
                }
            )
            return pd.DataFrame(
                {
                    "open": [100.0, 101.0],
                    "high": [101.0, 102.0],
                    "low": [99.0, 100.0],
                    "close": [100.5, 101.5],
                    "volume": [10.0, 11.0],
                },
                index=pd.DatetimeIndex(
                    [
                        datetime(2026, 3, 15, 0, 0),
                        datetime(2026, 3, 15, 1, 0),
                    ]
                ),
            )

    monkeypatch.setattr(ccxt_backtesting_data, "CcxtCacheDB", _FakeCcxtCache)

    ds = CcxtBacktestingData(
        datetime_start=datetime(2026, 3, 15, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 3, 16, tzinfo=timezone.utc),
        exchange_id="coinbase",
    )
    assert ds.get_timestep() == "minute"
    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)

    bars = ds.get_historical_prices_between_dates(
        asset=asset,
        quote=quote,
        timestep="hour",
        start_date=datetime(2026, 3, 15, tzinfo=timezone.utc),
        end_date=datetime(2026, 3, 15, 1, tzinfo=timezone.utc),
    )

    assert calls
    assert calls[0]["exchange_id"] == "coinbase"
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == "1h"
    assert bars is not None
    assert list(bars.df["close"]) == [100.5, 101.5]


def test_routed_ccxt_hour_history_handles_new_york_midnight_as_utc_cache_boundary(monkeypatch):
    import lumibot.tools.ccxt_data_store as ccxt_data_store
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    calls = []
    ny = pytz.timezone("America/New_York")

    class _FakeCcxtCache:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start_datetime, end_dt):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_datetime": start_datetime,
                    "end_dt": end_dt,
                }
            )
            idx = pd.DatetimeIndex(
                [
                    datetime(2026, 3, 15, 0, 0),
                    datetime(2026, 3, 15, 4, 0),
                    datetime(2026, 3, 15, 5, 0),
                ]
            )
            return pd.DataFrame(
                {
                    "open": [90.0, 100.0, 101.0],
                    "high": [91.0, 101.0, 102.0],
                    "low": [89.0, 99.0, 100.0],
                    "close": [90.5, 100.5, 101.5],
                    "volume": [9.0, 10.0, 11.0],
                },
                index=idx,
            )

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", _FakeCcxtCache)

    ds = RoutedBacktestingPandas(
        datetime_start=ny.localize(datetime(2026, 3, 15, 0, 0)),
        datetime_end=ny.localize(datetime(2026, 3, 15, 6, 0)),
        config={"backtesting_data_routing": {"crypto": "coinbase", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = ny.localize(datetime(2026, 3, 15, 0, 0))

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
    bars = ds.get_historical_prices(asset, length=1, timestep="hour", quote=quote, timeshift=-1)

    assert calls
    assert bars is not None
    assert getattr(bars, "df", None) is not None
    assert len(bars.df) == 1
    assert bars.df.index[-1].tz_convert("UTC") == pd.Timestamp("2026-03-15 04:00:00+00:00")
    assert bars.df["close"].iloc[-1] == 100.5


def _routed_crypto_source_with_fake_ccxt(monkeypatch, calls):
    import lumibot.tools.ccxt_data_store as ccxt_data_store
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_args, **_kwargs: None)

    class _FakeCcxtCache:
        def __init__(self, exchange_id):
            self.exchange_id = exchange_id

        def download_ohlcv(self, symbol, timeframe, start_datetime, end_dt):
            calls.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "start_datetime": start_datetime,
                    "end_dt": end_dt,
                }
            )
            idx = pd.DatetimeIndex(
                [
                    datetime(2026, 3, 15, 0, 0),
                    datetime(2026, 3, 15, 4, 0),
                    datetime(2026, 3, 15, 4, 1),
                ]
            )
            return pd.DataFrame(
                {
                    "open": [90.0, 100.0, 101.0],
                    "high": [91.0, 101.0, 102.0],
                    "low": [89.0, 99.0, 100.0],
                    "close": [90.5, 100.5, 101.5],
                    "volume": [9.0, 10.0, 11.0],
                },
                index=idx,
            )

    monkeypatch.setattr(ccxt_data_store, "CcxtCacheDB", _FakeCcxtCache)

    ds = RoutedBacktestingPandas(
        datetime_start=datetime(2026, 3, 15, tzinfo=timezone.utc),
        datetime_end=datetime(2026, 3, 16, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto": "coinbase", "default": "thetadata"}},
        username="dev",
        password="dev",
        use_quote_data=False,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    ds._datetime = datetime(2026, 3, 15, 4, 0, tzinfo=timezone.utc)
    ds._timestep = "day"
    ds._effective_day_mode = True
    ds._observed_intraday_cadence = False
    return ds


def test_routed_ccxt_crypto_last_price_stays_minute_even_when_day_mode_is_inferred(monkeypatch):
    calls = []
    ds = _routed_crypto_source_with_fake_ccxt(monkeypatch, calls)

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
    price = ds.get_last_price(asset, quote=quote)

    assert price == 100.5
    assert calls
    assert calls[0]["exchange_id"] == "coinbase"
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == "1m"


def test_routed_ccxt_crypto_quote_stays_minute_even_when_day_mode_is_inferred(monkeypatch):
    calls = []
    ds = _routed_crypto_source_with_fake_ccxt(monkeypatch, calls)

    asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
    quote_asset = Asset(symbol="USDT", asset_type=Asset.AssetType.CRYPTO)
    quote = ds.get_quote(asset, quote=quote_asset)

    assert quote.price == 100.5
    assert calls
    assert calls[0]["exchange_id"] == "coinbase"
    assert calls[0]["symbol"] == "BTC/USDT"
    assert calls[0]["timeframe"] == "1m"
