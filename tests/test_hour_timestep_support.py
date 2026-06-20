from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
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
