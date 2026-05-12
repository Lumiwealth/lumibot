from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from lumibot.backtesting import thetadata_backtesting_pandas as tdp_module
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset


def _make_df(columns: dict[str, list[float]]):
    idx = pd.DatetimeIndex(
        [
            datetime(2025, 1, 2, 14, 30, tzinfo=UTC),
            datetime(2025, 1, 2, 14, 31, tzinfo=UTC),
        ]
    )
    return pd.DataFrame(columns, index=idx)


def test_ndx_index_ohlc_is_proxied_via_qqq_and_scaled(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    calls: list[dict] = []

    def fake_get_price_data(asset, *_args, **kwargs):
        calls.append({"symbol": asset.symbol, "asset_type": asset.asset_type, "datastyle": kwargs.get("datastyle")})
        return _make_df(
            {
                "open": [10.0, 11.0],
                "high": [12.0, 13.0],
                "low": [9.0, 10.0],
                "close": [11.0, 12.0],
                "volume": [100.0, 200.0],
            }
        )

    monkeypatch.setattr(tdp_module.thetadata_helper, "get_price_data", fake_get_price_data)

    start = datetime(2025, 1, 2, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 3, 0, 0, tzinfo=UTC)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=False)
    ds._datetime = start

    ndx = Asset("NDX", asset_type="index")
    ds._update_pandas_data(ndx, quote=None, length=2, timestep="minute", start_dt=start)
    canonical_key, _legacy_key = ds._build_dataset_keys(ndx, quote=None, ts_unit="minute")
    out = ds.pandas_data[canonical_key].df

    assert calls, "Expected get_price_data to be called"
    assert calls[0]["symbol"] == "QQQ"
    assert calls[0]["datastyle"] == "ohlc"

    factor = 41.0
    assert out["open"].iloc[0] == 10.0 * factor
    assert out["high"].iloc[1] == 13.0 * factor
    assert out["close"].iloc[1] == 12.0 * factor
    assert float(out["volume"].iloc[0]) == 0.0


def test_ndx_stock_symbol_is_not_proxied(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    calls: list[str] = []

    def fake_get_price_data(asset, *_args, **_kwargs):
        calls.append(asset.symbol)
        return _make_df({"open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0], "close": [1.0, 1.0]})

    monkeypatch.setattr(tdp_module.thetadata_helper, "get_price_data", fake_get_price_data)

    start = datetime(2025, 1, 2, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 3, 0, 0, tzinfo=UTC)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=False)
    ds._datetime = start

    ndx = Asset("NDX", asset_type="stock")
    ds._update_pandas_data(ndx, quote=None, length=2, timestep="minute", start_dt=start)

    assert calls and calls[0] == "NDX"


def test_ndx_default_asset_type_is_stock_and_not_proxied(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    calls: list[str] = []

    def fake_get_price_data(asset, *_args, **_kwargs):
        calls.append(asset.symbol)
        return _make_df({"open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0], "close": [1.0, 1.0]})

    monkeypatch.setattr(tdp_module.thetadata_helper, "get_price_data", fake_get_price_data)

    start = datetime(2025, 1, 2, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 3, 0, 0, tzinfo=UTC)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=False)
    ds._datetime = start

    # Asset() defaults to STOCK by design; it must not be proxied as an index.
    ndx = Asset("NDX")
    assert ndx.asset_type == Asset.AssetType.STOCK
    ds._update_pandas_data(ndx, quote=None, length=2, timestep="minute", start_dt=start)

    assert calls and calls[0] == "NDX"


def test_ndxp_index_is_proxied_via_qqq(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    calls: list[str] = []

    def fake_get_price_data(asset, *_args, **_kwargs):
        calls.append(asset.symbol)
        return _make_df({"open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0], "close": [1.0, 1.0]})

    monkeypatch.setattr(tdp_module.thetadata_helper, "get_price_data", fake_get_price_data)

    start = datetime(2025, 1, 2, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 3, 0, 0, tzinfo=UTC)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=False)
    ds._datetime = start

    ndxp = Asset("NDXP", asset_type="index")
    ds._update_pandas_data(ndxp, quote=None, length=2, timestep="minute", start_dt=start)

    assert calls and calls[0] == "QQQ"


def test_ndx_quote_is_proxied_and_scaled(monkeypatch):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test")

    calls: list[dict] = []

    def fake_get_price_data(asset, *_args, **kwargs):
        calls.append({"symbol": asset.symbol, "datastyle": kwargs.get("datastyle")})
        return _make_df({"bid": [100.0, 101.0], "ask": [102.0, 103.0], "bid_size": [1.0, 2.0], "ask_size": [1.0, 2.0]})

    monkeypatch.setattr(tdp_module.thetadata_helper, "get_price_data", fake_get_price_data)

    start = datetime(2025, 1, 2, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 3, 0, 0, tzinfo=UTC)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, use_quote_data=True)
    ds._datetime = start

    ndx = Asset("NDX", asset_type="index")
    ds._update_pandas_data(
        ndx,
        quote=None,
        length=2,
        timestep="minute",
        start_dt=start,
        require_quote_data=True,
        require_ohlc_data=False,
    )
    canonical_key, _legacy_key = ds._build_dataset_keys(ndx, quote=None, ts_unit="minute")
    out = ds.pandas_data[canonical_key].df

    assert calls, "Expected get_price_data to be called"
    assert calls[0]["symbol"] == "QQQ"
    assert calls[0]["datastyle"] == "quote"

    factor = 41.0
    assert out["bid"].iloc[0] == 100.0 * factor
    assert out["ask"].iloc[1] == 103.0 * factor
    # Sizes should remain unscaled.
    assert float(out["bid_size"].iloc[0]) == 1.0
