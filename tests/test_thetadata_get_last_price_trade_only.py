from datetime import date

import pandas as pd
import pytest

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset, Data


def test_thetadata_get_last_price_trade_only_uses_previous_trade_when_today_has_no_prints(monkeypatch):
    """ThetaData backtests: get_last_price must be trade-only and may return stale last trade."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)

    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-10", tz="UTC")
    index = pd.date_range(start, periods=3, freq="1D", tz="UTC")

    option = Asset(
        "ZZOPT",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 1, 17),
        strike=100.0,
        right="call",
    )
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # First day has a trade close; subsequent days have no prints (close=0) but do have quotes.
    df = pd.DataFrame(
        {
            "open": [5.0, 0.0, 0.0],
            "high": [5.0, 0.0, 0.0],
            "low": [5.0, 0.0, 0.0],
            "close": [5.0, 0.0, 0.0],
            "volume": [1, 0, 0],
            "bid": [4.8, 1.0, 1.0],
            "ask": [5.2, 1.2, 1.2],
            "missing": [False, False, False],
        },
        index=index,
    )
    data = Data(asset=option, df=df, quote=quote, timestep="day")

    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data=[data])
    monkeypatch.setattr(ds, "_update_pandas_data", lambda *args, **kwargs: None)
    ds._datetime = index[-1]

    # Must return the most recent prior trade (5.0), not NBBO mid (1.1).
    price = ds.get_last_price(option, timestep="day", quote=quote)
    assert price == pytest.approx(5.0)


def test_thetadata_get_last_price_trade_only_returns_none_when_no_trade_exists(monkeypatch):
    """ThetaData backtests: if there are no trades at all, get_last_price returns None."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)

    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-10", tz="UTC")
    index = pd.date_range(start, periods=3, freq="1D", tz="UTC")

    option = Asset(
        "ZZOPT",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 1, 17),
        strike=100.0,
        right="call",
    )
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    df = pd.DataFrame(
        {
            "open": [0.0, 0.0, 0.0],
            "high": [0.0, 0.0, 0.0],
            "low": [0.0, 0.0, 0.0],
            "close": [0.0, 0.0, 0.0],
            "volume": [0, 0, 0],
            "bid": [1.0, 1.0, 1.0],
            "ask": [1.2, 1.2, 1.2],
            "missing": [False, False, False],
        },
        index=index,
    )
    data = Data(asset=option, df=df, quote=quote, timestep="day")

    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data=[data])
    monkeypatch.setattr(ds, "_update_pandas_data", lambda *args, **kwargs: None)
    ds._datetime = index[-1]

    assert ds.get_last_price(option, timestep="day", quote=quote) is None

