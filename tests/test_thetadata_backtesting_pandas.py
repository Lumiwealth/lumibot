from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset


def _ohlc_frame(start: datetime, rows: int = 12) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "open": [300 + i for i in range(rows)],
        "high": [301 + i for i in range(rows)],
        "low": [299 + i for i in range(rows)],
        "close": [300.5 + i for i in range(rows)],
        "volume": [20_000 + i * 200 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


@pytest.mark.parametrize("length", [8, 16])
def test_theta_pandas_quote_failure_stores_ohlc(monkeypatch, length):
    start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    counts = {"ohlc": 0, "quote": 0}

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            counts["quote"] += 1
            raise ValueError("Cannot connect to Theta Data!")
        counts["ohlc"] += 1
        return _ohlc_frame(start_datetime, rows=32)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    bars = backtester.get_historical_prices(asset, length=length, timestep="minute")

    assert counts["ohlc"] == 1
    assert counts["quote"] == 1
    assert bars is not None
    assert len(bars.df) == length
    assert "close" in bars.df.columns
