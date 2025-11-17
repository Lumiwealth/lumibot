import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _build_frame(start_ts: str, periods: int, freq: str) -> pd.DataFrame:
    index = pd.date_range(start=start_ts, periods=periods, freq=freq, tz=pytz.UTC)
    base = pd.DataFrame(
        {
            "open": [100 + i for i in range(periods)],
            "high": [101 + i for i in range(periods)],
            "low": [99 + i for i in range(periods)],
            "close": [100.5 + i for i in range(periods)],
            "volume": [1_000_000] * periods,
        },
        index=index,
    )
    return base


def test_update_pandas_data_reuses_cached_window(monkeypatch):
    tz = pytz.UTC
    start = tz.localize(datetime.datetime(2024, 1, 1))
    end = tz.localize(datetime.datetime(2024, 2, 1))

    with patch.object(ThetaDataBacktestingPandas, "kill_processes_by_name", return_value=None), patch.object(
        thetadata_helper, "reset_theta_terminal_tracking", return_value=None
    ):
        backtester = ThetaDataBacktestingPandas(
            datetime_start=start,
            datetime_end=end,
            pandas_data=[],
            username="user",
            password="pass",
        )

    backtester._use_quote_data = False
    backtester.get_datetime = MagicMock(return_value=end)

    fetch_counts = {"day": 0, "minute": 0}

    def fake_price_data(username, password, asset_param, start_datetime, end_datetime, timespan, **kwargs):
        fetch_counts[timespan] += 1
        if timespan == "day":
            return _build_frame("2023-10-01 00:00:00+00:00", periods=180, freq="D")
        return _build_frame("2024-01-01 09:30:00+00:00", periods=180, freq="T")

    monkeypatch.setattr(thetadata_helper, "get_price_data", fake_price_data)

    asset = Asset(asset_type="stock", symbol="MSFT")
    backtester._update_pandas_data(asset, None, length=55, timestep="day", start_dt=end)
    assert fetch_counts["day"] == 1

    # Second call with same parameters should reuse cached data entirely.
    backtester._update_pandas_data(asset, None, length=55, timestep="day", start_dt=end)
    assert fetch_counts["day"] == 1

    # Request minute data to force a new fetch for the same asset.
    backtester._update_pandas_data(asset, None, length=30, timestep="minute", start_dt=end)
    assert fetch_counts["minute"] == 1

    tuple_key = next(iter(backtester.pandas_data))
    day_meta = backtester._dataset_metadata.get((tuple_key, "day"))
    minute_meta = backtester._dataset_metadata.get((tuple_key, "minute"))
    assert day_meta is not None
    assert minute_meta is not None


def test_combine_duplicate_columns_preserves_first_non_null():
    tz = pytz.UTC
    idx = pd.date_range(start="2024-01-01 00:00:00+00:00", periods=2, freq="D", tz=tz)
    frame = pd.DataFrame(
        [
            [101.0, None, 1.0],
            [None, 202.0, 2.0],
        ],
        columns=["open", "open", "close"],
        index=idx,
    )

    combined = ThetaDataBacktestingPandas._combine_duplicate_columns(frame, ["open"])
    assert combined.columns.tolist() == ["open", "close"]
    assert combined.loc[idx[0], "open"] == 101.0
    assert combined.loc[idx[1], "open"] == 202.0
