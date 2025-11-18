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


def _build_backtester(start: datetime.datetime, end: datetime.datetime) -> ThetaDataBacktestingPandas:
    with patch.object(ThetaDataBacktestingPandas, "kill_processes_by_name", return_value=None), patch.object(
        thetadata_helper, "reset_theta_terminal_tracking", return_value=None
    ):
        tester = ThetaDataBacktestingPandas(
            datetime_start=start,
            datetime_end=end,
            pandas_data=[],
            username="user",
            password="pass",
        )
    tester._use_quote_data = False
    tester.get_datetime = MagicMock(return_value=end)
    return tester


def test_update_pandas_data_reuses_cached_window(monkeypatch):
    tz = pytz.UTC
    start = tz.localize(datetime.datetime(2024, 1, 1))
    end = tz.localize(datetime.datetime(2024, 2, 1))

    backtester = _build_backtester(start, end)
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

    backtester._update_pandas_data(asset, None, length=55, timestep="day", start_dt=end)
    assert fetch_counts["day"] == 1

    backtester._update_pandas_data(asset, None, length=30, timestep="minute", start_dt=end)
    assert fetch_counts["minute"] == 1

    tuple_key = next(iter(backtester.pandas_data))
    day_meta = backtester._dataset_metadata.get(tuple_key)
    assert day_meta is not None


def test_update_pandas_data_fetches_when_cache_starts_after_request(monkeypatch):
    tz = pytz.UTC
    start = tz.localize(datetime.datetime(2024, 1, 1))
    end = tz.localize(datetime.datetime(2024, 2, 1))

    backtester = _build_backtester(start, end)
    fetch_counts = {"day": 0}

    def fake_price_data(username, password, asset_param, start_datetime, end_datetime, timespan, **kwargs):
        fetch_counts["day"] += 1
        return _build_frame("2023-12-20 00:00:00+00:00", periods=60, freq="D")

    monkeypatch.setattr(thetadata_helper, "get_price_data", fake_price_data)
    asset = Asset(asset_type="stock", symbol="MSFT")

    backtester._update_pandas_data(asset, None, length=55, timestep="day", start_dt=end)
    assert fetch_counts["day"] == 1

    meta_key = next(iter(backtester._dataset_metadata))
    backtester._dataset_metadata[meta_key]["start"] = (
        backtester._dataset_metadata[meta_key]["start"] + datetime.timedelta(days=10)
    )

    backtester._update_pandas_data(asset, None, length=55, timestep="day", start_dt=end)
    assert fetch_counts["day"] == 2
