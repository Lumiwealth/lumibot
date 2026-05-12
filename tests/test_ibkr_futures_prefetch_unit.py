import datetime as dt
from unittest.mock import patch

import pandas as pd
import pytest
import pytz

from lumibot.backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset


def _minute_df(start: dt.datetime, end: dt.datetime) -> pd.DataFrame:
    idx = pd.date_range(start=start, end=end, freq="1min", tz="America/New_York", inclusive="left")
    df = pd.DataFrame(index=idx)
    df["open"] = 100.0
    df["high"] = 101.0
    df["low"] = 99.0
    df["close"] = 100.5
    df["volume"] = 1
    df["missing"] = False
    return df


@pytest.mark.parametrize("timestep", ["5minute", "15minute", "30minute"])
def test_ibkr_futures_prefetch_reuses_minute_series_for_minute_multiple_requests(timestep):
    tz = pytz.timezone("America/New_York")
    start = tz.localize(dt.datetime(2026, 1, 12, 0, 0, 0))
    end = tz.localize(dt.datetime(2026, 1, 19, 0, 0, 0))

    data_source = InteractiveBrokersRESTBacktesting(datetime_start=start, datetime_end=end, exchange="CME")
    data_source._update_datetime(start)

    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    df = _minute_df(start - dt.timedelta(days=10), end)

    with patch("lumibot.tools.ibkr_helper.get_price_data", return_value=df) as mocked:
        bars_1 = data_source.get_historical_prices(asset, 200, timestep)
        assert bars_1 is not None
        assert bars_1.df is not None

        data_source._update_datetime(start + dt.timedelta(minutes=15))
        bars_2 = data_source.get_historical_prices(asset, 200, timestep)
        assert bars_2 is not None

        # Prefetch should occur once; subsequent calls should reuse cached data.
        assert mocked.call_count == 1
