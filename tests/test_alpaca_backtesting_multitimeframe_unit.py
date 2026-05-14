from datetime import datetime

import pandas as pd
import pytz

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import Asset


def test_alpaca_backtesting_normalizes_common_multi_timeframe_aliases():
    assert AlpacaBacktesting._normalize_timestep_for_source("15min") == ("minute", "15min")
    assert AlpacaBacktesting._normalize_timestep_for_source("1h") == ("minute", "60min")
    assert AlpacaBacktesting._normalize_timestep_for_source("4 hours") == ("minute", "240min")
    assert AlpacaBacktesting._normalize_timestep_for_source("2d") == ("day", "2D")


def test_alpaca_backtesting_resamples_15min_request_from_minute_data():
    tzinfo = pytz.timezone("America/New_York")
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._remove_incomplete_current_bar = False
    data_source._timestep = "minute"
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 10, 45))
    data_source._data_datetime_start = tzinfo.localize(datetime(2026, 1, 2, 0, 0))
    data_source._data_datetime_end = tzinfo.localize(datetime(2026, 1, 2, 23, 59))
    data_source._auto_adjust = True
    data_source.tzinfo = tzinfo

    index = pd.date_range(
        tzinfo.localize(datetime(2026, 1, 2, 9, 30)),
        periods=76,
        freq="min",
    )
    minute_df = pd.DataFrame(
        {
            "open": range(len(index)),
            "high": [value + 0.5 for value in range(len(index))],
            "low": [value - 0.5 for value in range(len(index))],
            "close": [value + 0.25 for value in range(len(index))],
            "volume": [1] * len(index),
        },
        index=index,
    )

    requested_timesteps = []

    def fake_get_historical_prices_between_dates(**kwargs):
        requested_timesteps.append(kwargs["timestep"])
        return minute_df

    data_source.get_historical_prices_between_dates = fake_get_historical_prices_between_dates

    bars = data_source.get_historical_prices(Asset("TSLA"), length=3, timestep="15min")
    df = bars.pandas_df

    assert requested_timesteps == ["minute"]
    assert list(df.index) == list(pd.date_range(index[-31], periods=3, freq="15min"))
    assert list(df["open"]) == [45, 60, 75]
    assert list(df["close"]) == [59.25, 74.25, 75.25]
    assert list(df["volume"]) == [15, 15, 1]
