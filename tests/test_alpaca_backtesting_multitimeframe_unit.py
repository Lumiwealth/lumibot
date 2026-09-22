from datetime import datetime

import pandas as pd
import pytz

from lumibot.backtesting import AlpacaBacktesting
from lumibot.entities import Asset


def test_alpaca_option_history_uses_the_listed_contract():
    """Option backtests must request the OCC contract, not the underlying stock."""
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source.market = "NYSE"
    data_source.tzinfo = pytz.UTC
    data_source._auto_adjust = True
    data_source._timestep = "day"
    data_source._option_client = object()
    data_source._stock_client = object()
    start = datetime(2026, 1, 23, tzinfo=pytz.UTC)
    end = datetime(2026, 1, 27, tzinfo=pytz.UTC)
    quote = Asset("USD", asset_type="forex")
    call_100 = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration="2027-01-15",
        strike=100,
        right="CALL",
    )
    call_120 = Asset(
        "AAPL",
        asset_type=Asset.AssetType.OPTION,
        expiration="2027-01-15",
        strike=120,
        right="CALL",
    )
    key_100 = data_source._get_asset_key(
        base_asset=call_100,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
    )
    key_120 = data_source._get_asset_key(
        base_asset=call_120,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
    )
    assert key_100 != key_120
    assert "100" in key_100
    assert "120" in key_120

    client, request = data_source._history_request(
        base_asset=call_100,
        quote_asset=quote,
        timestep="day",
        data_datetime_start=start,
        data_datetime_end=end,
        auto_adjust=True,
    )
    assert client is data_source._option_client
    assert request.symbol_or_symbols == "AAPL270115C00100000"


def test_alpaca_backtesting_normalizes_common_multi_timeframe_aliases():
    assert AlpacaBacktesting._normalize_timestep_for_source("15min") == ("15minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("13 minutes") == ("13minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("20min") == ("20minute", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("1h") == ("hour", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("4 hours") == ("4hour", None)
    assert AlpacaBacktesting._normalize_timestep_for_source("2d") == ("day", "2D")


def test_alpaca_backtesting_uses_native_15min_request():
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
        periods=6,
        freq="15min",
    )
    native_15min_df = pd.DataFrame(
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
        return native_15min_df

    data_source.get_historical_prices_between_dates = fake_get_historical_prices_between_dates

    bars = data_source.get_historical_prices(Asset("TSLA"), length=3, timestep="15min")
    df = bars.pandas_df

    assert requested_timesteps == ["15minute"]
    assert list(df.index) == list(index[-3:])
    assert list(df["open"]) == [3, 4, 5]
    assert list(df["close"]) == [3.25, 4.25, 5.25]
    assert list(df["volume"]) == [1, 1, 1]


def test_alpaca_backtesting_uses_latest_completed_native_intraday_bar():
    tzinfo = pytz.timezone("America/New_York")
    data_source = AlpacaBacktesting.__new__(AlpacaBacktesting)
    data_source._remove_incomplete_current_bar = False
    data_source._timestep = "minute"
    data_source._datetime = tzinfo.localize(datetime(2026, 1, 2, 10, 50))
    data_source._data_datetime_start = tzinfo.localize(datetime(2026, 1, 2, 0, 0))
    data_source._data_datetime_end = tzinfo.localize(datetime(2026, 1, 2, 23, 59))
    data_source._auto_adjust = True
    data_source.tzinfo = tzinfo

    index = pd.DatetimeIndex(
        [
            tzinfo.localize(datetime(2026, 1, 2, 10, 0)),
            tzinfo.localize(datetime(2026, 1, 2, 10, 20)),
            tzinfo.localize(datetime(2026, 1, 2, 10, 40)),
            tzinfo.localize(datetime(2026, 1, 2, 11, 0)),
        ]
    )
    native_20min_df = pd.DataFrame(
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
        return native_20min_df

    data_source.get_historical_prices_between_dates = fake_get_historical_prices_between_dates

    bars = data_source.get_historical_prices(Asset("TSLA"), length=2, timestep="20min")
    df = bars.pandas_df

    assert requested_timesteps == ["20minute"]
    assert list(df.index) == list(index[1:3])
    assert list(df["open"]) == [1, 2]


def test_alpaca_backtesting_uses_alpaca_sdk_timeframes_for_intraday_multiples():
    assert str(AlpacaBacktesting._get_alpaca_timeframe("13minute")) == "13Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("15minute")) == "15Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("20minute")) == "20Min"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("hour")) == "1Hour"
    assert str(AlpacaBacktesting._get_alpaca_timeframe("4hour")) == "4Hour"
