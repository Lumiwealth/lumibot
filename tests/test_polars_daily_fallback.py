import datetime
from collections import OrderedDict

import polars as pl

from lumibot.data_sources.polars_data import PolarsData
from lumibot.entities.asset import Asset
from lumibot.entities.data_polars import DataPolars


UTC = datetime.timezone.utc


def _build_ohlc_frame(datetimes):
    size = len(datetimes)
    return pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [10.0 + i for i in range(size)],
            "high": [10.5 + i for i in range(size)],
            "low": [9.5 + i for i in range(size)],
            "close": [10.1 + i for i in range(size)],
            "volume": [1000 + i for i in range(size)],
        }
    )


def test_polars_day_request_uses_full_dataset():
    """Requesting 63 daily bars after minute trim should still return 63 rows."""

    asset = Asset("PLTR", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Minute data only spans the most recent 16 days (simulates sliding-window trim)
    minute_start = datetime.datetime(2024, 7, 29, 9, 30, tzinfo=UTC)
    minute_datetimes = [minute_start + datetime.timedelta(days=i) for i in range(16)]
    minute_df = _build_ohlc_frame(minute_datetimes)

    # Daily data covers the full 63-day lookback window ending on 2024-08-14
    daily_start = datetime.datetime(2024, 5, 27, tzinfo=UTC)
    daily_datetimes = [daily_start + datetime.timedelta(days=i) for i in range(80)]
    daily_df = _build_ohlc_frame(daily_datetimes)

    minute_data = DataPolars(asset=asset, df=minute_df, quote=quote, timestep="minute")
    daily_data = DataPolars(asset=asset, df=daily_df, quote=quote, timestep="day")

    data_source = PolarsData(
        datetime_start=datetime.datetime(2024, 7, 1, tzinfo=UTC),
        datetime_end=datetime.datetime(2024, 9, 1, tzinfo=UTC),
        pandas_data=OrderedDict(),
        show_progress_bar=False,
    )

    # Mimic real cache state: minute data overwrites day data in _data_store, but both remain in _polars_data
    data_source.pandas_data = OrderedDict({(asset, quote): minute_data})
    data_source._data_store = OrderedDict({(asset, quote): minute_data})
    data_source._aggregated_cache = OrderedDict()
    data_source._polars_data = {
        (asset, quote, "day"): daily_data,
        (asset, quote, "minute"): minute_data,
    }

    data_source._datetime = datetime.datetime(2024, 8, 15, 9, 30, tzinfo=UTC)

    # Sanity check: direct cache lookup should return the daily dataset
    cached_day = data_source._get_polars_data_entry(asset, quote, "day")
    assert cached_day is daily_data

    bars = data_source._pull_source_symbol_bars(
        asset=asset,
        length=63,
        timestep="day",
        quote=quote,
    )

    assert bars is not None
    if hasattr(bars, "height"):
        row_count = bars.height
    else:
        row_count = len(bars)
    assert row_count == 63
