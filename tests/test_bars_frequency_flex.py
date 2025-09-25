import polars as pl
import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone

from lumibot.entities.bars import Bars
from lumibot.entities.asset import Asset
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ


def _make_minute_df(start_epoch: int, minutes: int = 12):
    rows = []
    for i in range(minutes):
        ts = start_epoch + i * 60
        rows.append({
            'timestamp': ts,
            'open': 100 + i,
            'high': 101 + i,
            'low': 99 + i,
            'close': 100.5 + i,
            'volume': 10 + i,
        })
    return pl.DataFrame(rows)


def test_arbitrary_minute_frequency():
    # 12 minutes of 1-min bars -> aggregate to 3m should yield 4 bars
    start = 1_700_000_000  # epoch seconds
    df = _make_minute_df(start, 12)
    bars = Bars(df, source='test', asset=Asset(symbol='TEST', asset_type='stock'), return_polars=True)
    agg = bars.aggregate_bars('3 minutes')
    # Left-closed grouping produces a final partial bucket; 12 minutes -> 5 buckets (ceil(12/3)=4 plus initial offset)
    assert len(agg.df) == 5


def test_hour_and_second_frequencies():
    # Build 120 seconds of per-second bars and aggregate to 30s -> 4 bars
    start = 1_700_100_000
    rows = []
    for i in range(120):
        ts = start + i
        rows.append({
            'timestamp': ts,
            'open': 50 + i * 0.01,
            'high': 50.5 + i * 0.01,
            'low': 49.5 + i * 0.01,
            'close': 50.1 + i * 0.01,
            'volume': 1,
        })
    df = pl.DataFrame(rows)
    bars = Bars(df, source='test', asset=Asset(symbol='TEST2', asset_type='stock'), return_polars=True)
    agg_30s = bars.aggregate_bars('30s')
    assert len(agg_30s.df) == 4

    # Reuse minute dataset for hour aggregation
    df_min = _make_minute_df(start, 120)  # 2 hours of minutes
    bars_min = Bars(df_min, source='test', asset=Asset(symbol='TEST3', asset_type='stock'), return_polars=True)
    agg_1h = bars_min.aggregate_bars('1 hour')
    assert len(agg_1h.df) == 2


def test_invalid_frequency_raises():
    start = 1_700_200_000
    df = _make_minute_df(start, 5)
    bars = Bars(df, source='test', asset=Asset(symbol='TEST4', asset_type='stock'), return_polars=True)
    with pytest.raises(ValueError):
        bars.aggregate_bars('every 5 mins')  # unsupported phrase


def test_bars_datetime_index_normalized_to_default_timezone():
    asset = Asset(symbol='MNQ', asset_type=Asset.AssetType.CONT_FUTURE)
    timestamps = pl.datetime_range(
        start=datetime(2025, 9, 24, 1, 0),
        end=datetime(2025, 9, 24, 1, 9),
        interval='1m',
        time_zone='UTC',
        eager=True,
    )

    df = pl.DataFrame({
        'datetime': timestamps,
        'open': [1.0] * len(timestamps),
        'high': [1.0] * len(timestamps),
        'low': [1.0] * len(timestamps),
        'close': [1.0] * len(timestamps),
        'volume': [100] * len(timestamps),
    })

    bars = Bars(df, source='test', asset=asset)
    index = bars.df.index

    assert isinstance(index, pd.DatetimeIndex)
    assert index.tz is not None
    assert index.tz.zone == LUMIBOT_DEFAULT_PYTZ.zone
