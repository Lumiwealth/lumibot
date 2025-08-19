import pandas as pd
import polars as pl
from datetime import datetime, timedelta

from lumibot.entities.asset import Asset
from lumibot.entities.bars import Bars


def _make_minute_df(start: datetime, periods: int):
    # Generate simple ascending OHLCV minute bars
    rows = []
    for i in range(periods):
        ts = start + timedelta(minutes=i)
        rows.append({
            'datetime': ts,
            'open': float(i),
            'high': float(i) + 0.5,
            'low': float(i) - 0.5,
            'close': float(i) + 0.25,
            'volume': 100 + i,
        })
    return pl.DataFrame(rows)


def test_aggregate_accepts_lowercase_minute_variants():
    asset = Asset('TEST')
    start = datetime(2024, 1, 1, 9, 30)
    df = _make_minute_df(start, 20)  # 20 minutes of data
    bars = Bars(df, source='test', asset=asset, return_polars=True)

    # These variants previously could raise errors (e.g. '5min')
    variants = ['5min', '5MIN', '5Minute', '5MINUTE', '5minutes']
    for v in variants:
        agg = bars.aggregate_bars(v)
        # 20 minutes grouped into 5 minute buckets => 4 bars expected
        assert len(agg.df) == 4, f"Expected 4 aggregated bars for variant {v}, got {len(agg.df)}"
        # Ensure columns exist
        for col in ['open', 'high', 'low', 'close', 'volume']:
            assert col in agg.df.columns


def test_aggregate_rejects_unsupported_frequency():
    asset = Asset('TEST')
    start = datetime(2024, 1, 1, 9, 30)
    df = _make_minute_df(start, 10)
    bars = Bars(df, source='test', asset=asset, return_polars=True)
    try:
        bars.aggregate_bars('weirdfreq')
    except ValueError as e:
        assert 'Unsupported frequency' in str(e)
    else:
        assert False, 'Expected ValueError for unsupported frequency'
