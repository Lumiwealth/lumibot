import polars as pl
import pytest
from datetime import datetime, timezone

from lumibot.entities.bars import Bars


class DummyAsset:
    def __init__(self, symbol):
        self.symbol = symbol


def build_epoch_df(num_minutes=10, start_epoch=None):
    if start_epoch is None:
        # Pick a fixed start for reproducibility
        start_epoch = int(datetime(2024, 1, 1, 9, 30, tzinfo=timezone.utc).timestamp())
    rows = []
    for i in range(num_minutes):
        ts = start_epoch + i * 60  # 60 second steps (1 minute)
        price = 100 + i
        rows.append({
            'timestamp': ts,
            'open': float(price),
            'high': float(price + 0.5),
            'low': float(price - 0.5),
            'close': float(price + 0.25),
            'volume': 10 + i,
        })
    return pl.DataFrame(rows)


def test_aggregate_bars_epoch_int_timestamp_minutes_failure_current():
    """Currently expected to FAIL: Bars.aggregate_bars('5min') with int epoch timestamp should convert to datetime and aggregate.

    This test asserts the desired behavior (2 aggregated rows for 10 minutes @5min) which will fail BEFORE the fix
    because the underlying implementation attempts group_by_dynamic on an integer column.
    After implementing the fix (casting epoch ints to Datetime) this test should pass.
    """
    df = build_epoch_df()
    asset = DummyAsset('TEST')
    bars = Bars(df, 'TESTSOURCE', asset, return_polars=True)

    # Desired behavior: no exception, returns 2 groups (0-5m,5-10m)
    aggregated = bars.aggregate_bars('5min')
    # Depending on boundary handling, could be 2 exact groups
    assert len(aggregated.df) == 2, f"Expected 2 aggregated rows, got {len(aggregated.df)}"
    # Validate columns exist
    for col in ['open','high','low','close','volume']:
        assert col in aggregated.df.columns


def test_aggregate_bars_alias_variants_minutes():
    """Additional desired behavior: multiple minute aliases should work identically.
    Will also fail until core casting fix is applied.
    """
    df = build_epoch_df()
    asset = DummyAsset('TEST')
    bars = Bars(df, 'TESTSOURCE', asset, return_polars=True)

    variants = ['5m','5min','5MIN','5Minutes','5MINUTE','5 minutes','5   Min']
    results = []
    for v in variants:
        # We only need it not to raise; length check as in primary test
        agg = bars.aggregate_bars(v)
        results.append(len(agg.df))
    # All variants should produce consistent row counts
    assert len(set(results)) == 1, f"Inconsistent aggregation counts across variants: {results}"
