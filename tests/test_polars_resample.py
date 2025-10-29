from datetime import datetime, timedelta

import polars as pl
import pytest

from lumibot.tools.polars_utils import PolarsResampleError, resample_polars_ohlc


def _minute_frame(rows: int = 10) -> pl.DataFrame:
    start = datetime(2024, 1, 1, 9, 30)
    datetimes = [start + timedelta(minutes=i) for i in range(rows)]
    return pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [100 + i for i in range(rows)],
            "high": [101 + i for i in range(rows)],
            "low": [99 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [1_000 + 10 * i for i in range(rows)],
            "signal": [i for i in range(rows)],
        }
    )


def test_resample_polars_minute_to_5min():
    df = _minute_frame(10)
    resampled = resample_polars_ohlc(df, multiplier=5, base_unit="minute", length=2)

    assert resampled.height == 2
    # First bucket should cover rows 0-4
    first = resampled.row(0, named=True)
    assert first["open"] == 100
    assert first["high"] == 105
    assert first["low"] == 99
    assert first["close"] == pytest.approx(104.5)
    assert first["volume"] == sum(1_000 + 10 * i for i in range(5))
    # signal column keeps last observation in the bucket
    assert first["signal"] == 4

    # Tail limiting should keep last 2 buckets only
    assert resampled.row(1, named=True)["signal"] == 9


def test_resample_polars_day_bucket():
    start = datetime(2024, 1, 1, 0, 0)
    datetimes = [start + timedelta(hours=i) for i in range(48)]
    df = pl.DataFrame(
        {
            "datetime": datetimes,
            "open": [10 + i for i in range(48)],
            "high": [12 + i for i in range(48)],
            "low": [8 + i for i in range(48)],
            "close": [11 + i for i in range(48)],
            "volume": [100 + i for i in range(48)],
        }
    )

    resampled = resample_polars_ohlc(df, multiplier=1, base_unit="day", length=None)
    assert resampled.height == 2
    assert resampled["open"][0] == 10
    assert resampled["close"][1] == 11 + 47


def test_resample_polars_invalid_unit():
    df = _minute_frame(2)
    with pytest.raises(PolarsResampleError):
        resample_polars_ohlc(df, multiplier=1, base_unit="hour")
