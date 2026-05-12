from __future__ import annotations

from datetime import datetime, timedelta

import pytz

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas


def test_compute_prefetch_complete_true_when_day_coverage_satisfies_requirements():
    tz = pytz.UTC
    requested_start = tz.localize(datetime(2020, 1, 1))
    end_requirement = tz.localize(datetime(2020, 1, 10, 23, 59, 59))

    meta = {
        "data_start": tz.localize(datetime(2019, 12, 1)),
        "data_end": tz.localize(datetime(2020, 1, 10, 0, 0, 0)),
        "data_rows": 500,
    }

    assert (
        ThetaDataBacktestingPandas._compute_prefetch_complete(
            meta,
            requested_start=requested_start,
            effective_start_buffer=timedelta(days=5),
            end_requirement=end_requirement,
            ts_unit="day",
            requested_length=210,
        )
        is True
    )


def test_compute_prefetch_complete_false_when_day_end_is_insufficient():
    tz = pytz.UTC
    requested_start = tz.localize(datetime(2020, 1, 1))
    end_requirement = tz.localize(datetime(2020, 1, 10, 23, 59, 59))

    meta = {
        "data_start": tz.localize(datetime(2019, 12, 1)),
        "data_end": tz.localize(datetime(2020, 1, 9, 0, 0, 0)),
        "data_rows": 500,
    }

    assert (
        ThetaDataBacktestingPandas._compute_prefetch_complete(
            meta,
            requested_start=requested_start,
            effective_start_buffer=timedelta(days=5),
            end_requirement=end_requirement,
            ts_unit="day",
            requested_length=210,
        )
        is False
    )


def test_compute_prefetch_complete_false_when_row_count_is_insufficient():
    tz = pytz.UTC
    requested_start = tz.localize(datetime(2020, 1, 1))
    end_requirement = tz.localize(datetime(2020, 1, 10, 23, 59, 59))

    meta = {
        "data_start": tz.localize(datetime(2019, 12, 1)),
        "data_end": tz.localize(datetime(2020, 1, 10, 0, 0, 0)),
        "data_rows": 10,
    }

    assert (
        ThetaDataBacktestingPandas._compute_prefetch_complete(
            meta,
            requested_start=requested_start,
            effective_start_buffer=timedelta(days=5),
            end_requirement=end_requirement,
            ts_unit="day",
            requested_length=210,
        )
        is False
    )


def test_compute_prefetch_complete_true_for_negative_cache_or_tail_missing_permanent():
    tz = pytz.UTC
    requested_start = tz.localize(datetime(2020, 1, 1))
    end_requirement = tz.localize(datetime(2020, 1, 10, 23, 59, 59))

    assert (
        ThetaDataBacktestingPandas._compute_prefetch_complete(
            {"negative_cache": True},
            requested_start=requested_start,
            effective_start_buffer=timedelta(days=5),
            end_requirement=end_requirement,
            ts_unit="day",
            requested_length=210,
        )
        is True
    )

    assert (
        ThetaDataBacktestingPandas._compute_prefetch_complete(
            {"tail_missing_permanent": True},
            requested_start=requested_start,
            effective_start_buffer=timedelta(days=5),
            end_requirement=end_requirement,
            ts_unit="day",
            requested_length=210,
        )
        is True
    )
