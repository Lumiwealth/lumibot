from __future__ import annotations

from datetime import datetime

import pandas as pd

from lumibot.tools.ccxt_data_store import CcxtCacheDB


def test_range_count_for_supported_timeframes() -> None:
    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 2, 30)

    assert CcxtCacheDB._range_count_for_timeframe("1m", start, end) == 150
    assert CcxtCacheDB._range_count_for_timeframe("5m", start, end) == 30
    assert CcxtCacheDB._range_count_for_timeframe("1h", start, end) == 3

    day_end = datetime(2024, 1, 3, 0, 0)
    assert CcxtCacheDB._range_count_for_timeframe("1d", start, day_end) == 2


def test_find_non_overlapping_range_returns_gap_and_merged_cache_range() -> None:
    cache = object.__new__(CcxtCacheDB)
    ranges = pd.DataFrame(
        [
            ("id1", datetime(2023, 1, 1), datetime(2023, 1, 10)),
            ("id2", datetime(2023, 2, 3), datetime(2023, 3, 11)),
            ("id3", datetime(2023, 5, 1), datetime(2023, 6, 7)),
        ],
        columns=["id", "start_dt", "end_dt"],
    )

    download_ranges, overlap_ids, cache_range = cache._find_non_overlapping_range(
        ranges,
        datetime(2023, 1, 5),
        datetime(2023, 3, 7),
    )

    assert download_ranges == [(datetime(2023, 1, 10), datetime(2023, 2, 3))]
    assert overlap_ids == ["id1", "id2"]
    assert cache_range == (datetime(2023, 1, 1), datetime(2023, 3, 11))


def test_find_non_overlapping_range_returns_full_range_without_overlap() -> None:
    cache = object.__new__(CcxtCacheDB)
    ranges = pd.DataFrame(
        [("id1", datetime(2023, 1, 1), datetime(2023, 1, 10))],
        columns=["id", "start_dt", "end_dt"],
    )

    start = datetime(2023, 2, 1)
    end = datetime(2023, 2, 5)

    assert cache._find_non_overlapping_range(ranges, start, end) == ([(start, end)], [], (start, end))
