"""A bar number passed as a drawing time is an error, not an object drawn in 1970.

Why: ``self.draw`` read every plain number as epoch milliseconds, so a bar number (for
example 677, a Pine ``bar_index`` ported as written) placed the object at
1970-01-01 00:00:00.677, far off the chart, with no error. Epoch seconds had the same
problem (1.79e9 read as milliseconds is late January 1970).

Invariant under test: a number below 1e8 is refused with a message that names the argument,
says a real time is needed and how to get the bar's time; a number from 1e8 up to 1e11 is
epoch seconds; a number at or above 1e11 is epoch milliseconds (as before); datetimes,
pandas Timestamps and ISO strings are unchanged.
"""

import datetime

import pandas as pd
import pytest

from lumibot.indicators import Drawings

MS = 1795348800000  # 2026-11-22 12:00 UTC in milliseconds
WHEN = pd.Timestamp(MS, unit="ms", tz="UTC")


@pytest.mark.parametrize("bar_number", [0, 1, 677, 25_000, 99_999_999])
def test_a_bar_number_is_refused_with_how_to_get_the_bar_time(bar_number):
    board = Drawings()
    with pytest.raises(ValueError) as caught:
        board.label(bar_number, 100.0, "H")
    message = str(caught.value)
    assert "label x" in message
    assert "bar number" in message
    assert "bar.time" in message
    with pytest.raises(ValueError, match="line x2"):
        board.line(WHEN, 100.0, bar_number, 101.0)


@pytest.mark.parametrize(
    "value",
    [MS, float(MS), MS // 1000, MS / 1000.0, WHEN, WHEN.to_pydatetime(),
     datetime.datetime(2026, 11, 22, 12, 0), "2026-11-22T12:00:00Z"],
    ids=["ms", "ms_float", "seconds", "seconds_float", "timestamp", "datetime_utc", "datetime_naive", "iso"],
)
def test_real_times_draw_at_that_time(value):
    board = Drawings()
    drawing_id = board.label(value, 100.0, "H")
    [item] = [d for d in board.to_list() if d["id"] == drawing_id]
    assert item["x"] == MS


def test_polyline_points_refuse_bar_numbers_too():
    board = Drawings()
    with pytest.raises(ValueError, match="bar number"):
        board.polyline([(10, 1.0), (20, 2.0)])
    drawing_id = board.polyline([(WHEN, 1.0), (MS + 60_000, 2.0)])
    [item] = [d for d in board.to_list() if d["id"] == drawing_id]
    assert [p["x"] for p in item["points"]] == [MS, MS + 60_000]
