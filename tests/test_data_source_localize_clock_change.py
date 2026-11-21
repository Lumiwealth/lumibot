"""localize_datetime must not crash on the two daylight-saving clock changes.

A naive wall-clock time in the repeated 1 AM hour (first Sunday in November) or the
skipped 2 AM hour (second Sunday in March) has no single meaning in a daylight-saving
zone. pytz's is_dst=None raises for both, which crashed strategies twice a year.
"""

import datetime

import pytz

from lumibot.data_sources.data_source import DataSource


class _Source(DataSource):
    SOURCE = "test"
    TIMESTEP_MAPPING = []

    def get_chains(self, *args, **kwargs):
        return {}

    def get_historical_prices(self, *args, **kwargs):
        return None

    def get_last_price(self, *args, **kwargs):
        return None


def _source():
    return _Source(tzinfo=pytz.timezone("America/New_York"))


def test_repeated_hour_takes_the_earlier_instant():
    dt = _source().localize_datetime(datetime.datetime(2026, 11, 1, 1, 30))
    assert dt.utcoffset() == datetime.timedelta(hours=-4)  # first 1:30, still daylight time
    assert dt.astimezone(pytz.utc) == datetime.datetime(2026, 11, 1, 5, 30, tzinfo=pytz.utc)


def test_skipped_hour_moves_forward_to_a_real_time():
    dt = _source().localize_datetime(datetime.datetime(2026, 3, 8, 2, 30))
    assert dt.astimezone(pytz.utc) == datetime.datetime(2026, 3, 8, 7, 30, tzinfo=pytz.utc)
    assert (dt.hour, dt.minute) == (3, 30)


def test_ordinary_times_are_unchanged():
    dt = _source().localize_datetime(datetime.datetime(2026, 7, 1, 9, 30))
    assert dt.utcoffset() == datetime.timedelta(hours=-4) and dt.hour == 9
