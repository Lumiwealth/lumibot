import datetime as dt
import io
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import pytz

from lumibot import LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools import helpers as helpers_module
from lumibot.tools.helpers import (
    date_n_trading_days_from_date,
    get_timezone_from_datetime,
    get_trading_days,
    get_trading_times,
    has_more_than_n_decimal_places,
    is_market_open,
    print_progress_bar,
    quantize_to_num_decimals,
)


def test_has_more_than_n_decimal_places():
    assert has_more_than_n_decimal_places(1.2, 0)
    assert not has_more_than_n_decimal_places(1.2, 1)
    assert has_more_than_n_decimal_places(1.22, 0)
    assert has_more_than_n_decimal_places(1.22, 1)
    assert not has_more_than_n_decimal_places(1.22, 5)

    assert has_more_than_n_decimal_places(1.2345, 0)
    assert has_more_than_n_decimal_places(1.2345, 1)
    assert has_more_than_n_decimal_places(1.2345, 3)
    assert not has_more_than_n_decimal_places(1.2345, 4)
    assert not has_more_than_n_decimal_places(1.2345, 5)


def test_date_n_bars_from_date_valid_input(mocker):
    start_datetime = dt.datetime(2025, 1, 17)
    result = date_n_trading_days_from_date(
        n_days=1,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2025, 1, 16).date()

    result = date_n_trading_days_from_date(
        n_days=4,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2025, 1, 13).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = dt.datetime(2025, 1, 21)
    result = date_n_trading_days_from_date(
        n_days=1,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2025, 1, 17).date()

    # test days in the future using negative bars
    start_datetime = dt.datetime(2025, 1, 16)
    result = date_n_trading_days_from_date(
        n_days=-1,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2025, 1, 17).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = dt.datetime(2025, 1, 17)
    result = date_n_trading_days_from_date(
        n_days=-1,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2025, 1, 21).date()

    # test some more dates

    start_datetime = dt.datetime(2019, 3, 1)
    result = date_n_trading_days_from_date(
        n_days=30,
        start_datetime=start_datetime,
    )
    assert result == dt.datetime(2019, 1, 16).date()


def test_date_n_bars_from_date_zero_bars():
    start_datetime = dt.datetime(2023, 10, 15)
    result = date_n_trading_days_from_date(n_days=0, start_datetime=start_datetime)
    assert result == dt.datetime(2023, 10, 15).date()


def test_date_n_trading_days_from_date_with_24_7_market():
    start_datetime = dt.datetime(2024, 1, 13, tzinfo=pytz.UTC)
    result = date_n_trading_days_from_date(n_days=5, start_datetime=start_datetime, market="24/7")
    assert result == dt.datetime(2024, 1, 8).date()

    result = date_n_trading_days_from_date(n_days=-5, start_datetime=start_datetime, market="24/7")
    assert result == dt.datetime(2024, 1, 18).date()


def test_get_trading_days():

    # Test default parameters (NYSE market with default timezone)
    trading_days = get_trading_days()
    assert len(trading_days) > 0

    ny_tz = pytz.timezone("America/New_York")
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days("NYSE", start_date=start, end_date=end, tzinfo=ny_tz)
    assert len(trading_days) == 20  # https://www.nyse.com/publicdocs/ICE_NYSE_2025_Yearly_Trading_Calendar.pdf

    # Check all market opens and closes
    for open_time, close_time in zip(trading_days.market_open, trading_days.market_close, strict=False):
        # Check timezone
        assert str(open_time.tzinfo) == str(ny_tz)
        assert str(close_time.tzinfo) == str(ny_tz)

        # Check NYSE trading hours (9:30 AM - 4:00 PM)
        assert open_time.hour == 9
        assert open_time.minute == 30
        assert close_time.hour == 16
        assert close_time.minute == 0

    # Test 24/7 market
    utc = pytz.timezone("UTC")
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days("24/7", start_date=start, end_date=end, tzinfo=utc)
    assert len(trading_days) == 31
    assert all(dtm.hour == 0 and dtm.minute == 0 for dtm in trading_days.market_open)
    assert all(dtm.hour == 23 and dtm.minute == 59 for dtm in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(str(dtm.tzinfo) == str(utc) for dtm in trading_days.market_open)
    assert all(str(dtm.tzinfo) == str(utc) for dtm in trading_days.market_close)

    america_chicago = pytz.timezone("America/Chicago")
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days("24/7", start_date=start, end_date=end, tzinfo=america_chicago)
    assert len(trading_days) == 31
    assert all(dtm.hour == 0 and dtm.minute == 0 for dtm in trading_days.market_open)
    assert all(dtm.hour == 23 and dtm.minute == 59 for dtm in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(str(dtm.tzinfo) == str(america_chicago) for dtm in trading_days.market_open)
    assert all(str(dtm.tzinfo) == str(america_chicago) for dtm in trading_days.market_close)


def test_get_trading_days_long_window_uses_direct_schedule(monkeypatch, tmp_path):
    tzinfo = pytz.timezone("America/New_York")
    start = dt.datetime(2020, 1, 1)
    end = dt.datetime(2022, 1, 10)
    schedule_calls = []

    class _FakeCalendar:
        def schedule(self, start_date, end_date, tz=None):
            schedule_calls.append((start_date, end_date, tz))
            index = pd.DatetimeIndex(
                [
                    pd.Timestamp("2020-01-02", tz=tzinfo),
                    pd.Timestamp("2021-01-04", tz=tzinfo),
                ]
            )
            return pd.DataFrame(
                {
                    "market_open": [
                        pd.Timestamp("2020-01-02 09:30", tz=tzinfo),
                        pd.Timestamp("2021-01-04 09:30", tz=tzinfo),
                    ],
                    "market_close": [
                        pd.Timestamp("2020-01-02 16:00", tz=tzinfo),
                        pd.Timestamp("2021-01-04 16:00", tz=tzinfo),
                    ],
                },
                index=index,
            )

    monkeypatch.setenv("LUMIBOT_TRADING_DAYS_CACHE_DIR", str(Path(tmp_path) / "trading_days"))
    monkeypatch.setattr(helpers_module.mcal, "get_calendar", lambda market: _FakeCalendar())
    monkeypatch.setattr(
        helpers_module,
        "_get_trading_schedule_for_year",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("year cache path should not be used")),
    )
    helpers_module._TRADING_CALENDAR_CACHE.clear()

    trading_days = get_trading_days("NYSE", start_date=start, end_date=end, tzinfo=tzinfo)

    assert len(schedule_calls) == 1
    assert len(trading_days) == 2
    assert str(trading_days.index.tz) == str(tzinfo)


def test_get_trading_times_day_nyse():
    start_date = dt.datetime(2024, 1, 8)  # Monday
    end_date = dt.datetime(2024, 1, 13)  # Saturday
    pcal = get_trading_days(market="NYSE", start_date=start_date, end_date=end_date)

    result = get_trading_times(pcal=pcal, timestep="day")

    assert len(result) == 5  # 8th through 12th (Mon-Fri)
    # All timestamps the market open for NYSE
    assert all(ts.strftime("%H:%M:%S%z") == "09:30:00-0500" for ts in result)


def test_get_trading_times_minute_nyse():
    start_date = dt.datetime(2024, 1, 8)  # Monday
    end_date = dt.datetime(2024, 1, 13)  # Saturday
    pcal = get_trading_days(market="NYSE", start_date=start_date, end_date=end_date)

    result = get_trading_times(pcal=pcal, timestep="minute")

    assert len(result) == 6.5 * 60 * 5  # 8th through 12th (Mon-Fri)
    assert result[0].hour == 9
    assert result[0].minute == 30
    assert result[-1].hour == 15
    assert result[-1].minute == 59


def test_get_trading_times_minute_24_7_utc():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 9)
    tzinfo = pytz.timezone("UTC")
    pcal = get_trading_days(market="24/7", start_date=start_date, end_date=end_date, tzinfo=tzinfo)

    result = get_trading_times(pcal=pcal, timestep="minute")

    assert len(result) == 1440
    assert result[0].time().hour == 0
    assert result[0].time().minute == 0
    assert result[-1].time().hour == 23
    assert result[-1].time().minute == 59
    assert all(dtm.tzinfo.zone == tzinfo.zone for dtm in result)


def test_get_trading_times_minute_24_7_UTC():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 10)
    tzinfo = pytz.timezone("UTC")
    pcal = get_trading_days(market="24/7", start_date=start_date, end_date=end_date, tzinfo=tzinfo)

    result = get_trading_times(pcal=pcal, timestep="minute")

    assert len(result) == 1440 * 2
    assert result[0].time().hour == 0
    assert result[0].time().minute == 0
    assert result[-1].time().hour == 23
    assert result[-1].time().minute == 59
    assert all(dtm.tzinfo.zone == tzinfo.zone for dtm in result)


def test_print_progress_bar_throttles_output(monkeypatch):
    """Progress printing should be capped to ~1 line/sec to avoid log spam."""
    monkeypatch.setenv("BACKTESTING_QUIET_LOGS", "false")

    helpers_module._PROGRESS_LAST_PRINT.clear()
    buf = io.StringIO()

    monotonic_values = [0.0, 0.2, 1.2]

    def fake_monotonic():
        if monotonic_values:
            return monotonic_values.pop(0)
        # Avoid breaking other background loops that may also call time.monotonic().
        return 1.2

    monkeypatch.setattr(helpers_module.time, "monotonic", fake_monotonic)

    started = dt.datetime.now() - dt.timedelta(seconds=5)

    print_progress_bar(
        value=1,
        start_value=0,
        end_value=100,
        backtesting_started=started,
        file=buf,
        portfolio_value=100_000.0,
    )
    print_progress_bar(
        value=2,
        start_value=0,
        end_value=100,
        backtesting_started=started,
        file=buf,
        portfolio_value=100_000.0,
    )
    print_progress_bar(
        value=3,
        start_value=0,
        end_value=100,
        backtesting_started=started,
        file=buf,
        portfolio_value=100_000.0,
    )

    progress_lines = [line for line in buf.getvalue().splitlines() if "Progress |" in line]
    assert len(progress_lines) == 2


def test_get_trading_times_minute():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 10)
    tzinfo = pytz.timezone("America/New_York")
    pcal = get_trading_days(market="NYSE", start_date=start_date, end_date=end_date, tzinfo=tzinfo)

    result = get_trading_times(pcal=pcal, timestep="minute")

    assert len(result) == 780  # 390 minutes per day * 2 days
    assert result[0].time().hour == 9
    assert result[0].time().minute == 30
    assert result[-1].time().hour == 15
    assert result[-1].time().minute == 59
    assert all(dtm.tzinfo.zone == tzinfo.zone for dtm in result)


def test_get_timezone_from_datetime():
    # Test naive dt.datetime
    naive_dt = dt.datetime(2025, 1, 1)
    tzinfo = get_timezone_from_datetime(naive_dt)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))
    assert str(tzinfo) == LUMIBOT_DEFAULT_TIMEZONE

    # Test dt.datetime with ZoneInfo
    ny_zoneinfo = ZoneInfo("America/New_York")
    zoneinfo_dt = dt.datetime(2025, 1, 1, tzinfo=ny_zoneinfo)
    tzinfo = get_timezone_from_datetime(zoneinfo_dt)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))
    assert str(tzinfo) == "America/New_York"

    # Test dt.datetime with pytz
    ny_pytz = pytz.timezone("America/New_York")
    pytz_dt = dt.datetime(2025, 1, 1, tzinfo=ny_pytz)
    tzinfo = get_timezone_from_datetime(pytz_dt)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))
    assert str(tzinfo) == "America/New_York"

    # Test with different timezone
    tokyo_zoneinfo = ZoneInfo("Asia/Tokyo")
    tokyo_dt = dt.datetime(2025, 1, 1, tzinfo=tokyo_zoneinfo)
    tzinfo = get_timezone_from_datetime(tokyo_dt)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))
    assert str(tzinfo) == "Asia/Tokyo"


def test_get_timezone_from_datetime_types():
    dtm = dt.datetime(2025, 1, 1, tzinfo=ZoneInfo("America/New_York"))
    tzinfo = get_timezone_from_datetime(dtm)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))

    dtm = dt.datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))
    tzinfo = get_timezone_from_datetime(dtm)
    assert isinstance(tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo))

    # Test with None
    with pytest.raises(AttributeError):
        get_timezone_from_datetime(None)

    # Test with non-datetime
    with pytest.raises(AttributeError):
        get_timezone_from_datetime("not a datetime")


def test_quantize_to_num_decimals():
    assert quantize_to_num_decimals(123.4567, 2) == 123.46
    assert quantize_to_num_decimals(123.4567, 3) == 123.457
    assert quantize_to_num_decimals(Decimal("123.4567"), 1) == 123.5
    assert quantize_to_num_decimals(123.4567000001, 2) == 123.46


# Pytest functions
def test_is_market_open_during_trading_hours():
    tz = pytz.timezone("US/Eastern")
    dtm = tz.localize(dt.datetime.combine(dt.date(2024, 1, 5), dt.time(10, 30)))
    assert is_market_open(dtm, "NYSE") is True


def test_is_market_open_before_trading_hours():
    tz = pytz.timezone("US/Eastern")
    dtm = tz.localize(dt.datetime.combine(dt.date(2024, 1, 5), dt.time(4, 0)))
    assert is_market_open(dtm, "NYSE") is False


def test_is_market_open_after_trading_hours():
    tz = pytz.timezone("US/Eastern")
    dtm = tz.localize(dt.datetime.combine(dt.date(2024, 1, 5), dt.time(17, 0)))
    assert is_market_open(dtm, "NYSE") is False


def test_is_market_open_weekend():
    tz = pytz.timezone("US/Eastern")
    dtm = tz.localize(dt.datetime.combine(dt.date(2024, 1, 6), dt.time(12, 0)))  # Saturday
    assert is_market_open(dtm, "NYSE") is False


def test_is_market_open_invalid_market():
    tz = pytz.timezone("US/Eastern")
    dtm = tz.localize(dt.datetime.combine(dt.date(2024, 1, 5), dt.time(10, 30)))
    assert is_market_open(dtm, "INVALID") is False


@pytest.mark.parametrize(
    "market, tzname",
    [
        ("NYSE", "America/New_York"),
        # Using UTC also exhibits the same mismatch prior to the fix
        ("NYSE", "UTC"),
    ],
)
def test_get_trading_days_handles_tzaware_index_nyse(market, tzname):
    """
    Regression test for tz-aware index vs tz-naive slice bounds in get_trading_days.

    Before the fix, this raises:
        TypeError: Cannot compare tz-naive and tz-aware datetime-like objects

    After the fix, it should return a non-empty schedule for the requested window.
    """
    tz = pytz.timezone(tzname)

    # Intentionally pass timezone-aware datetimes with times that are not midnight
    # so code paths normalize to date-only and then slice the tz-aware index.
    start = tz.localize(dt.datetime(2025, 4, 14, 12, 0, 0))
    end = tz.localize(dt.datetime(2025, 4, 20, 12, 0, 0))

    sched = get_trading_days(market=market, start_date=start, end_date=end, tzinfo=tz)

    # After fix: we should get a non-empty DataFrame with DatetimeIndex
    assert isinstance(sched, pd.DataFrame)
    assert not sched.empty
    assert getattr(sched.index, "tz", None) is not None  # index should be tz-aware


def test_get_trading_days_handles_tzaware_index_247():
    """
    Same regression test for the built-in "24/7" calendar.

    Prior to the fix, this also triggers the tz-aware/naive slicing error.
    After the fix, it should return daily sessions for the range.
    """
    tz = pytz.UTC
    start = tz.localize(dt.datetime(2025, 1, 1, 8, 30, 0))
    end = tz.localize(dt.datetime(2025, 1, 5, 17, 45, 0))

    sched = get_trading_days(market="24/7", start_date=start, end_date=end, tzinfo=tz)

    assert isinstance(sched, pd.DataFrame)
    assert not sched.empty
    # Expect 4 sessions: Jan 1, 2, 3, 4 (inclusive of start day; end is exclusive)
    assert len(sched) == 4
    assert getattr(sched.index, "tz", None) is not None


def test_date_n_trading_days_from_date_no_tz_mismatch_nyse():
    """
    `date_n_trading_days_from_date` delegates to `get_trading_days`. Before the fix,
    this call raises a tz-aware/naive mismatch TypeError. After the fix, it should
    simply return a `datetime.date`.
    """
    tz = pytz.UTC
    start_dt = tz.localize(dt.datetime(2025, 7, 1, 12, 0, 0))

    result_back = date_n_trading_days_from_date(n_days=5, start_datetime=start_dt, market="NYSE")
    result_fwd = date_n_trading_days_from_date(n_days=-5, start_datetime=start_dt, market="NYSE")

    assert isinstance(result_back, dt.date)
    assert isinstance(result_fwd, dt.date)


def test_get_trading_days_on_tz_mismatch_then_fix(monkeypatch):
    """
    Construct a calendar whose schedule has a tz-aware DatetimeIndex (UTC),
    while get_trading_days currently builds tz-naive slice bounds.
    """
    import pandas as pd
    import pytz

    class FakeCalendar:
        def schedule(self, start_date, end_date, tz=None):
            # Build a tz-aware DatetimeIndex to trigger the mismatch
            idx = pd.date_range(
                start=pd.Timestamp("2025-01-01", tz=pytz.UTC), end=pd.Timestamp("2025-01-05", tz=pytz.UTC), freq="D"
            )
            # Market open/close columns can be naive datetimes; they aren't
            # used for the slicing that triggers the error.
            opens = pd.date_range("2025-01-01 00:00:00", periods=len(idx), freq="D")
            closes = pd.date_range("2025-01-01 23:59:00", periods=len(idx), freq="D")
            df = pd.DataFrame(
                {
                    "market_open": opens,
                    "market_close": closes,
                },
                index=idx,
            )
            return df

    # Monkeypatch pandas_market_calendars.get_calendar used inside helpers
    monkeypatch.setattr(helpers_module.mcal, "get_calendar", lambda market: FakeCalendar())

    tz = pytz.UTC
    start = tz.localize(dt.datetime(2025, 1, 1, 12, 0, 0))
    end = tz.localize(dt.datetime(2025, 1, 4, 12, 0, 0))

    sched = get_trading_days(market="FAKE", start_date=start, end_date=end, tzinfo=tz)

    assert isinstance(sched, pd.DataFrame)
    assert not sched.empty
    assert getattr(sched.index, "tz", None) is not None
