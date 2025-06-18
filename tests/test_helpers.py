import datetime as dt
from decimal import Decimal
from zoneinfo import ZoneInfo
import pytz
import pytest

from lumibot import LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import (
    has_more_than_n_decimal_places,
    date_n_trading_days_from_date,
    get_trading_days,
    get_trading_times,
    get_timezone_from_datetime,
    quantize_to_num_decimals, is_market_open
)


def test_has_more_than_n_decimal_places():
    assert has_more_than_n_decimal_places(1.2, 0) == True
    assert has_more_than_n_decimal_places(1.2, 1) == False
    assert has_more_than_n_decimal_places(1.22, 0) == True
    assert has_more_than_n_decimal_places(1.22, 1) == True
    assert has_more_than_n_decimal_places(1.22, 5) == False

    assert has_more_than_n_decimal_places(1.2345, 0) == True
    assert has_more_than_n_decimal_places(1.2345, 1) == True
    assert has_more_than_n_decimal_places(1.2345, 3) == True
    assert has_more_than_n_decimal_places(1.2345, 4) == False
    assert has_more_than_n_decimal_places(1.2345, 5) == False


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
    result = date_n_trading_days_from_date(
        n_days=0,
        start_datetime=start_datetime
    )
    assert result == dt.datetime(2023, 10, 15).date()


def test_date_n_trading_days_from_date_with_24_7_market():
    start_datetime = dt.datetime(2024, 1, 13, tzinfo=pytz.UTC)
    result = date_n_trading_days_from_date(
        n_days=5,
        start_datetime=start_datetime,
        market="24/7"
    )
    assert result == dt.datetime(2024, 1, 8).date()

    result = date_n_trading_days_from_date(
        n_days=-5,
        start_datetime=start_datetime,
        market="24/7"
    )
    assert result == dt.datetime(2024, 1, 18).date()


def test_get_trading_days():

    # Test default parameters (NYSE market with default timezone)
    trading_days = get_trading_days()
    assert len(trading_days) > 0

    ny_tz = pytz.timezone('America/New_York')
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days('NYSE', start_date=start, end_date=end, tzinfo=ny_tz)
    assert len(trading_days) == 20  # https://www.nyse.com/publicdocs/ICE_NYSE_2025_Yearly_Trading_Calendar.pdf

    # Check all market opens and closes
    for open_time, close_time in zip(trading_days.market_open, trading_days.market_close):
        # Check timezone
        assert str(open_time.tzinfo) == str(ny_tz)
        assert str(close_time.tzinfo) == str(ny_tz)

        # Check NYSE trading hours (9:30 AM - 4:00 PM)
        assert open_time.hour == 9
        assert open_time.minute == 30
        assert close_time.hour == 16
        assert close_time.minute == 0

    # Test 24/7 market
    utc = pytz.timezone('UTC')
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days('24/7', start_date=start, end_date=end, tzinfo=utc)
    assert len(trading_days) == 31
    assert all(dtm.hour == 0 and dtm.minute == 0 for dtm in trading_days.market_open)
    assert all(dtm.hour == 23 and dtm.minute == 59 for dtm in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(str(dtm.tzinfo) == str(utc) for dtm in trading_days.market_open)
    assert all(str(dtm.tzinfo) == str(utc) for dtm in trading_days.market_close)

    america_chicago = pytz.timezone('America/Chicago')
    start = dt.datetime(2025, 1, 1)
    end = dt.datetime(2025, 2, 1)
    trading_days = get_trading_days('24/7', start_date=start, end_date=end, tzinfo=america_chicago)
    assert len(trading_days) == 31
    assert all(dtm.hour == 0 and dtm.minute == 0 for dtm in trading_days.market_open)
    assert all(dtm.hour == 23 and dtm.minute == 59 for dtm in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(str(dtm.tzinfo) == str(america_chicago) for dtm in trading_days.market_open)
    assert all(str(dtm.tzinfo) == str(america_chicago) for dtm in trading_days.market_close)


def test_get_trading_times_day_nyse():
    start_date = dt.datetime(2024, 1, 8)  # Monday
    end_date = dt.datetime(2024, 1, 13)  # Saturday
    pcal = get_trading_days(market='NYSE', start_date=start_date, end_date=end_date)

    result = get_trading_times(pcal=pcal, timestep='day')

    assert len(result) == 5  # 8th through 12th (Mon-Fri)
    # All timestamps the market open for NYSE
    assert all(ts.strftime('%H:%M:%S%z') == '09:30:00-0500' for ts in result)


def test_get_trading_times_minute_nyse():
    start_date = dt.datetime(2024, 1, 8)  # Monday
    end_date = dt.datetime(2024, 1, 13)  # Saturday
    pcal = get_trading_days(market='NYSE', start_date=start_date, end_date=end_date)

    result = get_trading_times(pcal=pcal, timestep='minute')

    assert len(result) == 6.5 * 60 * 5  # 8th through 12th (Mon-Fri)
    assert result[0].hour == 9
    assert result[0].minute == 30
    assert result[-1].hour == 15
    assert result[-1].minute == 59


def test_get_trading_times_minute_24_7_utc():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 9)
    tzinfo = pytz.timezone('UTC')
    pcal = get_trading_days(
        market='24/7',
        start_date=start_date,
        end_date=end_date,
        tzinfo=tzinfo
    )

    result = get_trading_times(pcal=pcal, timestep='minute')

    assert len(result) == 1440
    assert result[0].time().hour == 0
    assert result[0].time().minute == 0
    assert result[-1].time().hour == 23
    assert result[-1].time().minute == 59
    assert all(dtm.tzinfo.zone == tzinfo.zone for dtm in result)


def test_get_trading_times_minute_24_7_america_chicago():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 10)
    tzinfo = pytz.timezone('America/Chicago')
    pcal = get_trading_days(
        market='24/7',
        start_date=start_date,
        end_date=end_date,
        tzinfo=tzinfo
    )

    result = get_trading_times(pcal=pcal, timestep='minute')

    assert len(result) == 1440 * 2
    assert result[0].time().hour == 0
    assert result[0].time().minute == 0
    assert result[-1].time().hour == 23
    assert result[-1].time().minute == 59
    assert all(dtm.tzinfo.zone == tzinfo.zone for dtm in result)


def test_get_trading_times_minute():
    start_date = dt.datetime(2024, 1, 8)
    end_date = dt.datetime(2024, 1, 10)
    tzinfo = pytz.timezone('America/New_York')
    pcal = get_trading_days(
        market='NYSE',
        start_date=start_date,
        end_date=end_date,
        tzinfo=tzinfo
    )

    result = get_trading_times(pcal=pcal, timestep='minute')

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
    assert quantize_to_num_decimals(Decimal('123.4567'), 1) == 123.5
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
