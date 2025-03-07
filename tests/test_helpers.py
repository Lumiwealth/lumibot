from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pytz import timezone

from lumibot import LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import (
    has_more_than_n_decimal_places,
    date_n_days_from_date,
    get_trading_days,
    get_trading_times
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

    start_datetime = datetime(2025, 1, 17)
    result = date_n_days_from_date(
        n_days=1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 16).date()

    result = date_n_days_from_date(
        n_days=4,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 13).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = datetime(2025, 1, 21)
    result = date_n_days_from_date(
        n_days=1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 17).date()

    # test days in the future using negative bars
    start_datetime = datetime(2025, 1, 16)
    result = date_n_days_from_date(
        n_days=-1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 17).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = datetime(2025, 1, 17)
    result = date_n_days_from_date(
        n_days=-1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 21).date()

    # test some more dates

    start_datetime = datetime(2019, 3, 1)
    result = date_n_days_from_date(
        n_days=30,
        start_datetime=start_datetime,
    )
    assert result == datetime(2019, 1, 16).date()


def test_date_n_bars_from_date_zero_bars():
    start_datetime = datetime(2023, 10, 15)
    result = date_n_days_from_date(
        n_days=0,
        start_datetime=start_datetime
    )
    assert result == datetime(2023, 10, 15).date()


def test_date_n_days_from_date_with_24_7_market():
    start_datetime = datetime(2024, 1, 13)
    result = date_n_days_from_date(
        n_days=5,
        start_datetime=start_datetime,
        market="24/7",
        tzinfo=ZoneInfo("UTC")
    )
    assert result == datetime(2024, 1, 8).date()

    result = date_n_days_from_date(
        n_days=-5,
        start_datetime=start_datetime,
        market="24/7",
        tzinfo=ZoneInfo("UTC")
    )
    assert result == datetime(2024, 1, 18).date()


def test_get_trading_days():

    # Test default parameters (NYSE market with default timezone)
    trading_days = get_trading_days()
    assert len(trading_days) > 0

    ny_tz = ZoneInfo('America/New_York')
    start = datetime(2025, 1, 1)
    end = datetime(2025, 2, 1)
    trading_days = get_trading_days('NYSE', start_date=start, end_date=end)
    assert len(trading_days) == 20

    # Check all market opens and closes
    for open_time, close_time in zip(trading_days.market_open, trading_days.market_close):
        # Check timezone
        assert open_time.tzinfo == ny_tz
        assert close_time.tzinfo == ny_tz

        # Check NYSE trading hours (9:30 AM - 4:00 PM)
        assert open_time.hour == 9
        assert open_time.minute == 30
        assert close_time.hour == 16
        assert close_time.minute == 0

    # Test 24/7 market
    utc = ZoneInfo('UTC')
    start = datetime(2025, 1, 1)
    end = datetime(2025, 2, 1)
    trading_days = get_trading_days('24/7', start_date=start, end_date=end, tzinfo=utc)
    assert len(trading_days) == 31
    assert all(dt.hour == 0 and dt.minute == 0 for dt in trading_days.market_open)
    assert all(dt.hour == 23 and dt.minute == 59 for dt in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(dt.tzinfo == utc for dt in trading_days.market_open)
    assert all(dt.tzinfo == utc for dt in trading_days.market_close)

    america_chicago = ZoneInfo('America/Chicago')
    start = datetime(2025, 1, 1)
    end = datetime(2025, 2, 1)
    trading_days = get_trading_days('24/7', start_date=start, end_date=end, tzinfo=america_chicago)
    assert len(trading_days) == 31
    assert all(dt.hour == 0 and dt.minute == 0 for dt in trading_days.market_open)
    assert all(dt.hour == 23 and dt.minute == 59 for dt in trading_days.market_close)
    # Check timezone of market_open and market_close times
    assert all(dt.tzinfo == america_chicago for dt in trading_days.market_open)
    assert all(dt.tzinfo == america_chicago for dt in trading_days.market_close)


def test_get_trading_times_day():
    start_date = datetime(2024, 1, 8)  # Monday
    end_date = datetime(2024, 1, 13)  # Saturday
    pcal = get_trading_days(market='NYSE', start_date=start_date, end_date=end_date)

    result = get_trading_times(pcal=pcal, timestep='day')

    assert len(result) == 5  # 8th through 12th (Mon-Fri)
    # All timestamps should be at midnight in market timezone
    assert all(ts.strftime('%H:%M:%S%z') == '00:00:00-0500' for ts in result)


def test_get_trading_times_minute_24_7_utc():
    start_date = datetime(2024, 1, 8)
    end_date = datetime(2024, 1, 9)
    tzinfo = ZoneInfo('UTC')
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
    assert all(dt.tzinfo == tzinfo for dt in result)


def test_get_trading_times_minute_24_7_america_chicago():
    start_date = datetime(2024, 1, 8)
    end_date = datetime(2024, 1, 10)
    tzinfo = ZoneInfo('America/Chicago')
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
    assert all(dt.tzinfo == tzinfo for dt in result)


def test_get_trading_times_minute():
    start_date = datetime(2024, 1, 8)
    end_date = datetime(2024, 1, 10)
    tzinfo = ZoneInfo('America/New_York')
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
    assert all(dt.tzinfo == tzinfo for dt in result)