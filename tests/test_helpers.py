from datetime import datetime, timedelta

from lumibot.tools.helpers import (
    has_more_than_n_decimal_places,
    date_n_bars_from_date,
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
    result = date_n_bars_from_date(
        n_bars=1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 16).date()

    result = date_n_bars_from_date(
        n_bars=4,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 13).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = datetime(2025, 1, 21)
    result = date_n_bars_from_date(
        n_bars=1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 17).date()


    # test days in the future using negative bars
    start_datetime = datetime(2025, 1, 16)
    result = date_n_bars_from_date(
        n_bars=-1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 17).date()

    # test skipping holidays (MLK) (and also a long weekend)
    start_datetime = datetime(2025, 1, 17)
    result = date_n_bars_from_date(
        n_bars=-1,
        start_datetime=start_datetime,
    )
    assert result == datetime(2025, 1, 21).date()


def test_date_n_bars_from_date_zero_bars():
    start_datetime = datetime(2023, 10, 15)
    result = date_n_bars_from_date(
        n_bars=0,
        start_datetime=start_datetime
    )
    assert result == datetime(2023, 10, 15).date()







