import datetime as dt

import pytz

from lumibot.tools.helpers import is_regular_trading_session


NY = pytz.timezone("America/New_York")


def _et(year, month, day, hour, minute=0):
    return NY.localize(dt.datetime(year, month, day, hour, minute))


def test_regular_trading_session_uses_nyse_hours_not_strategy_market():
    assert not is_regular_trading_session(_et(2024, 1, 2, 9, 29), market="NYSE")
    assert is_regular_trading_session(_et(2024, 1, 2, 9, 30), market="NYSE")
    assert is_regular_trading_session(_et(2024, 1, 2, 15, 59), market="NYSE")
    assert not is_regular_trading_session(_et(2024, 1, 2, 16, 0), market="NYSE")


def test_regular_trading_session_can_include_exact_close_when_requested():
    assert is_regular_trading_session(
        _et(2024, 1, 2, 16, 0),
        market="NYSE",
        include_close=True,
    )


def test_regular_trading_session_respects_nyse_early_close():
    # July 3, 2024 was a NYSE early-close session.
    assert is_regular_trading_session(_et(2024, 7, 3, 12, 59), market="NYSE")
    assert not is_regular_trading_session(_et(2024, 7, 3, 13, 0), market="NYSE")


def test_regular_trading_session_returns_false_on_nyse_holiday():
    assert not is_regular_trading_session(_et(2024, 7, 4, 10, 0), market="NYSE")


def test_regular_trading_session_converts_aware_datetime_to_market_timezone():
    utc = pytz.UTC

    assert is_regular_trading_session(
        utc.localize(dt.datetime(2024, 1, 2, 14, 30)),
        market="NYSE",
    )
    assert not is_regular_trading_session(
        utc.localize(dt.datetime(2024, 1, 2, 21, 0)),
        market="NYSE",
    )
