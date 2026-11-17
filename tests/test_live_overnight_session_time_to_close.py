"""A live strategy started inside an overnight session runs, it does not crash or stop.

Markets such as ``us_futures`` open in the evening: the session that opens at 6 PM New York
belongs to the next day's calendar row. In the hour after that open, today's row has already
closed while the market is open. A live broker must report the time to the close of the
session it is in, and the executor must never try to move a live broker's clock.
"""
import logging
from datetime import datetime as real_datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import pytest

import lumibot.brokers.broker as broker_module
from lumibot.brokers.broker import Broker
from lumibot.strategies.strategy_executor import StrategyExecutor

NEW_YORK = ZoneInfo("America/New_York")


class LiveBrokerStub:
    """Wall-clock broker: real market-hours code, no simulated clock to move."""

    IS_BACKTESTING_BROKER = False
    extended_trading_minutes = 0

    market_hours = Broker.market_hours
    utc_to_local = Broker.utc_to_local
    initialize_market_calendars = Broker.initialize_market_calendars
    _is_market_open_from_initialized_calendar = Broker._is_market_open_from_initialized_calendar
    _is_continuous_market = Broker._is_continuous_market
    _session_close_time = getattr(Broker, "_session_close_time", None)
    is_market_open = Broker.is_market_open
    market_close_time = Broker.market_close_time
    get_time_to_close = Broker.get_time_to_close

    def __init__(self, market):
        self.market = market
        self._trading_days = None
        self.logger = logging.getLogger("live-overnight-session-test")


def live_broker_at(monkeypatch, market, local_time):
    """A live broker whose wall clock reads `local_time` (New York)."""
    now = local_time.replace(tzinfo=NEW_YORK)

    class FrozenDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            # A naive now() is the machine's own local time (CI and servers run in UTC),
            # exactly like the real datetime.now().
            moment = now.astimezone(tz) if tz is not None else now.astimezone().replace(tzinfo=None)
            return cls(
                moment.year, moment.month, moment.day, moment.hour, moment.minute, moment.second,
                tzinfo=moment.tzinfo,
            )

    monkeypatch.setattr(broker_module, "datetime", FrozenDatetime)
    monkeypatch.setattr(broker_module, "_local_tz", lambda: NEW_YORK)

    broker = LiveBrokerStub(market)
    calendar = mcal.get_calendar(market)
    start = now.astimezone(timezone.utc) - broker_module.timedelta(days=5)
    end = now.astimezone(timezone.utc) + broker_module.timedelta(days=7)
    broker.initialize_market_calendars(calendar.schedule(start_date=start, end_date=end))
    return broker


def executor_for(broker, minutes_before_closing=1):
    executor = object.__new__(StrategyExecutor)
    executor.broker = broker
    executor.strategy = SimpleNamespace(
        logger=logging.getLogger("live-overnight-session-test"),
        minutes_before_closing=minutes_before_closing,
    )
    return executor


EVENING_STARTS = [
    # Tuesday 6:10 PM New York, winter time: the session opened 10 minutes ago.
    (real_datetime(2026, 11, 17, 18, 10), 23 * 3600 + 50 * 60),
    # Tuesday 7:30 PM New York, summer time: still before UTC midnight.
    (real_datetime(2026, 7, 14, 19, 30), 22 * 3600 + 30 * 60),
    # Tuesday 9 PM New York: past UTC midnight, the calendar row already matches.
    (real_datetime(2026, 11, 17, 21, 0), 21 * 3600),
    # Tuesday 11 AM New York: ordinary daytime start.
    (real_datetime(2026, 11, 17, 11, 0), 7 * 3600),
]


@pytest.mark.parametrize("local_time, seconds_to_close", EVENING_STARTS)
def test_live_futures_time_to_close_is_the_close_of_the_open_session(monkeypatch, local_time, seconds_to_close):
    broker = live_broker_at(monkeypatch, "us_futures", local_time)

    assert broker.is_market_open() is True
    assert broker.get_time_to_close() == pytest.approx(seconds_to_close, abs=1)
    close = broker.market_close_time()
    assert close > local_time.replace(tzinfo=NEW_YORK)


def test_live_start_in_the_evening_session_does_not_touch_a_broker_clock_or_stop(monkeypatch):
    broker = live_broker_at(monkeypatch, "us_futures", real_datetime(2026, 11, 17, 18, 10))
    assert not hasattr(broker, "_update_datetime")
    executor = executor_for(broker)

    time_to_close = executor._ensure_progress_inside_open_session(broker.get_time_to_close())

    assert time_to_close > 22 * 3600
    assert executor._calculate_should_we_stop() is False


@pytest.mark.parametrize("stale_time_to_close", [None, 0, -600])
def test_live_broker_is_never_asked_to_move_its_clock(stale_time_to_close):
    broker = SimpleNamespace(IS_BACKTESTING_BROKER=False, is_market_open=lambda: True)
    executor = executor_for(broker)

    assert executor._ensure_progress_inside_open_session(stale_time_to_close) == stale_time_to_close


def test_live_stock_market_hours_are_unchanged(monkeypatch):
    # 4:30 PM New York: the NYSE session is over and the next one has not opened.
    broker = live_broker_at(monkeypatch, "NYSE", real_datetime(2026, 11, 17, 16, 30))
    assert broker.is_market_open() is False
    assert broker.get_time_to_close() == 0
    assert broker.market_close_time() == real_datetime(2026, 11, 17, 16, 0, tzinfo=NEW_YORK)

    broker = live_broker_at(monkeypatch, "NYSE", real_datetime(2026, 11, 17, 15, 0))
    assert broker.is_market_open() is True
    assert broker.get_time_to_close() == pytest.approx(3600, abs=1)


@pytest.mark.parametrize("stale_time_to_close", [None, 0, -5])
def test_backtest_clock_is_still_nudged_when_stalled_inside_an_open_session(stale_time_to_close):
    moved = []
    broker = SimpleNamespace(
        IS_BACKTESTING_BROKER=True,
        is_market_open=lambda: True,
        _update_datetime=moved.append,
        get_time_to_close=lambda: 3599,
    )
    executor = executor_for(broker)

    assert executor._ensure_progress_inside_open_session(stale_time_to_close) == 3599
    assert moved == [1]


def test_backtest_clock_is_left_alone_when_time_remains_or_market_is_closed():
    moved = []
    broker = SimpleNamespace(
        IS_BACKTESTING_BROKER=True,
        is_market_open=lambda: True,
        _update_datetime=moved.append,
        get_time_to_close=lambda: 3599,
    )
    executor = executor_for(broker)
    assert executor._ensure_progress_inside_open_session(120) == 120

    broker.is_market_open = lambda: False
    assert executor._ensure_progress_inside_open_session(0) == 0
    assert moved == []
