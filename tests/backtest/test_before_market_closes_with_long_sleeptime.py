"""before_market_closes must run in backtests whose sleeptime is longer than the time left.

An hourly strategy's last iteration is at 15:30 on a 16:00 close. The sleep after it used to
go straight to the close, the session ended there, and the post-loop close hook only runs
while the market is still open, so before_market_closes was skipped on every day. A strategy
that sold everything there held overnight in every backtest.
"""

from __future__ import annotations

import logging

from lumibot.strategies.strategy_executor import StrategyExecutor


class _Broker:
    IS_BACKTESTING_BROKER = True
    market = "NYSE"

    def __init__(self, time_to_close):
        self.time_to_close = time_to_close
        self.data_source = None

    def get_time_to_close(self):
        return self.time_to_close


class _Strategy:
    is_backtesting = True
    name = "dummy"
    logger = logging.getLogger("test_before_market_closes")

    def __init__(self, broker, minutes_before_closing, sleeptime="60M"):
        self.broker = broker
        self.minutes_before_closing = minutes_before_closing
        self.sleeptime = sleeptime

    def on_trading_iteration(self):
        return

    def _update_cash_with_dividends(self):
        return


def _executor(time_to_close, minutes_before_closing, sleeptime="60M"):
    broker = _Broker(time_to_close)
    executor = StrategyExecutor(_Strategy(broker, minutes_before_closing, sleeptime))
    slept = []

    def fake_sleep(seconds):
        slept.append(seconds)
        broker.time_to_close -= seconds

    executor.safe_sleep = fake_sleep
    return executor, broker, slept


def test_long_sleep_stops_inside_the_before_close_window():
    executor, broker, slept = _executor(time_to_close=1800, minutes_before_closing=1)
    assert executor._strategy_sleep() is False
    # Stopped one minute before the close, with the market still open.
    assert slept == [1740]
    assert broker.time_to_close == 60


def test_zero_minutes_before_closing_keeps_sleeping_to_the_close():
    executor, broker, slept = _executor(time_to_close=1800, minutes_before_closing=0)
    assert executor._strategy_sleep() is False
    assert slept == [1800]


def test_sleep_shorter_than_the_time_left_is_unchanged():
    executor, broker, slept = _executor(time_to_close=7200, minutes_before_closing=1)
    assert executor._strategy_sleep() is True
    assert slept == [3600]
