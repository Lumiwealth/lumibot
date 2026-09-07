"""Regression tests for Alpaca's inherited live market-close wait."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

from lumibot.brokers.alpaca import Alpaca
from lumibot.strategies.strategy import Strategy


def _alpaca_wait_stub(
    *, market_open: bool, seconds_to_close: int = 900
) -> tuple[Alpaca, MagicMock, MagicMock, MagicMock]:
    """Build a credential-free Alpaca broker with a deterministic market clock.

    Args:
        market_open: Whether the simulated live market is open.
        seconds_to_close: Seconds remaining in the simulated market session.

    Returns:
        The broker and its market-open, time-to-close, and sleep mocks.
    """
    broker = Alpaca.__new__(Alpaca)
    broker.market = "NYSE"
    broker.logger = MagicMock()
    is_market_open = MagicMock(return_value=market_open)
    get_time_to_close = MagicMock(return_value=seconds_to_close)
    sleep = MagicMock()
    broker.is_market_open = is_market_open
    broker.get_time_to_close = get_time_to_close
    broker.sleep = sleep
    return broker, is_market_open, get_time_to_close, sleep


def test_strategy_await_market_close_uses_live_broker_wait() -> None:
    """Alpaca must not call the backtesting-only pending-order processor."""
    broker, is_market_open, get_time_to_close, sleep = _alpaca_wait_stub(market_open=True)
    strategy = cast(Strategy, SimpleNamespace(broker=broker, minutes_before_closing=5))

    Strategy.await_market_to_close(strategy)

    is_market_open.assert_called_once_with()
    get_time_to_close.assert_called_once_with()
    sleep.assert_called_once_with(600)


def test_strategy_await_market_close_returns_when_market_is_closed() -> None:
    """A closed Alpaca session must not wait for the next session's close."""
    broker, is_market_open, get_time_to_close, sleep = _alpaca_wait_stub(market_open=False)
    strategy = cast(Strategy, SimpleNamespace(broker=broker, minutes_before_closing=5))

    Strategy.await_market_to_close(strategy)

    is_market_open.assert_called_once_with()
    get_time_to_close.assert_not_called()
    sleep.assert_not_called()
