"""Live market-hours waits return promptly when the strategy is stopped (#1252)."""
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from alpaca.data.historical import (
    CryptoHistoricalDataClient,
    OptionHistoricalDataClient,
    StockHistoricalDataClient,
)
from alpaca.trading.client import TradingClient

from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData

A_TRADING_DAY = 6.5 * 3600


def live_alpaca(monkeypatch, market_open):
    data = AlpacaData(
        {"PAPER": True},
        stock_client=MagicMock(spec=StockHistoricalDataClient),
        crypto_client=MagicMock(spec=CryptoHistoricalDataClient),
        option_client=MagicMock(spec=OptionHistoricalDataClient),
    )
    broker = Alpaca(
        {"PAPER": True}, trading_client=MagicMock(spec=TradingClient), data_source=data,
        connect_stream=False, start_orders_thread=False,
    )
    monkeypatch.setattr(broker, "is_market_open", lambda: market_open)
    monkeypatch.setattr(broker, "get_time_to_open", lambda: A_TRADING_DAY)
    monkeypatch.setattr(broker, "get_time_to_close", lambda: A_TRADING_DAY)
    # Market-close pending-order processing is out of scope here (#1113).
    monkeypatch.setattr(broker, "process_pending_orders", lambda **_: None, raising=False)
    return broker


def strategy_with_stop_event():
    return SimpleNamespace(_executor=SimpleNamespace(stop_event=threading.Event()))


def timed(fn):
    start = time.monotonic()
    fn()
    return time.monotonic() - start


def test_open_wait_returns_at_once_when_already_stopped(monkeypatch):
    broker = live_alpaca(monkeypatch, market_open=False)
    strategy = strategy_with_stop_event()
    strategy._executor.stop_event.set()
    assert timed(lambda: broker._await_market_to_open(strategy=strategy)) < 1


def test_close_wait_returns_at_once_when_already_stopped(monkeypatch):
    broker = live_alpaca(monkeypatch, market_open=True)
    strategy = strategy_with_stop_event()
    strategy._executor.stop_event.set()
    assert timed(lambda: broker._await_market_to_close(strategy=strategy)) < 1


def test_open_wait_breaks_out_when_stopped_mid_wait(monkeypatch):
    broker = live_alpaca(monkeypatch, market_open=False)
    strategy = strategy_with_stop_event()
    threading.Timer(0.2, strategy._executor.stop_event.set).start()
    assert timed(lambda: broker._await_market_to_open(strategy=strategy)) < 2


def test_close_wait_breaks_out_when_stopped_mid_wait(monkeypatch):
    broker = live_alpaca(monkeypatch, market_open=True)
    strategy = strategy_with_stop_event()
    threading.Timer(0.2, strategy._executor.stop_event.set).start()
    assert timed(lambda: broker._await_market_to_close(strategy=strategy)) < 2


def test_waits_without_an_executor_still_sleep_the_full_interval(monkeypatch):
    broker = live_alpaca(monkeypatch, market_open=False)
    slept = []
    monkeypatch.setattr(broker, "sleep", slept.append)
    broker._await_market_to_open(strategy=None)
    assert slept == [A_TRADING_DAY]
