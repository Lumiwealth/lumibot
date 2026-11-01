"""Live Alpaca must not call the backtesting-only process_pending_orders at market close (#1113)."""
from unittest.mock import MagicMock

from alpaca.data.historical import (
    CryptoHistoricalDataClient,
    OptionHistoricalDataClient,
    StockHistoricalDataClient,
)
from alpaca.trading.client import TradingClient

from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData


def live_alpaca():
    data = AlpacaData(
        {"PAPER": True},
        stock_client=MagicMock(spec=StockHistoricalDataClient),
        crypto_client=MagicMock(spec=CryptoHistoricalDataClient),
        option_client=MagicMock(spec=OptionHistoricalDataClient),
    )
    return Alpaca(
        {"PAPER": True}, trading_client=MagicMock(spec=TradingClient), data_source=data,
        connect_stream=False, start_orders_thread=False,
    )


def test_live_alpaca_is_not_a_backtesting_broker():
    assert not live_alpaca().IS_BACKTESTING_BROKER


def test_await_market_to_close_after_the_bell_returns_without_attribute_error(monkeypatch):
    broker = live_alpaca()
    monkeypatch.setattr(broker, "get_time_to_close", lambda: -60)
    assert broker._await_market_to_close(strategy=MagicMock()) is None

