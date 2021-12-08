import datetime
from pandas import Timestamp
import pytest

from credentials import AlpacaConfig
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities.asset import Asset
from lumibot.entities.position import Position


alpaca = Alpaca(AlpacaConfig)


def test_get_timestamp(monkeypatch):
    def mock_clock():
        class Clock:
            timestamp = Timestamp(2021, 2, 1, 9, 45, 30, 234)

        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mock_clock)
    assert alpaca.get_timestamp() == 1612172730.000234


def test_is_market_open(monkeypatch):
    def mock_is_open():
        class Clock:
            is_open = True

        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mock_is_open)
    assert alpaca.is_market_open() == True


def test_get_time_to_open(monkeypatch):
    def mock_to_open():
        class Clock:
            next_open = Timestamp(2021, 2, 1, 9, 30, 0, 0)
            timestamp = Timestamp(2021, 2, 1, 8, 30, 0, 0)

        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mock_to_open)
    assert alpaca.get_time_to_open() == 3600


def test_get_time_to_close(monkeypatch):
    def mock_to_close():
        class Clock:
            timestamp = Timestamp(2021, 2, 1, 14, 0, 0, 0)
            next_close = Timestamp(2021, 2, 1, 16, 0, 0, 0)

        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mock_to_close)
    assert alpaca.get_time_to_close() == 7200


def test__get_cash_balance_at_broker(monkeypatch):
    def mock_cash():
        class Account:
            _raw = dict(cash="123456.78")

        return Account()

    monkeypatch.setattr(alpaca.api, "get_account", mock_cash)
    assert alpaca._get_cash_balance_at_broker() == 123456.78


@pytest.mark.parametrize("symbol, qty", [("MSFT", 9), ("GM", 100), ("FB", 500)])
def test__parse_broker_position(symbol, qty):
    class BPosition:
        _raw = {"symbol": symbol, "qty": str(qty)}

    bposition = BPosition()

    position = Position(
        "AlpacaTest",
        Asset(symbol=symbol, asset_type="stock"),
        quantity=qty,
        orders=None,
    )

    result = alpaca._parse_broker_position(bposition, "AlpacaTest", orders=None)
    assert result.asset == position.asset
    assert result.quantity == position.quantity
    assert result.asset.symbol == position.asset.symbol
    assert result.strategy == position.strategy
