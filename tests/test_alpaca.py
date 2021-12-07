import datetime
import pytest
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from credentials import AlpacaConfig
from pandas import Timestamp


alpaca = Alpaca(AlpacaConfig)

def test_get_timestamp(monkeypatch):
    def mockclock():
        class Clock:
            timestamp = Timestamp(2021, 2, 1, 9, 45, 30, 234)
        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mockclock)
    assert alpaca.get_timestamp() == 1612172730.000234

def test_is_market_open(monkeypatch):
    def mockisopen():
        class Clock:
           is_open = True
        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mockisopen)
    assert alpaca.is_market_open() == True

def test_get_time_to_open(monkeypatch):
    def mockisopen():
        class Clock:
            next_open = Timestamp(2021, 2, 1, 9, 30, 0, 0)
            timestamp = Timestamp(2021, 2, 1, 8, 30, 0, 0)
        return Clock()

    monkeypatch.setattr(alpaca.api, "get_clock", mockisopen)
    assert alpaca.get_time_to_open() == 3600



def run():
    x = dir("test_alpaca")
    # Run if calling from self.
    test_get_timestamp()
    test_is_market_open()
    test_get_time_to_open()

if __name__ == "__main__":
    run()