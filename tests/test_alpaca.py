import datetime
import pandas as pd
from pandas import Timestamp
import pytest

from credentials import AlpacaConfig
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities.asset import Asset
from lumibot.entities.position import Position
from lumibot.entities.order import Order

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


@pytest.mark.parametrize(
    "symbol, qty", [("MSFT", 9), ("GM", 100), ("FB", 500), ("TSLA", 1)]
)
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


@pytest.mark.parametrize("type", [("us_equity",), ("options",), ("USD",)])
def test_map_asset_type(type):
    try:
        alpaca.map_asset_type(type) == "stock"
    except:
        if type != "us_equity":
            assert True
        else:
            assert False


vars = "symbol, qty, side, limit_price, stop_price, time_in_force, id, status,"
params = [
    ("MSFT", 10, "buy", None, None, None, "100", "new"),
    ("FB", 100, "sell", "250", "255", "day", "101", "fill"),
]


@pytest.mark.parametrize(vars, params)
def test__parse_broker_order(
    symbol,
    qty,
    side,
    limit_price,
    stop_price,
    time_in_force,
    id,
    status,
):
    class BOrder:
        pass

    border = BOrder()
    params = dict(
        symbol=symbol,
        qty=str(qty),
        side=side,
        limit_price=limit_price,
        stop_price=stop_price,
        time_in_force=time_in_force,
        id=id,
        status=status,
    )
    for k, v in params.items():
        setattr(border, k, v)

    # Expected result.
    order = Order(
        "AlpacaTest",
        Asset(symbol=symbol, asset_type="stock"),
        quantity=qty,
        side=side,
        limit_price=limit_price,
        stop_price=stop_price,
        time_in_force=time_in_force,
    )
    order.identifier = id
    order.status = status

    result = alpaca._parse_broker_order(
        border,
        "AlpacaTest",
    )
    assert result.strategy == order.strategy
    assert result.asset == order.asset
    assert result.quantity == order.quantity
    assert result.side == order.side
    assert result.limit_price == order.limit_price
    assert result.stop_price == order.stop_price
    assert result.time_in_force == order.time_in_force
    assert result.identifier == order.identifier
    assert result.status == order.status


def test__pull_source_symbol_bars(monkeypatch):
    """Dataframe results will be mocked in _pull_source_bars"""
    asset = Asset(symbol="MSFT", asset_type="stock")

    def mock_pull_source_bars(asset, length, timestep="minute", timeshift=None):
        df = "dataframe"
        return {asset[0]: df}

    monkeypatch.setattr(alpaca, "_pull_source_bars", mock_pull_source_bars)

    expected = {asset: "dataframe"}

    result = alpaca._pull_source_symbol_bars(
        asset, length=10, timestep="day", timeshift=0
    )

    assert result == expected[asset]


vars = "length, timestep, timeshift"
params = [(10, "minute", 0), (20, "minute", 1), (30, "day", 4), (40, "day", 5)]
@pytest.mark.parametrize(vars, params)
def test__pull_source_bars(length, timestep, timeshift, monkeypatch):
    symbols = ["MSFT", "FB", "GM"]
    assets = []
    for symbol in symbols:
        assets.append(Asset(symbol=symbol, asset_type="stock"))

    def mock_pull_source_bars(method_assets, length, timestep="minute", timeshift=None):
        result = {}
        for asset in method_assets:
            df = pd.DataFrame(
                [[length, timestep, timeshift]],
                columns=["length", "timestep", "timeshift"],
            )
            result[asset] = df
        return result

    monkeypatch.setattr(alpaca, "_pull_source_bars", mock_pull_source_bars)

    expected = {}
    for symbol in symbols:
        df = pd.DataFrame(
            [[length, timestep, timeshift]], columns=["length", "timestep", "timeshift"]
        )
        asset = Asset(symbol=symbol, asset_type="stock")
        expected[asset] = df

    result = alpaca._pull_source_bars(
        [Asset(symbol=symbol) for symbol in symbols],
        length,
        timestep=timestep,
        timeshift=timeshift,
    )
    for asset in assets:
        assert result[asset]['length'][0] == expected[asset]['length'][0]
        assert result[asset]['timestep'][0] == expected[asset]['timestep'][0]
        assert result[asset]['timeshift'][0] == expected[asset]['timeshift'][0]



def test_get_barset_from_api():
    pass


def test__parse_source_symbol_bars():
    pass
