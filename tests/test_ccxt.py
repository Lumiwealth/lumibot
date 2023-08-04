from decimal import Decimal

import pytest
# from credentials import BINANCE_CONFIG  # Put back in when ready to test

from lumibot.brokers.ccxt import Ccxt
from lumibot.entities.asset import Asset
from lumibot.entities.position import Position

# Skip all the tests in this file
# pytestmark = pytest.mark.skip("all tests still WIP")
pytest.skip("all tests still WIP", allow_module_level=True)

exchange_id = "binance"
ccxt = Ccxt(BINANCE_CONFIG)


def test_get_timestamp(monkeypatch):
    def mock_clock():
        return 1639702229554235

    monkeypatch.setattr(ccxt.api, "microseconds", mock_clock)
    assert ccxt.get_timestamp() == 1639702229.554235


def test_is_market_open(monkeypatch):
    assert ccxt.is_market_open() == None


def test_get_time_to_open():
    assert ccxt.get_time_to_open() == None


def test_get_time_to_close():
    assert ccxt.get_time_to_close() == None


vars = "broker, exchangeId, expected_result, balances, markets, fetch_ticker"
params = [
    (
        "coinbase",
        "coinbasepro",
        30040000.00037,
        {
            "info": [
                {
                    "currency": "BTC",
                    "balance": "1.00000001",
                    "hold": "0",
                    "available": "1.00000001",
                },
                {
                    "currency": "ETH",
                    "balance": "9999.99999999",
                    "hold": "0",
                    "available": "9999.99999999",
                },
            ]
        },
        {
            "BTC/USD": {"precision": {"amount": 0.00000001, "price": 0.01}},
            "ETH/USD": {"precision": {"amount": 0.00000001, "price": 0.01}},
        },
        {
            "BTC/USD": {"last": 40000},
            "ETH/USD": {"last": 3000},
        },
    ),
    (
        "binance",
        "binance",
        30040000.00037,
        {
            "info": {
                "balances": [
                    {
                        "asset": "BTC",
                        "locked": "0",
                        "free": "1.00000001",
                    },
                    {
                        "asset": "ETH",
                        "locked": "0",
                        "free": "9999.99999999",
                    },
                ],
            },
        },
        {
            "BTC/USD": {"precision": {"amount": 0.00000001, "price": 0.01}},
            "ETH/USD": {"precision": {"amount": 0.00000001, "price": 0.01}},
        },
        {
            "BTC/USD": {"last": 40000},
            "ETH/USD": {"last": 3000},
        },
    ),
]


@pytest.mark.parametrize(vars, params)
def test__get_balances_at_broker(
    broker, exchangeId, expected_result, balances, markets, fetch_ticker, monkeypatch
):
    def mock_fetch_balance():
        return balances

    def mock_fetch_ticker(market):
        return fetch_ticker[market]

    monkeypatch.setattr(ccxt.api, "exchangeId", exchangeId)
    monkeypatch.setattr(ccxt.api, "fetch_balance", mock_fetch_balance)
    monkeypatch.setattr(ccxt.api, "markets", markets)
    monkeypatch.setattr(ccxt.api, "fetch_ticker", mock_fetch_ticker)

    (
        total_cash_value,
        gross_positions_value,
        net_liquidation_value,
    ) = ccxt._get_balances_at_broker()
    assert total_cash_value == 0
    assert gross_positions_value == expected_result
    assert net_liquidation_value == expected_result


vars = "broker, exchangeId, position, precision"
params = [
    (
        "coinbase",
        "coinbsepro",
        {
            "currency": "BTC",
            "balance": 1000,
            "hold": 0,
            "available": 1000,
        },
        "0.00000001",
    ),
    (
        "binance",
        "binance",
        {
            "asset": "BTC",
            "locked": 0,
            "free": 1000,
        },
        "8",
    ),
]


@pytest.mark.parametrize(vars, params)
def test__parse_broker_position(broker, exchangeId, position, precision, monkeypatch):

    monkeypatch.setattr(ccxt.api, "exchangeId", exchangeId)

    if exchangeId == "binance":
        symbol = position["asset"]
        precision = str(10 ** -Decimal(precision))
        quantity = Decimal(position["free"]) + Decimal(position["locked"])
        hold = position["locked"]
        available = position["free"]
    else:
        symbol = position["currency"]
        quantity = Decimal(position["balance"])
        hold = position["hold"]
        available = position["available"]

    expected = Position(
        "CcxtTest",
        Asset(symbol=symbol, asset_type="crypto", precision=precision),
        quantity=quantity,
        hold=hold,
        available=available,
        orders=None,
    )

    result = ccxt._parse_broker_position(position, "CcxtTest", orders=None)
    assert result.asset == expected.asset
    assert result.quantity == expected.quantity
    assert result.asset.symbol == expected.asset.symbol
    assert result.strategy == expected.strategy
    assert result.available == expected.available
    assert result.hold == expected.hold


# @pytest.mark.parametrize("type", [("us_equity",), ("options",), ("USD",)])
# def test_map_asset_type(type):
#     try:
#         alpaca.map_asset_type(type) == "stock"
#     except:
#         if type != "us_equity":
#             assert True
#         else:
#             assert False
#
#
# vars = "symbol, qty, side, limit_price, stop_price, time_in_force, id, status,"
# params = [
#     ("MSFT", 10, "buy", None, None, None, "100", "new"),
#     ("FB", 100, "sell", "250", "255", "day", "101", "fill"),
# ]
#
#
# @pytest.mark.parametrize(vars, params)
# def test__parse_broker_order(
#     symbol,
#     qty,
#     side,
#     limit_price,
#     stop_price,
#     time_in_force,
#     id,
#     status,
# ):
#     class BOrder:
#         pass
#
#     border = BOrder()
#     params = dict(
#         symbol=symbol,
#         qty=str(qty),
#         side=side,
#         limit_price=limit_price,
#         stop_price=stop_price,
#         time_in_force=time_in_force,
#         id=id,
#         status=status,
#     )
#     for k, v in params.items():
#         setattr(border, k, v)
#
#     # Expected result.
#     order = Order(
#         "AlpacaTest",
#         Asset(symbol=symbol, asset_type="stock"),
#         quantity=qty,
#         side=side,
#         limit_price=limit_price,
#         stop_price=stop_price,
#         time_in_force=time_in_force,
#     )
#     order.identifier = id
#     order.status = status
#
#     result = alpaca._parse_broker_order(
#         border,
#         "AlpacaTest",
#     )
#     assert result.strategy == order.strategy
#     assert result.asset == order.asset
#     assert result.quantity == order.quantity
#     assert result.side == order.side
#     assert result.limit_price == order.limit_price
#     assert result.stop_price == order.stop_price
#     assert result.time_in_force == order.time_in_force
#     assert result.identifier == order.identifier
#     assert result.status == order.status
#
#
# def test__pull_source_symbol_bars(monkeypatch):
#     """Dataframe results will be mocked in _pull_source_bars"""
#     asset = Asset(symbol="MSFT", asset_type="stock")
#
#     def mock_pull_source_bars(asset, length, timestep="minute", timeshift=None):
#         df = "dataframe"
#         return {asset[0]: df}
#
#     monkeypatch.setattr(alpaca, "_pull_source_bars", mock_pull_source_bars)
#
#     expected = {asset: "dataframe"}
#
#     result = alpaca._pull_source_symbol_bars(
#         asset, length=10, timestep="day", timeshift=0
#     )
#
#     assert result == expected[asset]
#
#
# vars = "length, timestep, timeshift"
# params = [(10, "minute", 0), (20, "minute", 1), (30, "day", 4), (40, "day", 5)]
# @pytest.mark.parametrize(vars, params)
# def test__pull_source_bars(length, timestep, timeshift, monkeypatch):
#     """Only returning dummy dataframe, testing get_barset_from_api separately"""
#     symbols = ["MSFT", "FB", "GM"]
#     assets = []
#     for symbol in symbols:
#         assets.append(Asset(symbol=symbol, asset_type="stock"))
#
#     def mock_get_barset_from_api(
#         method_assets, length, timestep="minute", timeshift=None
#     ):
#         result = {}
#         for asset in method_assets:
#             df = pd.DataFrame(
#                 [[length, timestep, timeshift]],
#                 columns=["length", "timestep", "timeshift"],
#             )
#             result[asset] = df
#         return result
#
#     monkeypatch.setattr(alpaca, "_pull_source_bars", mock_get_barset_from_api)
#
#     expected = {}
#     for symbol in symbols:
#         df = pd.DataFrame(
#             [[length, timestep, timeshift]], columns=["length", "timestep", "timeshift"]
#         )
#         asset = Asset(symbol=symbol, asset_type="stock")
#         expected[asset] = df
#
#     result = alpaca._pull_source_bars(
#         [Asset(symbol=symbol) for symbol in symbols],
#         length,
#         timestep=timestep,
#         timeshift=timeshift,
#     )
#     for asset in assets:
#         assert result[asset]["length"][0] == expected[asset]["length"][0]
#         assert result[asset]["timestep"][0] == expected[asset]["timestep"][0]
#         assert result[asset]["timeshift"][0] == expected[asset]["timeshift"][0]
#
#
# vars = "symbol, freq, limit, end"
# params = [
#     ("XYZ", "1D", 150, datetime.datetime(2019, 11, 1)),
#     ("XYZ", "1D", 100, datetime.datetime(2019, 9, 1)),
#     ("XYZ", "1D", 50, datetime.datetime(2019, 4, 1)),
#     ("XYZ", "1D", 5, datetime.datetime(2019, 12, 18)),
#     ("XYZ", "1Min", 500, datetime.datetime(2020, 4, 18)),
#     ("XYZ", "1Min", 1500, datetime.datetime(2020, 4, 18)),
# ]
#
#
# @pytest.mark.parametrize(vars, params)
# def test_get_barset_from_api(symbol, freq, limit, end):
#     """Testing _get_barset_from_api"""
#
#     class API:
#         barset = None
#
#         def get_barset(self, symbol, freq, limit=None, end=None):
#
#             barset = dict()
#             path = Path("data/")
#             filename = f"XYZ_{freq}.csv"
#             filepath = path / filename
#             df = pd.read_csv(filepath, index_col=0, parse_dates=True)
#             if freq == "1Min":
#                 df.index = df.index.tz_localize("US/Eastern")
#             df = df.loc[:end]
#             df = df.iloc[-limit:]
#
#             class BS:
#                 df = None
#
#             bs = BS()
#             bs.df = df
#             barset[symbol] = bs
#             return barset
#
#     api = API()
#
#     path = Path("data/")
#     filename = f"XYZ_{freq}.csv"
#     filepath = path / filename
#     expected = pd.read_csv(filepath, index_col=0, parse_dates=True)
#     if freq == "1Min":
#         expected.index = expected.index.tz_localize("US/Eastern")
#     expected = expected.loc[:end]
#     expected = expected.iloc[-limit:]
#
#     result = alpaca.get_barset_from_api(api, symbol, freq, limit, end)
#     assert result.shape[0] == limit
#     assert result.shape[1] == expected.shape[1]
#     assert result.index.name == expected.index.name
#     assert result.index.values[0] == expected.index.values[0]
#     assert result.index.values[-1] == expected.index.values[-1]
#     assert result["close"].values[0] == expected["close"].values[0]
#     assert result["close"].values[-1] == expected["close"].values[-1]
#
#
# def test__parse_source_symbol_bars():
#     asset = Asset(symbol="XYZ", asset_type="stock")
#     response = pd.DataFrame(
#         [
#             [
#                 datetime.datetime(2021, 12, 10, 11, 5, 0),
#                 175.82,
#                 175.885,
#                 175.655,
#                 175.73,
#                 4250,
#             ],
#         ],
#         columns=["time", "open", "high", "low", "close", "volume"],
#     )
#     response = response.set_index("time")
#     response["returns"] = response["close"].pct_change()
#     response_bars = Bars(response, "Alpaca", asset, raw=response)
#     result = alpaca._parse_source_symbol_bars(response, asset)
#     assert result.asset == response_bars.asset
#     assert result.source == response_bars.source
#     assert result.symbol == response_bars.symbol
#     assert result.df.shape == response_bars.df.shape
#     assert result.df.index.name == response_bars.df.index.name
#     assert result.df.index.values[0] == response_bars.df.index.values[0]
#     assert result.df["close"].values[0] == response_bars.df["close"].values[0]
