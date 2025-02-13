from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold

from datetime import datetime, timedelta

import math

# Fake credentials, they do not need to be real
ALPACA_CONFIG = {  # Paper trading!
    # Put your own Alpaca key here:
    "API_KEY": "",
    # Put your own Alpaca secret here:
    "API_SECRET": "",
    # If you want to use real money you must change this to False
    "PAPER": True,
}


class TestAlpacaBroker:

    def test_initialize_broker_legacy(self):
        """
        This test to make sure the legacy way of initializing the broker still works.
        """
        broker = Alpaca(ALPACA_CONFIG)
        strategy = BuyAndHold(
            broker=broker,
        )

        # Assert that strategy.broker is the same as broker
        assert strategy.broker == broker

        # Assert that strategy.data_source is AlpacaData object
        assert isinstance(strategy.broker.data_source, AlpacaData)

    def test_submit_order_calls_conform_order(self):
        broker = Alpaca(ALPACA_CONFIG)
        broker._conform_order = MagicMock()
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc')
        broker.submit_order(order=order)
        broker._conform_order.assert_called_once()

    def test_limit_order_conforms_when_limit_price_gte_one_dollar(self):
        broker = Alpaca(ALPACA_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", limit_price=1.123455, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 1.12

    def test_limit_order_conforms_when_limit_price_lte_one_dollar(self):
        broker = Alpaca(ALPACA_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", limit_price=0.12345, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 0.1235

    def test_mulitleg_options_order(self):
        broker = Alpaca(ALPACA_CONFIG)
        porders = broker._pull_all_orders('test', None)

        spy_price = broker.get_last_price(asset='SPY')
        casset = Asset('SPY', Asset.AssetType.OPTION, expiration=datetime.now(), strike=math.floor(spy_price), right='CALL')
        passet = Asset('SPY', Asset.AssetType.OPTION, expiration=datetime.now(), strike=math.floor(spy_price), right='PUT')
        orders = [Order(strategy='test', asset=passet, quantity=1, limit_price=0.01, side="buy"), Order(strategy='test', asset=casset, quantity=1, side="buy")]
        broker.submit_orders(orders)

        orders = broker._pull_all_orders('test', None)
        oids = {o.identifier for o in porders}
        orders = [o for o in orders if o.identifier not in oids]
        assert len(orders) == 2
        assert len([o.asset.right == 'CALL' and o.asset.symbol == 'SPY' for o in orders]) == 1
        assert len([o.asset.right == 'PUT' and o.asset.symbol == 'SPY' for o in orders]) == 1

    def test_option_order(self):
        broker = Alpaca(ALPACA_CONFIG)
        porders = broker._pull_all_orders('test', None)

        spy_price = broker.get_last_price(asset='SPY')
        casset = Asset('SPY', Asset.AssetType.OPTION, expiration=datetime.now(), strike=math.floor(spy_price), right='CALL')
        broker.submit_order(Order('test', asset=casset))

        orders = broker._pull_all_orders('test', None)
        oids = {o.identifier for o in porders}
        orders = [o for o in orders if o.identifier not in oids]
        assert len(orders) == 1
        assert len([o.asset.right == 'CALL' and o.asset.symbol == 'SPY' for o in orders]) == 1

    def test_option_get_last_price(self):
        broker = Alpaca(ALPACA_CONFIG)
        dte = datetime.now()
        spy_price = broker.get_last_price(asset='SPY')
        price = broker.get_last_price(asset=Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL'))
        assert price != 0

    def test_stock_get_last_price(self):
        broker = Alpaca(ALPACA_CONFIG)
        price = broker.get_last_price(asset='SPY')
        assert price != 0
        price = broker.get_last_price(asset=Asset(symbol='SPY'))
        assert price != 0

    def test_crypto_get_last_price(self):
        broker = Alpaca(ALPACA_CONFIG)
        base = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
        price = broker.get_last_price(asset=base, quote=quote)
        assert price != 0

    def test_option_get_historical_prices(self):
        broker = Alpaca(ALPACA_CONFIG)
        dte = datetime.now() - timedelta(days=2)
        spy_price = broker.get_last_price(asset='SPY')
        asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        bars = broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0