import os
import pytest
from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold

from datetime import datetime, timedelta

import math
import unittest


# Fake credentials, they do not need to be real
ALPACA_CONFIG = {  # Paper trading!
    # Put your own Alpaca key here:
    "API_KEY": os.getenv("ALPACA_API_KEY_PAPER", None),
    # Put your own Alpaca secret here:
    "API_SECRET": os.getenv("ALPACA_API_SECRET_PAPER", None),
    # If you want to use real money you must change this to False
    "PAPER": True,
}

def next_friday(date):
    days_until_friday = (4 - date.weekday() + 7) % 7
    days_until_friday = 7 if days_until_friday == 0 else days_until_friday

    return date + timedelta(days=days_until_friday+7)

@pytest.mark.skipif(
        not ALPACA_CONFIG['API_KEY'] or not ALPACA_CONFIG['API_SECRET'],
        reason="This test requires an alpaca API key"
    )
@pytest.mark.skipif(
    ALPACA_CONFIG['API_KEY'] == '<your key here>',
    reason="This test requires an alpaca API key"
)
class TestAlpacaBroker(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.broker = Alpaca(ALPACA_CONFIG)

    def test_initialize_broker_legacy(self):
        """
        This test to make sure the legacy way of initializing the broker still works.
        """
        strategy = BuyAndHold(
            broker=self.broker,
        )

        # Assert that strategy.broker is the same as broker
        assert strategy.broker == self.broker

        # Assert that strategy.data_source is AlpacaData object
        assert isinstance(strategy.broker.data_source, AlpacaData)

    def test_submit_order_calls_conform_order(self):
        self.broker._conform_order = MagicMock()
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc')
        self.broker.submit_order(order=order)
        self.broker._conform_order.assert_called_once()

    def test_limit_order_conforms_when_limit_price_gte_one_dollar(self):
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", limit_price=1.123455, strategy='abc')
        self.broker._conform_order(order)
        assert order.limit_price == 1.12

    def test_limit_order_conforms_when_limit_price_lte_one_dollar(self):
        order = Order(asset=Asset("SPY"), quantity=10, side="buy", limit_price=0.12345, strategy='abc')
        self.broker._conform_order(order)
        assert order.limit_price == 0.1235

    def test_mulitleg_options_order_spread(self):
        porders = self.broker._pull_all_orders('test', None)

        spy_price = self.broker.get_last_price(asset='SPY')
        dte = next_friday(datetime.now())
        basset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        sasset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price) + 2, right='CALL')
        orders = [
            Order(strategy='test', asset=basset, quantity=1, type='limit', limit_price=0.01, side="buy"), 
            Order(strategy='test', asset=sasset, quantity=1, side="sell", limit_price=0.01, type='limit')
        ]
        self.broker.submit_orders(orders, is_multileg=True)

        orders = self.broker._pull_all_orders('test', None)
        oids = {o.identifier for o in porders}
        orders = [o for o in orders if o.identifier not in oids]
        assert len(orders) == 1
        o = orders[0]
        assert sum([o.asset.right == 'CALL' and o.asset.symbol == 'SPY' for o in o.child_orders]) == 2

    def test_mulitleg_options_order_condor(self):
        porders = self.broker._pull_all_orders('test', None)

        spy_price = self.broker.get_last_price(asset='SPY')
        dte = next_friday(datetime.now())
        basset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        sasset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price) - 3, right='CALL')
        b1asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='PUT')
        s1asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price) + 3, right='PUT')
        orders = [
            Order(strategy='test', asset=basset, quantity=1, type='limit', limit_price=0.01, side="buy"), 
            Order(strategy='test', asset=sasset, quantity=1, side="sell", limit_price=0.01, type='limit'),
            Order(strategy='test', asset=b1asset, quantity=1, side="buy", limit_price=0.01, type='limit'),
            Order(strategy='test', asset=s1asset, quantity=1, side="sell", limit_price=0.01, type='limit')
        ]
        self.broker.submit_orders(orders, is_multileg=True)

        orders = self.broker._pull_all_orders('test', None)
        oids = {o.identifier for o in porders}
        orders = [o for o in orders if o.identifier not in oids]
        assert len(orders) == 1
        o = orders[0]
        assert sum([o.asset.right == 'CALL' and o.asset.symbol == 'SPY' for o in o.child_orders]) == 2
        assert sum([o.asset.right == 'PUT' and o.asset.symbol == 'SPY' for o in o.child_orders]) == 2

    def test_option_order(self):
        porders = self.broker._pull_all_orders('test', None)

        spy_price = self.broker.get_last_price(asset='QQQ')
        dte = next_friday(datetime.now())
        casset = Asset('QQQ', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        self.broker.submit_order(Order('test', asset=casset, quantity=1, side="buy", limit_price=0.01, type='limit'))

        orders = self.broker._pull_all_orders('test', None)
        oids = {o.identifier for o in porders}
        orders = [o for o in orders if o.identifier not in oids]
        assert len(orders) == 1
        assert len([o.asset.right == 'CALL' and o.asset.symbol == 'SPY' for o in orders]) == 1

    def test_option_get_last_price(self):
        dte = next_friday(datetime.now())
        spy_price = self.broker.get_last_price(asset='SPY')
        price = self.broker.get_last_price(asset=Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL'))
        assert price != 0

    def test_stock_get_last_price(self):
        price = self.broker.get_last_price(asset='SPY')
        assert price != 0
        price = self.broker.get_last_price(asset=Asset(symbol='SPY'))
        assert price != 0

    def test_crypto_get_last_price(self):
        base = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
        price = self.broker.get_last_price(asset=base, quote=quote)
        assert price != 0

    def test_option_get_historical_prices(self):
        dte = next_friday(datetime.now())
        spy_price = self.broker.get_last_price(asset='SPY')
        asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        bars = self.broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0
