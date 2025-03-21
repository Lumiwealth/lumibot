import pytest
from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.credentials import ALPACA_TEST_CONFIG

from datetime import datetime, timedelta

import math

from lumibot.tools import get_trading_days

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


class TestAlpacaBroker:

    def test_initialize_broker_legacy(self):
        """
        This test to make sure the legacy way of initializing the broker still works.
        """
        broker = Alpaca(ALPACA_TEST_CONFIG)
        strategy = BuyAndHold(
            broker=broker,
        )

        # Assert that strategy.broker is the same as broker
        assert strategy.broker == broker

        # Assert that strategy.data_source is AlpacaData object
        assert isinstance(strategy.broker.data_source, AlpacaData)

    def test_submit_order_calls_conform_order(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        broker._conform_order = MagicMock()
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, strategy='abc')
        broker.submit_order(order=order)
        broker._conform_order.assert_called_once()

    def test_limit_order_conforms_when_limit_price_gte_one_dollar(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, limit_price=1.123455, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 1.12

    def test_limit_order_conforms_when_limit_price_lte_one_dollar(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, limit_price=0.12345, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 0.1235

    # The tests below exist to make sure the BROKER calls pass through the data source correctly.
    # Testing that the DATA is CORRECT (vs just existing) happens in test_alpaca_data.

    @pytest.mark.skip(reason="This test is doesn't work.")
    def test_option_get_last_price(self):
        broker = Alpaca(
            ALPACA_TEST_CONFIG,
        )
        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]
        spy_price = broker.get_last_price(asset=Asset('SPY'))
        price = broker.get_last_price(asset=Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL'))
        assert price != 0

    def test_stock_get_last_price(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        price = broker.get_last_price(asset=Asset('SPY'))
        assert price != 0
        price = broker.get_last_price(asset=Asset(symbol='SPY'))
        assert price != 0

    def test_crypto_get_last_price(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        base = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
        price = broker.get_last_price(asset=base, quote=quote)
        assert price != 0

    def test_get_historical_prices(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        asset = Asset('SPY', Asset.AssetType.STOCK)
        bars = broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0

    def test_option_get_historical_prices(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)

        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]
        
        spy_price = broker.get_last_price(asset=Asset('SPY'))
        asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        print(asset)
        bars = broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0
