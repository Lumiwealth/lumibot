from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold

# Fake credentials, they do not need to be real
ALPACA_CONFIG = {  # Paper trading!
    # Put your own Alpaca key here:
    "API_KEY": "PKP00CIO3VSDTZIT1HSV",
    # Put your own Alpaca secret here:
    "API_SECRET": "sKdQGSgtOxdARoNwkELaZqgvhGaxlPtBq82t5MhR",
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
