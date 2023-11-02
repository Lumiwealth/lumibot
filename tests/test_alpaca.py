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


def test_initialize_broker_legacy():
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
