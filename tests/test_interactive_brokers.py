from lumibot.brokers.interactive_brokers import InteractiveBrokers
from lumibot.data_sources.interactive_brokers_data import InteractiveBrokersData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold

# Fake credentials, they do not need to be real
IBKR_CONFIG = {  # Paper trading!
    # Put your own IBKR TraderWorkstation IP here:
    "IP": "localhost:7497",
    # Put your own IBKR secret here:
    "CLIENT_ID": "a1b2c3d4",
    "SOCKET_PORT": 794,
}


def test_initialize_interactive_broker_legacy():
    """
    This test to make sure the legacy way of initializing the broker still works.
    """
    broker = InteractiveBrokers(IBKR_CONFIG)
    strategy = BuyAndHold(
        broker=broker,
    )

    # Assert that strategy.broker is the same as broker
    assert strategy.broker == broker

    # Assert that strategy.data_source is InteractiveBrokersData object
    assert isinstance(strategy.broker.data_source, InteractiveBrokersData)
