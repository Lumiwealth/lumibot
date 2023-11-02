from lumibot.brokers.ccxt import Ccxt
from lumibot.data_sources.ccxt_data import CcxtData
from lumibot.example_strategies.crypto_important_functions import ImportantFunctions

# Fake credentials, they do not need to be real
KUCOIN_LIVE = {
    "exchange_id": "kucoin",
    "password": "not_my_pass",
    "apiKey": "a1b2c3d4",
    "secret": "f6e5d4c3b2a1",
    "sandbox": True,
}


# KRAKEN_CONFIG = {
#     "exchange_id": "kraken",
#     "apiKey": "a1b2c3d4",
#     "secret": "f6e5d4c3b2a1",
#     "margin": True,
#     "sandbox": False,
# }
KRAKEN_CONFIG = {
    "exchange_id": "kraken",
    "apiKey": "K4VEtgr0dQ9E7UqkyS4/G60r+uH6fVj5mKmO3m1",
    "secret": "KWFQzQjvnXEzecSIy0+OBFaYUIrggNUfqrfn6Ksi0UAA==",
    "margin": True,
    "sandbox": False,
}


def test_initialize_ccxt_broker_legacy(mocker):
    """
    This test to make sure the legacy way of initializing the broker still works.
    """

    broker = Ccxt(KRAKEN_CONFIG)
    mocker.patch.object(broker, '_get_balances_at_broker', return_value=None)
    mocker.patch.object(broker, '_set_initial_positions')

    strategy = ImportantFunctions(
        broker=broker,
    )

    # Assert that strategy.broker is the same as broker
    assert strategy.broker == broker

    # Assert that strategy.data_source is InteractiveBrokersData object
    assert isinstance(strategy.broker.data_source, CcxtData)
