import datetime
import pytest
import conftest
from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.interactive_brokers import InteractiveBrokers
import credentials

def test_print_name(name):
    print("Displaying name: %s" % name)


# # @pytest.fixture(scope='module')
# def brokers():
#     tested_brokers = ['alpaca', 'ib']
#     tested_brokers =
#     brks = dict()
#
#     # Alpaca
#     alpaca = Alpaca(credentials.AlpacaConfig)
#     alpaca_account = alpaca.api.get_account()
#     if len(alpaca_account) > 0:
#         brks['alpaca'] = alpaca
#     # Interactive Brokers (ib)
#     ib = InteractiveBrokers(credentials.InteractiveBrokersConfig)
#
#     yield brks
#     for broker, connection in brks.items():
#         if broker == 'alpaca':
#             v.api.close()
#
#
# brokers()



def test_thetest():
    assert 1 == 1

