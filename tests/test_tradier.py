import os

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order

TRADIER_ACCOUNT_ID_PAPER = os.getenv("TRADIER_ACCOUNT_ID_PAPER")
TRADIER_TOKEN_PAPER = os.getenv("TRADIER_TOKEN_PAPER")


@pytest.fixture
def tradier_ds():
    return TradierData(account_number=TRADIER_ACCOUNT_ID_PAPER, access_token=TRADIER_TOKEN_PAPER, paper=True)


@pytest.fixture
def tradier():
    return Tradier(account_number=TRADIER_ACCOUNT_ID_PAPER, access_token=TRADIER_TOKEN_PAPER, paper=True)

@pytest.mark.apitest
class TestTradierData:
    def test_basics(self):
        tdata = TradierData(account_number="1234", access_token="a1b2c3", paper=True)
        assert tdata._account_number == "1234"

    def test_get_last_price(self, tradier_ds):
        asset = Asset("AAPL")
        price = tradier_ds.get_last_price(asset)
        assert isinstance(price, float)
        assert price > 0.0


@pytest.mark.apitest
class TestTradierBroker:
    def test_basics(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        assert broker.name == "Tradier"
        assert broker._tradier_account_number == "1234"

    def test_get_last_price(self, tradier):
        asset = Asset("AAPL")
        price = tradier.get_last_price(asset)
        assert isinstance(price, float)
        assert price > 0.0

    def test_submit_order(self, tradier):
        asset = Asset("AAPL")
        order = Order('strat_unittest', asset, 1, 'buy', type='market')
        submitted_order = tradier._submit_order(order)
        assert submitted_order.status == "submitted"
        assert submitted_order.identifier > 0

        # Cancel the testing order once we are done
        # How do we check this? Who changes the Lumibot order status to "canceled"?
        tradier.cancel_order(submitted_order)
