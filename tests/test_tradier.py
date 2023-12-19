import os

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset

TRADIER_ACCOUNT_ID_PAPER = os.getenv("TRADIER_ACCOUNT_ID_PAPER")
TRADIER_TOKEN_PAPER = os.getenv("TRADIER_TOKEN_PAPER")


@pytest.fixture
def tradier_ds():
    return TradierData(TRADIER_ACCOUNT_ID_PAPER, TRADIER_TOKEN_PAPER, True)


@pytest.mark.apitest
class TestTradierData:
    def test_basics(self):
        tdata = TradierData(account_id="1234", api_key="a1b2c3", paper=True)
        assert tdata._account_id == "1234"

    def test_get_last_price(self, tradier_ds):
        asset = Asset("AAPL")
        price = tradier_ds.get_last_price(asset)
        assert isinstance(price, float)
        assert price > 0.0


@pytest.mark.apitest
class TestTradierBroker:
    def test_basics(self):
        broker = Tradier(account_id="1234", api_token="a1b2c3", paper=True)
        assert broker.name == "Tradier"
        assert broker._tradier_account_id == "1234"
