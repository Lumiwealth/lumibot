from lumibot.brokers import Tradier
from lumibot.data_sources import TradierData


class TestTradierData:
    def test_basics(self):
        tdata = TradierData(account_id="1234", api_key="a1b2c3", paper=True)
        assert tdata._account_id == "1234"


class TestTradierBroker:
    def test_basics(self):
        broker = Tradier(account_id="1234", api_token="a1b2c3", paper=True)
        assert broker.name == "Tradier"
        assert broker._tradier_account_id == "1234"
