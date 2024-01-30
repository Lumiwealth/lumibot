import os

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order, Position

TRADIER_ACCOUNT_ID_PAPER = os.getenv("TRADIER_ACCOUNT_ID_PAPER")
TRADIER_TOKEN_PAPER = os.getenv("TRADIER_TOKEN_PAPER")


@pytest.fixture
def tradier_ds():
    return TradierData(account_number=TRADIER_ACCOUNT_ID_PAPER, access_token=TRADIER_TOKEN_PAPER, paper=True)


@pytest.fixture
def tradier():
    return Tradier(account_number=TRADIER_ACCOUNT_ID_PAPER, access_token=TRADIER_TOKEN_PAPER, paper=True)


@pytest.mark.apitest
class TestTradierDataAPI:
    """
    API Tests skipped by default. To run all API tests, use the following command:
    python -m pytest -m apitest
    """
    def test_basics(self):
        tdata = TradierData(account_number="1234", access_token="a1b2c3", paper=True)
        assert tdata._account_number == "1234"

    def test_get_last_price(self, tradier_ds):
        asset = Asset("AAPL")
        price = tradier_ds.get_last_price(asset)
        assert isinstance(price, float)
        assert price > 0.0


@pytest.mark.apitest
class TestTradierBrokerAPI:
    """
    API Tests skipped by default. To run all API tests, use the following command:
    python -m pytest -m apitest
    """
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


class TestTradierBroker:
    """
    Unit tests for the Tradier broker. These tests do not require any API calls.
    """
    def test_basics(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        assert broker.name == "Tradier"
        assert broker._tradier_account_number == "1234"

    def test_tradier_side2lumi(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        assert broker._tradier_side2lumi("buy") == "buy"
        assert broker._tradier_side2lumi("sell") == "sell"
        assert broker._tradier_side2lumi("buy_to_cover") == "buy"
        assert broker._tradier_side2lumi("sell_short") == "sell"

        with pytest.raises(ValueError):
            broker._tradier_side2lumi("blah")

    def test_lumi_side2tradier(self, mocker):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        mock_pull_positions = mocker.patch.object(broker, '_pull_position', return_value=None)
        strategy = "strat_unittest"
        stock_asset = Asset("SPY")
        option_asset = Asset("SPY", asset_type='option')
        stock_order = Order(strategy, stock_asset, 1, 'buy', type='market')
        option_order = Order(strategy, option_asset, 1, 'buy', type='market')

        assert broker._lumi_side2tradier(stock_order) == "buy"
        stock_order.side = "sell"
        assert broker._lumi_side2tradier(stock_order) == "sell"

        assert broker._lumi_side2tradier(option_order) == "buy_to_open"
        option_order.side = "sell"
        assert broker._lumi_side2tradier(option_order) == "sell_to_open"
        option_order.side = "blah"
        assert not broker._lumi_side2tradier(option_order)

        mock_pull_positions.return_value = Position(strategy=strategy, asset=option_asset, quantity=1)
        option_order.side = "buy"
        assert broker._lumi_side2tradier(option_order) == "buy_to_open"
        option_order.side = "sell"
        assert broker._lumi_side2tradier(option_order) == "sell_to_close"

        mock_pull_positions.return_value = Position(strategy=strategy, asset=option_asset, quantity=-1)
        option_order.side = "buy"
        assert broker._lumi_side2tradier(option_order) == "buy_to_close"
        option_order.side = "sell"
        assert broker._lumi_side2tradier(option_order) == "sell_to_open"
        option_order.side = "blah"
        assert not broker._lumi_side2tradier(option_order)

        # Sanity check case where we have an empty position
        mock_pull_positions.return_value = Position(strategy=strategy, asset=option_asset, quantity=0)
        option_order.side = "buy"
        assert broker._lumi_side2tradier(option_order) == "buy_to_open"
