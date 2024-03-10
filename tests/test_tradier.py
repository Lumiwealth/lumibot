import datetime as dt
import os
from time import sleep

import pandas as pd
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

    def test_get_chains(self, tradier_ds):
        asset = Asset("SPY")
        chain = tradier_ds.get_chains(asset)
        assert isinstance(chain, dict)
        assert 'Chains' in chain
        assert "CALL" in chain['Chains']
        assert len(chain['Chains']['CALL']) > 0
        expir_date = list(chain['Chains']['CALL'].keys())[0]
        assert len(chain['Chains']['CALL'][expir_date]) > 0
        strike = chain['Chains']['CALL'][expir_date][0]
        assert strike > 0
        assert chain['Multiplier'] == 100

    def test_query_greeks(self, tradier_ds):
        asset = Asset("SPY")
        chains = tradier_ds.get_chains(asset)
        expir_date = list(chains['Chains']['CALL'].keys())[0]
        num_strikes = len(chains['Chains']['CALL'][expir_date])
        strike = chains['Chains']['CALL'][expir_date][num_strikes // 2]  # Get a strike price in the middle
        option_asset = Asset(asset.symbol, asset_type='option', expiration=expir_date, strike=strike, right='CALL')
        greeks = tradier_ds.query_greeks(option_asset)
        assert greeks
        assert 'delta' in greeks
        assert 'gamma' in greeks
        assert greeks['delta'] > 0


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
        mock_pull_positions = mocker.patch.object(broker, 'get_tracked_position', return_value=None)
        strategy = "strat_unittest"
        stock_asset = Asset("SPY")
        option_asset = Asset("SPY", asset_type='option')
        stock_order = Order(strategy, stock_asset, 1, 'buy', type='market')
        option_order = Order(strategy, option_asset, 1, 'buy', type='market')

        # No Positions exist
        assert broker._lumi_side2tradier(stock_order) == "buy"
        stock_order.side = "sell"
        assert broker._lumi_side2tradier(stock_order) == "sell"

        assert broker._lumi_side2tradier(option_order) == "buy_to_open"
        option_order.side = "sell"
        assert broker._lumi_side2tradier(option_order) == "sell_to_open"
        option_order.side = "blah"
        assert not broker._lumi_side2tradier(option_order)

        # Stoploss always submits as a "to_close" order
        stop_stock_order = Order(strategy, stock_asset, 1, 'sell', type='stop', stop_price=100.0)
        assert broker._lumi_side2tradier(stop_stock_order) == "sell"
        stop_option_order = Order(strategy, option_asset, 1, 'sell', type='stop', stop_price=100.0)
        assert broker._lumi_side2tradier(stop_option_order) == "sell_to_close"
        limit_option_order = Order(strategy, option_asset, 1, 'sell', type='limit', limit_price=100.0)
        assert broker._lumi_side2tradier(limit_option_order) == "sell_to_close"

        # Positions exist
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

    def test_parse_broker_order(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        strategy = "strat_unittest"
        stock_symbol = "SPY"
        option_symbol = "SPY200101C00150000"

        response = {
            "id": 123,
            "type": "market",
            "side": "buy",
            "symbol": stock_symbol,
            "quantity": 1,
            "status": "submitted",
            "tag": strategy,
            "duration": "day",
            "create_date": dt.datetime.today(),
        }
        stock_order = broker._parse_broker_order(response, "")
        assert stock_order.identifier == 123
        assert stock_order.strategy == strategy
        assert stock_order.asset.symbol == stock_symbol
        assert stock_order.quantity == 1
        assert stock_order.side == "buy"
        assert stock_order.status == "submitted"
        assert stock_order.type == "market"

        # Test with an option and stop order
        response = {
            "id": 123,
            "type": "stop",
            "side": "sell",
            "symbol": option_symbol,
            "quantity": 1,
            "status": "submitted",
            "tag": strategy,
            "duration": "gtc",
            "create_date": dt.datetime.today(),
            "stop_price": 1.0,
        }
        option_order = broker._parse_broker_order(response, "lumi_strat")
        assert option_order.identifier == 123
        assert option_order.strategy == "lumi_strat"
        assert option_order.asset.symbol == "SPY"
        assert option_order.asset.asset_type == "option"
        assert option_order.asset.expiration == dt.date(2020, 1, 1)
        assert option_order.asset.strike == 150
        assert option_order.asset.right == "CALL"
        assert option_order.quantity == 1
        assert option_order.side == "sell"
        assert option_order.status == "submitted"
        assert option_order.type == "stop"
        assert option_order.stop_price == 1.0

    def test_do_polling(self, mocker):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True, polling_interval=None)
        strategy = "strat_unittest"
        mock_get_orders = mocker.patch.object(broker, '_pull_broker_all_orders', return_value=[])
        submit_response = {'id': 123, 'status': 'ok'}
        mock_submit_order = mocker.patch.object(broker.tradier.orders, 'order', return_value=submit_response)
        mocker.patch.object(broker, 'sync_positions', return_value=None)

        # Test polling with no orders
        broker.do_polling()
        known_orders = broker.get_all_orders()
        tracked_orders = broker.get_tracked_orders()
        assert not known_orders
        assert not tracked_orders

        # Submit an order to test polling with
        stock_asset = Asset("SPY")
        stock_qty = 10
        order = Order(strategy, stock_asset, stock_qty, 'buy', type='market')
        sub_order = broker._submit_order(order)
        assert sub_order.status == "submitted"
        assert sub_order.identifier == 123
        assert sub_order.was_transmitted()
        assert broker.get_all_orders()
        assert broker.get_tracked_orders()
        assert broker._unprocessed_orders
        assert not broker._new_orders

        # Test polling with the order
        first_response = {
            "id": 123,
            "type": "market",
            "side": "buy",
            "symbol": "SPY",
            "quantity": stock_qty,
            "status": "pending",
            "duration": "day",
            "create_date": dt.datetime.today(),
            "avg_fill_price": 0.0,
            "exec_quantity": 0,
            "tag": strategy,
        }
        mock_get_orders.return_value = [first_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert known_orders
        assert not len(broker._unprocessed_orders)
        assert len(broker._new_orders) == 1
        order1 = known_orders[0]
        assert order1.identifier == 123
        assert order1.status in ["new", "open"]
        assert not order1.get_fill_price()

        # Poll again, nothing changes
        broker.do_polling()
        known_orders = broker.get_tracked_orders(strategy=strategy)
        order1 = known_orders[0]
        assert len(known_orders) == 1, "No duplicate orders were created."
        assert not len(broker._unprocessed_orders)
        assert len(broker._new_orders) == 1
        assert order1.status == "open"

        # Add a 2nd order: stop loss
        mock_submit_order.return_value = {'id': 124, 'status': 'ok'}
        stop_order = Order(strategy, stock_asset, 1, 'sell', type='stop', stop_price=100.0)
        sub_order = broker._submit_order(stop_order)
        assert sub_order.status == "submitted"
        assert sub_order.identifier == 124
        assert sub_order.was_transmitted()
        assert len(broker._unprocessed_orders) == 1
        assert len(broker._new_orders) == 1
        assert len(broker.get_all_orders()) == 2
        assert len(broker.get_tracked_orders()) == 2

        second_response = {
            "id": 124,
            "type": "stop",
            "side": "sell",
            "symbol": "SPY",
            "quantity": stock_qty,
            "status": "open",
            "duration": "gtc",
            "create_date": dt.datetime.today(),
            "stop_price": 100.0,
            "avg_fill_price": 0.0,
            "exec_quantity": 0,
            "tag": strategy,
        }
        mock_get_orders.return_value = [first_response, second_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders)
        assert not len(broker._unprocessed_orders)
        assert len(broker._new_orders) == 2
        assert len(known_orders) == 2
        order2 = known_orders[1]
        assert order2.identifier == 124
        assert order2.status in ["new", "open"]
        assert not order2.get_fill_price()

        # Fill the first order (market)
        first_response["status"] = "filled"
        first_response["avg_fill_price"] = 101.0
        first_response["exec_quantity"] = stock_qty
        mock_get_orders.return_value = [first_response, second_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        filled_orders = broker._filled_orders
        assert len(known_orders) == 1, "Tracked does not include filled orders."
        assert len(filled_orders) == 1
        order1 = filled_orders[0]
        assert order1.identifier == 123
        assert order1.type == "market"
        assert order1.is_filled()
        assert order1.get_fill_price() == 101.0
        assert len(broker._new_orders) == 1
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 2, "Includes Filled orders"

        # Cancel the 2nd order (stoploss)
        second_response["status"] = "canceled"
        mock_get_orders.return_value = [first_response, second_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 0, "Canceled orders no longer tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 2, "Includes Filled/Cancelled orders"
        assert len(broker._canceled_orders) == 1
        order2 = broker._canceled_orders[0]
        assert order2.identifier == 124
        assert order2.type == "stop"
        assert order2.is_canceled()
        assert not order2.get_fill_price()

        # Poll again, nothing changes
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 0, "Canceled orders no longer tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 2, "Includes Filled/Cancelled orders"
        assert len(broker._canceled_orders) == 1

        # 3rd Order: Submit an order that causes a broker error
        mock_submit_order.return_value = {'id': 125, 'status': 'ok'}
        stop_order = Order(strategy, stock_asset, stock_qty, 'sell', type='limit', limit_price=1000.0)
        sub_order = broker._submit_order(stop_order)
        assert sub_order.status == "submitted"
        assert sub_order.identifier == 125
        assert sub_order.was_transmitted()
        assert len(broker._unprocessed_orders) == 1
        assert len(broker._new_orders) == 0
        assert len(broker.get_all_orders()) == 3, "Includes Filled/Cancelled orders"
        assert len(broker.get_tracked_orders()) == 1

        third_response = {
            "id": 125,
            "type": "limit",
            "side": "sell",
            "symbol": "SPY",
            "quantity": stock_qty,
            "status": "rejected",
            "reason_description": "API Error Msg",
            "duration": "gtc",
            "create_date": dt.datetime.today(),
            "avg_fill_price": 0.0,
            "exec_quantity": 0,
            "tag": strategy,
        }
        mock_get_orders.return_value = [first_response, second_response, third_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 0, "Rejected orders no longer tracked. They get canceled and set with error msg."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker._canceled_orders) == 2
        assert len(broker.get_all_orders()) == 3, "Includes Filled/Cancelled orders"
        error_order = broker._canceled_orders[1]
        assert error_order.identifier == 125
        assert error_order.type == "limit"
        assert error_order.status == "error"
        assert not error_order.get_fill_price()
        assert error_order.error_message == "API Error Msg"

        # There is a Lumibot order that is not tracked by the broker, so it should be canceled.
        mock_submit_order.return_value = {'id': 126, 'status': 'ok'}
        drop_order = Order(strategy, stock_asset, stock_qty, 'sell', type='market')
        broker._submit_order(drop_order)
        assert len(broker._unprocessed_orders) == 1
        assert len(broker._new_orders) == 0
        assert len(broker.get_all_orders()) == 4, "Includes Filled/Cancelled orders"

        # Poll, but broker returns no info on the Lumibot order so it should be canceled.
        mock_get_orders.return_value = [first_response, second_response, third_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 0
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker._canceled_orders) == 3
        order = broker._canceled_orders[2]
        assert order.identifier == 126
        assert order.is_canceled()

        # Broker returns an order that Lumibot doesn't know about, so it should be added to Lumibot.
        fourth_response = {
            "id": 127,
            "type": "market",
            "side": "buy",
            "symbol": "SPY",
            "quantity": stock_qty,
            "status": "filled",
            "duration": "day",
            "create_date": dt.datetime.today(),
            "avg_fill_price": 102.0,
            "exec_quantity": stock_qty,
            "tag": strategy,
        }
        mock_get_orders.return_value = [first_response, second_response, third_response, fourth_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 1, "New orders are tracked."
        assert len(broker._new_orders) == 1
        assert not len(broker._unprocessed_orders)
        order = known_orders[0]
        assert order.identifier == 127
        assert order.status == "new"
        assert not order.get_fill_price()

        # Poll again, order fill will now be processed
        mock_get_orders.return_value = [first_response, second_response, third_response, fourth_response]
        broker.do_polling()
        sleep(0.25)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert not len(known_orders), "Filled orders no longer tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 5, "Includes Filled/Cancelled orders and order not in Broker info"
        order = broker.get_order(127)
        assert order.identifier == 127
        assert order.is_filled()
        assert order.get_fill_price() == 102.0
