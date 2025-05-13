import datetime as dt
import os
from time import sleep

import numpy as np
import pandas as pd
import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.data_sources.tradier_data import TradierData
from lumibot.entities import Asset, Order, Position
from lumibot.credentials import TRADIER_TEST_CONFIG


def pytest_collection_modifyitems(config, items):
    for item in items:
        if 'API' in item.cls.__name__:
            if not TRADIER_TEST_CONFIG['ACCESS_TOKEN'] or TRADIER_TEST_CONFIG['ACCESS_TOKEN'] == '<your key here>':
                item.add_marker(pytest.mark.skip(reason="These tests require a Tradier API key"))


@pytest.fixture
def tradier():
    return Tradier(
        account_number=TRADIER_TEST_CONFIG['ACCOUNT_NUMBER'],
        access_token=TRADIER_TEST_CONFIG['ACCESS_TOKEN'],
        paper=True
    )


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
        order = Order('strat_unittest', asset, 1, 'buy', order_type='market')
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

    def test_modify_order(self, mocker):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        mock_modify = mocker.patch.object(broker.tradier.orders, "modify")

        stock_asset = Asset("SPY")
        order = Order("my_strat", stock_asset, 10, Order.OrderSide.SELL, order_type="limit")

        # Errors if no ID exists for this order
        order.identifier = None
        with pytest.raises(ValueError):
            broker._modify_order(order, limit_price=100.0)
        assert not mock_modify.called

        # Modify sent to API
        order.identifier = "123456"
        broker._modify_order(order, limit_price=100.0)
        assert mock_modify.called

        # Filled or cancelled orders are not touched
        mock_modify.reset_mock()
        order.status = order.OrderStatus.FILLED
        broker._modify_order(order, limit_price=100.0)
        assert not mock_modify.called

    def test_tradier_side2lumi(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        assert broker._tradier_side2lumi("buy") == Order.OrderSide.BUY
        assert broker._tradier_side2lumi("sell") == Order.OrderSide.SELL
        assert broker._tradier_side2lumi("buy_to_open") == Order.OrderSide.BUY_TO_OPEN
        assert broker._tradier_side2lumi("sell_to_open") == Order.OrderSide.SELL_TO_OPEN
        assert broker._tradier_side2lumi("buy_to_close") == Order.OrderSide.BUY_TO_CLOSE
        assert broker._tradier_side2lumi("sell_to_close") == Order.OrderSide.SELL_TO_CLOSE
        assert broker._tradier_side2lumi("buy_to_cover") == Order.OrderSide.BUY_TO_COVER
        assert broker._tradier_side2lumi("sell_short") == Order.OrderSide.SELL_SHORT
        assert broker._tradier_side2lumi("buy_something_something") == Order.OrderSide.BUY
        assert broker._tradier_side2lumi("sell_something_something") == Order.OrderSide.SELL

        with pytest.raises(ValueError):
            broker._tradier_side2lumi("blah")

    def test_lumi_side2tradier(self, mocker):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        mock_pull_positions = mocker.patch.object(broker, 'get_tracked_position', return_value=None)
        strategy = "strat_unittest"
        stock_asset = Asset("SPY")
        option_asset = Asset("SPY", asset_type='option')
        stock_order = Order(strategy, stock_asset, 1, 'buy', order_type='market')
        option_order = Order(strategy, option_asset, 1, 'buy', order_type='market')

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
        stop_stock_order = Order(strategy, stock_asset, 1, 'sell', order_type='stop', stop_price=100.0)
        assert broker._lumi_side2tradier(stop_stock_order) == "sell"
        stop_option_order = Order(strategy, option_asset, 1, 'sell', order_type='stop', stop_price=100.0)
        assert broker._lumi_side2tradier(stop_option_order) == "sell_to_close"

        # TODO: Fix this test, it's commented out temporarily until we can figure out how to handle this case.
        # limit_option_order = Order(strategy, option_asset, 1, 'sell', order_type='limit', limit_price=100.0)
        # assert broker._lumi_side2tradier(limit_option_order) == "sell_to_close"

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

    def test_pull_broker_all_orders(self, mocker):
        """
        Test the _pull_broker_all_orders function by mocking the get_orders() call.
        """
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        mock_get_orders = mocker.patch.object(broker.tradier.orders, 'get_orders', return_value=pd.DataFrame([
            {"id": 1, "symbol": "AAPL", "quantity": 10, "status": "filled", "side": "buy", "type": "market"},
            {"id": 2, "symbol": "GOOGL", "quantity": 5, "status": "open", "side": "sell", "type": "market"},
            {"id": 3, "symbol": "MSFT", "quantity": 15, "status": "open", "side": "buy",
                "type": "limit", "price": 250.00003},
            {"id": 4, "symbol": "TSLA", "quantity": 20, "status": "open", "side": "sell",
                "type": "stop", "stop_price": 600.0091}
        ]))

        orders = broker._pull_broker_all_orders()
        assert len(orders) == 4
        assert orders[0]["id"] == 1
        assert orders[0]["symbol"] == "AAPL"
        assert orders[0]["quantity"] == 10
        assert orders[0]["status"] == "filled"
        assert orders[0]["side"] == "buy"
        assert orders[0]["type"] == "market"
        assert orders[0]["price"] is None
        assert orders[0]["stop_price"] is None

        assert orders[1]["id"] == 2
        assert orders[1]["symbol"] == "GOOGL"
        assert orders[1]["quantity"] == 5
        assert orders[1]["status"] == "open"
        assert orders[1]["side"] == "sell"
        assert orders[1]["type"] == "market"
        assert orders[1]["price"] is None
        assert orders[1]["stop_price"] is None

        assert orders[2]["id"] == 3
        assert orders[2]["symbol"] == "MSFT"
        assert orders[2]["quantity"] == 15
        assert orders[2]["status"] == "open"
        assert orders[2]["side"] == "buy"
        assert orders[2]["type"] == "limit"
        assert orders[2]["price"] == 250.0
        assert orders[2]["stop_price"] is None

        assert orders[3]["id"] == 4
        assert orders[3]["symbol"] == "TSLA"
        assert orders[3]["quantity"] == 20
        assert orders[3]["status"] == "open"
        assert orders[3]["side"] == "sell"
        assert orders[3]["type"] == "stop"
        assert orders[3]["stop_price"] == 600.01
        assert orders[3]["price"] is None

        mock_get_orders.assert_called_once()

    def test_parse_broker_order(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        strategy = "strat_unittest"
        tag = "my_tag"
        stock_symbol = "SPY"
        option_symbol = "SPY200101C00150000"

        response = {
            "id": 123,
            "type": "market",
            "side": "buy",
            "symbol": stock_symbol,
            "class": "equity",
            "quantity": 1,
            "status": "submitted",
            "tag": tag,
            "duration": "day",
            "create_date": dt.datetime.today(),
        }
        stock_order = broker._parse_broker_order(response, strategy)
        assert stock_order.identifier == 123
        assert stock_order.strategy == strategy
        assert stock_order.asset.symbol == stock_symbol
        assert stock_order.quantity == 1
        assert stock_order.side == "buy"
        assert stock_order.status == "submitted"
        assert stock_order.order_type == "market"
        assert stock_order.tag == tag

        # Test with an option and stop order
        response = {
            "id": 123,
            "type": "stop",
            "side": "sell",
            "symbol": option_symbol,
            "class": "option",
            "quantity": 1,
            "status": "submitted",
            "tag": tag,
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
        assert option_order.order_type == "stop"
        assert option_order.stop_price == 1.0
        assert option_order.tag == tag

    def test_oco_parse_broker_order(self):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True)
        strategy = "strat_unittest"
        tag = "StressStrat"
        stock_symbol = "SPY"
        option_symbol = "SPY200101C00150000"

        response = {
            'avg_fill_price': np.nan,
            'class': 'oco',
            'create_date': '2025-02-26T20:52:33.982Z',
            'duration': np.nan,
            'exec_quantity': np.nan,
            'id': 16710952,
            'last_fill_price': np.nan,
            'last_fill_quantity': np.nan,
            'leg': [
                {
                    'avg_fill_price': 3.34,
                    'class': 'option',
                    'create_date': '2025-02-26T20:52:33.982Z',
                    'duration': 'gtc',
                    'exec_quantity': 1.0,
                    'id': 16710953,
                    'last_fill_price': 3.34,
                    'last_fill_quantity': 1.0,
                    'option_symbol': option_symbol,
                    'price': 3.34,
                    'quantity': 1.0,
                    'remaining_quantity': 0.0,
                    'side': 'sell_to_close',
                    'status': 'filled',
                    'symbol': stock_symbol,
                    'transaction_date': '2025-02-26T20:58:31.116Z',
                    'type': 'limit'
                },
                {
                    'avg_fill_price': 0.0,
                    'class': 'option',
                    'create_date': '2025-02-26T20:52:33.982Z',
                    'duration': 'gtc',
                    'exec_quantity': 0.0,
                    'id': 16710954,
                    'last_fill_price': 0.0,
                    'last_fill_quantity': 0.0,
                    'option_symbol': option_symbol,
                    'quantity': 1.0,
                    'remaining_quantity': 0.0,
                    'side': 'sell_to_close',
                    'status': 'canceled',
                    'stop_price': 2.78,
                    'symbol': stock_symbol,
                    'transaction_date': '2025-02-26T20:58:31.259Z',
                    'type': 'stop'
                }
            ],
            'option_symbol': np.nan,
            'quantity': np.nan,
            'remaining_quantity': np.nan,
            'side': np.nan,
            'status': 'filled',
            'stop_price': np.nan,
            'symbol': np.nan,
            'tag': 'StressStrat',
            'transaction_date': '2025-02-26T20:58:31.266Z',
            'type': np.nan
        }

        # OCO Checks
        oco_order = broker._parse_broker_order(response, strategy)
        assert oco_order.identifier == 16710952
        assert oco_order.strategy == strategy
        assert oco_order.asset.symbol == stock_symbol
        assert oco_order.quantity == 1
        assert oco_order.side == "sell_to_close"
        assert oco_order.is_filled()
        assert oco_order.order_class == Order.OrderClass.OCO
        assert oco_order.tag == tag
        assert len(oco_order.child_orders) == 2

        # Child Order Checks
        limit_order = oco_order.child_orders[0]
        assert limit_order.strategy == strategy
        assert limit_order.asset.symbol == stock_symbol
        assert limit_order.quantity == 1
        assert limit_order.side == "sell_to_close"
        assert limit_order.order_type == Order.OrderType.LIMIT

        stop_order = oco_order.child_orders[1]
        assert stop_order.strategy == strategy
        assert stop_order.asset.symbol == stock_symbol
        assert stop_order.quantity == 1
        assert stop_order.side == "sell_to_close"
        assert stop_order.order_type == Order.OrderType.STOP

    def test_do_polling(self, mocker):
        broker = Tradier(account_number="1234", access_token="a1b2c3", paper=True, polling_interval=None)
        strategy = "strat_unittest"
        sleep_amt = 0.1
        broker._strategy_name = strategy
        mock_get_orders = mocker.patch.object(broker, '_pull_broker_all_orders', return_value=[])
        submit_response = {'id': 123, 'status': 'ok'}
        mock_submit_order = mocker.patch.object(broker.tradier.orders, 'order', return_value=submit_response)
        mocker.patch.object(broker, 'sync_positions', return_value=None)

        # Set to false for testing purposes
        broker._first_iteration = False

        # Test polling with no orders
        broker.do_polling()
        known_orders = broker.get_all_orders()
        tracked_orders = broker.get_tracked_orders()
        assert not known_orders
        assert not tracked_orders

        # Submit an order to test polling with
        stock_asset = Asset("SPY")
        stock_qty = 10
        order = Order(strategy, stock_asset, stock_qty, 'buy', order_type='market')
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
            "class": "equity",
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
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
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
        stop_order = Order(strategy, stock_asset, 1, 'sell', order_type='stop', stop_price=100.0)
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
            "class": "equity",
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
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
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
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        filled_orders = broker._filled_orders
        assert len(known_orders) == 2, "Tracked should include filled orders."
        assert len(filled_orders) == 1
        order1 = filled_orders[0]
        assert order1.identifier == 123
        assert order1.order_type == "market"
        assert order1.is_filled()
        assert order1.get_fill_price() == 101.0
        assert len(broker._new_orders) == 1
        assert not len(broker._unprocessed_orders)
        all_orders = broker.get_all_orders()
        assert len(all_orders) == 2, "Includes Filled orders"

        # Cancel the 2nd order (stoploss)
        second_response["status"] = "canceled"
        mock_get_orders.return_value = [first_response, second_response]
        broker.do_polling()
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 2, "Canceled orders stay tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 2, "Includes Filled/Cancelled orders"
        assert len(broker._canceled_orders) == 1
        order2 = broker._canceled_orders[0]
        assert order2.identifier == 124
        assert order2.order_type == "stop"
        assert order2.is_canceled()
        assert not order2.get_fill_price()

        # Poll again, nothing changes
        broker.do_polling()
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 2, "Canceled orders stay tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 2, "Includes Filled/Cancelled orders"
        assert len(broker._canceled_orders) == 1

        # 3rd Order: Submit an order that causes a broker error
        mock_submit_order.return_value = {'id': 125, 'status': 'ok'}
        stop_order = Order(strategy, stock_asset, stock_qty, 'sell', order_type='limit', limit_price=1000.0)
        sub_order = broker._submit_order(stop_order)
        assert sub_order.status == "submitted"
        assert sub_order.identifier == 125
        assert sub_order.was_transmitted()
        assert len(broker._unprocessed_orders) == 1
        assert len(broker._new_orders) == 0
        assert len(broker.get_all_orders()) == 3, "Includes Filled/Cancelled orders"
        assert len(broker.get_tracked_orders()) == 3

        third_response = {
            "id": 125,
            "type": "limit",
            "side": "sell",
            "symbol": "SPY",
            "class": "equity",
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
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 3, "Rejected orders still stay tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker._canceled_orders) == 1
        assert len(broker._error_orders) == 1
        assert len(broker.get_all_orders()) == 3, "Includes Filled/Cancelled orders"
        error_order = broker._error_orders[0]
        assert error_order.identifier == 125
        assert error_order.order_type == "limit"
        assert error_order.status == "error"
        assert not error_order.get_fill_price()
        assert error_order.error_message == "API Error Msg"

        # There is a Lumibot order that is not tracked by the broker, so it should be canceled.
        mock_submit_order.return_value = {'id': 126, 'status': 'ok'}
        drop_order = Order(strategy, stock_asset, stock_qty, 'sell', order_type='market')
        broker._submit_order(drop_order)
        assert len(broker._unprocessed_orders) == 1
        assert len(broker._new_orders) == 0
        assert len(broker.get_all_orders()) == 4, "Includes Filled/Cancelled orders"

        # Poll, but broker returns no info on the Lumibot order so it should be canceled.
        mock_get_orders.return_value = [first_response, second_response, third_response]
        broker.do_polling()
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 4
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        assert len(broker._canceled_orders) == 2
        order = broker._canceled_orders[-1]
        assert order.identifier == 126
        assert order.is_canceled()

        # Broker returns an order that Lumibot doesn't know about, so it should be added to Lumibot.
        fourth_response = {
            "id": 127,
            "type": "market",
            "side": "buy",
            "symbol": "SPY",
            "class": "equity",
            "quantity": stock_qty,
            "status": "filled",
            "duration": "day",
            "create_date": dt.datetime.today(),
            "exec_quantity": stock_qty,
            "tag": strategy,
        }
        mock_get_orders.return_value = [first_response, second_response, third_response, fourth_response]
        broker.do_polling()
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 5, "New orders are tracked."
        assert len(broker._new_orders) == 1
        assert not len(broker._unprocessed_orders)
        # Find the new order in the list of known orders
        order = next((order for order in known_orders if order.status == "new"), None)
        assert order.identifier == 127
        assert order.status == "new"
        assert not order.is_filled()
        assert not order.get_fill_price()

        # Poll again, order fill will now be processed
        fourth_response["avg_fill_price"] = 102.0
        mock_get_orders.return_value = [first_response, second_response, third_response, fourth_response]
        broker.do_polling()
        sleep(sleep_amt)  # Sleep gives a chance for order processing thread to finish
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 5, "Filled orders are still being tracked."
        assert len(broker._new_orders) == 0  # New order is no longer new
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 5, "Includes Filled/Cancelled orders and order not in Broker info"
        order = broker.get_order(127)
        assert order.identifier == 127
        assert order.is_filled()
        assert order.get_fill_price() == 102.0

        # ---------------------------------
        # Include an OCO order case.
        # Lumibot doesn't know about it, but the broker does. Add to Lumibot trackers
        fifth_response = {
            'id': 128,
            'type': 'oco',
            'side': 'buy',
            'symbol': "SPY",
            'class': 'oco',
            'quantity': 1,
            'avg_fill_price': None,
            'status': 'open',
            'tag': strategy,
            'duration': 'gtc',
            'create_date': dt.datetime.today(),
            'leg': [
                {
                    'avg_fill_price': None,
                    'class': 'equity',
                    'create_date': dt.datetime.today(),
                    'duration': 'gtc',
                    'exec_quantity': None,
                    'id': 129,
                    'last_fill_price': None,
                    'last_fill_quantity': None,
                    'option_symbol': None,
                    'price': 103.0,
                    'quantity': 1.0,
                    'remaining_quantity': 1.0,
                    'side': 'buy_to_open',
                    'status': 'open',
                    'stop_price': None,
                    'symbol': "SPY",
                    'transaction_date': dt.datetime.today(),
                    'type': 'limit',
                    'tag': strategy,
                },
                {
                    'avg_fill_price': None,
                    'class': 'equity',
                    'create_date': dt.datetime.today(),
                    'duration': 'gtc',
                    'exec_quantity': None,
                    'id': 130,
                    'last_fill_price': None,
                    'last_fill_quantity': None,
                    'option_symbol': "SPY",
                    'price': None,
                    'quantity': 1.0,
                    'remaining_quantity': 1.0,
                    'side': 'buy_to_open',
                    'status': 'open',
                    'stop_price': 101.0,
                    'symbol': "SPY",
                    'transaction_date': dt.datetime.today(),
                    'type': 'stop',
                    'tag': strategy,
                }
            ],
        }
        mock_get_orders.return_value = [first_response, second_response, third_response,
                                         fourth_response, fifth_response]
        broker.do_polling()
        sleep(sleep_amt)
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 5 + 3, "OCO and child orders are tracked."
        assert len(broker._new_orders) == 3, "OCO and child orders are new."
        assert not len(broker._unprocessed_orders)
        assert len(broker.get_all_orders()) == 5 + 3, "Includes Filled/Cancelled orders and order not in Broker info"
        oco_order = broker.get_order(128)
        assert not oco_order.is_filled()
        assert oco_order.is_active()
        assert not oco_order.child_orders[0].is_filled()

        # Update broker response for a Race condition fill - OCO and child limit order are marked as status=filled,
        # but there is not a fill price provided yet.
        fifth_response["status"] = "filled"
        fifth_response["avg_fill_price"] = None
        fifth_response["exec_quantity"] = 1
        fifth_response["leg"][0]["status"] = "filled"
        fifth_response["leg"][0]["avg_fill_price"] = None
        fifth_response["leg"][0]["exec_quantity"] = 1
        fifth_response["leg"][1]["status"] = "cancelled"
        fifth_response["leg"][1]["avg_fill_price"] = None
        mock_get_orders.return_value = [first_response, second_response, third_response,
                                            fourth_response, fifth_response]
        broker.do_polling()
        sleep(sleep_amt)
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 5 + 3, "OCO orders are tracked."
        assert len(broker._new_orders) == 2, "OCO and limit are in incomplete fill state, Stop is canceled."
        # OCO order status has not yet been updated to filled
        oco_order = broker.get_order(128)
        assert oco_order.identifier == 128
        assert not oco_order.is_filled()
        assert not oco_order.get_fill_price()
        assert not oco_order.child_orders[0].is_filled()
        assert oco_order.child_orders[0].is_active()
        assert not oco_order.child_orders[1].is_filled()
        assert not oco_order.child_orders[1].is_active()
        assert oco_order.child_orders[1].is_canceled()

        # OCO now has a fill price and should finally get filled
        fifth_response["leg"][0]["avg_fill_price"] = 103.0
        mock_get_orders.return_value = [first_response, second_response, third_response,
                                            fourth_response, fifth_response]
        broker.do_polling()
        sleep(sleep_amt)
        known_orders = broker.get_tracked_orders(strategy=strategy)
        assert len(known_orders) == 5 + 3, "OCO orders are tracked."
        assert len(broker._new_orders) == 0
        assert not len(broker._unprocessed_orders)
        oco_order = broker.get_order(128)
        assert oco_order.identifier == 128
        assert oco_order.is_filled()
        assert oco_order.get_fill_price() == 103.0
        assert oco_order.child_orders[0].is_filled()
        assert oco_order.child_orders[0].get_fill_price() == 103.0
