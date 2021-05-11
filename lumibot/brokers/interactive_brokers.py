from datetime import timezone
from dateutil import tz
import datetime
import traceback

import pandas_market_calendars as mcal
import pandas as pd

from lumibot.data_sources import InteractiveBrokersData

# Naming conflict on Order between IB and Lumibot.
from lumibot.entities import Order as OrderLum
from lumibot.entities import Position
from .broker import Broker

from lumibot.brokers import IBApp
from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *


class InteractiveBrokers(InteractiveBrokersData, Broker):
    """Inherit InteractiveBrokerData first and all the price market
    methods than inherits broker"""

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=True):
        # Calling init methods
        InteractiveBrokersData.__init__(
            self,
            config,
            max_workers=max_workers,
            chunk_size=chunk_size,
        )
        Broker.__init__(self, name="interactive_brokers", connect_stream=False)

        # For checking duplicate order status events from IB.
        self.order_status_duplicates = []

        # Connection to interactive brokers
        self.ib = None
        self.start_ib(config.IP, config.SOCKET_PORT, config.CLIENT_ID)

    def start_ib(self, ip, socket_port, client_id):
        # Connect to interactive brokers.
        if not self.ib:
            self.ib = IBApp(ip, socket_port, client_id, ib_broker=self)

    # =========Clock functions=====================

    def utc_to_local(self, utc_dt):
        return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())

    def get_timestamp(self):
        """return current timestamp"""
        clock = self.ib.get_timestamp()
        return clock

    def market_hours(self, market="NASDAQ", close=True, next=False, date=None):
        mkt_cal = mcal.get_calendar(market)
        date = date if date is not None else datetime.datetime.now()
        trading_hours = mkt_cal.schedule(
            start_date=date, end_date=date + datetime.timedelta(weeks=1)
        ).head(2)

        row = 0 if not next else 1
        th = trading_hours.iloc[row, :]
        # market_open, market_close = th[0], th[1]
        # todo: remove this, it's temp to have full trading hours.
        market_open = self.utc_to_local(datetime.datetime(2005, 1, 1))
        market_close = self.utc_to_local(datetime.datetime(2025, 1, 1))

        if close:
            return market_close
        else:
            return market_open

    def market_close_time(self):
        return self.utc_to_local(self.market_hours(close=True))

    def is_market_open(self):
        """return True if market is open else false"""
        open_time = self.utc_to_local(self.market_hours(close=False))
        close_time = self.utc_to_local(self.market_hours(close=True))

        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())

        return (current_time >= open_time) and (close_time >= current_time)

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        open_time_this_day = self.utc_to_local(
            self.market_hours(close=False, next=False)
        )
        open_time_next_day = self.utc_to_local(
            self.market_hours(close=False, next=True)
        )
        now = self.utc_to_local(datetime.datetime.now())
        open_time = (
            open_time_this_day if open_time_this_day > now else open_time_next_day
        )
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return None
        else:
            return open_time.timestamp() - current_time.timestamp()

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        close_time = self.utc_to_local(self.market_hours(close=True))
        current_time = datetime.datetime.now().astimezone(tz=tz.tzlocal())
        if self.is_market_open():
            return close_time.timestamp() - current_time.timestamp()
        else:
            return None

    # =========Positions functions==================

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        asset = broker_position.asset
        quantity = int(broker_position.Quantity)
        position = Position(strategy, asset, quantity, orders=orders)
        return position

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for account, broker_position in broker_positions.iterrows():
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
        result = self._pull_broker_positions()
        result = result[result["Symbol"] == asset].squeeze()
        return result

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        current_positions = self.ib.get_positions()

        current_positions_df = pd.DataFrame(
            data=current_positions,
        )
        current_positions_df.columns = [
            "Account",
            "Symbol",
            "Quantity",
            "Average_Cost",
            "Sec_Type",
        ]

        current_positions_df = current_positions_df.set_index("Account", drop=True)
        current_positions_df["Quantity"] = current_positions_df["Quantity"].astype(
            "int"
        )
        current_positions_df = current_positions_df[
            current_positions_df["Quantity"] != 0
        ]

        return current_positions_df

    # =======Orders and assets functions=========

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""

        order = OrderLum(
            strategy,
            response.contract.localSymbol,
            response.totalQuantity,
            response.action.lower(),
            limit_price=response.lmtPrice,
            stop_price=response.adjustedStopPrice,
            time_in_force=response.tif,
        )
        order._transmitted = True
        order.set_identifier(response.orderId)
        order.update_status(response.orderState.status)
        order.update_raw(response)
        return order

    def _pull_broker_order(self, order_id):
        """Get a broker order representation by its id"""
        pull_order = [
            order for order in self.ib.get_open_orders() if order.orderId == order_id
        ]
        response = pull_order[0] if len(pull_order) > 0 else None
        return response

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        orders = self.ib.get_open_orders()
        return orders

    def _flatten_order(self, orders):  # implement for stop loss.
        """Used for alpaca, just return orders."""
        return orders

    def submit_order(self, order):
        """Submit an order for an asset"""
        orders_new = [order]
        orders = list()
        try:
            # Initial order
            order.identifier = self.ib.nextOrderId()
            if order.stop_price:
                order.transmit = False

                # Stop loss order.
                stop_loss_order = OrderLum(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    "sell",
                    stop_price=order.stop_price,
                )
                stop_loss_order.type = "stop"
                stop_loss_order.transmit = True
                stop_loss_order.parent_id = order.identifier
                orders_new.append(stop_loss_order)

            responses = self.ib.execute_order(orders_new)

            for response in responses:
                order_parsed = self._parse_broker_order(response, order.strategy)
                orders.append(order_parsed)
                self._unprocessed_orders.append(order_parsed)

        except Exception as e:
            order.set_error(e)
            logging.info(
                "%r did not go through. The following error occurred: %s" % (order, e)
            )

        return orders

    def cancel_order(self, order_id):
        """Cancel an order"""
        self.ib.cancel_order(order_id)

    def cancel_open_orders(self, strategy=None):
        """cancel all the strategy open orders"""
        self.ib.reqGlobalCancel()

    def sell_all(self, strategy, cancel_open_orders=True, at_broker=False):
        """sell all positions"""
        logging.warning("Strategy %s: sell all" % strategy)
        if cancel_open_orders:
            self.cancel_open_orders(strategy)

        orders = []
        if at_broker:
            positions = self.ib.get_positions()
        else:
            positions = self.get_tracked_positions(strategy)

        for position in positions:
            if position["position"] == 0:
                continue
            close_order = OrderLum(
                strategy, position["symbol"], position["position"], "sell"
            )
            orders.append(close_order)
        self.submit_orders(orders)

    # todo at start up, discuss with team
    def load_positions(self):
        """ Use to load any existing positions with the broker on start. """
        positions = self.ib.get_positions()
        print("Load Positions", positions)

    # =========Market functions=======================

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets from the market"""
        unavail_warning = (
            f"ERROR: When working with Interactive Brokers it is not possible to "
            f"acquire all of the tradable assets in the markets. "
            f"Please do not use `get_tradable_assets`."
        )
        logging.info(unavail_warning)
        print(unavail_warning)

        return

    def _close_connection(self):
        self.ib.disconnect()

    def get_account_summary(self):
        return self.ib.get_account_summary()

    def get_contract_details(self, asset):
        return self.ib.get_contract_details(asset=asset)

    def option_params(self, asset, exchange="", underlyingConId=""):
        return self.ib.option_params(
            asset=asset, exchange=exchange, underlyingConId=underlyingConId
        )

    # =======Stream functions=========
    def on_status_event(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFullPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ):
        """
        Information received from IBWrapper.orderStatus(). This can sometimes fire
        duplicates so a list must be kept for checking.

        The following are possible for status:
          - PendingSubmit
          - PendingCancel
          - PreSubmitted
          - Submitted
          - ApiCancelled
          - Cancelled
          - Filled
          - Inactive

          Filled is problematic. - Filled indicates that the order has been completely
          filled. Market orders executions  will not always trigger a Filled status.
          Therefore this must also be checked using ExecDetails
        """
        if status in [
            "PendingSubmit",
            "PendingCancel",
            "PreSubmitted",
            "Filled",
        ]:
            return

        order_status = [
            orderId,
            status,
            filled,
            remaining,
            avgFullPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        ]
        if order_status in self.order_status_duplicates:
            logging.info(
                f"Duplicate order status event ignored. Order id {orderId} "
                f"and status {status} "
            )
            return
        else:
            self.order_status_duplicates.append(order_status)

        stored_order = self.get_tracked_order(orderId)
        if stored_order is None:
            logging.info(
                "Untracker order %s was logged by broker %s" % (orderId, self.name)
            )
            return False

        # Check the order status submit changes.
        if status == "Submitted":
            type_event = self.NEW_ORDER
        elif status in ["ApiCancelled", "Cancelled", "Inactive"]:
            type_event = self.CANCELED_ORDER
        else:
            raise ValueError(
                "A status event with an order of unknown order type. Should only be: "
                "`Submitted`, `ApiCancelled`, `Cancelled`, `Inactive`"
            )
        self._process_trade_event(
            stored_order,
            type_event,
            price=None,
            filled_quantity=None,
        )

    def on_trade_event(self, reqId, contract, execution):
        # print("on_trade_event: ", reqId, contract, execution) todo delete

        try:
            orderId = execution.orderId
            stored_order = self.get_tracked_order(orderId)

            if stored_order is None:
                logging.info(
                    "Untracker order %s was logged by broker %s" % (orderId, self.name)
                )
                return False
                # Check the order status submit changes.
            if execution.cumQty < stored_order.quantity:
                type_event = self.PARTIALLY_FILLED_ORDER
            elif execution.cumQty == stored_order.quantity:
                type_event = self.FILLED_ORDER
            else:
                raise ValueError(
                    f"An order type should not have made it this far. " f"{execution}"
                )

            price = execution.price
            filled_quantity = execution.shares

            self._process_trade_event(
                stored_order,
                type_event,
                price=price,
                filled_quantity=filled_quantity,
            )

            return True
        except:
            logging.error(traceback.format_exc())
