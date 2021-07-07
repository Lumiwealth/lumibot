import datetime
import time
import traceback
from datetime import timezone
from threading import Thread

import pandas as pd
import pandas_market_calendars as mcal
from dateutil import tz
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.wrapper import *

from lumibot.data_sources import InteractiveBrokersData

# Naming conflict on Order between IB and Lumibot.
from lumibot.entities import Order as OrderLum
from lumibot.entities import Position

from .broker import Broker


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
        """Return if market open or closed.
        params:
          - market (str: default `NASDAQ`): which market to test.
          - close (bool: default `True`): Choose open or close to check.
          - next (bool: default `False`): Check current day or next day.
          - date (datetime.date, default `None`) Date to check, `None` for today.
        """

        mkt_cal = mcal.get_calendar(market)
        date = date if date is not None else datetime.datetime.now()
        trading_hours = mkt_cal.schedule(
            start_date=date, end_date=date + datetime.timedelta(weeks=1)
        ).head(2)

        row = 0 if not next else 1
        th = trading_hours.iloc[row, :]
        market_open, market_close = th[0], th[1]

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

    def _submit_order(self, order):
        """Submit an order for an asset"""
        # Initial order
        order.identifier = self.ib.nextOrderId()
        kwargs = {
            "type": order.type,
            "order_class": order.order_class,
            "time_in_force": order.time_in_force,
            "limit_price": order.limit_price,
            "stop_price": order.stop_price,
            "trail_price": order.trail_price,
            "trail_percent": order.trail_percent,
        }
        # Remove items with None values
        kwargs = {k: v for k, v in kwargs.items() if v}

        if order.take_profit_price:
            kwargs["take_profit"] = {"limit_price": order.take_profit_price}

        if order.stop_loss_price:
            kwargs["stop_loss"] = {"stop_price": order.stop_loss_price}
            if order.stop_loss_limit_price:
                kwargs["stop_loss"]["limit_price"] = order.stop_loss_limit_price

        self._unprocessed_orders.append(order)
        self.ib.execute_order(order)
        order.update_status("submitted")
        return order

    def cancel_order(self, order_id):
        """Cancel an order"""
        self.ib.cancel_order(order_id)

    def cancel_open_orders(self, strategy=None):
        """cancel all the strategy open orders"""
        self.ib.reqGlobalCancel()

    def sell_all(self, strategy, cancel_open_orders=True):
        """sell all positions"""
        logging.warning("Strategy %s: sell all" % strategy)
        if cancel_open_orders:
            self.cancel_open_orders(strategy)

        orders = []
        positions = self.get_tracked_positions(strategy)

        for position in positions:
            size = position.quantity
            if size == 0:
                continue
            side = "sell" if size > 0 else "buy"
            close_order = OrderLum(
                strategy, position.asset, size, side
            )
            orders.append(close_order)
        self.submit_orders(orders)

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
        # Used for Interactive Brokers. Convert an asset into a IB Contract.
        return self.ib.get_contract_details(asset=asset)

    def option_params(self, asset, exchange="", underlyingConId=""):
        # Returns option chain data, list of strikes and list of expiry dates.
        return self.ib.option_params(
            asset=asset, exchange=exchange, underlyingConId=underlyingConId
        )

    def get_chains(self, asset):
        """Returns option chain."""
        contract_details = self.get_contract_details(asset=asset)
        contract_id = contract_details[0].contract.conId
        chains = self.option_params(asset, underlyingConId=contract_id)
        if len(chains) == 0:
            raise AssertionError(f"No option chain for {asset.symbol}")
        return chains

    def get_chain(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange."""
        for x, p in chains.items():
            if x == exchange:
                return p

    def get_expiration(self, chains, exchange='SMART'):
        """Returns expirations and strikes high/low of target price."""
        return sorted(list(self.get_chain(chains, exchange=exchange)["Expirations"]))

    def get_multiplier(self, chains, exchange='SMART'):
        """Returns the multiplier"""
        return self.get_chain(chains, exchange)["Multiplier"]

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
                "Untracked order %s was logged by broker %s" % (orderId, self.name)
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
        orderId = execution.orderId
        stored_order = self.get_tracked_order(orderId)

        if stored_order is None:
            logging.info(
                "Untracked order %s was logged by broker %s" % (orderId, self.name)
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
        multiplier = stored_order.asset.multiplier if stored_order.asset.multiplier \
            else 1

        self._process_trade_event(
            stored_order,
            type_event,
            price=price,
            filled_quantity=filled_quantity,
            multiplier=multiplier,
        )

        return True


TYPE_MAP = dict(
    stock="STK",
    option="OPT",
)
# ===================INTERACTIVE BROKERS CLASSES===================
class IBWrapper(EWrapper):
    """Listens and collects data from IB."""

    # Error handling code.
    def init_error(self):
        error_queue = queue.Queue()
        self.my_errors_queue = error_queue

    def is_error(self):
        error_exist = not self.my_errors_queue.empty()
        return error_exist

    def get_error(self, timeout=6):
        if self.is_error():
            try:
                return self.my_errors_queue.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def error(self, id, errorCode, errorString):
        if not hasattr(self, "my_errors_queue"):
            self.init_error()
        errormessage = "IB returns an error with %d error code %d that says %s" % (
            id,
            errorCode,
            errorString,
        )
        logging.info(errormessage)
        self.my_errors_queue.put(errormessage)

    # Time.
    def init_time(self):
        time_queue = queue.Queue()
        self.my_time_queue = time_queue
        return time_queue

    def currentTime(self, server_time):
        if not hasattr(self, "my_time_queue"):
            self.init_time()
        self.my_time_queue.put(server_time)

    # Tick Data. NOT USED YET
    def init_tick(self):
        self.tick = list()
        tick_queue = queue.Queue()
        self.my_tick_queue = tick_queue
        return tick_queue

    def tickPrice(self, reqId, tickType, price, attrib):
        if not hasattr(self, "tick"):
            self.init_tick()
        if tickType == 4:
            self.tick.append(price)

    def tickSnapshotEnd(self, reqId):
        super().tickSnapshotEnd(reqId)
        self.my_tick_queue.put(self.tick)

    # Historical Data.
    def init_historical(self):
        self.historical = list()
        historical_queue = queue.Queue()
        self.my_historical_queue = historical_queue
        return historical_queue

    def historicalData(self, reqId, bar):
        if not hasattr(self, "historical"):
            self.init_historical()
        self.historical.append(vars(bar))

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self.my_historical_queue.put(self.historical)

    # Positions.
    def init_positions(self):
        self.positions = list()
        positions_queue = queue.Queue()
        self.my_positions_queue = positions_queue
        return positions_queue

    def position(self, account, contract, pos, avgCost):
        if not hasattr(self, "positions"):
            self.init_positions()

        positionsdict = {
            "account": account,
            "symbol": contract.symbol,
            "position": pos,
            "cost": avgCost,
            "type": contract.secType,
        }

        self.positions.append(positionsdict)

        positionstxt = ", ".join(f"{k}: {v}" for k, v in positionsdict.items())

        logging.info(positionstxt)

    def positionEnd(self):
        self.my_positions_queue.put(self.positions)

    # Account summary
    def init_accounts(self):
        self.accounts = list()
        accounts_queue = queue.Queue()
        self.my_accounts_queue = accounts_queue
        return accounts_queue

    def accountSummary(
        self, reqId: int, account: str, tag: str, value: str, currency: str
    ):
        if not hasattr(self, "accounts"):
            self.init_accounts()

        accountSummarydict = {
            "ReqId": reqId,
            "Account": account,
            "Tag": tag,
            "Value": value,
            "Currency": currency,
        }

        self.accounts.append(accountSummarydict)

        accountSummarytxt = ", ".join(
            [f"{k}: {v}" for k, v in accountSummarydict.items()]
        )

        logging.info(accountSummarytxt)

    def accountSummaryEnd(self, reqId):
        super().accountSummaryEnd(reqId)
        self.my_accounts_queue.put(self.accounts)

    # Order IDs
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

    def nextOrderId(self):
        while not hasattr(self, "nextValidOrderId"):
            print("Waiting for next order id")
            time.sleep(0.1)
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def init_orders(self):
        self.orders = list()
        orders_queue = queue.Queue()
        self.my_orders_queue = orders_queue
        return orders_queue

    def init_new_orders(self):
        new_orders_queue = queue.Queue()
        self.my_new_orders_queue = new_orders_queue
        return new_orders_queue

    def init_order_updates(self):
        order_updates_queue = queue.Queue()
        self.my_order_updates_queue = order_updates_queue
        return order_updates_queue

    def openOrder(
        self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState
    ):
        if not hasattr(self, "orders"):
            self.init_orders()
        openOrdertxt = (
            f"openOrder - "
            f"PermId:  {order.permId}, "
            f"ClientId: {order.clientId}, "
            f"OrderId: {orderId}, "
            f"Account: {order.account},"
            f"Symbol: {contract.symbol}, "
            f"SecType: {contract.secType}, "
            f"Exchange: {contract.exchange}, "
            f"Action: {order.action}, "
            f"OrderType: {order.orderType}, "
            f"TotalQty: {order.totalQuantity},"
            f"CashQty: {order.cashQty}, "
            f"LmtPrice: {order.lmtPrice}, "
            f"AuxPrice: {order.auxPrice}, "
            f"Status: {orderState.status}) "
        )

        logging.info(openOrdertxt)

        order.contract = contract
        order.orderState = orderState
        self.orders.append(order)

        # Capture new orders.
        if not hasattr(self, "my_new_orders_queue"):
            self.init_new_orders()
        if orderState.status == "PreSubmitted":
            self.my_new_orders_queue.put(order)

    def openOrderEnd(self):
        super().openOrderEnd()
        self.my_orders_queue.put(self.orders)

    def orderStatus(
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
        orderStatustxt = (
            f"orderStatus - "
            f"orderid: {orderId}, "
            f"status: {status}, "
            f"filled: {filled}, "
            f"remaining: {remaining}, "
            f"lastFillPrice: {lastFillPrice}, "
        )
        logging.info(orderStatustxt)
        self.ib_broker.on_status_event(
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
        )

    def execDetails(self, reqId, contract, execution):
        execDetailstxt = (
            f"execDetails - "
            f"{reqId}, "
            f"{contract.symbol}, "
            f"{contract.secType}, "
            f"{contract.currency}, "
            f"{execution.execId}, "
            f"{execution.orderId}, "
            f"{execution.shares}, "
            f"{execution.lastLiquidity} "
        )
        logging.info(execDetailstxt)

        return self.ib_broker.on_trade_event(reqId, contract, execution)

    def init_contract_details(self):
        self.contract_details = list()
        contract_details_queue = queue.Queue()
        self.my_contract_details_queue = contract_details_queue
        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        if not hasattr(self, "my_contract_details_queue"):
            self.init_contract_details()
        self.contract_details.append(contractDetails)

    def contractDetailsEnd(self, reqId):
        super().contractDetailsEnd(reqId)
        self.my_contract_details_queue.put(self.contract_details)

    def init_option_params(self):
        self.option_params_dict = dict()
        option_params_queue = queue.Queue()
        self.my_option_params_queue = option_params_queue
        return option_params_queue

    def securityDefinitionOptionParameter(
        self,
        reqId: int,
        exchange: str,
        underlyingConId: int,
        tradingClass: str,
        multiplier: str,
        expirations: SetOfString,
        strikes: SetOfFloat,
    ):
        super().securityDefinitionOptionParameter(
            reqId,
            exchange,
            underlyingConId,
            tradingClass,
            multiplier,
            expirations,
            strikes,
        )
        if not hasattr(self, "my_option_params_queue"):
            self.init_option_params()
        self.option_params_dict[exchange] = {
            "Underlying conId": underlyingConId,
            "TradingClass": tradingClass,
            "Multiplier": multiplier,
            "Expirations": expirations,
            "Strikes": strikes,
        }

    def securityDefinitionOptionParameterEnd(self, reqId):
        super().securityDefinitionOptionParameterEnd(reqId)
        self.my_option_params_queue.put(self.option_params_dict)

class IBClient(EClient):
    """Sends data to IB"""
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        self.max_wait_time = 11
        self.reqId = 10000

    def get_reqid(self):
        self.reqId += 1
        return self.reqId

    def get_timestamp(self):

        print("Asking server for Unix time")

        # Creates a queue to store the time
        time_storage = self.wrapper.init_time()

        # Sets up a request for unix time from the Eclient
        self.reqCurrentTime()

        try:
            requested_time = time_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for timestamp.")
            requested_time = None

        while self.wrapper.is_error():
            print("Error:", self.get_error(timeout=5))

        return requested_time

    def get_tick(
            self,
            asset="",
    ):
        """
        Bid Price	1	Highest priced bid for the contract.	IBApi.EWrapper.tickPrice	-
        Ask Price	2	Lowest price offer on the contract.	IBApi.EWrapper.tickPrice	-
        Last Price	4	Last price at which the contract traded (does not include some trades in RTVolume).	IBApi.EWrapper.tickPrice	-
        """
        tick_storage = self.wrapper.init_tick()

        contract = self.create_contract(asset)
        reqId = self.get_reqid()

        self.reqMktData(reqId, contract, "", True, False, [])

        try:
            requested_tick = tick_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for tick data.")
            requested_tick = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_tick

    def get_historical_data(
        self,
        reqId=0,
        symbol=[],
        end_date_time="",
        parsed_duration="1 D",
        parsed_timestep="1 day",
        type="TRADES",
        useRTH=1,
        formatDate=2,
        keepUpToDate=False,
        chartOptions=[],
    ):
        historical_storage = self.wrapper.init_historical()

        contract = self.create_contract(symbol)
        # Call the historical data.
        self.reqHistoricalData(
            self.get_reqid(),
            contract,
            end_date_time,
            parsed_duration,
            parsed_timestep,
            type,
            useRTH,
            formatDate,
            keepUpToDate,
            chartOptions,
        )

        try:
            requested_historical = historical_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for historical data.")
            requested_historical = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_historical

    def get_positions(self):
        positions_storage = self.wrapper.init_positions()

        # Call the positions data.
        self.reqPositions()

        try:
            requested_positions = positions_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for positions")
            requested_positions = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_positions

    def get_account_summary(self):
        accounts_storage = self.wrapper.init_accounts()

        tags = (
            f"AccountType, TotalCashValue, AccruedCash, "
            f"NetLiquidation, BuyingPower, GrossPositionValue"
        )
        self.reqAccountSummary(self.get_reqid(), "All", tags)

        try:
            requested_accounts = accounts_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for account summary")
            requested_accounts = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_accounts

    def get_open_orders(self):
        orders_storage = self.wrapper.init_orders()

        # Call the orders data.
        self.reqOpenOrders()

        try:
            requested_orders = orders_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for orders.")
            requested_orders = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_orders

    def cancel_order(self, order_id):
        if not order_id or not isinstance(order_id, int):
            logging.info(
                f"An attempt to cancel an order without supplying a proper "
                f"`order_id` was made. This was your `order_id` {order_id}. "
                f"An integer is required. No action was taken."
            )
            return

        self.cancelOrder(order_id)

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return 0

    def get_contract_details(self, asset=None):
        contract_details_storage = self.wrapper.init_contract_details()

        # Call the contract details.
        contract = self.create_contract(asset)

        self.reqContractDetails(self.get_reqid(), contract)

        try:
            requested_contract_details = contract_details_storage.get(
                timeout=self.max_wait_time
            )
        except queue.Empty:
            print("The queue was empty or max time reached for contract details")
            requested_contract_details = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_contract_details

    def option_params(self, asset=None, exchange="", underlyingConId=""):
        options_params_storage = self.wrapper.init_option_params()

        # Call the orders data.
        self.reqSecDefOptParams(
            self.get_reqid(),
            asset.symbol,
            exchange,
            TYPE_MAP[asset.asset_type],
            underlyingConId,
        )

        try:
            requested_option_params = options_params_storage.get(
                timeout=self.max_wait_time
            )
        except queue.Empty:
            print(
                "The queue was empty or max time reached for option contract "
                "details."
            )
            requested_option_params = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_option_params


class IBApp(IBWrapper, IBClient):
    ORDERTYPE_MAPPING = dict(
        market="MKT",
        limit="LMT",
        stop="STP",
        stop_limit="STP LMT",
        trailing_stop="TRAIL",
    )

    def __init__(self, ipaddress, portid, clientid, ib_broker=None):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)
        self.ib_broker = ib_broker
        self.connect(ipaddress, portid, clientid)

        thread = Thread(target=self.run)
        thread.start()
        setattr(self, "_thread", thread)

        self.init_error()

    def create_contract(
        self,
        asset,
        exchange="SMART",
        currency="USD",
        primaryExchange="ISLAND",
    ):
        """Creates new contract objects. """
        contract = Contract()

        contract.symbol = str(asset.symbol)
        contract.secType = TYPE_MAP[asset.asset_type]
        contract.exchange = exchange
        contract.currency = currency

        if asset.asset_type == "stock":
            contract.primaryExchange = primaryExchange
        elif asset.asset_type == "option":
            contract.lastTradeDateOrContractMonth = asset.expiration
            contract.strike = asset.strike_str
            contract.right = asset.right
            contract.multiplier = asset.multiplier
            contract.primaryExchange="CBOE"
        else:
            raise ValueError(
                f"The asset {asset.symbol} has a type of {asset.asset_type}. "
                f"It must be one of {asset.types}"
            )

        return contract

    def create_order(self, order):
        ib_order = Order()
        if order.order_class == "bracket":
            if not order.limit_price:
                logging.info(
                    f"All bracket orders must have limit price for the originating "
                    f"order. The bracket order for {order.symbol} is cancelled.")
                return []
            parent = Order()
            parent.orderId = order.identifier if order.identifier else self.nextOrderId()
            parent.action = order.side.upper()
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            takeProfit = Order()
            takeProfit.orderId = parent.orderId + 1
            takeProfit.action = "SELL" if parent.action == "BUY" else "BUY"
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.take_profit_price
            takeProfit.parentId = parent.orderId
            takeProfit.transmit = False

            stopLoss = Order()
            stopLoss.orderId = parent.orderId + 2
            stopLoss.action = "SELL" if parent.action == "BUY" else "BUY"
            stopLoss.orderType = "STP"
            stopLoss.auxPrice = order.stop_loss_price
            stopLoss.totalQuantity = order.quantity
            stopLoss.parentId = parent.orderId
            stopLoss.transmit = True

            bracketOrder = [parent, takeProfit, stopLoss]

            return bracketOrder

        elif order.order_class == "oto":
            if not order.limit_price:
                logging.info(
                    f"All oto orders must have limit price for the originating order. "
                    f"The one triggers other order for {order.symbol} is cancelled.")
                return []

            parent = Order()
            parent.orderId = order.identifier if order.identifier else self.nextOrderId()
            parent.action = order.side.upper()
            parent.orderType = "LMT"
            parent.totalQuantity = order.quantity
            parent.lmtPrice = order.limit_price
            parent.transmit = False

            if order.take_profit_price:
                takeProfit = Order()
                takeProfit.orderId = parent.orderId + 1
                takeProfit.action = "SELL" if parent.action == "BUY" else "BUY"
                takeProfit.orderType = "LMT"
                takeProfit.totalQuantity = order.quantity
                takeProfit.lmtPrice = order.take_profit_price
                takeProfit.parentId = parent.orderId
                takeProfit.transmit = True
                return [parent, takeProfit]

            elif order.stop_loss_price:
                stopLoss = Order()
                stopLoss.orderId = parent.orderId + 1
                stopLoss.action = "SELL" if parent.action == "BUY" else "BUY"
                stopLoss.orderType = "STP"
                stopLoss.auxPrice = order.stop_loss_price
                stopLoss.totalQuantity = order.quantity
                stopLoss.parentId = parent.orderId
                stopLoss.transmit = True
                return [parent, stopLoss]

        elif order.order_class == "oco":
            takeProfit = Order()
            takeProfit.orderId = order.identifier if order.identifier else self.nextOrderId()
            takeProfit.action = order.side.upper()
            takeProfit.orderType = "LMT"
            takeProfit.totalQuantity = order.quantity
            takeProfit.lmtPrice = order.take_profit_price
            takeProfit.transmit = False

            oco_Group = f"oco_{takeProfit.orderId}"
            takeProfit.ocaGroup = oco_Group
            takeProfit.ocaType = 1

            stopLoss = Order()
            stopLoss.orderId = takeProfit.orderId + 1
            stopLoss.action = order.side.upper()
            stopLoss.orderType = "STP"
            stopLoss.totalQuantity = order.quantity
            stopLoss.auxPrice = order.stop_loss_price
            stopLoss.transmit = True

            stopLoss.ocaGroup = oco_Group
            stopLoss.ocaType = 1


            return [takeProfit, stopLoss]
        else:
            ib_order.action = order.side.upper()
            ib_order.orderType = self.ORDERTYPE_MAPPING[order.type]
            ib_order.totalQuantity = order.quantity
            ib_order.lmtPrice = order.limit_price if order.limit_price else 0
            ib_order.auxPrice = order.stop_price if order.stop_price else ""
            ib_order.trailingPercent = order.trail_percent if order.trail_percent else ""
            if order.trail_price:
                ib_order.auxPrice = order.trail_price
            ib_order.orderId = order.identifier if order.identifier else self.nextOrderId()

            return [ib_order]

    def execute_order(self, orders):
        # Create a queue to store the new order.
        new_order_storage = self.wrapper.init_new_orders()

        if not isinstance(orders, list):
            orders = [orders]

        ib_orders = []
        for order in orders:
            # Places the order with the returned contract and order objects
            contract_object = self.create_contract(
                order.asset,
                exchange=order.exchange,
            )
            order_objects = self.create_order(order)
            if len(order_objects) == 0:
                continue

            # There was an update by IB mid april/2021 that ceased support for
            # `eTradeOnly` and `firmQuoteOnly` attributes. However the defaults
            # where left `True` causing errors. Set to False
            for order_object in order_objects:
                order_object.eTradeOnly = False
                order_object.firmQuoteOnly = False

                nextID = (
                    order_object.orderId if order_object.orderId else self.nextOrderId()
                )
                ib_orders.append((nextID, contract_object, order_object))

        for ib_order in ib_orders:
            if len(ib_order) == 0:
                continue
            self.placeOrder(*ib_order)
