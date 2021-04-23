import asyncio
import logging
import time
import traceback
from asyncio import CancelledError
from datetime import timezone
from dateutil import tz
import datetime
import queue
import time

import pandas_market_calendars as mcal
import pandas as pd

from lumibot.data_sources import InteractiveBrokersData

# Naming conflict on Order between IB and Lumibot.
from lumibot.entities import Order as OrderLum
from lumibot.entities import Position
from .broker import Broker

from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from threading import Thread


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
        Broker.__init__(self, name="interactive_brokers", connect_stream=connect_stream)
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
        symbol = broker_position.Symbol
        quantity = int(broker_position.Quantity)
        position = Position(strategy, symbol, quantity, orders=orders)
        return position

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for account, broker_position in broker_positions.iterrows():
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_broker_position(self, symbol):
        """Given a symbol, get the broker representation
        of the corresponding symbol"""
        result = self._pull_broker_positions()
        result = result[result["Symbol"] == symbol].squeeze()
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
        """Get a broker order representation by its id"""  # todo check api
        pull_order = [
            order for order in self.api.openOrders() if order.orderId == order_id
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
                    order.symbol,
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
                self._unprocessed_orders.append(
                    order_parsed
                )


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


############ INTERACTIVE BROKERS CLASSES #################


class IBWrapper(EWrapper):

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
        errormessage = "IB returns an error with %d errorcode %d that says %s" % (
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

    def openOrder(
        self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState
    ):
        if not hasattr(self, "orders"):
            self.init_orders()
        openOrdertxt = (
            f"From openOrder -- "
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
        print(openOrdertxt)

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
        print(orderStatustxt)

    def execDetails(self, reqId, contract, execution):
        execDetailstxt = (
            f"Order Executed: "
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
        print(execDetailstxt)


class IBClient(EClient):
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)
        self.max_wait_time = 4

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
            reqId,
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

        # Call the accounts data.

        tags = (
            f"AccountType, TotalCashValue, AccruedCash, "
            f"NetLiquidation, BuyingPower, GrossPositionValue"
        )
        self.reqAccountSummary(9001, "All", tags)

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
                f"An integer is required. No action was taken.")
            return

        self.cancelOrder(order_id)

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return 0


class IBApp(IBWrapper, IBClient):
    ORDERTYPE_MAPPING = dict(market="MKT", limit="LMT", stop="STP")

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
        symbol,
        secType="STK",
        exchange="SMART",
        currency="USD",
        primaryExchage="ISLAND",
        lastTradeDateOrContractMonth="",
        strike="",
        right="",
        multiplier="",
    ):
        """Creates new contract objects. """
        contract = Contract()

        contract.symbol = str(symbol)
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        contract.primaryExchange = primaryExchage
        contract.lastTradeDateOrContractMonth = lastTradeDateOrContractMonth
        contract.strike = strike
        contract.right = right
        contract.multiplier = multiplier

        return contract

    def create_order(self, order):
        ib_order = Order()
        ib_order.action = order.side.upper()
        ib_order.orderType = self.ORDERTYPE_MAPPING[order.type]
        ib_order.totalQuantity = order.quantity
        ib_order.limit_price = (order.limit_price,)
        ib_order.stop_price = (order.stop_price,)
        ib_order.auxPrice = order.stop_price
        ib_order.transmit = order.transmit
        ib_order.orderId = order.identifier if order.identifier else self.nextOrderId()
        ib_order.parentId = order.parent_id

        return ib_order

    def execute_order(self, orders):
        # Create a queue to store the new order.
        new_order_storage = self.wrapper.init_new_orders()

        if not isinstance(orders, list):
            orders = [orders]

        ib_orders = []
        for order in orders:
            # Places the order with the returned contract and order objects
            contract_object = self.create_contract(order.symbol)
            order_object = self.create_order(order)
            nextID = (
                order_object.orderId if order_object.orderId else self.nextOrderId()
            )
            ib_orders.append((nextID, contract_object, order_object))

        for ib_order in ib_orders: # todo Single orders not making it through
            if ib_order[2].action == "BUY":
                print(ib_order[0])
                for k, v in ib_order[1].__dict__.items():
                    print(k, "\t", " - ", v)
                print("\n\n")
                for k, v in ib_order[2].__dict__.items():
                    print(k, "\t", " - ", v)
            self.placeOrder(*ib_order)

        try:
            requested_new_orders = []
            order_ids = [ibo[0] for ibo in ib_orders]
            get_order = True
            while get_order:
                requested_new_order = new_order_storage.get(timeout=self.max_wait_time)
                if requested_new_order not in requested_new_orders:
                    requested_new_orders.append(requested_new_order)

                # Check if all orders received.
                get_order = False
                for order_id in order_ids:
                    if order_id not in [rno.orderId for rno in requested_new_orders]:
                        get_order = True

        except queue.Empty:
            print(f"The queue was empty or max time reached for new order.")
            requested_new_orders = None

        while self.wrapper.is_error():
            print("Error:", self.get_error(timeout=5))

        # Sort the list by order number.
        requested_new_orders = sorted(
            requested_new_orders, key=lambda x: x.orderId, reverse=False
        )
        return requested_new_orders

    #
    # # =======Stream functions=========
    #
    # def _get_stream_object(self):
    #     """get the broker stream connection"""
    #     # stream = tradeapi.StreamConn(self.api_key, self.api_secret, self.endpoint)
    #     stream = self.get_connection()
    #     return stream
    #
    # def _register_stream_events(self):
    #     """Register the function on_trade_event
    #     to be executed on each trade_update event"""
    #     broker = self
    #
    #     @self.stream.on(r"^trade_updates$")
    #     async def on_trade_event(conn, channel, data):
    #         try:
    #             logged_order = data.order
    #             type_event = data.event
    #             identifier = logged_order.get("id")
    #             stored_order = broker.get_tracked_order(identifier)
    #             if stored_order is None:
    #                 logging.info(
    #                     "Untracker order %s was logged by broker %s"
    #                     % (identifier, broker.name)
    #                 )
    #                 return False
    #
    #             price = data.price if hasattr(data, "price") else None
    #             filled_quantity = data.qty if hasattr(data, "qty") else None
    #             broker._process_trade_event(
    #                 stored_order,
    #                 type_event,
    #                 price=price,
    #                 filled_quantity=filled_quantity,
    #             )
    #
    #             return True
    #         except:
    #             logging.error(traceback.format_exc())
    #
    # def _run_stream(self):
    #     """Overloading default alpaca_trade_api.STreamCOnnect().run()
    #     Run forever and block until exception is raised.
    #     initial_channels is the channels to start with.
    #     """
    #     loop = self.stream.loop
    #     should_renew = True  # should renew connection if it disconnects
    #     while should_renew:
    #         try:
    #             if loop.is_closed():
    #                 self.stream.loop = asyncio.new_event_loop()
    #                 loop = self.stream.loop
    #             loop.run_until_complete(self.stream.subscribe(["trade_updates"]))
    #             self._stream_established()
    #             loop.run_until_complete(self.stream.consume())
    #         except KeyboardInterrupt:
    #             logging.info("Exiting on Interrupt")
    #             should_renew = False
    #         except Exception as e:
    #             m = "consume cancelled" if isinstance(e, CancelledError) else e
    #             logging.error(f"error while consuming ws messages: {m}")
    #             if self.stream._debug:
    #                 logging.error(traceback.format_exc())
    #             loop.run_until_complete(self.stream.close(should_renew))
    #             if loop.is_running():
    #                 loop.close()
