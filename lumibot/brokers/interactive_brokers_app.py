from threading import Thread
import time

from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *

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

    def tickPrice(self, reqId, tickType):
        if not hasattr(self, "tick"):
            self.init_tick()
        self.tick.append(vars(tickType))

    def tickSnapshotEnd(self, reqId):
        super().tickSnapshotEnd(reqId)
        print("TickSnapshotEnd. TickerId:", reqId)
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
        self.max_wait_time = 4
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

        self.reqMktData(reqId, contract, "1, 2, 4", True, False, [])

        try:
            requested_tick = tick_storage.get(timeout=self.max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached for tick data.")
            requested_tick = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        self.cancelMktData(reqId)

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
            contract.strike = str(asset.strike)
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
        ib_order.action = order.side.upper()
        ib_order.orderType = self.ORDERTYPE_MAPPING[order.type]
        ib_order.totalQuantity = order.quantity
        ib_order.lmtPrice = order.limit_price if order.limit_price else 0
        ib_order.auxPrice = order.stop_price if order.stop_price else ""
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
            contract_object = self.create_contract(
                order.asset,
                exchange=order.exchange,
            )
            order_object = self.create_order(order)

            # There was an update by IB mid april/2021 that ceased support for
            # `eTradeOnly` and `firmQuoteOnly` attributes. However the defaults
            # where left `True` causing errors. Set to False
            order_object.eTradeOnly = False
            order_object.firmQuoteOnly = False

            nextID = (
                order_object.orderId if order_object.orderId else self.nextOrderId()
            )
            ib_orders.append((nextID, contract_object, order_object))

        for ib_order in ib_orders:
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