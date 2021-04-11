from datetime import datetime
import pandas as pd

from lumibot.entities import Bars

from .data_source import DataSource

from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from threading import Thread
import queue
import datetime
import time


class InteractiveBrokersData(DataSource):
    """Make Interactive Brokers connection and gets data.

    Create connection to Interactive Brokers market through either Gateway or TWS
    which must be running locally for connection to be made.
    """

    SOURCE = "InteractiveBrokers"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [
                "1 min",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "1 day",
            ],
        },
    ]

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    @staticmethod
    def _format_ib_datetime(dt):
        return pd.Timestamp(dt).strftime("%Y%m%d %H:%M:%S")

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        self.name = "interactivebrokers"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to interactive brokers
        self.ib = None
        self.start_ib(config.IP, config.SOCKET_PORT, config.CLIENT_ID)

    def start_ib(self, ip, socket_port, client_id):
        # Connect to interactive brokers.
        if not self.ib:
            self.ib = IBApp(ip, socket_port, client_id)
            self.ib.reqHistoricalDataId = -1

    def _parse_duration(self, length, timestep):
        # Converts length and timestep into IB `durationStr`
        if timestep == "minute":
            # IB has a max for seconds of 86400.
            return f"{str(min(length * 60, 86400))} S"
        elif timestep == "day":
            return f"{str(length)} D"
        else:
            raise ValueError(
                f"Timestep must be `day` or `minute`, you entered: {timestep}"
            )

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull broker bars for a given symbol"""
        response = self._pull_source_bars(
            [symbol], length, timestep=timestep, timeshift=timeshift
        )
        return response[symbol]

    def _pull_source_bars(self, symbols, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list symbols"""

        # Initial vars,
        self.ib.data = list()
        symbol_ref = dict()
        reqId = self.ib.reqHistoricalDataId

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        parsed_duration = self._parse_duration(length, timestep)

        if timeshift:
            end = datetime.datetime.now() - timeshift
            end = self.to_default_timezone(end)
            end_date_time = self._format_ib_datetime(end)
            type="TRADES"
        else:
            end_date_time = ""
            type="ADJUSTED_LAST"

        # Call data.
        for symbol in symbols:
            reqId += 1
            contract = self.create_contract(symbol)
            self.ib.reqHistoricalData(
                reqId,
                contract,
                end_date_time,
                parsed_duration,
                parsed_timestep,
                type,
                1,
                2,
                False,
                [],
            )
            symbol_ref[reqId] = symbol

        # Wait for data to return.
        while self.ib.historicalDataEndId != reqId:
            time.sleep(0.1)
        time.sleep(1)  # todo modify or remove, check to see better way to handle.

        # Collect results.
        result = dict()
        for reqId, symbol in symbol_ref.items():
            df_list = [
                bar_data for bar_data in self.ib.data if bar_data["reqId"] == reqId
            ]
            df = pd.DataFrame(df_list)
            cols = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "barCount",
                "average",
            ]
            df = df[cols]
            result[symbol] = df
            if parsed_timestep == "1 min":
                df["date"] = pd.to_datetime(df["date"], unit="s", origin="unix")
            elif parsed_timestep == "1 day":
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
        return result

    def _parse_source_symbol_bars(self, response, symbol):
        df = response.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["stock_splits"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]

        df = df[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "price_change",
                "dividend",
                "stock_splits",
                "dividend_yield",
                "return",
            ]
        ]
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars

    def get_yesterday_dividend(self, symbol):
        """ Unavailable """
        return 0

    def get_yesterday_dividends(self, symbols):
        """ Unavailable """
        return 0

    ##### IB TWS METHODS #####
    def create_contract(
        self,
        symbol,
        secType="STK",
        exchange="SMART",
        currency="USD",
        primaryExchage="ISLAND",
    ):
        """Creates new contract objects. """
        contract = Contract()

        contract.symbol = str(symbol)
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        contract.primaryExchange = primaryExchage

        return contract

    def create_order(self):
        # Fills out the order object
        order1 = Order()  # Creates an order object from the import
        order1.action = "BUY"  # Sets the order action to buy
        order1.orderType = "MKT"  # Sets order type to market buy
        order1.transmit = True
        order1.totalQuantity = 10  # Setting a static quantity of 10
        return order1  # Returns the order object

    def execute_order(self):
        # Places the order with the returned contract and order objects
        contract_object = self.create_contract()
        order_object = self.create_order()
        nextID = self.ib.nextOrderId()
        print("The next valid id is - " + str(nextID))
        self.ib.placeOrder(nextID, contract_object, order_object)
        print("order was placed")


##### IB TWS CLASSES #####
class IBWrapper(EWrapper):
    def __init__(self):
        super().__init__()
        self.data = []
        self.historicalDataEndId = -1
        self.all_positions = pd.DataFrame(
            [], columns=["Account", "Symbol", "Quantity", "Average Cost", "Sec Type"]
        )

    ## error handling code
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
        ## Overrides the native method
        errormessage = "IB returns an error with %d errorcode %d that says %s" % (
            id,
            errorCode,
            errorString,
        )
        self.my_errors_queue.put(errormessage)

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def init_time(self):
        time_queue = queue.Queue()
        self.my_time_queue = time_queue
        return time_queue

    def currentTime(self, server_time):
        ## Overriden method
        self.my_time_queue.put(server_time)

    def historicalData(self, reqId, bar):
        bar_data = vars(bar)
        bar_data["reqId"] = reqId
        self.data.append(bar_data)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self.historicalDataEndId = reqId

    def position(self, account, contract, pos, avgCost):
        index = str(account) + str(contract.symbol)
        self.all_positions.loc[index] = (
            account,
            contract.symbol,
            pos,
            avgCost,
            contract.secType,
        )


class IBClient(EClient):
    # Below is the IBClient/EClient Class
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

    def get_timestamp(self):

        print("Asking server for Unix time")

        # Creates a queue to store the time
        time_storage = self.wrapper.init_time()

        # Sets up a request for unix time from the Eclient
        self.reqCurrentTime()

        # Specifies a max wait time if there is no connection
        max_wait_time = 10

        try:
            requested_time = time_storage.get(timeout=max_wait_time)
        except queue.Empty:
            print("The queue was empty or max time reached")
            requested_time = None

        while self.wrapper.is_error():
            print(f"Error: {self.get_error(timeout=5)}")

        return requested_time


class IBApp(IBWrapper, IBClient):
    # Intializes our main classes
    def __init__(self, ipaddress, portid, clientid):
        IBWrapper.__init__(self)
        IBClient.__init__(self, wrapper=self)

        # Connects to the server with the ipaddress, portid, and clientId specified in the program execution area
        self.connect(ipaddress, portid, clientid)

        # Initializes the threading
        thread = Thread(target=self.run)
        thread.start()
        setattr(self, "_thread", thread)

        # Starts listening for errors
        self.init_error()
