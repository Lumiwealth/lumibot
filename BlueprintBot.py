from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

import datetime as dt
import time, logging, math

import alpaca_trade_api as tradeapi
from alpaca_trade_api.common import URL

class BlueprintBot:
    def __init__(
        self, api_key, api_secret, api_base_url="https://paper-api.alpaca.markets",
        version='v2', logfile=None, max_workers=200, chunk_size=100,
        minutes_before_closing=15, sleeptime=1
    ):

        #Setting Logging to both console and a file if logfile is specified
        self.logfile = logfile
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        logFormater = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormater)
        logger.addHandler(consoleHandler)
        if logfile:
            fileHandler = logging.FileHandler(logfile, mode='w')
            fileHandler.setFormatter(logFormater)
            logger.addHandler(fileHandler)

        #Alpaca authorize 200 requests per minute and per API key
        #Setting the max_workers for multithreading to 200
        #to go full speed if needed
        self.max_workers = min(max_workers, 200)

        #When requesting data for assets for example,
        #if there is too many assets, the best thing to do would
        #be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        #Setting how many minutes before market closes
        #The bot should stop
        self.minutes_before_closing = minutes_before_closing

        #Timesleep after each on_market_open execution
        self.sleeptime = sleeptime

        #Connection to alpaca REST API
        self.alpaca = tradeapi.REST(api_key, api_secret, URL(api_base_url), version)

        #getting the account object
        self.account = self.get_account()

    #======Builtin helper functions=========
    def get_positions(self):
        """Get the account positions"""
        positions = self.alpaca.list_positions()
        return positions

    def get_open_orders(self):
        """Get the account open orders"""
        orders = self.alpaca.list_orders(status="open")
        return orders

    def cancel_buying_orders(self):
        """Cancel all the buying orders with status still open"""
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            logging.info("Market order of | %d %s %s | completed." % (int(order.qty), order.symbol, order.side))
            self.alpaca.cancel_order(order.id)

    def get_ongoing_assets(self):
        """Get the list of symbols for positions
        and open orders"""
        orders = self.get_open_orders()
        positions = self.get_positions()
        result = [o.symbol for o in orders] + [p.symbol for p in positions]
        return list(set(result))

    def is_market_open(self):
        """return True if market is open else false"""
        isOpen = self.alpaca.get_clock().is_open
        return isOpen

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        clock = self.alpaca.get_clock()
        opening_time = clock.next_open.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        clock = self.alpaca.get_clock()
        closing_time = clock.next_close.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    def get_account(self):
        """Get the account data from the API"""
        account = self.alpaca.get_account()
        return account

    def get_tradable_assets(self):
        """Get the list of all tradable assets from the market"""
        assets = self.alpaca.list_assets()
        assets = [asset for asset in assets if asset.tradable]
        return assets

    def get_last_price(self, symbol):
        """Takes and asset symbol and returns the last known price"""
        bars = self.alpaca.get_barset(symbol, 'minute', 1)
        last_price = bars[symbol][0].c
        return last_price

    def get_chunks(self, l, chunk_size=None):
        if chunk_size is None: chunk_size=self.chunk_size
        chunks = []
        for i in range(0, len(l), chunk_size):
            chunks.append(l[i:i + chunk_size])
        return chunks

    def get_bars(self, symbols, time_unity, length):
        bar_sets = {}
        chunks = self.get_chunks(symbols)
        with ThreadPoolExecutor() as executor:
            tasks = []
            for chunk in chunks:
                tasks.append(executor.submit(
                    self.alpaca.get_barset, chunk, time_unity, length
                ))

            for task in as_completed(tasks):
                bar_sets.update(task.result())

        return bar_sets

    def submit_order(self, symbol, quantity, side, stop_price_func=None):
        """Submit an order for an asset"""
        if(quantity > 0):
            try:
                stop_loss = {}
                if stop_price_func:
                    last_price = self.get_last_price(symbol)
                    stop_loss['stop_price'] = stop_price_func(last_price)

                kwargs = {
                    'type': 'market',
                    'time_in_force' : 'day',
                }

                if stop_loss:
                    kwargs['order_class'] = 'oto'
                    kwargs['stop_loss'] = stop_loss

                self.alpaca.submit_order(symbol, quantity, side, **kwargs)
                logging.info("Market order of | %d %s %s | completed." % (quantity, symbol, side))
                return True
            except Exception as e:
                logging.error(
                    "Market order of | %d %s %s | did not go through. The following eeror occured: %s" %
                    (quantity, symbol, side, e)
                )
                return False
        else:
            logging.error("Market order of | %d %s %s | not completed" % (quantity, symbol, side))
            return True

    def submit_orders(self, orders):
        """submit orders"""
        all_threads = []
        for order in orders:
            kwargs = {}
            symbol = order.get('symbol')
            quantity = order.get('quantity')
            side = order.get('side')
            stop_price_func = order.get('stop_price_func')
            if stop_price_func: kwargs['stop_price_func'] = stop_price_func

            t = Thread(target=self.submit_order, args=[symbol, quantity, side], kwargs=kwargs)
            t.start()
            all_threads.append(t)

        for t in all_threads:
            t.join()

    def sell_all(self, cancel_open_orders=True):
        """sell all positions"""
        orders = []
        positions = self.get_positions()
        for position in positions:
            order = {
                'symbol': position.symbol,
                'quantity': int(position.qty),
                'side': 'sell'
            }
            orders.append(order)
        self.submit_orders(orders)

        if cancel_open_orders:
            self.cancel_buying_orders()

    #=======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Lifecycle method executed before market opens
        Example: self.cancel_buying_orders()"""
        pass

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        isOpen = self.is_market_open()
        while(not isOpen):
            time_to_open = self.get_time_to_open()
            if time_to_open > 60 * 60:
                delta = dt.timedelta(seconds=time_to_open)
                logging.info("Market will open in %s." % str(delta))
                time.sleep(60 *60)
            elif time_to_open > 60:
                logging.info("%d minutes til market open." % int(time_to_open / 60))
                time.sleep(60)
            else:
                logging.info("%d seconds til market open." % time_to_open)
                time.sleep(time_to_open)

            isOpen = self.is_market_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            sleeptime = max(0, time_to_close)
            time.sleep(sleeptime)

    def on_market_open(self):
        """Use this lifecycle method for trading.
        Will be executed indefinetly until there
        will be only self.minutes_before_closing
        minutes before market closes"""
        pass

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
        pass

    def run(self):
        """The main execution point.
        Execute the lifecycle methods"""
        logging.info("Executing the initialize lifecycle method")
        self.initialize()

        logging.info("Executing the before_market_opens lifecycle method")
        if not self.is_market_open():
            self.before_market_opens()

        self.await_market_to_open()
        time_to_close = self.get_time_to_close()
        while time_to_close > self.minutes_before_closing * 60:
            logging.info("Executing the on_market_open lifecycle method")
            self.on_market_open()
            time_to_close = self.get_time_to_close()
            sleeptime = time_to_close - 15 * 60
            sleeptime = max(min(sleeptime, 60 * self.sleeptime), 0)
            logging.info("Sleeping for %d seconds" % sleeptime)
            time.sleep(sleeptime)

        if self.is_market_open():
            logging.info("Executing the before_market_closes lifecycle method")
            self.before_market_closes()

        self.await_market_to_close()
        logging.info("Executing the after_market_closes lifecycle method")
        self.after_market_closes()
