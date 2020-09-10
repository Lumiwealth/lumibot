from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Thread

import datetime as dt
import time, logging
import alpaca_trade_api as tradeapi

class BlueprintBot:
    def __init__(self, api_key, api_secret, api_base_url="https://paper-api.alpaca.markets",
                 version='v2', logfile=None, max_workers=200):

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

        self.max_workers = min(max_workers, 200)
        self.logfile = logfile
        self.alpaca = tradeapi.REST(api_key, api_secret, api_base_url, version)
        self.positions = self.alpaca.list_positions()
        self.account = self.get_account()

    def update_positions(self):
        positions = self.alpaca.list_positions()
        self.positions = positions

    def cancel_buying_orders(self):
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)

    def is_market_open(self):
        isOpen = self.alpaca.get_clock().is_open
        return isOpen

    def get_time_to_open(self):
        clock = self.alpaca.get_clock()
        opening_time = clock.next_open.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_open = opening_time - curr_time
        return time_to_open

    def get_time_to_close(self):
        clock = self.alpaca.get_clock()
        closing_time = clock.next_close.replace(tzinfo=dt.timezone.utc).timestamp()
        curr_time = clock.timestamp.replace(tzinfo=dt.timezone.utc).timestamp()
        time_to_close = closing_time - curr_time
        return time_to_close

    def await_market_to_open(self):
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

    def get_account(self):
        account = self.alpaca.get_account()
        return account

    def get_tradable_assets(self):
        assets = self.alpaca.list_assets()
        assets = [asset for asset in assets if asset.tradable]
        return assets

    def get_last_price(self, symbol):
        bars = self.alpaca.get_barset(symbol, length=1)
        last_price = bars[symbol][0].c
        return last_price

    def get_percentage_change(self, symbol, time_unity='minute', length=10):
        bars = self.alpaca.get_barset(symbol, time_unity, length)
        first_value = bars[symbol][0].o
        last_value = bars[symbol][len(bars[symbol]) - 1].c
        change = (last_value - first_value) / first_value
        return change

    def get_percentage_changes(self, symbols, time_unity='minute', length=10):
        results = []
        threads = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for symbol in symbols:
                threads.append(executor.submit(
                    get_percentage_change, symbol,
                    time_unity=time_unity, length=length
                ))

            for task in as_completed(threads):
                results.append(task.result())
        return results

    def submit_order(self, qty, symbol, side, func_stop_price=None):
        if(qty > 0):
            try:
                stop_loss = {}
                if func_stop_price:
                    last_price = self.get_last_price(symbol)
                    stop_loss['stop_price'] = func_stop_price(last_price)

                kwargs = {
                    'type': 'market',
                    'time_in_force' : 'day'
                }
                if stop_loss: kwargs['stop_loss'] = stop_loss
                self.alpaca.submit_order(symbol, qty, side, **kwargs)
                logging.info("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                return True
            except:
                logging.error("Order of | " + str(qty) + " " + symbol + " " + side + " | did not go through.")
                return False
        else:
            logging.error \
                ("Quantity is not positive, order of | " + str(qty) + " " + symbol + " " + side + " | not completed.")
            return True

    def submit_orders(self, orders):
        all_threads = []
        for order in orders:
            kwargs = {}
            qty = order.get('qty')
            stock = order.get('stock')
            side = order.get('side')
            func_stop_price = order.get('func_stop_price')

            if func_stop_price: kwargs['func_stop_price'] = func_stop_price
            t = Thread(target=self.submit_order, args=[qty, stock, side], kwargs=kwargs)

            t.start()
            all_threads.append(t)

        for t in all_threads:
            t.join()

    def run(self):
        """This method needs to be overloaded
        by the child bot class"""
        self.cancel_buying_orders()
        self.await_market_to_open()
