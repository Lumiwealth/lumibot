import datetime
import logging
import random
import sys
import time

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Buys and sells 10 of self.buy_symbol every day (not meant to make money, just an example).
For example, Day 1 it will buy 10 shares, Day 2 it will sell all of them, Day 3 it will 
buy 10 shares again, etc.
"""


class Simple(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        pass
        # Set the initial variables or constants

        # Built in Variables
        # self.sleeptime = 1

        # # Our Own Variables
        self.counter = 0
        # self.buy_symbol = "AGG"

    def before_market_opens(self):

        symbols = ["FB", "TSLA", "MSFT", "F", "AAPL"]
        symbol = "BRK A"

        # self.cancel_open_orders()
        # self.sell_all(cancel_open_orders=True)
        # for symbol in symbols:
        #     new_order = self.create_order(symbol, 10, "buy")
        #     self.submitted_order = self.submit_order(new_order)

        # Dictionary for inserting methods to log, parameters in sub-dict.
        ##############
        # Data Source #
        ##############_
        # log_methods = dict(
        #     get_datetime={},
        #     get_timestamp={},
        #     get_round_minute={},
        #     get_last_minute={},
        #     get_round_day={},
        #     get_last_day={},
        #     get_datetime_range={"length": 40},
        #     get_symbol_bars={
        #         "symbol": symbol,
        #         "timestep": "day",
        #         "length": 100,
        #         "timeshift": datetime.timedelta(days=20),
        #     },
        #     get_bars={"symbols": symbols, "length": 5},
        #     get_last_price={"symbol": symbol},
        #     get_last_prices={"symbols": symbols},
        # )
        ###############
        #   Broker    #
        ###############
        # log_methods = {
        #     "broker.is_market_open": {},
        #     "broker.get_time_to_open": {},
        #     "broker.get_time_to_close": {},
        # }

        ###############
        #   Strategy    #
        ###############
        # Attributes
        # log_attributes = {
        #     "name": self._name,
        #     "initial_budget": self.initial_budget,
        #     "minutes_before_closing": self.minutes_before_closing,
        #     "sleeptime": self.sleeptime,
        #     "parameters": self.parameters,
        #     "is_backtesting": self.is_backtesting,
        #     "portfolio_value": self.portfolio_value,
        #     "unspent_money": self.unspent_money,
        #     "stats_file": self.stats_file,
        #     "stats": self.stats,
        #     "analysis": self.analysis,
        #     "risk_free_rate": self.risk_free_rate,
        # }
        # for la in log_attributes.items():
        #     print(la[0], ": ", la[1])

        # Methods
        # log_methods = {
        #     "broker.get_time_to_open": {},
        #     "broker.get_time_to_close": {},
        #     "broker._pull_broker_position": {"symbol": "EUR"},
        #     "broker._pull_broker_positions": {},
        #     "broker._pull_position": {"strategy": "Simple", "symbol": "EUR"},
        #     "broker._pull_positions": {"strategy": "Simple"},
        # }
        #
        # self.check_function(log_methods)
        pass

    def check_function(self, log_methods):
        for lm, kwargs in log_methods.items():
            lm_eval = f"self.{lm}(**kwargs)"
            print(f"{lm}: \n{eval(lm_eval)}")


    def on_trading_iteration(self):
        symbols = ["FB", "TSLA", "MSFT", "F", "AAPL"]
        symbol = "BRK A"

        # self.get_tradable_assets(easy_to_borrow=True, filter_func=None)
        # self.cancel_open_orders()
        # self.cancel_order(1000324)
        # self.sell_all(cancel_open_orders=True)

        for symbol in symbols[:1]:
            new_order = self.create_order(symbol, 10, "buy", stop_price=295)
            # new_order = self.create_order(symbol, 10, "buy") # todo Not working
            self.submitted_order = self.submit_order(new_order)

        # Dictionary for inserting methods to log, parameters in sub-dict.
        ##############
        # Data Source #
        ##############_
        log_methods = dict(
            # get_datetime={},
            get_timestamp={},
            # get_round_minute={},
            # get_last_minute={},
            # get_round_day={},
            # get_last_day={},
            # get_datetime_range={"length": 40},
            # get_symbol_bars={
            #     "symbol": symbol,
            #     "timestep": "day",
            #     "length": 100,
            #     "timeshift": datetime.timedelta(days=20),
            # },
            # get_bars={"symbols": symbols, "length": 5},
            # get_last_price={"symbol": symbol},
            # get_last_prices={"symbols": symbols},
        )

        self.check_function(log_methods)

        ###############
        #   Broker    #
        ###############
        # log_methods = {
            # "broker.is_market_open": {},
            # "broker.get_time_to_open": {},
            # "broker.get_time_to_close": {},
        # }
        #
        # self.check_function(log_methods)

        ###############
        #   Strategy    #
        ###############
        # Attributes
        # log_attributes = {
        #     "name": self._name,
        #     "initial_budget": self.initial_budget,
        #     "minutes_before_closing": self.minutes_before_closing,
        #     "sleeptime": self.sleeptime,
        #     "parameters": self.parameters,
        #     "is_backtesting": self.is_backtesting,
        #     "portfolio_value": self.portfolio_value,
        #     "unspent_money": self.unspent_money,
        #     "stats_file": self.stats_file,
        #     "stats": self.stats,
        #     "analysis": self.analysis,
        #     "risk_free_rate": self.risk_free_rate,
        # }
        # for la in log_attributes.items():
        #     print(la[0], ": ", la[1])

        # Methods
        # log_methods = {
        #     "broker.get_time_to_open": {},
        #     "broker.get_time_to_close": {},
        #     "broker._pull_broker_position": {"symbol": "EUR"},
        #     "broker._pull_broker_positions": {},
        #     "broker._pull_position": {"strategy": "Simple", "symbol": "EUR"},
        #     "broker._pull_positions": {"strategy": "Simple"},
        # }
        #
        # self.check_function(log_methods)

    def on_abrupt_closing(self):
        # self.sell_all()
        pass

    def trace_stats(self, context, snapshot_before):
        random_number = random.randint(0, 100)
        row = {"my_custom_stat": random_number, "counter": self.counter}

        return row
