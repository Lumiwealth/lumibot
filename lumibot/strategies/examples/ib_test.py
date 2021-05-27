import datetime
import time
import numpy as np
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset

"""
Testing for IB Broker implementation. 
"""


class IBTest(Strategy):
    # =====Overloading lifecycle methods=============

    def __init__(self, name, budget, broker, **kwargs):
        super().__init__(name, budget, broker, **kwargs)
        self.symbols = ["FB", "TSLA", "MSFT", "F", "AAPL", "IBM"]
        self.assets = [Asset(t) for t in self.symbols]
        self.option = self.create_asset(
            symbol="UBER",
            asset_type="option",
            expiration="20210604",
            strike=51.5,
            right="CALL",
            multiplier=100,
        )
        self.count = 0

    def on_trading_iteration(self):

        # log_methods = dict(
        #     get_datetime={},
        #     get_timestamp={},
        #     get_round_minute={},
        #     get_last_minute={},
        #     get_round_day={},
        #     get_last_day={},
        #     get_datetime_range={"length": 40},
        # )
        #
        # self.check_function(log_methods)

        ##############
        #   Broker   #
        ##############
        # broker_methods = {
        # "broker.is_market_open": {},
        # "broker.get_time_to_open": {},
        # "broker.get_time_to_close": {},
        # }
        #
        # self.check_function(broker_methods)
        #
        # print(
        #     f"Unspent money before:  "
        #     f"{self._initial_budget}, "
        #     f"{self.unspent_money}, "
        #     f"{self._portfolio_value}"
        # )

        # Create orders, check status, cancel order.
        self.submit_order(self.create_order(self.symbols[-1], 10, "buy"))
        self.submit_order(self.create_order("IQ", 250, "buy", limit_price=80))
        self.submit_order(self.create_order("D", 10, "buy", stop_price=295))
        # self.submit_orders([
        #     self.create_order("TSLA", 10, "buy", limit_price=710),
        #     self.create_order("AAPL", 10, "buy", limit_price=135),
        # ])

        time.sleep(10)
        # Cancelling orders.
        # self.cancel_open_orders()
        self.sell_all(cancel_open_orders=True)

        # time.sleep(6)
        # print(
        #     f"Unspent money after:  "
        #     f"Initial Budget: {self._initial_budget} "
        #     f"Unspent money: {self.unspent_money} "
        #     f"Portfolio value: {self._portfolio_value} "
        #     f"Position in D:  {self.get_tracked_position(self.assets[2])} "
        # )
        #
        # print(
        #     f"From ib_test ORDERS: "
        #     f"{self.get_tracked_orders()}"
        # )
        # for symbol in self.symbols[-1:]:
        #     print(
        #         f"Positions: {symbol}: {self.get_tracked_position(symbol)} "
        #     )
        #
        # ##############
        # #  Options   #
        # ##############
        # # Buy an option creating an asset.
        # symbol = "AAPL"
        # exchange = "SMART"
        # right = "CALL"
        # expiration = "20210528"
        # strike = 150
        # opt_asset = self.create_asset(
        #     symbol,
        #     asset_type="option",
        #     expiration=expiration,
        #     strike=strike,
        #     right=right,
        #     multiplier=100,
        # )
        # self.submit_order(
        #     self.create_order(
        #         opt_asset,
        #         20,
        #         "sell",
        #         exchange="SMART",
        #     )
        # )

        # Buy an option with asset already created.
        # self.submit_order(
        #     self.create_order(
        #         self.option,
        #         20,
        #         "buy",
        #         exchange="SMART",
        #     )
        # )

        ##############
        # Data Source #
        ##############_
        # log_methods = dict(
            # get_symbol_bars={
            #     "asset": self.option,
            #     "timestep": "day",
            #     "length": 100,
            #     "timeshift": datetime.timedelta(days=20),
            # },
            # get_bars={"assets": self.assets, "length": 5},
            # get_last_price={"asset": self.assets[-1]},
            # get_last_prices={"assets": self.assets},
        # )
        #
        # self.check_function(log_methods)



        # ##############
        # #  Strategy  #
        # ##############
        # # Attributes #
        #
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

    def on_abrupt_closing(self):
        # self.sell_all()
        pass

    def check_function(self, log_methods):
        for lm, kwargs in log_methods.items():
            lm_eval = f"self.{lm}(**kwargs)"
            print(f"{lm}: {eval(lm_eval)}")
