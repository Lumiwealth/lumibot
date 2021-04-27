import datetime
from lumibot.strategies.strategy import Strategy

"""
Testing for IB Broker implementation. 
"""


class IBTest(Strategy):
    # =====Overloading lifecycle methods=============

    def __init__(self, name, budget, broker, **kwargs):
        super().__init__(name, budget, broker, **kwargs)
        self.symbols = ["FB", "TSLA", "MSFT", "F", "AAPL"]

    def on_trading_iteration(self):
        """
        This needs to be run during market hours to check trades being received and
        executed by Interactive Brokers.
        """

        # Options
        # symbol = "IBM"
        # last_date = "202105"
        # self.broker.options_details(symbol=symbol, last_date=last_date)
        # self.broker.options_params(symbol)

    # Check connection and times
    #     log_methods = dict(
    #         get_datetime={},
    #         get_timestamp={},
    #         get_round_minute={},
    #         get_last_minute={},
    #         get_round_day={},
    #         get_last_day={},
    #         get_datetime_range={"length": 40},
    #     )
    #
    #     self.check_function(log_methods)

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

        # print(
        #     f"Unspent money before:  "
        #     f"{self._initial_budget}, "
        #     f"{self.unspent_money}, "
        #     f"{self._portfolio_value}"
        # )

        # Create orders, check status, cancel order.
        # self.submit_order(self.create_order("IBM", 10, "buy"))
        # self.submit_order(self.create_order("FB", 10, "buy", stop_price=295))
        # self.submit_orders([
        #     self.create_order("TSLA", 10, "buy", limit_price=710),
        #     self.create_order("AAPL", 10, "buy", limit_price=135),
        # ])

        ## Cancelling orders.
        # self.cancel_open_orders()
        # self.cancel_order(1000324)
        # self.sell_all(cancel_open_orders=True)

        # print(
        #     f"Unspent money after:  "
        #     f"{self._initial_budget}, "
        #     f"{self.unspent_money}, "
        #     f"{self._portfolio_value}"
        # )
        #
        # print(
        #     f"From ib_test ORDERS: "
        #     f"{self.get_tracked_orders()}"
        # )
        # for symbol in self.symbols:
        #     print(
        #         f"Positions: {symbol}: {self.get_tracked_position(symbol)} "
        #         # todo not getting positions
        #     )

        # todo discuss Not a think in IB
        # self.get_tradable_assets(easy_to_borrow=True, filter_func=None)


        ##############
        # Data Source #
        ##############_
        log_methods = dict(

        get_symbol_bars={
            "symbol": self.symbols[0],
            "timestep": "day",
            "length": 100,
            "timeshift": datetime.timedelta(days=20),
        },
        get_bars={"symbols": self.symbols, "length": 5},
        get_last_price={"symbol": self.symbols[-1]},
        get_last_prices={"symbols": self.symbols},
        )

        self.check_function(log_methods)

        ##############
        #  Strategy  #
        ##############
        # Attributes #

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


