import datetime
import time
import numpy as np
from lumibot.strategies.strategy import Strategy

"""
Testing for IB Broker implementation. 
"""


class IBTest(Strategy):
    # =====Overloading lifecycle methods=============

    def __init__(self, name, budget, broker, **kwargs):
        super().__init__(name, budget, broker, **kwargs)
        self.symbols = ["FB", "TSLA", "MSFT", "F", "AAPL", "IBM"]

    def on_trading_iteration(self):
        """
        This needs to be run during market hours to check trades being received and
        executed by Interactive Brokers.
        """
        # self.sell_all(cancel_open_orders=True, at_broker=True)
        # return

        # contract.lastTradeDateOrContractMonth = lastTradeDateOrContractMonth
        # contract.strike = strike
        # contract.right = right
        # Options
        symbol = "AAPL"
        exchange = "SMART"
        last_price = self.get_last_price(symbol=symbol)
        print(f"{symbol} Price: ", last_price)
        contract_details = self.broker.get_contract_details(symbol=symbol)
        contract_id = contract_details[0].contract.conId
        chains = self.broker.options_params(symbol, underlyingConId=contract_id)
        print(chains)
        for x, p in chains.items():
            if x == exchange:
                print(type(p), p)
                expirations = sorted(list(p["Expirations"]))
                strikes = sorted(list(p["Strikes"]))
                strike_high = sorted([n for n in strikes if n > last_price])
                strike_low = sorted(
                    [n for n in strikes if n < last_price], reverse=True
                )
                print(strike_high)
                print(strike_low)
                print(f"{exchange}:\n{expirations}\n{strikes}")
        return
            # buy a call 145, expire: "20210507"
        right = "CALL"
        expiration = "20210521"
        strike = 150
        self.submit_order(
            self.create_order(
                symbol,
                10,
                "sell",
                exchange="CBOE",
                sec_type="OPT",
                expiration=expiration,
                strike=strike,
                right=right,
                multiplier=100,
            )
        )
        time.sleep(5555)
        # Check connection and times
        log_methods = dict(
            #     get_datetime={},
            get_timestamp={},
            #     get_round_minute={},
            #     get_last_minute={},
            #     get_round_day={},
            #     get_last_day={},
            #     get_datetime_range={"length": 40},
        )

        self.check_function(log_methods)

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
        # self.submit_order(self.create_order(self.symbols[-1], 10, "buy"))
        # self.submit_order(self.create_order("IQ", 250, "buy", limit_price=80))
        # self.submit_order(self.create_order("D", 10, "buy", stop_price=295))
        # self.submit_orders([
        #     self.create_order("TSLA", 10, "buy", limit_price=710),
        #     self.create_order("AAPL", 10, "buy", limit_price=135),
        # ])

        ## Cancelling orders.
        # self.cancel_open_orders()
        # self.cancel_order(1000463)
        # self.sell_all(cancel_open_orders=True)

        # time.sleep(6)
        # print(
        #     f"Unspent money after:  "
        #     f"Initial Budget: {self._initial_budget} "
        #     f"Unspent money: {self.unspent_money} "
        #     f"Portfolio value: {self._portfolio_value} "
        #     f"Position in D:  {self.get_tracked_position('D')} "
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

        # todo discuss Not a think in IB
        # self.get_tradable_assets(easy_to_borrow=True, filter_func=None)

        ##############
        # Data Source #
        ##############_
        # log_methods = dict(
        #
        # get_symbol_bars={
        #     "symbol": self.symbols[0],
        #     "timestep": "day",
        #     "length": 100,
        #     "timeshift": datetime.timedelta(days=20),
        # },
        # get_bars={"symbols": self.symbols, "length": 5},
        # get_last_price={"symbol": self.symbols[-1]},
        # get_last_prices={"symbols": self.symbols},
        # )
        #
        # self.check_function(log_methods)

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
