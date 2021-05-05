import datetime
import logging
import time

from lumibot.strategies.strategy import Strategy

"""
Strategy Description
Sells to open 1 month 10% OTM call options from list of companies and buys to open same 
company 1 month 15% OTM calls every time the net premium from both legs of that trade 
give an annual yield ((365/days to expiry)*(premium/strike) equal to or greater than 
10%.
"""


class Option(Strategy):
    IS_BACKTESTABLE = False

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set how often (in minutes) we should be running on_trading_iteration
        self.sleeptime = 1

        # Set the symbols that we want to be monitoring
        self.symbols = ["AAPL", "BAC", "AMZN", "MSFT", "TSLA", "FB"]
        self.symbol = "FB"

        # Initialize our variables
        self.total_trades = 0
        self.max_trades = 10
        self.quantity = 100
        self.sell_strike = 1.1
        self.buy_strike = 1.15
        self.annual_yield = .10
        self.close_premium = 0.8
        self.trades_per_company = 1
        self.max_days_expiry = 35
        self.underlying_price_min = 20
        self.underlying_price_max = 300
        self.days_to_earnings = 15


    def on_trading_iteration(self):

        self.last_price = self.get_last_price(symbol=self.symbol)
        print(f"We a are connected, this is the last price. {self.last_price}")
        exchange = "SMART"
        contract_details = self.broker.get_contract_details(symbol=self.symbol)
        contract_id = contract_details[0].contract.conId
        chains = self.broker.options_params(self.symbol, underlyingConId=contract_id)
        if len(chains) == 0:
            raise AssertionError(f"No option chain for {self.symbol}")
        print(chains)

        for x, p in chains.items():
            if x == exchange:
                print(type(p), p)
                expirations = sorted(list(p["Expirations"]))
                strikes = sorted(list(p["Strikes"]))
                strike_highs = sorted([n for n in strikes if n > self.last_price])
                strike_lows = sorted(
                    [n for n in strikes if n < self.last_price], reverse=True
                )
                print(strike_highs)
                print(strike_lows)
                print(f"{exchange}:\n{expirations}\n{strikes}")

        sell_call_strike = None
        buy_call_strike = None

        # Loop through strike_highs in order.
        for strike_high in strike_highs:
            if strike_high < self.last_price * self.sell_strike:
                continue
            elif strike_high > self.last_price * self.sell_strike and not \
                    sell_call_strike:
                sell_call_strike = strike_high
            elif strike_high > self.last_price * self.buy_strike and \
                    sell_call_strike and not buy_call_strike:
                buy_call_strike = strike_high
            else:
                continue

        expiration_date = None
        # Expiration
        for expiration in expirations:
            current_date = datetime.datetime.now().date()
            ex_date = datetime.datetime.strptime(expiration, "%Y%m%d").date()
            net_days = (ex_date - current_date).days
            if net_days < self.max_days_expiry:
                expiration_date = expiration

        # Create the trade
        # Sell near call.
        self.submit_order(
            self.create_order(
                self.symbol,
                self.quantity,
                "sell",
                exchange="CBOE",
                sec_type="OPT",
                expiration=expiration_date,
                strike=sell_call_strike,
                right="CALL",
                multiplier=100,
            )
        )

        # Buy far call.
        self.submit_order(
            self.create_order(
                self.symbol,
                self.quantity,
                "buy",
                exchange="CBOE",
                sec_type="OPT",
                expiration=expiration_date,
                strike=buy_call_strike,
                right="CALL",
                multiplier=100,
            )
        )
        time.sleep(55555555)
        x=1

    def before_market_closes(self):
        # Make sure that we sell everything before the market closes
        self.sell_all()
        self.quantity = 0
        self.asset = ""

    def on_abrupt_closing(self):
        self.sell_all()
        self.quantity = 0
        self.asset = ""

