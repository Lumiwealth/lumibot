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
        self.symbols = [
            "SPY",
            "EEM",
            "AAPL",
            "QQQ",
            "FXI",
            "AMD",
            "BAC",
            "MSFT",
            "GLD",
            "FB",
            "DIS",
            "TSLA",
        ]
        # Underlying Asset Objects.
        self.trading_pairs = dict()
        for symbol in self.symbols:
            self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
                "near": None,
                "far": None,
                "expirations": None,
                "strike_lows": None,
                "strike_highs": None,
                "buy_call_strike": None,
                "sell_call_strike": None,
                "expiration_date": None,
                "price_underlying": None,
                "price_near": None,
                "price_far": None,
                "trade_created_time": None,
                "trade_yield": None,
            }

        # Initialize our variables
        self.total_trades = 0
        self.max_trades = 3
        self.quantity = 100
        self.sell_strike = 1.06
        self.buy_strike = 1.10
        self.annual_yield = 0.05  # todo change this back to .1
        self.close_premium_factor = 0.8
        self.trades_per_company = 1
        self.max_days_expiry = 25  # todo 35
        self.underlying_price_min = 20
        self.underlying_price_max = 300
        self.days_to_earnings = 15
        self.exchange = "SMART"

    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        for asset, options in self.trading_pairs.items():
            self.last_price = self.get_last_price(asset)
            if (
                self.last_price < self.underlying_price_min
                or self.last_price > self.underlying_price_max
            ):
                logging.info(
                    f"The price {self.last_price} of {asset.symbol} is outside the "
                    f"acceptable price range of {self.underlying_price_min} to"
                    f" {self.underlying_price_max}"
                )
                continue

            chains = self.get_chains(asset)
            (
                options["expirations"],
                options["strike_lows"],
                options["strike_highs"],
            ) = self.get_expiration_strikes(chains)
            (
                options["buy_call_strike"],
                options["sell_call_strike"],
            ) = self.buy_sell_strike(self.last_price, options["strike_highs"])
            options["expiration_date"] = self.get_expiration_date(
                options["expirations"]
            )
            # Create option assets.
            options["near"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["sell_call_strike"],
                right="CALL",
                multiplier=100,
            )
            options["far"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["buy_call_strike"],
                right="CALL",
                multiplier=100,
            )

    def on_trading_iteration(self):
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]

        # Sell positions:
        for asset, options in self.trading_pairs.items():
            if (
                options["near"] not in filled_assets
                and options["far"] not in filled_assets
            ):
                continue

            asset_prices = self.get_last_prices(
                [asset, options["near"], options["far"]]
            )
            close_premium = options["price_far"] - options["price_near"]
            if options["premium_received"] >= close_premium * self.close_premium_factor:
                # Buy near call.
                self.submit_order(
                    self.create_order(
                        options["near"],
                        self.quantity,
                        "buy",
                        exchange="CBOE",
                    )
                )

                # Sell far call.
                self.submit_order(
                    self.create_order(
                        options["far"],
                        self.quantity,
                        "sell",
                        exchange="CBOE",
                    )
                )

        # Create positions:
        if self.total_trades > self.max_trades:
            return

        for asset, options in self.trading_pairs.items():
            if options["near"] in filled_assets or options["far"] in filled_assets:
                continue

            try:
                asset_prices = self.get_last_prices(
                    [asset, options["near"], options["far"]]
                )

                options["price_underlying"] = asset_prices[asset]
                options["price_near"] = asset_prices[options["near"]]
                options["price_far"] = asset_prices[options["far"]]

                print(
                    f"Called Prices for: ",
                    asset.symbol,
                    [v for v in asset_prices.values()],
                )

                # (365/days to expiry)*(premium/strike)
                exp_date = datetime.datetime.strptime(
                    options["expiration_date"], "%Y%m%d"
                ).date()
                current_date = datetime.datetime.now().date()
                days_to_expiry = (exp_date - current_date).days
                premium_received = options["price_near"] - options["price_far"]
                trade_yield = (365 / days_to_expiry) * (
                    (premium_received) / options["sell_call_strike"]
                )
                if trade_yield > self.annual_yield:
                    self.total_trades += 1
                    options["trade_created_time"] = datetime.datetime.now()
                    options["trade_yield"] = trade_yield
                    options["premium_received"] = premium_received

                    # Sell near call.
                    self.submit_order(
                        self.create_order(
                            options["near"],
                            self.quantity,
                            "sell",
                            exchange="CBOE",
                        )
                    )

                    # Buy far call.
                    self.submit_order(
                        self.create_order(
                            options["far"],
                            self.quantity,
                            "buy",
                            exchange="CBOE",
                        )
                    )

            except:
                logging.info(f"Failed to get price data for {asset.symbol}")
                continue


    def before_market_closes(self):
        # Make sure that we sell everything before the market closes
        self.sell_all()
        self.quantity = 0
        self.asset = ""

    def on_abrupt_closing(self):
        self.sell_all()
        self.quantity = 0
        self.asset = ""

    # =============Helper methods====================
    def get_chains(self, asset):
        """Returns option chain on specific exchange. ."""
        contract_details = self.broker.get_contract_details(asset=asset)
        contract_id = contract_details[0].contract.conId
        chains = self.options_params(asset, underlyingConId=contract_id)
        if len(chains) == 0:
            raise AssertionError(f"No option chain for {asset.symbol}")
        # print(chains)
        return chains

    def get_expiration_strikes(self, chains):
        """Returns expirations and strikes high/low of target price."""
        expirations = []
        strike_lows = []
        strike_highs = []
        for x, p in chains.items():
            if x == self.exchange:
                # print(type(p), p)
                expirations = sorted(list(p["Expirations"]))
                strikes = sorted(list(p["Strikes"]))
                strike_highs = sorted([n for n in strikes if n > self.last_price])
                strike_lows = sorted(
                    [n for n in strikes if n < self.last_price], reverse=True
                )
                # print(strike_highs)
                # print(strike_lows)
                # print(f"{self.exchange}:\n{expirations}\n{strikes}")

        return expirations, strike_lows, strike_highs

    def buy_sell_strike(self, last_price, strike_highs):
        """Returns strikes for pair."""
        buy_call_strike = None
        sell_call_strike = None

        # Loop through strike_highs in order.
        for strike_high in strike_highs:
            if strike_high < last_price * self.sell_strike:
                continue
            elif strike_high > last_price * self.sell_strike and not sell_call_strike:
                sell_call_strike = strike_high
            elif (
                strike_high > last_price * self.buy_strike
                and sell_call_strike
                and not buy_call_strike
            ):
                buy_call_strike = strike_high
            else:
                continue

        return buy_call_strike, sell_call_strike

    def get_expiration_date(self, expirations):
        """Expiration date that is closest to, but less than max days to expriry. """
        expiration_date = None
        # Expiration
        current_date = datetime.datetime.now().date()
        for expiration in expirations:
            ex_date = datetime.datetime.strptime(expiration, "%Y%m%d").date()
            net_days = (ex_date - current_date).days
            if net_days < self.max_days_expiry:
                expiration_date = expiration

        return expiration_date
