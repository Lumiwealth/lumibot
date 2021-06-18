from itertools import cycle
import datetime
import logging
import time

from yfinance import Ticker, download
import pandas as pd

from lumibot.strategies.strategy import Strategy

"""
Strategy Description: Long Call Spread
Sells to open 1 month 10% OTM call options from list of companies and buys to open same 
company 1 month 15% OTM calls every time the net premium from both legs of that trade 
give an annual yield ((365/days to expiry)*(premium/strike) equal to or greater than 
10%.
"""


class LongCallSpread(Strategy):
    IS_BACKTESTABLE = False

    # =====Overloading lifecycle methods=============

    def initialize(self):
        self.time_start = time.time()
        # Set how often (in minutes) we should be running on_trading_iteration
        self.sleeptime = 2
        self.iter_count = 0

        # Initialize our variables
        self.total_trades = 0
        self.max_trades = 4
        self.quantity = 10
        self.sell_strike = 1.06
        self.buy_strike = 1.10
        self.annual_yield = 0.025  # 0.10 todo revert
        self.close_premium_factor = 0.8
        self.max_days_expiry = 35
        self.underlying_price_min = 20
        self.underlying_price_max = 300
        self.days_to_earnings_min = 15
        self.exchange = "SMART"
        self.time_to_fill = 2  # time to fill both sides of the trade. Minutes

        # Use these for higher options volume stocks.
        symbols_universe = [
            "AAL",
            "ABNB",
            "AMC",
            "AMD",
            "AMZN",
            "BABA",
            "BAC",
            "DIS",
            "DKNG",
            "EEM",
            "ET",
            "F",
            "FB",
            "FXI",
            "MARA",
            "MSFT",
            "NIO",
            "PLTR",
            "PLUG",
            "SQ",
            "TSLA",
            "UBER",
        ]

        # Underlying Asset Objects.
        self.trading_pairs = dict()
        for symbol in symbols_universe:
            self.create_trading_pair(symbol)

        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())

    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        for asset, options in self.trading_pairs.items():
            try:
                chains = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")
                continue

            # Get latest price, re-check if value not received.
            attempts = 2
            while attempts > 0:
                # Obtain latest price
                last_price = self.get_last_price(asset)
                if last_price == 0:
                    attempts -= 1
                    if attempts == 0:
                        logging.warning(f"Unable to get price data for {asset.symbol}.")
                        options["price_underlying"] = 0
                    continue
                else:
                    options["price_underlying"] = last_price
                    attempts = 0

            # Get dates from the options chain.
            options["expirations"] = self.get_expiration(chains, self.exchange)

            options["expiration_date"] = self.get_expiration_date(
                options["expirations"]
            )

            multiplier = self.get_chain(chains, self.exchange)["Multiplier"]

            (
                options["buy_call_strike"],
                options["sell_call_strike"],
            ) = self.buy_sell_strike(
                options["price_underlying"], asset.symbol, options["expiration_date"]
            )

            if not options["buy_call_strike"] or not options["sell_call_strike"]:
                logging.info(f"No options data for {asset.symbol}")
                continue

            # Create option assets.
            options["near"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["sell_call_strike"],
                right="CALL",
                multiplier=multiplier,
            )
            options["far"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["buy_call_strike"],
                right="CALL",
                multiplier=multiplier,
            )

    def on_trading_iteration(self):

        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]
        # Check if orders are properly filled, if not exit positions.
        # self.check_fills()

        # Sell positions:
        for asset, options in self.trading_pairs.items():
            if (
                options["near"] not in filled_assets
                and options["far"] not in filled_assets
            ):
                continue

            if options["status"] > 1:
                continue

            self.last_price = self.get_last_price(asset)
            if self.last_price == 0:
                continue

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

                options["status"] = 2
                self.total_trades -= 1

        # Create positions:
        if self.total_trades >= self.max_trades:
            return

        for _ in range(len(self.trading_pairs.keys())):
            asset = next(self.asset_gen)
            print(f"In trading iteration create position {asset}")
            options = self.trading_pairs[asset]
            if options["status"] > 0:
                continue
            if self.total_trades > self.max_trades:
                return

            # Check for symbol in positions.
            if len([p.symbol for p in positions if p.symbol == asset.symbol]) > 0:
                continue
            # Check if options already traded.
            if options["near"] in filled_assets or options["far"] in filled_assets:
                continue

            # Get the latest prices for stock and options.
            try:
                print(asset, options["near"], options["far"])
                asset_prices = self.get_last_prices(
                    [asset, options["near"], options["far"]]
                )

            except:
                logging.info(f"Failed to get price data for {asset.symbol}")
                continue

            options["price_underlying"] = asset_prices[asset]
            options["price_near"] = asset_prices[options["near"]]
            options["price_far"] = asset_prices[options["far"]]

            print(
                f"Called Prices for: ",
                asset.symbol,
                [v for v in asset_prices.values()],
            )

            # If the trade yield is high enough, trade.
            options = self.check_trade_yield(asset, options)
            if not options["trade_yield_ok"]:
                continue

            options["trade_created_time"] = datetime.datetime.now()
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

            self.total_trades += 1
            options['status'] = 1

        pos = self.get_tracked_positions()
        filla = [pos.asset for pos in positions]
        print(
            f"Positions: {pos} "
            f"Filled_assets: {filla} "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
        )

        # self.await_market_to_close()

    def on_abrupt_closing(self):
        # self.sell_all()
        self.quantity = 0
        self.asset = ""

    # =============Helper methods====================
    def create_trading_pair(self, symbol):
        # Add/update trading pair to self.trading_pairs
        self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
            "near": None,
            "far": None,
            "expirations": None,
            "strike_highs": None,
            "buy_call_strike": None,
            "sell_put_strike": None,
            "expiration_date": None,
            "price_underlying": None,
            "price_near": None,
            "price_far": None,
            "trade_created_time": None,
            "trade_yield": None,
            "trade_yield_ok": None,
            "status": 0,
        }

    def asset_cycle(self, assets):
        for asset in cycle(assets):
            yield asset

    def buy_sell_strike(self, last_price, symbol, expiration_date):
        """Returns strikes for pair."""
        buy_call_strike = None
        sell_call_strike = None

        asset = self.create_asset(
            symbol,
            asset_type="option",
            expiration=expiration_date,
            right="call",
        )
        contract_details = self.get_contract_details(asset)
        if not contract_details:
            return None, None

        strike_highs = sorted(
            [
                cd.contract.strike
                for cd in contract_details
                if cd.contract.strike > last_price
            ]
        )

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

    def check_trade_yield(self, asset, options):
        # Check to determine if trade yield is high enough to trade.
        exp_date = datetime.datetime.strptime(
            options["expiration_date"], "%Y%m%d"
        ).date()
        current_date = datetime.datetime.now().date()
        days_to_expiry = (exp_date - current_date).days
        premium_received = options["price_near"] - options["price_far"]
        trade_yield = (365 / days_to_expiry) * (
            premium_received / options["sell_call_strike"]
        )
        options["trade_yield_ok"] = trade_yield > self.annual_yield
        if not options["trade_yield_ok"]:
            logging.info(
                f"Trade yield for {asset.symbol} is {trade_yield} "
                f"which is less than {self.annual_yield} so "
                f"{asset.symbol} will not be traded."
            )

        options["trade_yield"] = trade_yield
        options["premium_received"] = premium_received
        return options

    def check_fills(self):
        # Check if positions are properly filled, if not liquidate the position.
        for asset, options in self.trading_pairs.items():
            if options["trade_created_time"] and datetime.datetime.now() > (
                options["trade_created_time"]
                + datetime.timedelta(minutes=self.time_to_fill)
            ):
                positions = self.get_tracked_positions()
                filla = [pos.asset for pos in positions]
                near_filled = options["near"] in filla
                far_filled = options["far"] in filla

                orders = []
                for order in self.get_tracked_orders():
                    if order.asset == options["near"] or order.asset == options["far"]:
                        orders.append(order)

                # Check if the option positions are only partially filled.
                if near_filled and far_filled:
                    print(f"CF: Positions filled: {asset}")
                    continue
                elif near_filled and not far_filled:
                    print(f"CF: LIQUIDATE  -  filled: {near_filled}, not filled"
                          f" {far_filled}")
                    self.cancel_orders(orders)
                    # Buy near filled cancel orders.
                    self.submit_order(
                        self.create_order(
                            options["near"],
                            self.quantity,
                            "buy",
                            exchange="CBOE",
                        )
                    )
                    options['status'] = 2
                    self.total_trades -= 1
                elif far_filled and not near_filled:
                    print(f"CF: LIQUIDATE  -  filled: {far_filled}, not "
                          f"filled {near_filled}")
                    self.cancel_orders(orders)
                    # Sell far filled cancel orders.
                    self.submit_order(
                        self.create_order(
                            options["far"],
                            self.quantity,
                            "sell",
                            exchange="CBOE",
                        )
                    )
                    options['status'] = 2
                    self.total_trades -= 1
