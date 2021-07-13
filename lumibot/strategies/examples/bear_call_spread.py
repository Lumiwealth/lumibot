from itertools import cycle
import datetime
import logging
import time

from yfinance import Ticker, download
import pandas as pd

from lumibot.strategies.strategy import Strategy

"""
Bear Call Spread
Strategy Description
Sells to open 1 month 10% OTM call options from list of companies and buys to open same 
company 1 month 15% OTM calls every time the net premium from both legs of that trade 
give an annual yield ((365/days to expiry)*(premium/strike) equal to or greater than 
10%.
"""


class BearCallSpread(Strategy):
    IS_BACKTESTABLE = False

    # =====Overloading lifecycle methods=============

    def initialize(self):
        self.time_start = time.time()
        # Set how often (in minutes) we should be running on_trading_iteration
        self.sleeptime = 4
        self.iter_count = 0


        # Initialize our variables
        self.total_trades = 0
        self.max_trades = 4
        self.quantity = 10
        self.sell_strike = 1.06
        self.buy_strike = 1.10
        self.annual_yield = 0.10
        self.close_premium_factor = 0.8
        self.max_days_expiry = 35
        self.underlying_price_min = 20
        self.underlying_price_max = 300
        self.days_to_earnings_min = 15
        self.exchange = "SMART"

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

        # Set the symbols that we want to be monitoring. Leave blank for s&p500
        self.symbols_price = self.get_symbols_universe(symbols_universe)

        # Underlying Asset Objects.
        self.trading_pairs = dict()
        for symbol, price in self.symbols_price.iterrows():
            self.create_trading_pair(symbol, price)


    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())

        for asset, options in self.trading_pairs.items():
            try:
                chains = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")
                continue

            options["expirations"] = self.get_expiration(chains)

            options["expiration_date"] = self.get_expiration_date(
                options["expirations"]
            )

            multiplier = self.get_chain(chains)["Multiplier"]

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

        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]

        # Sell positions:
        for asset, options in self.trading_pairs.items():
            if (
                options["near"] not in filled_assets
                and options["far"] not in filled_assets
            ):
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
            if self.total_trades >= self.max_trades:
                break

            asset = next(self.asset_gen)
            print(f"In trading iteration create position {asset}")
            options = self.trading_pairs[asset]
            if options["status"] > 0:
                continue

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
                assert(len(asset_prices) == 3)
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
            options['near_order'] = self.create_order(
                    options["near"],
                    self.quantity,
                    "sell",
                    exchange="CBOE",
                )
            self.submit_order(options["near_order"])

            # Buy far call.
            options["far_order"] =  self.create_order(
                    options["far"],
                    self.quantity,
                    "buy",
                    exchange="CBOE",
                )
            self.submit_order(options["far_order"])

            self.total_trades += 1
            options["status"] = 1

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
    def create_trading_pair(self, symbol, price):
        # Add/update trading pair to self.trading_pairs
        self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
            "near": None,
            "far": None,
            "expirations": None,
            "strike_highs": None,
            "buy_call_strike": None,
            "sell_call_strike": None,
            "expiration_date": None,
            "price_underlying": price[0],
            "price_near": None,
            "price_far": None,
            "trade_created_time": None,
            "trade_yield": None,
            "trade_yield_ok": None,
            "near_order": None,
            "far_order": None,
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

    def get_symbols_universe(self, symbols_universe=None):
        """Get all symbols that qualify for option trading."""
        if not symbols_universe:
            # Set the symbols that we want to be monitoring
            table = pd.read_html(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            )
            symbols_universe = table[0]["Symbol"].tolist()
            symbols_universe = [s.replace(".", "-") for s in symbols_universe]

        print(
            f"Downloading {len(symbols_universe)} symbols from yfinance to get last "
            f"prices. This may take a minute..."
        )

        start = "2021-05-11"
        end = "2021-05-16"

        if len(symbols_universe) == 1:
            symbols = download(
                symbols_universe, start=start, end=end)[["Close"]]
            symbols.columns = symbols_universe
            symbols = pd.DataFrame(symbols.iloc[-1, :].T)
        else:
            symbols = download(
                symbols_universe, start=start, end=end, group_by="column", threads=True
            )["Close"].T.iloc[:, -1:]

        symbols[
            (symbols > self.underlying_price_min)
            & (symbols < self.underlying_price_max)
        ].dropna()

        return symbols

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
