from itertools import cycle
import datetime
import logging
import time

from yfinance import Ticker, download
import pandas as pd

from lumibot.strategies.strategy import Strategy


class Strangle(Strategy):
    """Strategy Description: Strangle

    In a long strangle—the more common strategy—the investor simultaneously buys an
    out-of-the-money call and an out-of-the-money put option. The call option's strike
    price is higher than the underlying asset's current market price, while the put has a
    strike price that is lower than the asset's market price. This strategy has large profit
    potential since the call option has theoretically unlimited upside if the underlying
    asset rises in price, while the put option can profit if the underlying asset falls.
    The risk on the trade is limited to the premium paid for the two options.

    Place the strangle two weeks before earnings announcement.

    params:
      - take_profit_threshold (float): Percentage to take profit.
      - sleeptime (int): Number of minutes to wait between trading iterations.
      - total_trades (int): Tracks the total number of pairs traded.
      - max_trades (int): Maximum trades at any time.
      - max_days_expiry (int): Maximum number of days to to expiry.
      - days_to_earnings_min(int): Minimum number of days to earnings.
      - exchange (str): Exchange, defaults to `SMART`

      - symbol_universe (list): is the stock symbols expected to have a sharp
      movement in either direction.
      - trading_pairs (dict): Used to track all information for each
      symbol/options.
    """

    IS_BACKTESTABLE = False

    # =====Overloading lifecycle methods=============

    def initialize(self):
        self.time_start = time.time()
        # Set how often (in minutes) we should be running on_trading_iteration

        # Initialize our variables
        self.take_profit_threshold = 0.001  # 0.015
        self.sleeptime = 5
        self.total_trades = 0
        self.max_trades = 4
        self.max_days_expiry = 15
        self.days_to_earnings_min = 100  # 15
        self.exchange = "SMART"

        # Stock expected to move.
        self.symbols_universe = [
            "AAL",
            "AAPL",
            "AMD",
            "AMZN",
            "BAC",
            "DIS",
            "EEM",
            "FB",
            "FXI",
            "MSFT",
            "TSLA",
            "UBER",
        ]

        # Underlying Asset Objects.
        self.trading_pairs = dict()
        for symbol in self.symbols_universe:
            self.create_trading_pair(symbol)

    def before_starting_trading(self):
        """Create the option assets object for each underlying. """
        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())

        for asset, options in self.trading_pairs.items():
            try:
                if not options["chains"]:
                    options["chains"] = self.get_chains(asset)
            except Exception as e:
                logging.info(f"Error: {e}")
                continue

            try:
                last_price = self.get_last_price(asset)
                options["price_underlying"] = last_price
                assert(last_price != 0)
            except:
                logging.warning(f"Unable to get price data for {asset.symbol}.")
                options["price_underlying"] = 0
                continue

            # Get dates from the options chain.
            options["expirations"] = self.get_expiration(options["chains"], exchange=self.exchange)

            # Find the first date that meets the minimum days requirement.
            options["expiration_date"] = self.get_expiration_date(
                options["expirations"]
            )

            multiplier = self.get_multiplier(options["chains"])

            # Get the call and put strikes to buy.
            options["buy_call_strike"], options["buy_put_strike"] = self.call_put_strike(
                options["price_underlying"], asset.symbol, options["expiration_date"]
            )

            if not options["buy_call_strike"] or not options["buy_put_strike"]:
                logging.info(f"No options data for {asset.symbol}")
                continue

            # Create option assets.
            options["call"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["buy_call_strike"],
                right="CALL",
                multiplier=multiplier,
            )
            options["put"] = self.create_asset(
                asset.symbol,
                asset_type="option",
                expiration=options["expiration_date"],
                strike=options["buy_put_strike"],
                right="PUT",
                multiplier=multiplier,
            )

    def on_trading_iteration(self):
        value = self.portfolio_value
        cash = self.unspent_money
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]
        trade_cash = self.portfolio_value / (self.max_trades * 2)

        # Sell positions:
        for asset, options in self.trading_pairs.items():
            if (
                options["call"] not in filled_assets
                and options["put"] not in filled_assets
            ):
                continue

            if options["status"] > 1:
                continue

            last_price = self.get_last_price(asset)
            if last_price == 0:
                continue

            # The sell signal will be the maximum percent movement of original price
            # away from strike, greater than the take profit threshold.
            price_move = max(
                [
                    (last_price - options["call"].strike),
                    (options["put"].strike - last_price),
                ]
            )

            if price_move / options["price_underlying"] > self.take_profit_threshold:
                self.submit_order(
                    self.create_order(
                        options["call"],
                        options["call_order"].quantity,
                        "sell",
                        exchange="CBOE",
                    )
                )
                self.submit_order(
                    self.create_order(
                        options["put"],
                        options["put_order"].quantity,
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
            options = self.trading_pairs[asset]
            if options["status"] > 0:
                continue

            # Check for symbol in positions.
            if len([p.symbol for p in positions if p.symbol == asset.symbol]) > 0:
                continue
            # Check if options already traded.
            if options["call"] in filled_assets or options["put"] in filled_assets:
                continue

            # Get the latest prices for stock and options.
            try:
                print(asset, options["call"], options["put"])
                asset_prices = self.get_last_prices(
                    [asset, options["call"], options["put"]]
                )
                assert(len(asset_prices) == 3)
            except:
                logging.info(f"Failed to get price data for {asset.symbol}")
                continue

            options["price_underlying"] = asset_prices[asset]
            options["price_call"] = asset_prices[options["call"]]
            options["price_put"] = asset_prices[options["put"]]

            # Check to make sure date is not too close to earnings.
            print(f"Getting earnings date for {asset.symbol}")
            edate_df = Ticker(asset.symbol).calendar
            if edate_df is None:
                print(
                    f"There was no calendar information for {asset.symbol} so it "
                    f"was not traded."
                )
                continue
            edate = edate_df.iloc[0, 0].date()
            current_date = datetime.datetime.now().date()
            days_to_earnings = (edate - current_date).days
            if days_to_earnings > self.days_to_earnings_min:
                logging.info(
                    f"{asset.symbol} is too far from earnings at" f" {days_to_earnings}"
                )
                continue

            options["trade_created_time"] = datetime.datetime.now()

            quantity_call = int(
                trade_cash / (options["price_call"] * options["call"].multiplier)
            )
            quantity_put = int(
                trade_cash / (options["price_put"] * options["put"].multiplier)
            )

            # Check to see if the trade size it too big for cash available.
            if quantity_call == 0 or quantity_put == 0:
                options["status"] = 2
                continue

            # Buy call.
            options["call_order"] = self.create_order(
                options["call"],
                quantity_call,
                "buy",
                exchange="CBOE",
            )
            self.submit_order(options["call_order"])

            # Buy put.
            options["put_order"] = self.create_order(
                options["put"],
                quantity_put,
                "buy",
                exchange="CBOE",
            )
            self.submit_order(options["put_order"])

            self.total_trades += 1
            options["status"] = 1


        positions = self.get_tracked_positions()
        filla = [pos.asset for pos in positions]
        print(
            f"**** End of iteration ****\n"
            f"Cash: {self.unspent_money}, Value: {self.portfolio_value}  "
            f"Positions: {positions} "
            f"Filled_assets: {filla} "
            f"*******  END ELAPSED TIME  "
            f"{(time.time() - self.time_start):5.0f}   "
            f"*******"
        )

        # self.await_market_to_close()


    def before_market_closes(self):
        self.sell_all()
        self.trading_pairs = dict()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================
    def create_trading_pair(self, symbol):
        # Add/update trading pair to self.trading_pairs
        self.trading_pairs[self.create_asset(symbol, asset_type="stock")] = {
            "call": None,
            "put": None,
            "chains": None,
            "expirations": None,
            "strike_lows": None,
            "strike_highs": None,
            "buy_call_strike": None,
            "buy_put_strike": None,
            "expiration_date": None,
            "price_underlying": None,
            "price_call": None,
            "price_put": None,
            "trade_created_time": None,
            "call_order": None,
            "put_order": None,
            "status": 0,
        }

    def asset_cycle(self, assets):
        # Used to cycle through the assets for investing, prevents starting
        # at the beginning of the asset list on each iteration.
        for asset in cycle(assets):
            yield asset

    def call_put_strike(self, last_price, symbol, expiration_date):
        """Returns strikes for pair."""

        buy_call_strike = 0
        buy_put_strike = 0

        asset = self.create_asset(
            symbol,
            asset_type="option",
            expiration=expiration_date,
            right="call",
        )

        strikes = self.get_strikes(asset)

        for strike in strikes:
            if strike < last_price:
                buy_put_strike = strike
                buy_call_strike = strike
            elif strike > last_price and buy_call_strike < last_price:
                buy_call_strike = strike
            elif strike > last_price and buy_call_strike > last_price:
                break

        return buy_call_strike, buy_put_strike

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
