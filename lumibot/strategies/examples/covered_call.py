from itertools import cycle
import datetime
import logging
import time

# import btalib
import finta as TA
from yfinance import Ticker, download
import pandas as pd


from lumibot.strategies.strategy import Strategy


class CoveredCall(Strategy):
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
        self.symbol = "SPY"
        self.asset = self.create_asset(self.symbol, asset_type="stock")
        self.sleeptime = 5
        self.exchange = "SMART"
        self.max_days_expiry = 45
        self.strike_range_less = 4
        self.strike_range_more = 4
        self.rsi_period = 14

        self.buy_signal = True

        self.option = self.set_option_dict()

    def before_starting_trading(self):
        """Create the option assets object for each underlying. """

        try:
            self.option["chains"] = self.get_chains(self.asset)
        except Exception as e:
            logging.info(f"Error: {e}")
            if self.option["chains"] is None:
                raise ValueError("Unable to obtain the initial option chain. ")

        # options["put"] = self.create_asset(
        #     asset.symbol,
        #     asset_type="option",
        #     expiration=options["expiration_date"],
        #     strike=options["buy_put_strike"],
        #     right="PUT",
        #     multiplier=multiplier,
        # )

    def on_trading_iteration(self):

        value = self.portfolio_value
        cash = self.unspent_money
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]

        print("Start iteration: ", value, cash, positions, filled_assets)

        # Determine if there's an exising call, Check if it's matured, if so remove from dict.
        # and proceed, else return.

        if (
            self.option["status"] == 1
            and self.option["expiration_date"] < datetime.datetime.now()
        ):
            return

        if (
            self.option["status"] == 1
            and self.option["expiration_date"] > datetime.datetime.now()
        ):
            self.remove_expired_option(self.option["call"])
            self.option = self.set_option_dict()

        try:
            self.option["price_underlying"] = self.get_last_price(self.asset)
            assert self.option["price_underlying"] != 0
        except:
            logging.warning(f"Unable to get price data for {self.asset.symbol}.")
            self.option["price_underlying"] = 0
            raise ValueError("Unable to obtain the underlying price. ")

        # Get dates from the self.option chain.
        self.option["expirations"] = self.get_expiration(
            self.option["chains"], exchange=self.exchange
        )

        # Find the first date that meets the minimum days requirement.
        self.option["expiration_date"] = self.get_expiration_date(
            self.option["expirations"], self.max_days_expiry
        )

        self.option["multiplier"] = self.get_multiplier(self.option["chains"])

        self.option["strikes"] = self.get_strike(
            self.option["price_underlying"],
            self.symbol,
            self.option["expiration_date"],
            self.strike_range_less,
            self.strike_range_more,
        )

        if not self.option["strikes"]:
            raise ValueError(f"No options data for {self.asset.symbol}")

        option_assets = [
            self.create_asset(
                self.symbol,
                asset_type="option",
                expiration=self.option["expiration_date"],
                strike=strike,
                right="CALL",
                multiplier=self.option["multiplier"],
            )
            for strike in self.option["strikes"]
        ]

        last_option_prices = self.get_last_prices(option_assets)

        # Determine most profitable option.
        self.option["call"] = self.select_option(
            self.option["price_underlying"], last_option_prices
        )

        # Buy stock and sell put.
        if self.asset not in filled_assets:
            self.submit_order(self.create_order(self.asset, 100, "buy"))

        if self.option["call_order"] is None:
            self.option["call_order"] = self.submit_order(
                self.create_order(self.option["call"], 1, "sell", exchange="SMART")
            )
            self.option["status"] = 1

        self.await_market_to_close()

    def before_market_closes(self):
        self.sell_all()
        self.trading_pairs = dict()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================
    def get_strike(
        self, last_price, symbol, expiration_date, nStrikesLess, nStrikesMore
    ):
        """Returns strikes for pair."""

        asset = self.create_asset(
            symbol,
            asset_type="option",
            expiration=expiration_date,
        )

        strikes = self.get_strikes(asset)
        nearest_strike = min(
            range(len(strikes)),
            key=lambda i: abs(strikes[i] - self.option["price_underlying"]),
        )
        cover_strikes = strikes[
            (nearest_strike - nStrikesLess) : (nearest_strike + nStrikesMore)
        ]

        return cover_strikes

    def set_option_dict(self):
        return {
            "call": None,
            "chains": None,
            "expirations": None,
            "strikes": None,
            "sell_call_strike": None,
            "expiration_date": None,
            "price_underlying": None,
            "price_call": None,
            "trade_created_time": None,
            "call_order": None,
            "multiplier": 100,
            "status": 0,
        }

    def get_expiration_date(self, expirations, max_days):
        """Expiration date that is closest to, but less than max days to expriry. """
        expiration_date = None
        # Expiration
        current_date = datetime.datetime.now().date()
        for expiration in expirations:
            ex_date = datetime.datetime.strptime(expiration, "%Y%m%d").date()
            net_days = (ex_date - current_date).days
            if net_days < max_days:
                expiration_date = expiration

        return expiration_date

    def select_option(self, stock_price, options_price_list):
        """Strike minus the latest price to get stock profit. Then add premium."""
        max_profit = None
        option_asset = None
        for option, option_price in options_price_list.items():
            stock_profit = option.strike - stock_price
            total_profit = stock_profit + option_price

            if max_profit is None or total_profit > max_profit:
                max_profit = total_profit
                option_asset = option

        return option_asset

    def remove_expired_option(self, option):
        # remove matured options
        for position in self.get_tracked_positions():
            if position.asset == option:
                position.remove(option)
