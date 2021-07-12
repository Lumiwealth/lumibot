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

        self.option = {
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

        # rsi_bars = self.get_symbol_bars(self.asset, self.rsi_period).df["close"]

        last_option_prices = self.get_last_prices(option_assets)

        # Determine most profitable option.
        selected_option = self.select_option(self.option["price_underlying"], last_option_prices)

        x = 1

        # # Sell positions:
        # for asset, options in self.trading_pairs.items():
        #     if (
        #         options["call"] not in filled_assets
        #         and options["put"] not in filled_assets
        #     ):
        #         continue
        #
        #     if options["status"] > 1:
        #         continue
        #
        #     last_price = self.get_last_price(asset)
        #     if last_price == 0:
        #         continue
        #
        #     # The sell signal will be the maximum percent movement of original price
        #     # away from strike, greater than the take profit threshold.
        #     price_move = max(
        #         [
        #             (last_price - options["call"].strike),
        #             (options["put"].strike - last_price),
        #         ]
        #     )
        #
        #     if price_move / options["price_underlying"] > self.take_profit_threshold:
        #         self.submit_order(
        #             self.create_order(
        #                 options["call"],
        #                 options["call_order"].quantity,
        #                 "sell",
        #                 exchange="CBOE",
        #             )
        #         )
        #         self.submit_order(
        #             self.create_order(
        #                 options["put"],
        #                 options["put_order"].quantity,
        #                 "sell",
        #                 exchange="CBOE",
        #             )
        #         )
        #
        #         options["status"] = 2
        #         self.total_trades -= 1
        #
        # # Create positions:
        # if self.total_trades >= self.max_trades:
        #     return
        #
        # for _ in range(len(self.trading_pairs.keys())):
        #     if self.total_trades >= self.max_trades:
        #         break
        #
        #     asset = next(self.asset_gen)
        #     options = self.trading_pairs[asset]
        #     if options["status"] > 0:
        #         continue
        #
        #     # Check for symbol in positions.
        #     if len([p.symbol for p in positions if p.symbol == asset.symbol]) > 0:
        #         continue
        #     # Check if options already traded.
        #     if options["call"] in filled_assets or options["put"] in filled_assets:
        #         continue
        #
        #     # Get the latest prices for stock and options.
        #     try:
        #         print(asset, options["call"], options["put"])
        #         asset_prices = self.get_last_prices(
        #             [asset, options["call"], options["put"]]
        #         )
        #         assert len(asset_prices) == 3
        #     except:
        #         logging.info(f"Failed to get price data for {asset.symbol}")
        #         continue
        #
        #     options["price_underlying"] = asset_prices[asset]
        #     options["price_call"] = asset_prices[options["call"]]
        #     options["price_put"] = asset_prices[options["put"]]
        #
        #     # Check to make sure date is not too close to earnings.
        #     print(f"Getting earnings date for {asset.symbol}")
        #     edate_df = Ticker(asset.symbol).calendar
        #     if edate_df is None:
        #         print(
        #             f"There was no calendar information for {asset.symbol} so it "
        #             f"was not traded."
        #         )
        #         continue
        #     edate = edate_df.iloc[0, 0].date()
        #     current_date = datetime.datetime.now().date()
        #     days_to_earnings = (edate - current_date).days
        #     if days_to_earnings > self.days_to_earnings_min:
        #         logging.info(
        #             f"{asset.symbol} is too far from earnings at" f" {days_to_earnings}"
        #         )
        #         continue
        #
        #     options["trade_created_time"] = datetime.datetime.now()
        #
        #     quantity_call = int(
        #         trade_cash / (options["price_call"] * options["call"].multiplier)
        #     )
        #     quantity_put = int(
        #         trade_cash / (options["price_put"] * options["put"].multiplier)
        #     )
        #
        #     # Check to see if the trade size it too big for cash available.
        #     if quantity_call == 0 or quantity_put == 0:
        #         options["status"] = 2
        #         continue
        #
        #     # Buy call.
        #     options["call_order"] = self.create_order(
        #         options["call"],
        #         quantity_call,
        #         "buy",
        #         exchange="CBOE",
        #     )
        #     self.submit_order(options["call_order"])
        #
        #     # Buy put.
        #     options["put_order"] = self.create_order(
        #         options["put"],
        #         quantity_put,
        #         "buy",
        #         exchange="CBOE",
        #     )
        #     self.submit_order(options["put_order"])
        #
        #     self.total_trades += 1
        #     options["status"] = 1
        #
        # positions = self.get_tracked_positions()
        # filla = [pos.asset for pos in positions]
        # print(
        #     f"**** End of iteration ****\n"
        #     f"Cash: {self.unspent_money}, Value: {self.portfolio_value}  "
        #     f"Positions: {positions} "
        #     f"Filled_assets: {filla} "
        #     f"*******  END ELAPSED TIME  "
        #     f"{(time.time() - self.time_start):5.0f}   "
        #     f"*******"
        # )

        # self.await_market_to_close()

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
            (nearest_strike - nStrikesLess): (nearest_strike + nStrikesMore)
        ]

        return cover_strikes

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
        strike = None
        max_profit = None
        for option, option_price in options_price_list.items():
            stock_profit = (option.strike - stock_price)
            total_profit = stock_profit + option_price

            if max_profit is None or total_profit > max_profit:
                max_profit = total_profit
                strike = option.strike

        return strike, max_profit
