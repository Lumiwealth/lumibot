from itertools import cycle
import datetime
from pathlib import Path
from time import perf_counter, time

from yfinance import Ticker, download
import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.brokers import InteractiveBrokers
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from credentials import InteractiveBrokersConfig


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
      - assets (list of Asset) Stock only asset objects loaded for the backtest.
      - take_profit_threshold (float): Percentage to take profit.
      - stop_loss_threshold (float): Percentage protection.
      - sleeptime (int): Number of minutes to wait between trading iterations.
      - total_trades (int): Tracks the total number of pairs traded.
      - max_trades (int): Maximum trades at any time.
      - max_days_expiry (int): Maximum number of days to to expiry.
      - days_to_earnings_min(int): Minimum number of days to earnings.
      - exchange (str): Exchange, defaults to `SMART`

      - symbols (list): is the stock symbols expected to have a sharp
      movement in either direction.
      - trading_pairs (dict): Used to track all information for each
      symbol/options.
    """

    IS_BACKTESTABLE = True

    # =====Overloading lifecycle methods=============

    def initialize(
        self,
        assets,
        take_profit_threshold=0.05,
        stop_loss_threshold=-0.025,
        strike_spread=2,
        sleeptime=5,
        total_trades=0,
        max_trades=4,
        max_days_expiry=15,
        days_to_earnings_min=100,  # 15
        exchange="SMART",
    ):

        self.time_start = time()
        # Set how often (in minutes) we should be running on_trading_iteration

        # Initialize our variables
        self.take_profit_threshold = take_profit_threshold
        self.stop_loss_threshold = stop_loss_threshold
        self.strike_spread = strike_spread
        self.sleeptime = sleeptime
        self.total_trades = total_trades
        self.max_trades = max_trades
        self.max_days_expiry = max_days_expiry
        self.days_to_earnings_min = days_to_earnings_min
        self.exchange = exchange

        # Separate the stocks from the options.
        self.assets = assets
        self.stocks = [asset for asset in self.assets if asset.asset_type == "stock"]

        self.trading_pairs = dict()

    def before_starting_trading(self):
        """Create the option assets object for each underlying.

        When trading multple securities, this method will always give
        the next asset for trading, not the first asset.
        """
        self.asset_gen = self.asset_cycle(self.trading_pairs.keys())

    def on_trading_iteration(self):
        positions = self.get_tracked_positions()
        filled_assets = [p.asset for p in positions]
        trade_cash = self.portfolio_value / (self.max_trades * 2)
        datetime = self.get_datetime()

        # Check the status of the current trading_pairs and update.
        self.check_trading_pairs()

        # Sell positions:
        # Always trade to raise cash first. This will reduce the chance
        # of margin calls.
        for asset, options in self.trading_pairs.items():
            if (
                options["call"] not in filled_assets
                and options["put"] not in filled_assets
            ):
                continue

            if options["status"] < 0 or options["status"] > 1:
                continue

            last_price = self.get_last_price(asset)
            if last_price == 0:
                continue

            # The sell signal will be the maximum percent movement of original price
            # away from strike, greater than the take profit threshold.

            position_cost = options["cost_call"] + options["cost_put"]
            call_value = self.get_last_price(options["call"])
            put_value = self.get_last_price(options["put"])
            if call_value is None or put_value is None:
                self.log_message(
                    f"Unable to retrieve call or put value to determine current market value."
                )
                continue

            market_value = (call_value * options["quantity_call"]) + (
                put_value * options["quantity_put"]
            )

            profit_ratio = (market_value / position_cost) - 1
            self.log_message(
                f"Stop - Profit ratio - Limit: {self.stop_loss_threshold:.2f} {profit_ratio:.2f} {self.take_profit_threshold:.2f}"
            )
            if (
                profit_ratio > self.take_profit_threshold
                or profit_ratio < self.stop_loss_threshold
            ):
                options["call_order"] = self.create_order(
                    options["call"],
                    options["call_order"].quantity,
                    "sell",
                    exchange="CBOE",
                )
                self.submit_order(options["call_order"])

                options["put_order"] = self.create_order(
                    options["put"],
                    options["put_order"].quantity,
                    "sell",
                    exchange="CBOE",
                )
                self.submit_order(options["put_order"])

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
            price_error_msg = f"Failed to get price data for {asset.symbol}"
            try:
                asset_prices = self.get_last_prices(
                    [asset, options["call"], options["put"]]
                )
                assert len(asset_prices) == 3
            except:
                self.log_message(price_error_msg)
                continue

            if (
                asset_prices[options["call"]] is None
                or asset_prices[options["call"]] is None
            ):
                self.log_message(price_error_msg)

            options["price_underlying"] = asset_prices[asset]
            options["price_call"] = asset_prices[options["call"]]
            options["price_put"] = asset_prices[options["put"]]

            # Check to make sure date is not too close to earnings.
            self.log_message(f"Getting earnings date for {asset.symbol}")
            edate_df = Ticker(asset.symbol).calendar
            if edate_df is None:
                self.log_message(
                    f"There was no calendar information for {asset.symbol} so it "
                    f"was not traded."
                )
                continue
            edate = edate_df.iloc[0, 0].date()
            current_date = self.get_datetime().date()
            days_to_earnings = (edate - current_date).days
            if days_to_earnings > self.days_to_earnings_min:
                self.log_message(
                    f"{asset.symbol} is too far from earnings at" f" {days_to_earnings}"
                )
                continue

            options["trade_created_time"] = self.get_datetime()

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
            options["quantity_call"] = quantity_call
            self.submit_order(options["call_order"])

            # Buy put.
            options["put_order"] = self.create_order(
                options["put"],
                quantity_put,
                "buy",
                exchange="CBOE",
            )
            options["quantity_put"] = quantity_put
            self.submit_order(options["put_order"])

            self.total_trades += 1
            options["status"] = 1

        positions = self.get_tracked_positions()
        filla = [pos.asset for pos in positions]
        self.log_message(
            f"**** End of iteration ****\n"
            f"Cash: {self.cash}, Value: {self.portfolio_value}  "
            f"Positions: {positions} "
            f"Filled_assets: {filla} "
            f"*******"
        )

    def on_canceled_order(self, order):
        for asset, options in self.trading_pairs.items():
            if order in [options["call_order"], options["put_order"]]:
                right = order.asset.right
                options[f"{right.lower()}_status"] = -1

    def on_filled_order(self, position, order, price, quantity, multiplier):
        for asset, options in self.trading_pairs.items():
            if order in [options["call_order"], options["put_order"]]:
                right = order.asset.right
                status = 1 if order.side == "buy" else 2
                options[f"{right.lower()}_status"] = status
                # Save the costs of the transaction.
                if status == 1:
                    options[f"cost_{right.lower()}"] = price * quantity

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================
    def create_trading_pair(self, asset):
        # Add/update trading pair to self.trading_pairs
        self.trading_pairs[asset] = {
            "call": None,
            "put": None,
            "chains": None,
            "expirations": None,
            "buy_call_strike": None,
            "buy_put_strike": None,
            "expiration_date": None,
            "price_underlying": None,
            "price_call": None,
            "price_put": None,
            "trade_created_time": None,
            "call_order": None,
            "put_order": None,
            "cost_call": 0,
            "cost_put": 0,
            "quantity_call": 0,
            "quantity_put": 0,
            "status": -1,
            "call_status": -1,
            "put_status": -1,
        }

    def create_strangle(self, asset):
        """Wipes out the previous strangle for the asset and creates
        a new strangle trade.

        Erases the previous strangle attempt. Finds new calls and puts.
        Sets status to `0`.
        Status of `1` shows traded. Status of `2` shows complete.
        """
        self.create_trading_pair(asset)
        options = self.trading_pairs[asset]
        try:
            if not options["chains"]:
                options["chains"] = self.get_chains(asset)
        except Exception as e:
            self.log_message(f"Error: {e}")
            return

        try:
            last_price = self.get_last_price(asset)
            options["price_underlying"] = last_price
            assert last_price != 0
        except:
            self.log_message(f"Unable to get price data for {asset.symbol}.")
            options["price_underlying"] = 0

        # Get dates from the options chain.
        options["expirations"] = self.get_expiration(options["chains"], exchange=self.exchange)


        # Find the first date that meets the minimum days requirement.
        options["expiration_date"] = self.get_expiration_date(options["expirations"])

        multiplier = self.get_multiplier(options["chains"])

        # Get the call and put strikes to buy.
        (options["buy_call_strike"], options["buy_put_strike"],) = self.call_put_strike(
            options["price_underlying"], asset.symbol, options["expiration_date"]
        )

        if not options["buy_call_strike"] or not options["buy_put_strike"]:
            self.log_message(f"No options data for {asset.symbol}")
            return

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
        options["status"] = 0
        options["call_status"] = 0
        options["put_status"] = 0

    def check_trading_pairs(self):
        for asset in self.stocks:
            if asset not in self.trading_pairs:
                self.create_strangle(asset)

            status_list = [
                self.trading_pairs[asset]["status"],
                self.trading_pairs[asset]["call_status"],
                self.trading_pairs[asset]["put_status"],
            ]

            if -1 in status_list:
                self.sell_all(cancel_open_orders=True)
                positions = self.get_tracked_positions()
                sell_orders = [
                    position.get_sell_order()
                    for position in positions
                    if position.symbol == asset.symbol
                ]
                self.submit_orders(sell_orders)
                self.create_strangle(asset)

            elif status_list.count(2) == len(status_list):
                self.create_strangle(asset)

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
            right="CALL",
            multiplier=100,
        )

        strikes = [float(strike) for strike in self.get_strikes(asset)]
        if last_price <= strikes[0] or last_price >= strikes[-1]:
            return None, None

        min_dis_val = float("inf")
        min_dis_index = None
        for i in range(len(strikes)):
            dis = abs(last_price - strikes[i])
            if dis < min_dis_val:
                min_dis_val = dis
                min_dis_index = i

        buy_call_strike_index = min_dis_index + self.strike_spread
        buy_put_strike_index = min_dis_index - self.strike_spread

        buy_call_strike = strikes[buy_call_strike_index]
        buy_put_strike = strikes[buy_put_strike_index]

        # Confirm pricing for buy_put_strike
        count_put = 4
        while count_put > 0:
            asset = self.create_asset(
                symbol,
                asset_type="option",
                expiration=expiration_date,
                right="CALL",
                strike=buy_put_strike,
                multiplier=100,
            )
            price = self.get_last_price(asset)
            if price is None:
                buy_put_strike_index -= 1
                buy_put_strike = strikes[buy_call_strike_index]
                count_put -= 1
            else:
                count_put = 0

            # Confirm pricing for buy_call_strike
            count_call = 4
            while count_call > 0:
                asset = self.create_asset(
                    symbol,
                    asset_type="option",
                    expiration=expiration_date,
                    right="CALL",
                    strike=buy_call_strike,
                    multiplier=100,
                )
                price = self.get_last_price(asset)
                if price is None:
                    buy_call_strike_index -= 1
                    buy_call_strike = strikes[buy_call_strike_index]
                    count_call -= 1
                else:
                    count_call = 0

        return buy_call_strike, buy_put_strike

    def get_expiration_date(self, expirations):
        """Expiration date that is closest to, but less than max days to expriry."""
        expiration_date = None

        current_date = self.get_datetime().date()
        for expiration in expirations:
            net_days = (expiration - current_date).days
            if net_days < self.max_days_expiry:
                expiration_date = expiration

        return expiration_date


def main(backtest=False):
    logfile = "logs/logfile.log"
    benchmark_asset = "SPY"

    strategy_class = Strangle

    symbols = [
        "AAPL",
    ]

    if backtest:
        backtesting_start = datetime.datetime(2021, 9, 16)  # Earliest is 5th
        backtesting_end = datetime.datetime(2021, 9, 19)  # up to 20th

        trading_hours_start = datetime.time(9, 30)
        trading_hours_end = datetime.time(16, 00)


        backtesting_datasource = PandasDataBacktesting

        # Stores all of the assets/datas.
        pandas_data = dict()

        for symbol in symbols:
            # Store the underlying stock asset.
            asset = Asset(
                symbol=symbol,
                asset_type="stock",
            )

            df = pd.read_csv(
                f"data/{symbol}.csv",
                parse_dates=True,
                index_col=0,
                header=0,
                names=["datetime", "high", "low", "open", "close", "volume"],
            )

            pandas_data[asset] = Data(
                asset,
                df,
                trading_hours_start=trading_hours_start,
                trading_hours_end=trading_hours_end,
                timestep="minute",
            )

            # Load the options data.
            files = sorted(list(Path(f"data/options/{symbol}").glob("*.csv")))
            for file in files:  # [file for file in files if file.suffix == ".csv"]:
                fn = file.name.split(".")[0]
                filepath = file
                fn_params = fn.split("_")
                symbol = fn_params[0]
                expiry = datetime.datetime.strptime(fn_params[1], "%Y-%m-%d").date()
                right = fn_params[2][:-1].upper()
                strike = fn_params[3]

                asset = Asset(
                    symbol=symbol,
                    asset_type="option",
                    expiration=expiry,
                    right=right.upper(),
                    strike=strike,
                    multiplier=100,
                )

                df = pd.read_csv(
                    filepath,
                    parse_dates=True,
                    index_col=0,
                    header=0,
                    names=["datetime", "high", "low", "open", "close", "volume"],
                )
                df = df[["open", "high", "low", "close", "volume"]]
                # df.index = df.index.tz_localize("America/New_York")

                pandas_data[asset] = Data(
                    asset,
                    df,
                    trading_hours_start=trading_hours_start,
                    trading_hours_end=trading_hours_end,
                    timestep="minute",
                )
    # optimization
    kwargs = {
        "assets": [Asset(symbol=symbol) for symbol in symbols],
        "take_profit_threshold": 0.03,
        "stop_loss_threshold": -0.03,
        "strike_spread": 2,
        "sleeptime": 3,
        "total_trades": 0,
        "max_trades": 4,
        "max_days_expiry": 30,
        "days_to_earnings_min": 100,  # 15
    }

    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

    if not backtest:
        # Initialize all our classes
        trader = Trader(logfile=logfile)
        broker = InteractiveBrokers(InteractiveBrokersConfig)

        strategy_class = Strangle(broker=broker, **kwargs)

        trader.add_strategy(strategy_class)
        trader.run_all()

    elif backtest:
        tic = perf_counter()
        strategy_class.backtest(
            backtesting_datasource,
            backtesting_start,
            backtesting_end,
            pandas_data=pandas_data,
            stats_file=stats_file,
            logfile=logfile,
            **kwargs,
        )
        toc = perf_counter()
        print(f"Elapsed time: {(toc - tic):.2f}")

if __name__ == "__main__":
    main(backtest=True)
