import datetime
import logging
from termcolor import colored
from asyncio.log import logger
from decimal import Decimal
import os
import string
import random

import pandas as pd
from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting, ThetaDataBacktesting
from lumibot.entities import Asset, Position, Order
from lumibot.tools import (
    create_tearsheet,
    day_deduplicate,
    get_symbol_returns,
    plot_indicators,
    plot_returns,
    stats_summary,
    to_datetime_aware,
)
from lumibot.traders import Trader

from .strategy_executor import StrategyExecutor
from ..credentials import (
    THETADATA_CONFIG, 
    STRATEGY_NAME, 
    BROKER, 
    POLYGON_API_KEY, 
    DISCORD_WEBHOOK_URL, 
    DB_CONNECTION_STR,
    MARKET,
    HIDE_POSITIONS,
    HIDE_TRADES,
    LUMIWEALTH_API_KEY,
    SHOW_INDICATORS,
    SHOW_PLOT,
    SHOW_TEARSHEET,
    LIVE_CONFIG,
    POLYGON_MAX_MEMORY_BYTES,
)


class CustomLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra):
        super().__init__(logger, extra)
        self.prefix = f'[{self.extra["strategy_name"]}] '

    def process(self, msg, kwargs):
        try:
            return self.prefix + msg, kwargs
        except Exception as e:
            return msg, kwargs

class Vars:
    def __init__(self):
        super().__setattr__('_vars_dict', {})

    def __getattr__(self, name):
        try:
            return self._vars_dict[name]
        except KeyError:
            raise AttributeError(f"'Vars' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        self._vars_dict[name] = value

    def set(self, name, value):
        self._vars_dict[name] = value

    def all(self):
        return self._vars_dict.copy()


class _Strategy:
    IS_BACKTESTABLE = True
    _trader = None

    def __init__(
        self,
        broker=None,
        minutes_before_closing=1,
        minutes_before_opening=60,
        minutes_after_closing=0,
        sleeptime="1M",
        stats_file=None,
        risk_free_rate=None,
        benchmark_asset="SPY",
        backtesting_start=None,
        backtesting_end=None,
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        starting_positions=None,
        filled_order_callback=None,
        name=None,
        budget=None,
        parameters={},
        buy_trading_fees=[],
        sell_trading_fees=[],
        force_start_immediately=False,
        discord_webhook_url=None,
        account_history_db_connection_str=None,
        db_connection_str=None,
        strategy_id=None,
        discord_account_summary_footer=None,
        should_backup_variables_to_database=True,
        should_send_summary_to_discord=True,
        save_logfile=False,
        lumiwealth_api_key=None,
        **kwargs,
    ):
        """Initializes a Strategy object.

        Parameters
        ----------
        broker : Broker
            The broker to use for the strategy. Required. For backtesting, use the BacktestingBroker class.
        data_source : DataSource
            The data source to use for the strategy. Required.
        minutes_before_closing : int
            The number of minutes before closing that the before_market_closes lifecycle method will be called and the
            strategy will be stopped.
        minutes_before_opening : int
            The number of minutes before opening that the before_market_opens lifecycle method will be called.
        sleeptime : str
            The number of seconds to sleep between the start of each iteration of the strategy (on_trading_iteration).
            For example "1S" for 1 second, "5M" for 5 minutes, "2H" for 2 hours, or "1D" for 1 day.
            Defaults to "1M" (1 minute).
        stats_file : str
            The file name to save the stats to.
        risk_free_rate : float
            The risk-free rate to use for calculating the Sharpe ratio.
        benchmark_asset : Asset or str
            The asset to use as the benchmark for the strategy. Defaults to "SPY". Strings are converted to
            Asset objects with an asset_type="stock".
        backtesting_start : datetime.datetime
            The date and time to start backtesting from. Required for backtesting.
        backtesting_end : datetime.datetime
            The date and time to end backtesting. Required for backtesting.
        pandas_data : pd.DataFrame
            The pandas dataframe to use for backtesting. Required if using the PandasDataBacktesting data source.
        quote_asset : Asset
            The asset to use as the quote asset. Defaults to a USD forex Asset object.
        starting_positions : dict
            A dictionary of starting positions to use for backtesting. The keys are the symbols of the assets and the
            values are the quantities of the assets to start with.
        filled_order_callback : function
            A function to call when an order is filled. The function should take two parameters: the strategy object
            and the order object.
        name : str
            The name of the strategy. Defaults to the name of the class.
        budget : float
            The starting budget to use for backtesting. Defaults to $100,000.
        parameters : dict
            A dictionary of parameters to use for the strategy, this will override parameters set in the strategy
            class. The keys are the names of the parameters and the values are the values of the parameters.
            Defaults to an empty dictionary.
        buy_trading_fees : list
            A list of TradingFee objects to use for buying assets. Defaults to an empty list.
        sell_trading_fees : list
            A list of TradingFee objects to use for selling assets. Defaults to an empty list.
        force_start_immidiately : bool
            If True, the strategy will start immediately. If False, the strategy will wait until the market opens
            to start. Defaults to True.
        discord_webhook_url : str
            The discord webhook url to use for sending alerts from the strategy. You can send alerts to a discord
            channel by setting broadcast=True in the log_message method. The strategy will also by default send
            and account summary to the discord channel at the end of each day (db_connection_str
            must be set for this to work). Defaults to None (no discord alerts).
            For instructions on how to create a discord webhook url, see this link:
            https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks
        discord_account_summary_footer : str
            The footer to use for the account summary sent to the discord channel if discord_webhook_url is set and the
            db_connection_str is set.
            Defaults to None (no footer).
        db_connection_str : str
            The connection string to use for the account history database. This is used to store the account history
            for the strategy. The account history is sent to the discord channel at the end of each day. The connection
            string should be in the format: "sqlite:///path/to/database.db". The database should have a table named
            "strategy_tracker". If that table does not exist, it will be created. Defaults to None (no account history).
        strategy_id : str
            The id of the strategy that will be used to identify the strategy in the account history database.
            Defaults to None (lumibot will use the name of the strategy as the id).
        should_backup_variables_to_database : bool
            If True, the strategy will backup its variables to the account history database at the end of each day.
            Defaults to True.
        should_send_summary_to_discord : bool
            If True, the strategy will send an account summary to the discord channel at the end of each day.
            Defaults to True.
        save_logfile : bool
            Whether to save the logfile. Defaults to False. If True, the logfile will be saved to the logs directory.
            Turning on this option will slow down the backtest.
        lumiwealth_api_key : str
            The API key to use for the LumiWealth data source. Defaults to None (saving to the cloud is off).
        kwargs : dict
            A dictionary of additional keyword arguments to pass to the strategy.

        """
        # TODO: Break up this function, too long!

        self.buy_trading_fees = buy_trading_fees
        self.sell_trading_fees = sell_trading_fees
        self.save_logfile = save_logfile
        self.broker = broker

        if name is not None:
            self._name = name

        elif STRATEGY_NAME is not None:
            self._name = STRATEGY_NAME
        
        else:
            self._name = self.__class__.__name__

        # Create an adapter with 'strategy_name' set to the instance's name
        if not hasattr(self, "logger") or self.logger is None:
            self.logger = CustomLoggerAdapter(logger, {'strategy_name': self._name})

        # Set the log level to INFO so that all logs INFO and above are displayed
        self.logger.setLevel(logging.INFO)
        
        if self.broker == None:
            self.broker = BROKER

        self.hide_positions = HIDE_POSITIONS
        self.hide_trades = HIDE_TRADES

        # If the MARKET env variable is set, use it as the market
        if MARKET:
            # Log the market being used
            colored_message = colored(f"Using market from environment variables: {MARKET}", "green")
            self.logger.info(colored_message)
            self.set_market(MARKET)

        self.live_config = LIVE_CONFIG
        self.discord_webhook_url = discord_webhook_url if discord_webhook_url is not None else DISCORD_WEBHOOK_URL
        
        if account_history_db_connection_str: 
            self.db_connection_str = account_history_db_connection_str  
            logging.warning("account_history_db_connection_str is deprecated and will be removed in future versions, please use db_connection_str instead") 
        elif db_connection_str:
            self.db_connection_str = db_connection_str
        else:
            self.db_connection_str = DB_CONNECTION_STR if DB_CONNECTION_STR else None
            
        self.discord_account_summary_footer = discord_account_summary_footer
        self.backup_table_name="vars_backup"

        # Set the LumiWealth API key
        if lumiwealth_api_key:
            self.lumiwealth_api_key = lumiwealth_api_key
        else:
            self.lumiwealth_api_key = LUMIWEALTH_API_KEY

        if strategy_id is None:
            self.strategy_id = self._name
        else:
            self.strategy_id = strategy_id

        self._quote_asset = quote_asset

        # Check if self.broker is set
        if self.broker is None:
            self.logger.error(colored("No broker is set. Please set a broker using environment variables, secrets or by passing it as an argument.", "red"))
            raise ValueError("No broker is set. Please set a broker using environment variables, secrets or by passing it as an argument.")

        # Check if the quote_assets exists on the broker
        if not hasattr(self.broker, "quote_assets"):
            self.broker.quote_assets = set()

        self.broker.quote_assets.add(self._quote_asset)

        # Setting the broker object
        if self.broker == None:
            self.is_backtesting = True
        else:
            self.is_backtesting = self.broker.IS_BACKTESTING_BROKER

        self._benchmark_asset = benchmark_asset

        # Get the backtesting start and end dates from the broker data source if we are backtesting
        if self.is_backtesting:
            if self.broker.data_source.datetime_start is not None and self.broker.data_source.datetime_end is not None:
                self._backtesting_start = self.broker.data_source.datetime_start
                self._backtesting_end = self.broker.data_source.datetime_end

        # Force start immediately if we are backtesting
        self.force_start_immediately = force_start_immediately

        # Initialize the chart markers list
        self._chart_markers_list = []

        # Initialize the chart lines list
        self._chart_lines_list = []

        # Hold the asset objects for strings for stocks only.
        self._asset_mapping = dict()

        # Setting the data provider
        if self.is_backtesting:
            if self.broker.data_source.SOURCE == "PANDAS":
                self.broker.data_source.load_data()

            # Create initial starting positions.
            self.starting_positions = starting_positions
            if self.starting_positions is not None and len(self.starting_positions) > 0:
                for asset, quantity in self.starting_positions.items():
                    position = Position(
                        self._name,
                        asset,
                        Decimal(quantity),
                        orders=None,
                        hold=0,
                        available=Decimal(quantity),
                    )
                    self.broker._filled_positions.append(position)

        # Set the the state of first iteration to True. This will later be updated to False by the strategy executor
        self._first_iteration = True

        # Setting execution parameters
        self._last_on_trading_iteration_datetime = None
        if not self.is_backtesting:
            self.update_broker_balances()

            # Set initial positions if live trading.
            self.broker._set_initial_positions(self)
        else:
            if budget is None:
                if self.cash is None:
                    # Default to $100,000 if no budget is set.
                    budget = 100000
                    self._set_cash_position(budget)
                else:
                    budget = self.cash
            else:
                self._set_cash_position(budget)

            # #############################################
            # ## TODO: Should all this just use _update_portfolio_value()?
            # ## START
            self._portfolio_value = self.cash

            store_assets = list(self.broker.data_source._data_store.keys())
            if len(store_assets) > 0:
                positions_value = 0
                for position in self.get_positions():
                    price = None
                    if position.asset == self.quote_asset:
                        # Don't include the quote asset since it's already included with cash
                        price = 0
                    else:
                        price = self.get_last_price(position.asset, quote=self.quote_asset)
                    value = float(position.quantity) * price
                    positions_value += value

                self._portfolio_value = self._portfolio_value + positions_value

            else:
                self._position_value = 0

            # END
            ##############################################

        self._initial_budget = budget
        self._minutes_before_closing = minutes_before_closing
        self._minutes_before_opening = minutes_before_opening
        self._minutes_after_closing = minutes_after_closing
        self._sleeptime = sleeptime
        self._risk_free_rate = risk_free_rate
        self._executor = StrategyExecutor(self)
        self.broker._add_subscriber(self._executor)

        # Stats related variables
        self._stats_file = stats_file
        self._stats = None
        self._stats_list = []
        self._analysis = {}

        # Variable backup related variables
        self.should_backup_variables_to_database = should_backup_variables_to_database
        self.should_send_summary_to_discord = should_send_summary_to_discord
        self._last_backup_state = None
        self.vars = Vars()

        # Storing parameters for the initialize method
        if not hasattr(self, "parameters") or not isinstance(self.parameters, dict) or self.parameters is None:
            self.parameters = {}
        self.parameters = {**self.parameters, **kwargs}
        if parameters is not None and isinstance(self.parameters, dict):
            self.parameters = {**self.parameters, **parameters}

        self._strategy_returns_df = None
        self._benchmark_returns_df = None

        self._filled_order_callback = filled_order_callback

    # =============Internal functions===================
    def _copy_dict(self):
        result = {}
        ignored_fields = ["broker", "data_source", "trading_pairs", "asset_gen"]
        for key in self.__dict__:
            if key[0] != "_" and key not in ignored_fields:
                try:
                    result[key] = self.__dict__[key]
                except KeyError:
                    pass
                    # self.logger.warning(
                    #     "Cannot perform deepcopy on %r" % self.__dict__[key]
                    # )
            elif key in [
                "_name",
                "_initial_budget",
                # "_cash",
                "_portfolio_value",
                "_minutes_before_closing",
                "_minutes_before_opening",
                "_sleeptime",
                "is_backtesting",
            ]:
                result[key[1:]] = self.__dict__[key]

        return result

    def _validate_order(self, order):
        """
        Validates an order to ensure it meets the necessary criteria before submission.

        Parameters:
        order (Order): The order to be validated.

        Returns:
        bool: True if the order is valid, False otherwise.

        Validation checks:
        - The order is not None.
        - The order is an instance of the Order class.
        - The order quantity is not zero.
        """

        # Check if order is None
        if order is None:
            self.logger.error(
                "Cannot submit a None order, please check to make sure that you have actually created an order before submitting."
            )
            return False

        # Check if the order is an Order object
        if not isinstance(order, Order):
            self.logger.error(
                f"Order must be an Order object. You entered {order}."
            )
            return False

        # Check if the order does not have a quantity of zero
        if order.quantity == 0:
            self.logger.error(
                f"Order quantity cannot be zero. You entered {order.quantity}."
            )
            return False

        return True

    def _set_cash_position(self, cash: float):
        # Check if cash is in the list of positions yet
        for x in range(len(self.broker._filled_positions.get_list())):
            position = self.broker._filled_positions[x]
            if position is not None and position.asset == self.quote_asset:
                position.quantity = cash
                self.broker._filled_positions[x] = position
                return

        # If not in positions, create a new position for cash
        position = Position(
            self._name,
            self.quote_asset,
            Decimal(cash),
            orders=None,
            hold=0,
            available=Decimal(cash),
        )
        self.broker._filled_positions.append(position)

    def _sanitize_user_asset(self, asset):
        if isinstance(asset, Asset):
            return asset
        elif isinstance(asset, tuple):
            return asset
        elif isinstance(asset, str):
            # Make sure the asset is uppercase for consistency (and because some brokers require it)
            asset = asset.upper()
            return Asset(symbol=asset)
        else:
            if self.broker.data_source.SOURCE != "CCXT":
                raise ValueError(f"You must enter a symbol string or an asset object. You " f"entered {asset}")
            else:
                raise ValueError(
                    "You must enter symbol string or an asset object. If you "
                    "getting a quote, you may enter a string like `ETH/BTC` or "
                    "asset objects in a tuple like (Asset(ETH), Asset(BTC))."
                )

    def _log_strat_name(self):
        """Returns the name of the strategy as a string if not default"""
        return f"{self._name} " if self._name is not None else ""

    def update_broker_balances(self, force_update=True):
        """Updates the broker's balances, including cash and portfolio value

        Parameters
        ----------
        force_update : bool, optional
            If True, forces the broker to update the balances immediately.
            If False, the broker will only update the balances if the last
            update was more than 1 minute ago. The default is True.

        Returns
        -------
        bool
            True if the broker's balances were updated, False otherwise
        """
        if self.is_backtesting:
            return True

        if "last_broker_balances_update" not in self.__dict__:
            self.last_broker_balances_update = None

        UPDATE_INTERVAL = 59
        if (
            self.last_broker_balances_update is None
            or force_update
            or (
                self.last_broker_balances_update + datetime.timedelta(seconds=UPDATE_INTERVAL) < datetime.datetime.now()
            )
        ):
            try:
                broker_balances = self.broker._get_balances_at_broker(self.quote_asset, self)
            except Exception as e:
                self.logger.error(f"Error getting broker balances: {e}")
                return False

            if broker_balances is not None:
                (
                    self._cash,
                    self._position_value,
                    self._portfolio_value,
                ) = broker_balances

                self.last_broker_balances_update = datetime.datetime.now()
                return True

            else:
                self.logger.error(
                    "Unable to get balances (cash, portfolio value, etc) from broker. "
                    "Please check your broker and your broker configuration."
                )
                return False
        else:
            self.logger.debug("Balances already updated recently. Skipping update.")

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        if not self.is_backtesting:
            try:
                broker_balances = self.broker._get_balances_at_broker(self.quote_asset, self)
            except Exception as e:
                self.logger.error(f"Error getting broker balances: {e}")
                return None

            if broker_balances is not None:
                return broker_balances[2]
            else:
                return None

        with self._executor.lock:
            # Used for traditional brokers, for crypto this could be 0
            portfolio_value = self.cash

            positions = self.broker.get_tracked_positions(self._name)
            assets_original = [position.asset for position in positions]
            # Set the base currency for crypto valuations.

            prices = {}
            for asset in assets_original:
                if asset != self.quote_asset:
                    asset_is_option = False
                    if asset.asset_type == "crypto" or asset.asset_type == "forex":
                        asset = (asset, self.quote_asset)
                    elif asset.asset_type == "option":
                        asset_is_option = True

                    if self.broker.option_source is not None and asset_is_option:
                        price = self.broker.option_source.get_last_price(asset)
                        prices[asset] = price
                    else:
                        price = self.broker.data_source.get_last_price(asset)
                        prices[asset] = price
                        
            for position in positions:
                # Turn the asset into a tuple if it's a crypto asset
                asset = (
                    position.asset
                    if (position.asset.asset_type != "crypto") and (position.asset.asset_type != "forex")
                    else (position.asset, self.quote_asset)
                )
                quantity = position.quantity
                price = prices.get(asset, 0)

                # If the asset is the quote asset, then we already have included it from cash
                # Eg. if we have a position of USDT and USDT is the quote_asset then we already consider it as cash
                if self.quote_asset is not None:
                    if isinstance(asset, tuple) and asset == (
                        self.quote_asset,
                        self.quote_asset,
                    ):
                        price = 0
                    elif isinstance(asset, Asset) and asset == self.quote_asset:
                        price = 0

                if self.is_backtesting and price is None:
                    if isinstance(asset, Asset):
                        raise ValueError(
                            f"A security has returned a price of None while trying "
                            f"to set the portfolio value. This usually happens when there "
                            f"is no data data available for the Asset or pair. "
                            f"Please ensure data exists at "
                            f"{self.broker.datetime} for the security: \n"
                            f"symbol: {asset.symbol}, \n"
                            f"type: {asset.asset_type}, \n"
                            f"right: {asset.right}, \n"
                            f"expiration: {asset.expiration}, \n"
                            f"strike: {asset.strike}.\n"
                        )
                    elif isinstance(asset, tuple):
                        raise ValueError(
                            f"A security has returned a price of None while trying "
                            f"to set the portfolio value. This usually happens when there "
                            f"is no data data available for the Asset or pair. "
                            f"Please ensure data exists at "
                            f"{self.broker.datetime} for the pair: {asset}"
                        )
                if isinstance(asset, tuple):
                    multiplier = 1
                else:
                    multiplier = asset.multiplier if asset.asset_type in ["option", "future"] else 1
                portfolio_value += float(quantity) * price * multiplier
            self._portfolio_value = portfolio_value
        return portfolio_value

    def _update_cash(self, side, quantity, price, multiplier):
        """update the self.cash"""
        with self._executor.lock:
            cash = self.cash
            if cash is None:
                cash = 0

            if side == "buy":
                cash -= float(quantity) * price * multiplier
            if side == "sell":
                cash += float(quantity) * price * multiplier

            self._set_cash_position(cash)

            # Todo also update the cash asset in positions?

            return self.cash

    def _update_cash_with_dividends(self):
        with self._executor.lock:
            positions = self.broker.get_tracked_positions(self._name)

            assets = []
            for position in positions:
                if position.asset != self.quote_asset:
                    assets.append(position.asset)

            dividends_per_share = self.get_yesterday_dividends(assets)
            for position in positions:
                asset = position.asset
                quantity = position.quantity
                dividend_per_share = 0 if dividends_per_share is None else dividends_per_share.get(asset, 0)
                cash = self.cash
                if cash is None:
                    cash = 0
                cash += dividend_per_share * float(quantity)
                self._set_cash_position(cash)
            return self.cash

    # =============Stats functions=====================

    def _append_row(self, row):
        self._stats_list.append(row)

    def _format_stats(self):
        self._stats = pd.DataFrame(self._stats_list)
        if "datetime" in self._stats.columns:
            self._stats = self._stats.set_index("datetime")
        self._stats["return"] = self._stats["portfolio_value"].pct_change()

        return self._stats

    def _dump_stats(self):
        logger = logging.getLogger()
        current_level = logging.getLevelName(logger.level)
        for handler in logger.handlers:
            if handler.__class__.__name__ == "StreamHandler":
                current_stream_handler_level = handler.level
                handler.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
        if len(self._stats_list) > 0:
            self._format_stats()
            if self._stats_file:
                # Get the directory name from the stats file path
                stats_directory = os.path.dirname(self._stats_file)

                # Check if the directory exists
                if not os.path.exists(stats_directory):
                    os.makedirs(stats_directory)

                self._stats.to_csv(self._stats_file)

            self._strategy_returns_df = day_deduplicate(self._stats)

            self._analysis = stats_summary(self._strategy_returns_df, self.risk_free_rate)

            # Get performance for the benchmark asset
            self._dump_benchmark_stats()

        for handler in logger.handlers:
            if handler.__class__.__name__ == "StreamHandler":
                handler.setLevel(current_stream_handler_level)
        logger.setLevel(current_level)

    def _dump_benchmark_stats(self):
        if not self.is_backtesting:
            return
        if self._backtesting_start is not None and self._backtesting_end is not None:
            # Need to adjust the backtesting end date because the data from Yahoo
            # is at the start of the day, so the graph cuts short. This may be needed
            # for other timeframes as well
            backtesting_end_adjusted = self._backtesting_end

            # If we are using the polgon data source, then get the benchmark returns from polygon
            if type(self.broker.data_source) == PolygonDataBacktesting:
                benchmark_asset = self._benchmark_asset
                # If the benchmark asset is a string, then convert it to an Asset object
                if isinstance(benchmark_asset, str):
                    benchmark_asset = Asset(benchmark_asset)

                timestep = "minute"
                # If the strategy sleeptime is in days then use daily data, eg. "1D"
                if "D" in str(self._sleeptime):
                    timestep = "day"

                bars = self.broker.data_source.get_historical_prices_between_dates(
                    benchmark_asset,
                    timestep,
                    start_date=self._backtesting_start,
                    end_date=backtesting_end_adjusted,
                    quote=self._quote_asset,
                )
                df = bars.df

                # Add returns column
                df["return"] = df["close"].pct_change()

                # Add the symbol_cumprod column
                df["symbol_cumprod"] = (1 + df["return"]).cumprod()

                self._benchmark_returns_df = df

            # For data sources of type CCXT, benchmark_asset gets bechmark_asset from the CCXT backtest data source.
            elif self.broker.data_source.SOURCE.upper() == "CCXT":
                benchmark_asset = self._benchmark_asset
                # If the benchmark asset is a string, then convert it to an Asset object
                if isinstance(benchmark_asset, str):
                    asset_quote = benchmark_asset.split("/")
                    if len(asset_quote) == 2:
                        benchmark_asset = (Asset(symbol=asset_quote[0], asset_type="crypto"),
                                           Asset(symbol=asset_quote[1], asset_type="crypto"))
                    else:
                        benchmark_asset = Asset(symbol=benchmark_asset, asset_type="crypto")

                timestep = "minute"
                # If the strategy sleeptime is in days then use daily data, eg. "1D"
                if "D" in str(self._sleeptime):
                    timestep = "day"

                bars = self.broker.data_source.get_historical_prices_between_dates(
                    benchmark_asset,
                    timestep,
                    start_date=self._backtesting_start,
                    end_date=backtesting_end_adjusted,
                    quote=self._quote_asset,
                )
                df = bars.df

                # Add the symbol_cumprod column
                df["symbol_cumprod"] = (1 + df["return"]).cumprod()

                self._benchmark_returns_df = df

            # If we are using any other data source, then get the benchmark returns from yahoo
            else:
                benchmark_asset = self._benchmark_asset

                # If the benchmark asset is a string, then just use the string as the symbol
                if isinstance(benchmark_asset, str):
                    benchmark_symbol = benchmark_asset
                # If the benchmark asset is an Asset object, then use the symbol of the asset
                elif isinstance(benchmark_asset, Asset):
                    benchmark_symbol = benchmark_asset.symbol
                # If the benchmark asset is a tuple, then use the symbols of the assets in the tuple
                elif isinstance(benchmark_asset, tuple):
                    benchmark_symbol = f"{benchmark_asset[0].symbol}/{benchmark_asset[1].symbol}"

                self._benchmark_returns_df = get_symbol_returns(
                    benchmark_symbol,
                    self._backtesting_start,
                    backtesting_end_adjusted,
                )

    def plot_returns_vs_benchmark(
        self,
        plot_file_html="backtest_result.html",
        trades_df=None,
        show_plot=True,
    ):
        if not show_plot:
            return
        elif self._strategy_returns_df is None:
            self.logger.warning("Cannot plot returns because the strategy returns are missing")
        elif self._benchmark_returns_df is None:
            self.logger.warning("Cannot plot returns because the benchmark returns are missing")
        else:
            plot_returns(
                self._strategy_returns_df,
                f"{self._log_strat_name()}Strategy",
                self._benchmark_returns_df,
                str(self._benchmark_asset),
                plot_file_html,
                trades_df,
                show_plot,
                initial_budget=self._initial_budget,
            )

    def tearsheet(
        self,
        save_tearsheet=True,
        tearsheet_file=None,
        show_tearsheet=True,
    ):
        if not save_tearsheet and not show_tearsheet:
            return None

        if show_tearsheet:
            save_tearsheet = True

        if self._strategy_returns_df is None:
            self.logger.warning("Cannot create a tearsheet because the strategy returns are missing")
        else:
            # Get the strategy parameters
            strategy_parameters = self.parameters

            # Remove pandas_data from the strategy parameters if it exists
            if "pandas_data" in strategy_parameters:
                del strategy_parameters["pandas_data"]

            strat_name = self._name if self._name is not None else "Strategy"

            result = create_tearsheet(
                self._strategy_returns_df,
                strat_name,
                tearsheet_file,
                self._benchmark_returns_df,
                self._benchmark_asset,
                show_tearsheet,
                save_tearsheet,
                risk_free_rate=self.risk_free_rate,
                strategy_parameters=strategy_parameters,
            )

            return result

    @classmethod
    def run_backtest(
        self,
        datasource_class,
        backtesting_start,
        backtesting_end,
        minutes_before_closing=5,
        minutes_before_opening=60,
        sleeptime=1,
        stats_file=None,
        risk_free_rate=None,
        logfile=None,
        config=None,
        auto_adjust=False,
        name=None,
        budget=None,
        benchmark_asset="SPY",
        plot_file_html=None,
        trades_file=None,
        settings_file=None,
        pandas_data=None,
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        starting_positions=None,
        show_plot=None,
        tearsheet_file=None,
        save_tearsheet=True,
        show_tearsheet=None,
        parameters={},
        buy_trading_fees=[],
        sell_trading_fees=[],
        polygon_api_key=None,
        polygon_has_paid_subscription=False, # Deprecated, will be removed in future versions
        use_other_option_source=False,
        thetadata_username=None,
        thetadata_password=None,
        indicators_file=None,
        show_indicators=None,
        save_logfile=False,
        use_quote_data=False,
        show_progress_bar=True,
        quiet_logs=False,
        trader_class=Trader,
        **kwargs,
    ):
        """Backtest a strategy.

        Parameters
        ----------
        datasource_class : class
            The datasource class to use. For example, if you want to use the yahoo finance datasource,
            then you would pass YahooDataBacktesting as the datasource_class.
        backtesting_start : datetime
            The start date of the backtesting period.
        backtesting_end : datetime
            The end date of the backtesting period.
        minutes_before_closing : int
            The number of minutes before closing that the minutes_before_closing strategy method will be called.
        minutes_before_opening : int
            The number of minutes before opening that the minutes_before_opening strategy method will be called.
        sleeptime : int
            The number of seconds to sleep between each iteration of the backtest.
        stats_file : str
            The file to write the stats to.
        risk_free_rate : float
            The risk-free rate to use.
        logfile : str
            The file to write the log to.
        config : dict
            The config to use to set up the brokers in live trading.
        auto_adjust : bool
            Whether to automatically adjust the strategy.
        name : str
            The name of the strategy.
        budget : float
            The initial budget to use for the backtest.
        benchmark_asset : str or Asset
            The benchmark asset to use for the backtest to compare to. If it is a string then it will be converted
            to a stock Asset object.
        plot_file_html : str
            The file to write the plot html to.
        trades_file : str
            The file to write the trades to.
        pandas_data : list
            A list of Data objects that are used when the datasource_class object is set to PandasDataBacktesting.
            This contains all the data that will be used in backtesting.
        quote_asset : Asset (crypto)
            An Asset object for the cryptocurrency that will get used
            as a valuation asset for measuring overall porfolio values.
            Usually USDT, USD, USDC.
        starting_positions : dict
            A dictionary of starting positions for each asset. For example,
            if you want to start with $100 of SPY, and $200 of AAPL, then you
            would pass in starting_positions={'SPY': 100, 'AAPL': 200}.
        show_plot : bool
            Whether to show the plot.
        show_tearsheet : bool
            Whether to show the tearsheet.
        save_tearsheet : bool
            Whether to save the tearsheet.
        parameters : dict
            A dictionary of parameters to pass to the strategy. These parameters
            must be set up within the initialize() method.
        buy_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the buy orders during backtests.
        sell_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the sell orders during backtests.
        polygon_api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class.
        indicators_file : str
            The file to write the indicators to.
        show_indicators : bool
            Whether to show the indicators plot.
        save_logfile : bool
            Whether to save the logfile. Defaults to False. If True, the logfile will be saved to the logs directory. Turning on this option will slow down the backtest.
        use_quote_data : bool
            Whether to use quote data for the backtest. Defaults to False. If True, the backtest will use quote data for the backtest. (Currently this is specific to ThetaData)
            When set to true this requests Quote data in addition to OHLC which adds time to backtests.
        show_progress_bar : bool
            Whether to show the progress bar during the backtest. Defaults to True.
        quiet_logs : bool
            Whether to quiet the logs during the backtest. Defaults to True.
        trader_class : class
            The class to use for the trader. Defaults to Trader.

        Returns
        -------
        tuple of (dict, Strategy)
            A tuple of the analysis dictionary and the strategy object. The analysis dictionary contains the
            analysis of the strategy returns. The strategy object is the strategy object that was backtested, where 
            you can access the strategy returns and other attributes.

        Examples
        --------

        >>> from datetime import datetime
        >>> from lumibot.backtesting import YahooDataBacktesting
        >>> from lumibot.strategies import Strategy
        >>>
        >>> # A simple strategy that buys AAPL on the first day
        >>> class MyStrategy(Strategy):
        >>>    def on_trading_iteration(self):
        >>>        if self.first_iteration:
        >>>            order = self.create_order("AAPL", quantity=1, side="buy")
        >>>            self.submit_order(order)
        >>>
        >>> # Create a backtest
        >>> backtesting_start = datetime(2018, 1, 1)
        >>> backtesting_end = datetime(2018, 1, 31)
        >>>
        >>> # The benchmark asset to use for the backtest to compare to
        >>> benchmark_asset = Asset(symbol="QQQ", asset_type="stock")
        >>>
        >>> backtest = MyStrategy.backtest(
        >>>     datasource_class=YahooDataBacktesting,
        >>>     backtesting_start=backtesting_start,
        >>>     backtesting_end=backtesting_end,
        >>>     benchmark_asset=benchmark_asset,
        >>> )
        """

        if name is None:
            name = self.__name__

        self._name = name

        # Create an adapter with 'strategy_name' set to the instance's name
        if not hasattr(self, "logger") or self.logger is None:
            self.logger = CustomLoggerAdapter(logger, {'strategy_name': self._name})

        # If show_plot is None, then set it to True
        if show_plot is None:
            show_plot = SHOW_PLOT

        # If show_tearsheet is None, then set it to True
        if show_tearsheet is None:
            show_tearsheet = SHOW_TEARSHEET

        # If show_indicators is None, then set it to True
        if show_indicators is None:
            show_indicators = SHOW_INDICATORS

        # Log a warning for polygon_has_paid_subscription as it is deprecated
        if polygon_has_paid_subscription:
            self.logger.warning(
                "polygon_has_paid_subscription is deprecated and will be removed in future versions. "
                "Please remove it from your code."
            )

        # check if datasource_class is a class or a dictionary
        if isinstance(datasource_class, dict):
            optionsource_class = datasource_class["OPTION"]
            datasource_class = datasource_class["STOCK"]
            # check if optionsource_class and datasource_class are the same type of class
            if optionsource_class == datasource_class:
                use_other_option_source = False
            else:
                use_other_option_source = True
        else:
            optionsource_class = None
            use_other_option_source = False

        # Make a string with 6 random numbers/letters (upper and lowercase) to avoid overwriting
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=6))

        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        base_filename = f"{name + '_' if name is not None else ''}{datestring}_{random_string}"

        logdir = "logs"
        if logfile is None and save_logfile:
            logfile = f"{logdir}/{base_filename}_logs.csv"
        if stats_file is None:
            stats_file = f"{logdir}/{base_filename}_stats.csv"

        # #############################################
        # Check the data types of the parameters
        # #############################################

        # Check datasource_class
        if not isinstance(datasource_class, type):
            raise ValueError(f"`datasource_class` must be a class. You passed in {datasource_class}")

        # Check optionsource_class
        if use_other_option_source and not isinstance(optionsource_class, type):
            raise ValueError(f"`optionsource_class` must be a class. You passed in {optionsource_class}")

        self.verify_backtest_inputs(backtesting_start, backtesting_end)

        # Make sure polygon_api_key is set if using PolygonDataBacktesting
        polygon_api_key = polygon_api_key if polygon_api_key is not None else POLYGON_API_KEY
        if datasource_class == PolygonDataBacktesting and polygon_api_key is None:
            raise ValueError(
                "Please set `POLYGON_API_KEY` to your API key from polygon.io as an environment variable if "
                "you are using PolygonDataBacktesting. If you don't have one, you can get a free API key "
                "from https://polygon.io/."
            )

        # Make sure thetadata_username and thetadata_password are set if using ThetaDataBacktesting
        if thetadata_username is None or thetadata_password is None:
            # Try getting the Theta Data credentials from credentials
            thetadata_username = THETADATA_CONFIG.get('THETADATA_USERNAME')
            thetadata_password = THETADATA_CONFIG.get('THETADATA_PASSWORD')
            
            # Check again if theta data username and pass are set
            if (thetadata_username is None or thetadata_password is None) and (datasource_class == ThetaDataBacktesting or optionsource_class == ThetaDataBacktesting):
                raise ValueError(
                    "Please set `thetadata_username` and `thetadata_password` in the backtest() function if "
                    "you are using ThetaDataBacktesting. If you don't have one, you can do registeration "
                    "from https://www.thetadata.net/."
                )

        if not self.IS_BACKTESTABLE:
            logging.warning(f"Strategy {name + ' ' if name is not None else ''}cannot be " f"backtested at the moment")
            return None

        try:
            backtesting_start = to_datetime_aware(backtesting_start)
            backtesting_end = to_datetime_aware(backtesting_end)
        except AttributeError:
            logging.error(
                "`backtesting_start` and `backtesting_end` must be datetime objects. \n"
                "You are receiving this error most likely because you are using \n"
                "the original positional arguments for backtesting. \n\n"
            )
            return None

        self._trader = trader_class(logfile=logfile, backtest=True, quiet_logs=quiet_logs)

        if datasource_class == PolygonDataBacktesting:
            data_source = datasource_class(
                backtesting_start,
                backtesting_end,
                config=config,
                auto_adjust=auto_adjust,
                api_key=polygon_api_key,
                pandas_data=pandas_data,
                show_progress_bar=show_progress_bar,
                max_memory=POLYGON_MAX_MEMORY_BYTES,
                **kwargs,
            )
        elif datasource_class == ThetaDataBacktesting or optionsource_class == ThetaDataBacktesting:
            data_source = datasource_class(
                backtesting_start,
                backtesting_end,
                config=config,
                auto_adjust=auto_adjust,
                username=thetadata_username,
                password=thetadata_password,
                pandas_data=pandas_data,
                use_quote_data=use_quote_data,
                show_progress_bar=show_progress_bar,
                **kwargs,
            )
        else:
            data_source = datasource_class(
                backtesting_start,
                backtesting_end,
                config=config,
                auto_adjust=auto_adjust,
                pandas_data=pandas_data,
                show_progress_bar=show_progress_bar,
                **kwargs,
            )

        if not use_other_option_source:
            backtesting_broker = BacktestingBroker(data_source)
        else:
            options_source = optionsource_class(
                backtesting_start,
                backtesting_end,
                config=config,
                auto_adjust=auto_adjust,
                username=thetadata_username,
                password=thetadata_password,
                pandas_data=pandas_data,
                show_progress_bar=show_progress_bar,
                **kwargs,
            )
            backtesting_broker = BacktestingBroker(data_source, options_source)

        strategy = self(
            backtesting_broker,
            minutes_before_closing=minutes_before_closing,
            minutes_before_opening=minutes_before_opening,
            sleeptime=sleeptime,
            risk_free_rate=risk_free_rate,
            stats_file=stats_file,
            benchmark_asset=benchmark_asset,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data,
            quote_asset=quote_asset,
            starting_positions=starting_positions,
            name=name,
            budget=budget,
            parameters=parameters,
            buy_trading_fees=buy_trading_fees,
            sell_trading_fees=sell_trading_fees,
            save_logfile=save_logfile,
            **kwargs,
        )
        self._trader.add_strategy(strategy)

        logger.info("Starting backtest...")
        start = datetime.datetime.now()

        result = self._trader.run_all(
            show_plot=show_plot,
            show_tearsheet=show_tearsheet,
            save_tearsheet=save_tearsheet,
            show_indicators=show_indicators,
            tearsheet_file=tearsheet_file,
            base_filename=base_filename,
        )

        end = datetime.datetime.now()
        backtesting_length = backtesting_end - backtesting_start
        backtesting_run_time = end - start
        logger.info(
            f"Backtest took {backtesting_run_time} for a speed of {backtesting_run_time / backtesting_length:,.3f}"
        )

        return result[name], strategy

    def write_backtest_settings(self, settings_file):
        """
        Redefined in the Strategy class to that it has access to all the needed variables.
        """
        pass

    def backtest_analysis(
        self,
        logdir=None,
        show_plot=True,
        show_tearsheet=True,
        show_indicators=True,
        save_tearsheet=True,
        plot_file_html=None,
        tearsheet_file=None,
        trades_file=None,
        settings_file=None,
        indicators_file=None,
        tearsheet_csv_file=None,
        base_filename="",  # This is the base filename for the backtest
    ):
        name = self._name

        # Filename defaults
        if not logdir:
            logdir = "logs"

        if not plot_file_html:
            plot_file_html = f"{logdir}/{base_filename}_trades.html"
        if not trades_file:
            trades_file = f"{logdir}/{base_filename}_trades.csv"
        if not tearsheet_file:
            tearsheet_file = f"{logdir}/{base_filename}_tearsheet.html"
        if not settings_file:
            settings_file = f"{logdir}/{base_filename}_settings.json"
        if not indicators_file:
            indicators_file = f"{logdir}/{base_filename}_indicators.html"
        if not tearsheet_csv_file:
            tearsheet_csv_file = f"{logdir}/{base_filename}_tearsheet.csv"

        self.write_backtest_settings(settings_file)

        backtesting_broker = self.broker
        backtesting_broker.export_trade_events_to_csv(trades_file)
        self.plot_returns_vs_benchmark(
            plot_file_html,
            backtesting_broker._trade_event_log_df,
            show_plot=show_plot,
        )
        # Create chart lines dataframe
        chart_lines_df = pd.DataFrame(self._chart_lines_list)
        # Create chart markers dataframe
        chart_markers_df = pd.DataFrame(self._chart_markers_list)

        # Check if we have at least one indicator to plot
        if chart_markers_df is not None and chart_lines_df is not None:
            plot_indicators(
                indicators_file,
                chart_markers_df,
                chart_lines_df,
                f"{self._log_strat_name()}Strategy Indicators",
                show_indicators=show_indicators,
            )

        tearsheet_result = self.tearsheet(
            save_tearsheet=save_tearsheet,
            tearsheet_file=tearsheet_file,
            show_tearsheet=show_tearsheet,
        )

        # Save the result to a csv file
        if tearsheet_result is not None:
            tearsheet_result.to_csv(tearsheet_csv_file)

        return tearsheet_result

    @classmethod
    def verify_backtest_inputs(cls, backtesting_start, backtesting_end):
        """
        Helper function to check that the inputs are set correctly for BackTest.
        Parameters
        ----------
        backtesting_start: datetime.datetime
            The start datetime of the backtesting period.
        backtesting_end: datetime.datetime
            The end datetime of the backtesting period.

        Raises
        -------
        ValueError
            If the inputs are not set correctly.
        """
        # Check backtesting_start and backtesting_end
        if not isinstance(backtesting_start, datetime.datetime):
            raise ValueError(f"`backtesting_start` must be a datetime object. You passed in {backtesting_start}")

        if not isinstance(backtesting_end, datetime.datetime):
            raise ValueError(f"`backtesting_end` must be a datetime object. You passed in {backtesting_end}")

        # Check that backtesting end is after backtesting start
        if backtesting_end <= backtesting_start:
            raise ValueError(
                f"`backtesting_end` must be after `backtesting_start`. You passed in "
                f"{backtesting_end} and {backtesting_start}"
            )

    @classmethod
    def backtest(
        self,
        datasource_class,
        backtesting_start,
        backtesting_end,
        minutes_before_closing=1,
        minutes_before_opening=60,
        sleeptime=1,
        stats_file=None,
        risk_free_rate=None,
        logfile=None,
        config=None,
        auto_adjust=False,
        name=None,
        budget=None,
        benchmark_asset="SPY",
        plot_file_html=None,
        trades_file=None,
        settings_file=None,
        pandas_data=None,
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        starting_positions=None,
        show_plot=None,
        tearsheet_file=None,
        save_tearsheet=True,
        show_tearsheet=None,
        parameters={},
        buy_trading_fees=[],
        sell_trading_fees=[],
        polygon_api_key=None,
        indicators_file=None,
        show_indicators=None,
        save_logfile=False,
        thetadata_username=None,
        thetadata_password=None,
        use_quote_data=False,
        show_progress_bar=True,
        quiet_logs=True,
        trader_class=Trader,
        **kwargs,
    ):
        """Backtest a strategy.

        Parameters
        ----------
        datasource_class : class
            The datasource class to use. For example, if you want to use the yahoo finance datasource, then you
            would pass YahooDataBacktesting as the datasource_class.
        backtesting_start : datetime
            The start date of the backtesting period.
        backtesting_end : datetime
            The end date of the backtesting period.
        minutes_before_closing : int
            The number of minutes before closing that the minutes_before_closing strategy method will be called.
        minutes_before_opening : int
            The number of minutes before opening that the minutes_before_opening strategy method will be called.
        sleeptime : int
            The number of seconds to sleep between each iteration of the backtest.
        stats_file : str
            The file to write the stats to.
        risk_free_rate : float
            The risk free rate to use.
        logfile : str
            The file to write the log to.
        config : dict
            The config to use to set up the brokers in live trading.
        auto_adjust : bool
            Whether or not to automatically adjust the strategy.
        name : str
            The name of the strategy.
        budget : float
            The initial budget to use for the backtest.
        benchmark_asset : str or Asset
            The benchmark asset to use for the backtest to compare to. If it is a string then it will be converted
            to a stock Asset object.
        plot_file_html : str
            The file to write the plot html to.
        trades_file : str
            The file to write the trades to.
        pandas_data : list
            A list of Data objects that are used when the datasource_class object is set to PandasDataBacktesting.
            This contains all the data that will be used in backtesting.
        quote_asset : Asset (crypto)
            An Asset object for the crypto currency that will get used
            as a valuation asset for measuring overall porfolio values.
            Usually USDT, USD, USDC.
        starting_positions : dict
            A dictionary of starting positions for each asset. For example,
            if you want to start with $100 of SPY, and $200 of AAPL, then you
            would pass in starting_positions={'SPY': 100, 'AAPL': 200}.
        show_plot : bool
            Whether to show the plot.
        show_tearsheet : bool
            Whether to show the tearsheet.
        save_tearsheet : bool
            Whether to save the tearsheet.
        parameters : dict
            A dictionary of parameters to pass to the strategy. These parameters
            must be set up within the initialize() method.
        buy_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the buy orders during backtests.
        sell_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the sell orders during backtests.
        polygon_api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class.
        indicators_file : str
            The file to write the indicators to.
        show_indicators : bool
            Whether to show the indicators plot.
        save_logfile : bool
            Whether to save the logs to a file. If True, the logs will be saved to the logs directory. Defaults to False.
            Turning on this option will slow down the backtest.
        thetadata_username : str
            The username to use for the ThetaDataBacktesting datasource. Only required if you are using ThetaDataBacktesting as the datasource_class.
        thetadata_password : str
            The password to use for the ThetaDataBacktesting datasource. Only required if you are using ThetaDataBacktesting as the datasource_class.
        use_quote_data : bool
            Whether to use quote data for the backtest. Defaults to False. If True, the backtest will use quote data for the backtest. (Currently this is specific to ThetaData)
            When set to true this requests Quote data in addition to OHLC which adds time to backtests.
        show_progress_bar : bool
            Whether to show the progress bar. Defaults to True.
        quiet_logs : bool
            Whether to quiet noisy logs by setting the log level to ERROR. Defaults to True.
        trader_class : Trader class
            The trader class to use. Defaults to Trader.

        Returns
        -------
        result : dict
            A dictionary of the backtest results. Eg.

        Examples
        --------

        >>> from datetime import datetime
        >>> from lumibot.backtesting import YahooDataBacktesting
        >>> from lumibot.strategies import Strategy
        >>>
        >>> # A simple strategy that buys AAPL on the first day
        >>> class MyStrategy(Strategy):
        >>>    def on_trading_iteration(self):
        >>>        if self.first_iteration:
        >>>            order = self.create_order("AAPL", quantity=1, side="buy")
        >>>            self.submit_order(order)
        >>>
        >>> # Create a backtest
        >>> backtesting_start = datetime(2018, 1, 1)
        >>> backtesting_end = datetime(2018, 1, 31)
        >>>
        >>> # The benchmark asset to use for the backtest to compare to
        >>> benchmark_asset = Asset(symbol="QQQ", asset_type="stock")
        >>>
        >>> backtest = MyStrategy.backtest(
        >>>     datasource_class=YahooDataBacktesting,
        >>>     backtesting_start=backtesting_start,
        >>>     backtesting_end=backtesting_end,
        >>>     benchmark_asset=benchmark_asset,
        >>> )
        """
        results, strategy = self.run_backtest(
            datasource_class=datasource_class,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=minutes_before_closing,
            minutes_before_opening=minutes_before_opening,
            sleeptime=sleeptime,
            stats_file=stats_file,
            risk_free_rate=risk_free_rate,
            logfile=logfile,
            config=config,
            auto_adjust=auto_adjust,
            name=name,
            budget=budget,
            benchmark_asset=benchmark_asset,
            plot_file_html=plot_file_html,
            trades_file=trades_file,
            settings_file=settings_file,
            pandas_data=pandas_data,
            quote_asset=quote_asset,
            starting_positions=starting_positions,
            show_plot=show_plot,
            tearsheet_file=tearsheet_file,
            save_tearsheet=save_tearsheet,
            show_tearsheet=show_tearsheet,
            parameters=parameters,
            buy_trading_fees=buy_trading_fees,
            sell_trading_fees=sell_trading_fees,
            polygon_api_key=polygon_api_key,
            indicators_file=indicators_file,
            show_indicators=show_indicators,
            save_logfile=save_logfile,
            thetadata_username=thetadata_username,
            thetadata_password=thetadata_password,
            use_quote_data=use_quote_data,
            show_progress_bar=show_progress_bar,
            quiet_logs=quiet_logs,
            trader_class=trader_class,
            **kwargs,
        )
        return results
