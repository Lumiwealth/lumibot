import datetime
import logging
from typing import Union, List, Dict

from termcolor import colored
from asyncio.log import logger
from decimal import Decimal
import os
import string
import random
import traceback
import math
import time
from sqlalchemy.exc import OperationalError
import pytz
import requests
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import uuid
import json
import io
from sqlalchemy import create_engine, inspect, text

import pandas as pd
from lumibot import LUMIBOT_DEFAULT_PYTZ
from ..backtesting import BacktestingBroker, PolygonDataBacktesting, ThetaDataBacktesting, AlpacaBacktesting, InteractiveBrokersRESTBacktesting
from ..entities import Asset, Position, Order, Data
from ..tools import (
    create_tearsheet,
    day_deduplicate,
    get_symbol_returns,
    plot_indicators,
    plot_returns,
    stats_summary,
    to_datetime_aware,
)
from ..traders import Trader
from .strategy_executor import StrategyExecutor
from ..credentials import (
    THETADATA_CONFIG, 
    STRATEGY_NAME, 
    BROKER,
    DATA_SOURCE,
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
    BACKTESTING_START,
    BACKTESTING_END,
    LOG_BACKTEST_PROGRESS_TO_FILE,
    LOG_ERRORS_TO_CSV,
    INTERACTIVE_BROKERS_REST_CONFIG,
    BACKTESTING_QUIET_LOGS,
    BACKTESTING_SHOW_PROGRESS_BAR
)
# Set the stats table name for when storing stats in a database, defined by db_connection_str
STATS_TABLE_NAME = "strategy_tracker"

class SafeJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for Lumibot objects.
    
    Handles:
    - Objects with to_dict() method -> dictionary 
    - datetime.date and datetime.datetime -> ISO format string
    - Decimal -> float
    - Sets -> list
    """
    def default(self, obj):
        # Handle objects with to_dict method (Asset, Order, Position etc)
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
            
        # Handle dates and times
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
            
        # Handle Decimal
        if isinstance(obj, Decimal):
            return float(obj)

        # Handle sets
        if isinstance(obj, set):
            return list(obj)
            
        return super().default(obj)

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

    def get(self, name, default=None):
        """Gets the value of a variable, returning a default value if it doesn't exist."""
        return self._vars_dict.get(name, default)

    def all(self):
        return self._vars_dict.copy()


class _Strategy:
    IS_BACKTESTABLE = True
    _trader = None

    def __init__(
        self,
        broker=None,
        data_source=None,
        minutes_before_closing=1,
        minutes_before_opening=60,
        minutes_after_closing=0,
        sleeptime="1M",
        stats_file=None,
        risk_free_rate=None,
        benchmark_asset: str | Asset | None = "SPY",
        analyze_backtest: bool = True,
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
        include_cash_positions=False,
        **kwargs,
    ):
        """Initializes a Strategy object.

        Parameters
        ----------
        broker : Broker
            The broker to use for the strategy. Required. For backtesting, use the BacktestingBroker class.
        data_source : DataSource
            The data source to use for the strategy. If not specified, uses the broker's default data source.
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
        benchmark_asset : Asset or str or None
            The asset to use as the benchmark for the strategy. Defaults to "SPY". Strings are converted to
            Asset objects with an asset_type="stock". None, means don't benchmark the strategy.
        analyze_backtest: bool
            Run the backtest_analysis function at the end.
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
        include_cash_positions : bool
            If True, the strategy will include cash positions in the positions list returned by the get_positions
            method. Defaults to False.
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

        # initialize cash variables
        self._position_value = None
        self._portfolio_value = None

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
        
        # Track which assets we've logged "Getting historical prices" for to reduce noise
        self._logged_get_historical_prices_assets = set()
        
        if self.broker == None:
            self.broker = BROKER

        # Handle data source initialization
        self._data_source = data_source
        if self._data_source is None:
            self._data_source = DATA_SOURCE
            
        # If we have a custom data source, attach it to the broker
        if self._data_source is not None and self.broker is not None:
            # Store the original data source for reference
            self._original_broker_data_source = self.broker.data_source
            
            # Set the custom data source
            self.broker.data_source = self._data_source

        self.hide_positions = HIDE_POSITIONS
        self.hide_trades = HIDE_TRADES
        self.include_cash_positions = include_cash_positions

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

        self._quote_asset = quote_asset if self.broker.name != "bitunix" else Asset("USDT", Asset.AssetType.CRYPTO)

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
        self._analyze_backtest = analyze_backtest

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
            # If budget is not provided to run_backtest, default it
            effective_budget = budget
            if effective_budget is None:
                effective_budget = 100000  # Default budget
            
            self._set_cash_position(effective_budget)
            self._initial_budget = effective_budget # Store the budget used

            # ## TODO: Should all this just use _update_portfolio_value()?
            # ## START
            # Portfolio value should start with the cash set by the budget
            self._portfolio_value = self.cash # Calls property, should reflect effective_budget now

            store_assets = list(self.broker.data_source._data_store.keys())
            if len(store_assets) > 0:
                positions_value = 0
                for position in self.get_positions():
                    price = None
                    if position.asset == self._quote_asset:
                        # Don't include the quote asset since it's already included with cash
                        price = 0
                    else:
                        price = self.get_last_price(position.asset, quote=self._quote_asset)
                    value = float(position.quantity) * price
                    positions_value += value

                self._portfolio_value = self._portfolio_value + positions_value

            else:
                self._position_value = 0

            # END
            ##############################################

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

        # Check if the order quantity is None
        if order.quantity is None:
            self.logger.error(
                f"Order quantity cannot be None. Please provide a valid quantity value."
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
            if position is not None and position.asset == self._quote_asset:
                position.quantity = cash
                self.broker._filled_positions[x] = position
                return

        # If not in positions, create a new position for cash
        position = Position(
            self._name,
            self._quote_asset,
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
                broker_balances = self.broker._get_balances_at_broker(self._quote_asset, self)
            except Exception as e:
                self.logger.error(f"Error getting broker balances: {e}")
                return False

            if broker_balances is not None:
                cash, position_value, portfolio_value = broker_balances
                
                # Update cash position instead of setting _cash directly
                self._set_cash_position(cash)
                self._position_value = position_value
                self._portfolio_value = portfolio_value

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
                broker_balances = self.broker._get_balances_at_broker(self._quote_asset, self)
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
                if asset != self._quote_asset:
                    asset_is_option = False
                    if asset.asset_type == "crypto" or asset.asset_type == "forex":
                        asset = (asset, self._quote_asset)
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
                    else (position.asset, self._quote_asset)
                )
                quantity = position.quantity
                price = prices.get(asset, 0)

                # If the asset is the quote asset, then we already have included it from cash
                # Eg. if we have a position of USDT and USDT is the quote_asset then we already consider it as cash
                if self._quote_asset is not None:
                    if isinstance(asset, tuple) and asset == (
                        self._quote_asset,
                        self._quote_asset,
                    ):
                        price = 0
                    elif isinstance(asset, Asset) and asset == self._quote_asset:
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
                portfolio_value += float(quantity) * float(price) * multiplier
            self._portfolio_value = portfolio_value
        return portfolio_value

    def _update_cash(self, side, quantity, price, multiplier):
        """update the self.cash"""
        with self._executor.lock:
            cash_val = self.cash # Calls property
            if cash_val is None: # Handle if property somehow still returns None despite the fix in its getter
                # self.logger.warning("_update_cash: self.cash (property) returned None. Defaulting to 0.0 for calculation.")
                cash_val = 0.0
            
            current_cash = Decimal(str(cash_val)) # Convert to Decimal robustly

            # Ensure all operands are Decimal for precision
            quantity_dec = Decimal(str(quantity))
            price_dec = Decimal(str(price))
            multiplier_dec = Decimal(str(multiplier))

            if side == "buy":
                current_cash -= quantity_dec * price_dec * multiplier_dec
            if side == "sell":
                current_cash += quantity_dec * price_dec * multiplier_dec

            self._set_cash_position(float(current_cash)) # _set_cash_position expects float

            # Todo also update the cash asset in positions?

            return self.cash # Return the updated cash by calling the property again

    def _update_cash_with_dividends(self):
        with self._executor.lock:
            positions = self.broker.get_tracked_positions(self._name)

            assets = []
            for position in positions:
                if position.asset != self._quote_asset:
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
            self._stats = self._stats.sort_index()
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
        if not self.is_backtesting or not self._benchmark_asset:
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
                df["return"] = df["close"].pct_change(fill_method=None)

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

            if type(self.broker.data_source) == AlpacaBacktesting:
                benchmark_asset = self._benchmark_asset

                df = self.broker.data_source.get_historical_prices_between_dates(
                    base_asset=benchmark_asset
                )

                if df is None or df.empty:
                    logger.error(f"Couldn't get_historical_prices_between_dates: {benchmark_asset}")
                    return
                df = df.loc[self._backtesting_start:self._backtesting_end].copy()
                df["return"] = df["close"].pct_change(fill_method=None)
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
        backtesting_start: datetime = None,
        backtesting_end: datetime = None,
        minutes_before_closing = 5,
        minutes_before_opening = 60,
        sleeptime = 1,
        stats_file = None,
        risk_free_rate = None,
        logfile = None,
        config = None,
        auto_adjust = False,
        name = None,
        budget = None,
        benchmark_asset: str | Asset | None="SPY",
        analyze_backtest: bool = True,
        plot_file_html = None,
        trades_file = None,
        settings_file = None,
        pandas_data: Union[List, Dict[Asset, Data]] = None,
        quote_asset = Asset(symbol="USD", asset_type="forex"),
        starting_positions = None,
        show_plot = None,
        tearsheet_file = None,
        save_tearsheet = True,
        show_tearsheet = None,
        parameters = {},
        buy_trading_fees = [],
        sell_trading_fees = [],
        polygon_api_key = None,
        use_other_option_source = False,
        thetadata_username = None,
        thetadata_password = None,
        indicators_file = None,
        show_indicators = None,
        save_logfile = False,
        use_quote_data = False,
        show_progress_bar = True,
        quiet_logs = False,
        trader_class = Trader,
        include_cash_positions=False,
        save_stats_file = True,
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
        benchmark_asset : str or Asset or None
            The benchmark asset to use for the backtest to compare to. If it is a string then it will be converted
            to a stock Asset object. If it is None, no benchmarking will occur.
        analyze_backtest: bool = True
            Run the backtest_analysis method on the strategy.
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
        self._analyze_backtest = analyze_backtest

        # Set backtesting_start: priority 1 - BACKTESTING_START env var, 2 - passed argument, 3 - default to 1 year ago
        if BACKTESTING_START is not None:
            backtesting_start = BACKTESTING_START
        elif backtesting_start is not None:
            pass
        else:
            backtesting_start = datetime.datetime.now() - datetime.timedelta(days=365)
            logging.warning(
            colored(
                "backtesting_start is set to one year ago by default. You can set it to a specific date by passing in the backtesting_start parameter or by setting the BACKTESTING_START environment variable.",
                "yellow"
            )
            )

        # Set backtesting_end: priority 1 - BACKTESTING_END env var, 2 - passed argument, 3 - default to yesterday
        if BACKTESTING_END is not None:
            backtesting_end = BACKTESTING_END
        elif backtesting_end is not None:
            pass
        else:
            backtesting_end = datetime.datetime.now() - datetime.timedelta(days=1)
            logging.warning(
            colored(
                "backtesting_end is set to the current date by default. You can set it to a specific date by passing in the backtesting_end parameter or by setting the BACKTESTING_END environment variable.",
                "yellow"
            )
            )

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
        if stats_file is None and save_stats_file:
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
        
        if BACKTESTING_QUIET_LOGS is not None:
            quiet_logs = BACKTESTING_QUIET_LOGS

        if BACKTESTING_SHOW_PROGRESS_BAR is not None:
            show_progress_bar = BACKTESTING_SHOW_PROGRESS_BAR
        
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
                log_backtest_progress_to_file=LOG_BACKTEST_PROGRESS_TO_FILE,
                log_errors_to_csv=LOG_ERRORS_TO_CSV,
                progress_csv_path=f"{logdir}/{base_filename}_progress.csv",
                errors_csv_path=f"{logdir}/{base_filename}_errors.csv",
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
                progress_csv_path=f"{logdir}/{base_filename}_progress.csv",
                errors_csv_path=f"{logdir}/{base_filename}_errors.csv",
                log_backtest_progress_to_file=LOG_BACKTEST_PROGRESS_TO_FILE,
                log_errors_to_csv=LOG_ERRORS_TO_CSV,
                **kwargs,
            )
        elif datasource_class == InteractiveBrokersRESTBacktesting:
            data_source = datasource_class(
                backtesting_start,
                backtesting_end,
                config=INTERACTIVE_BROKERS_REST_CONFIG,
                auto_adjust=auto_adjust,
                pandas_data=pandas_data,
                show_progress_bar=show_progress_bar,
                progress_csv_path=f"{logdir}/{base_filename}_progress.csv",
                errors_csv_path=f"{logdir}/{base_filename}_errors.csv",
                log_backtest_progress_to_file=LOG_BACKTEST_PROGRESS_TO_FILE,
                log_errors_to_csv=LOG_ERRORS_TO_CSV,
                **kwargs,
            )
        else:
            data_source = datasource_class(
                datetime_start=backtesting_start,
                datetime_end=backtesting_end,
                config=config,
                auto_adjust=auto_adjust,
                pandas_data=pandas_data,
                show_progress_bar=show_progress_bar,
                progress_csv_path=f"{logdir}/{base_filename}_progress.csv",
                errors_csv_path=f"{logdir}/{base_filename}_errors.csv",
                log_backtest_progress_to_file=LOG_BACKTEST_PROGRESS_TO_FILE,
                log_errors_to_csv=LOG_ERRORS_TO_CSV,
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
            analyze_backtest=analyze_backtest,
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
            include_cash_positions=include_cash_positions,
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
        base_filename=None
    ):
        if not self._analyze_backtest:
            return

        if not base_filename:
            base_filename = self._name

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

        # Check that backtesting_end is not in the future
        now = datetime.datetime.now(backtesting_end.tzinfo) if backtesting_end.tzinfo else datetime.datetime.now()
        if backtesting_end > now:
            raise ValueError(
                f"`backtesting_end` cannot be in the future. You passed in {backtesting_end}, now is {now}"
            )

    def send_update_to_cloud(self):
        """
        Sends an update to the LumiWealth cloud server with the current portfolio value, cash, positions, and any outstanding orders.
        There is an API Key that is required to send the update to the cloud. 
        The API Key is stored in the environment variable LUMIWEALTH_API_KEY.
        """
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return
        
        # Check if self.lumiwealth_api_key has been set, if not, return
        if not hasattr(self, "lumiwealth_api_key") or self.lumiwealth_api_key is None or self.lumiwealth_api_key == "":
        
            # TODO: Set this to a warning once the API is ready
            # Log that we are not sending the update to the cloud
            self.logger.debug("LUMIWEALTH_API_KEY not set. Not sending an update to the cloud because lumiwealth_api_key is not set. If you would like to be able to track your bot performance on our website, please set the lumiwealth_api_key parameter in the strategy initialization or the LUMIWEALTH_API_KEY environment variable.")
            return

        # Get the current portfolio value
        portfolio_value = self.get_portfolio_value()

        # Get the current cash
        cash = self.get_cash()

        # Get the current positions
        positions = self.get_positions()

        # Get the current orders
        orders = self.get_orders()
        
        LUMIWEALTH_URL = "https://listener.lumiwealth.com/portfolio_events"

        headers = {
            "x-api-key": f"{self.lumiwealth_api_key}",
            "Content-Type": "application/json",
        }

        # Create the data to send to the cloud
        data = {
            "data_type": "portfolio_event",
            "portfolio_value": portfolio_value,
            "cash": cash,
            "positions": [position.to_dict() for position in positions],
            "orders": [order.to_dict() for order in orders],
            "strategy_name": self._name,
            "broker_name": self.broker.name,
        }

        # Helper function to recursively replace NaN in dictionaries
        def replace_nan(value):
            if isinstance(value, float) and math.isnan(value):
                return None  # or 0 if you prefer
            elif isinstance(value, dict):
                return {k: replace_nan(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [replace_nan(v) for v in value]
            else:
                return value

        # Apply to your data dictionary
        data = replace_nan(data)

        try:
            # Send the data to the cloud
            json_data = json.dumps(data, default=str)
            response = requests.post(LUMIWEALTH_URL, headers=headers, data=json_data)
        except Exception as e:
            self.logger.error(f"Failed to send update to the cloud because of lumibot error. Error: {e}")
            # Add the traceback to the log
            self.logger.error(traceback.format_exc())
            return False

        # Check if the message was sent successfully
        if response.status_code == 200:
            self.logger.debug("Update sent to the cloud successfully")
            return True
        else:
            self.logger.error(
                f"Failed to send update to the cloud because of cloud error. Status code: {response.status_code}, message: {response.text}"
            )
            return False

    def should_send_account_summary_to_discord(self):
        # Check if db_connection_str has been set, if not, return False
        if not hasattr(self, "db_connection_str"):
            # Log that we are not sending the account summary to Discord
            self.logger.info(
                "Not sending account summary to Discord because self does not have db_connection_str attribute")
            return False

        if self.db_connection_str is None or self.db_connection_str == "":
            # Log that we are not sending the account summary to Discord
            self.logger.debug("Not sending account summary to Discord because db_connection_str is not set")
            return False

        # Check if discord_webhook_url has been set, if not, return False
        if not self.discord_webhook_url or self.discord_webhook_url == "":
            # Log that we are not sending the account summary to Discord
            self.logger.info("Not sending account summary to Discord because discord_webhook_url is not set")
            return False

        # Check if should_send_summary_to_discord has been set, if not, return False
        if not self.should_send_summary_to_discord:
            # Log that we are not sending the account summary to Discord
            self.logger.info(
                f"Not sending account summary to Discord because should_send_summary_to_discord is False or not set. The value is: {self.should_send_summary_to_discord}")
            return False

        # Check if last_account_summary_dt has been set, if not, set it to None
        if not hasattr(self, "last_account_summary_dt"):
            self.last_account_summary_dt = None

        # Get the current datetime
        now = datetime.datetime.now()

        # Calculate the time since the last account summary if it has been set
        if self.last_account_summary_dt is not None:
            time_since_last_account_summary = now - self.last_account_summary_dt
        else:
            time_since_last_account_summary = None

        # Check if it has been at least 24 hours since the last account summary
        if self.last_account_summary_dt is None or time_since_last_account_summary.total_seconds() >= 86400: # 24 hours
            # Set the last account summary datetime to now
            self.last_account_summary_dt = now

            # Sleep for 5 seconds to make sure all the orders go through first
            time.sleep(5)

            # Return True because we should send the account summary to Discord
            return True

        else:
            # Log that we are not sending the account summary to Discord
            self.logger.info(f"Not sending account summary to Discord because it has not been at least 24 hours since the last account summary. It is currently {now} and the last account summary was at: {self.last_account_summary_dt}, which was {time_since_last_account_summary} ago.")

            # Return False because we should not send the account summary to Discord
            return False

    # ====== Messaging Methods ========================

    def send_discord_message(self, message, image_buf=None, silent=True):
        """
        Sends a message to Discord
        """

        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Check if the message is empty
        if message == "" or message is None:
            # If the message is empty, log and return
            self.logger.debug("The discord message is empty. Please provide a message to send to Discord.")
            return

        # Check if the discord webhook URL is set
        if self.discord_webhook_url is None or self.discord_webhook_url == "":
            # If the webhook URL is not set, log and return
            self.logger.debug(
                "The discord webhook URL is not set. Please set the discord_webhook_url parameter in the strategy \
                initialization if you want to send messages to Discord."
            )
            return

        # Remove the extra spaces at the beginning of each line
        message = "\n".join(line.lstrip() for line in message.split("\n"))

        # Get the webhook URL from the environment variables
        webhook_url = self.discord_webhook_url

        # The payload for text content
        payload = {"content": message}

        # If silent is true, set the discord message to be silent
        if silent:
            payload["flags"] = [4096]

        # Check if we have an image
        if image_buf is not None:
            # The files that you want to send
            files = {"file": ("results.png", image_buf, "image/png")}

            # Make a POST request to the webhook URL with the payload and file
            response = requests.post(webhook_url, data=payload, files=files)
        else:
            # Make a POST request to the webhook URL with the payload
            response = requests.post(webhook_url, data=payload)

        # Check if the message was sent successfully
        if response.status_code == 200 or response.status_code == 204:
            self.logger.info("Discord message sent successfully.")
        else:
            self.logger.error(
                f"Failed to send message to Discord. Status code: {response.status_code}, message: {response.text}"
            )

    def send_spark_chart_to_discord(self, stats_df, portfolio_value, now, days=1095):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Only keep the stats for the past X days
        stats_df = stats_df.loc[stats_df["datetime"] >= (now - pd.Timedelta(days=days))]

        # Set the default color
        color = "black"

        # Check what return we made over the past week
        if stats_df.shape[0] > 0:
            # Resanple the stats dataframe to daily but keep the datetime column
            stats_df = stats_df.resample("D", on="datetime").last().reset_index()

            # Drop the cash column because it's not needed
            stats_df = stats_df.drop(columns=["cash"])

            # Remove nan values
            stats_df = stats_df.dropna()

            # Get the portfolio value at the beginning of the dataframe
            portfolio_value_start = stats_df.iloc[0]["portfolio_value"]

            # Calculate the return over the past 7 days
            total_return = ((portfolio_value / portfolio_value_start) - 1) * 100

            # Check if we made a positive return, if so, set the color to green, otherwise set it to red
            if total_return > 0:
                color = "green"
            else:
                color = "red"

        # Plotting the DataFrame
        plt.figure()

        # Create an axes instance, setting the facecolor to white
        ax = plt.axes(facecolor="white")

        # Convert 'datetime' to Matplotlib's numeric format right after cleaning
        stats_df['mpl_datetime'] = mdates.date2num(stats_df['datetime'])

        # Plotting with a thicker line
        ax = stats_df.plot(
            x="mpl_datetime",
            y="portfolio_value",
            kind="line",
            linewidth=5,
            color=color,
            # label="Account Value",
            ax=ax,
            legend=False,
        )
        plt.title(f"{self._name} Account Value", fontsize=32, pad=60)
        plt.xlabel("")
        plt.ylabel("")

        # # Increase the font size of the tick labels
        # ax.tick_params(axis="both", which="major", labelsize=18)

        # Use a custom formatter for currency
        formatter = ticker.FuncFormatter(lambda x, pos: "${:1,}".format(int(x)))
        ax.yaxis.set_major_formatter(formatter)

        # Custom formatter function
        def custom_date_formatter(x, pos):
            try:
                date = mdates.num2date(x)
                if pos % 2 == 0:  # Every second tick
                    return date.strftime("%d\n%b\n%Y")
                else:  # Other ticks
                    return date.strftime("%d")
            except Exception:
                return ""

        # Set the locator for the x-axis to automatically find the dates
        locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
        ax.xaxis.set_major_locator(locator)

        # Use custom formatter for the x-axis
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(custom_date_formatter))

        # Use the ConciseDateFormatter to format the x-axis dates
        formatter = mdates.ConciseDateFormatter(locator)

        # Increase the font size of the tick labels
        ax.tick_params(axis="x", which="major", labelsize=18, rotation=0)  # For x-axis
        ax.tick_params(axis="y", which="major", labelsize=18)  # For y-axis

        # Center align x-axis labels
        for label in ax.get_xticklabels():
            label.set_horizontalalignment("center")

        # Save the plot to an in-memory file
        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight", pad_inches=0.25)
        buf.seek(0)

        # Send the image to Discord
        self.send_discord_message("-----------\n", buf)

    def send_result_text_to_discord(self, returns_text, portfolio_value, cash):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return
        
        # Check if we should hide positions
        if self.hide_positions:
            # Log that we are hiding positions in the account summary
            self.logger.info("Hiding positions because hide_positions is set to True")

            # Set the positions text to hidden 
            positions_text = "Positions are hidden"
        else:
            # Get the current positions
            positions = self.get_positions()

            # Log the positions
            self.logger.info(f"Positions for send_result_text_to_discord: {positions}")

            # Create the positions text
            positions_details_list = []
            for position in positions:
                # Check if the position asset is the quote asset

                if position.asset == self._quote_asset:
                    last_price = 1
                else:
                    # Get the last price
                    last_price = self.get_last_price(position.asset)

                # Make sure last_price is a number
                if last_price is None or not isinstance(last_price, (int, float, Decimal)):
                    self.logger.info(f"Last price for {position.asset} is not a number: {last_price}")
                    continue

                # Calculate the value of the position
                position_value = position.quantity * last_price

                # If option, multiply % of portfolio by 100
                if position.asset.asset_type == "option":
                    position_value = position_value * 100

                if position_value > 0 and portfolio_value > 0:
                    # Calculate the percent of the portfolio that this position represents
                    percent_of_portfolio = position_value / portfolio_value
                else:
                    percent_of_portfolio = 0

                # Add the position details to the list
                positions_details_list.append(
                    {
                        "asset": position.asset,
                        "quantity": position.quantity,
                        "value": position_value,
                        "percent_of_portfolio": percent_of_portfolio,
                    }
                )

            # Sort the positions by the percent of the portfolio
            positions_details_list = sorted(positions_details_list, key=lambda x: x["percent_of_portfolio"], reverse=True)

            # Create the positions text
            positions_text = ""
            for position in positions_details_list:
                # positions_text += f"{position.quantity:,.2f} {position.asset} (${position.value:,.0f} or {position.percent_of_portfolio:,.0%})\n"
                positions_text += (
                    f"{position['quantity']:,.2f} {position['asset']} (${position['value']:,.0f} or {position['percent_of_portfolio']:,.0%})\n"
                )

        # Create a message to send to Discord (round the values to 2 decimal places)
        cash_str = f"{cash:,.2f}" if cash is not None else "N/A"
        portfolio_value_str = f"{portfolio_value:,.2f}" if portfolio_value is not None else "N/A"
        message = f"""
                **Update for {self._name}**
                **Account Value:** ${portfolio_value_str}
                **Cash:** ${cash_str}
                {returns_text}
                **Positions:**
                {positions_text}
                """

        # Remove any leading whitespace
        # Remove the extra spaces at the beginning of each line
        message = "\n".join(line.lstrip() for line in message.split("\n"))

        # Add self.discord_account_summary_footer to the message
        if hasattr(self, "discord_account_summary_footer") and self.discord_account_summary_footer is not None:
            message += f"{self.discord_account_summary_footer}\n\n"

        # Add powered by Lumiwealth to the message
        message += "[**Powered by 💡 Lumiwealth**](<https://lumiwealth.com>)\n-----------"

        # Send the message to Discord
        self.send_discord_message(message, None)

    def send_account_summary_to_discord(self):
        # Log that we are sending the account summary to Discord
        self.logger.debug("Considering sending account summary to Discord")

        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            # Log that we are not sending the account summary to Discord
            self.logger.debug("Not sending account summary to Discord because we are in backtesting mode")
            return

        # Check if last_account_summary_dt has been set, if not, set it to None
        if not hasattr(self, "last_account_summary_dt"):
            self.last_account_summary_dt = None

        # Check if we should send an account summary to Discord
        should_send_account_summary = self.should_send_account_summary_to_discord()
        if not should_send_account_summary:
            # Log that we are not sending the account summary to Discord
            return

        # Log that we are sending the account summary to Discord
        self.logger.info("Sending account summary to Discord")

        # Get the current portfolio value
        portfolio_value = self.get_portfolio_value()

        # Get the current cash
        cash = self.get_cash()

        # # Get the datetime
        now = pd.Timestamp(datetime.datetime.now()).tz_localize(LUMIBOT_DEFAULT_PYTZ)

        # Get the returns
        returns_text, stats_df = self.calculate_returns()

        # Send a spark chart to Discord
        self.send_spark_chart_to_discord(stats_df, portfolio_value, now)

        # Send the results text to Discord
        self.send_result_text_to_discord(returns_text, portfolio_value, cash)

    def get_stats_from_database(self, stats_table_name, retries=5, delay=5):
        attempt = 0
        while attempt < retries:
            try:
                # Create or verify the database connection
                if not hasattr(self, 'db_engine') or not self.db_engine:
                    self.db_engine = create_engine(self.db_connection_str)
                else:
                    # Verify the connection
                    with self.db_engine.connect() as conn:
                        conn.execute(text("SELECT 1"))

                # Check if the table exists
                if not inspect(self.db_engine).has_table(stats_table_name):
                    # Log that the table does not exist and we are creating it
                    self.logger.info(f"Table {stats_table_name} does not exist. Creating it now.")

                    # Get the current time in New York
                    ny_tz = LUMIBOT_DEFAULT_PYTZ
                    now = datetime.datetime.now(ny_tz)

                    # Create an empty stats dataframe
                    stats_new = pd.DataFrame(
                        {
                            "id": [str(uuid.uuid4())],
                            "datetime": [now],
                            "portfolio_value": [0.0],  # Default or initial value
                            "cash": [0.0],             # Default or initial value
                            "strategy_id": ["INITIAL VALUE"], # Default or initial value
                        }
                    )

                    # Set the index
                    stats_new.set_index("id", inplace=True)

                    # Create the table by saving this empty DataFrame to the database
                    self.to_sql(stats_new, stats_table_name, if_exists='replace', index=True)
                
                # Load the stats dataframe from the database
                stats_df = pd.read_sql_table(stats_table_name, self.db_engine)
                return stats_df

            except OperationalError as e:
                self.logger.error(f"OperationalError: {e}")
                attempt += 1
                if attempt < retries:
                    self.logger.info(f"Retrying in {delay} seconds and recreating db_engine...")
                    time.sleep(delay)
                    self.db_engine = create_engine(self.db_connection_str)  # Recreate the db_engine
                else:
                    self.logger.error("Max retries reached for get_stats_from_database. Failing operation.")
                    raise

    def to_sql(self, stats_df, stats_table_name, if_exists='replace', index=True, retries=5, delay=5):
        attempt = 0
        while attempt < retries:
            try:
                stats_df.to_sql(stats_table_name, self.db_engine, if_exists=if_exists, index=index)
                return
            except OperationalError as e:
                self.logger.error(f"OperationalError during to_sql: {e}")
                attempt += 1
                if attempt < retries:
                    self.logger.info(f"Retrying in {delay} seconds and recreating db_engine...")
                    time.sleep(delay)
                    self.db_engine = create_engine(self.db_connection_str)  # Recreate the db_engine
                else:
                    self.logger.error("Max retries reached for to_sql. Failing operation.")
                    raise
    
    def backup_variables_to_db(self):
        if self.is_backtesting:
            return

        if not hasattr(self, "db_connection_str") or self.db_connection_str is None or self.db_connection_str == "" or not self.should_backup_variables_to_database:
            return

        # Ensure we have a self.db_engine
        if not hasattr(self, 'db_engine') or not self.db_engine:
            self.db_engine = create_engine(self.db_connection_str)

        # Get the current time in New York
        ny_tz = LUMIBOT_DEFAULT_PYTZ
        now = datetime.datetime.now(ny_tz)

        if not inspect(self.db_engine).has_table(self.backup_table_name):
            # Log that the table does not exist and we are creating it
            self.logger.info(f"Table {self.backup_table_name} does not exist. Creating it now.")

            # Create an empty stats dataframe
            stats_new = pd.DataFrame(
                {
                    "id": [str(uuid.uuid4())],
                    "last_updated": [now],
                    "variables": ["INITIAL VALUE"],
                    "strategy_id": ["INITIAL VALUE"]
                }
            )

            # Set the index
            stats_new.set_index("id", inplace=True)

            # Create the table by saving this empty DataFrame to the database
            stats_new.to_sql(self.backup_table_name, self.db_engine, if_exists='replace', index=True)

        current_state = json.dumps(self.vars.all(), sort_keys=True, cls=SafeJSONEncoder)
        if current_state == self._last_backup_state:
            self.logger.info("No variables changed. Not backing up.")
            return

        try:
            data_to_save = self.vars.all()
            if data_to_save:
                json_data_to_save = json.dumps(data_to_save, cls=SafeJSONEncoder)
                with self.db_engine.connect() as connection:
                    with connection.begin():
                        # Check if the row exists
                        check_query = text(f"""
                            SELECT 1 FROM {self.backup_table_name} WHERE strategy_id = :strategy_id
                        """)
                        result = connection.execute(check_query, {'strategy_id': self._name}).fetchone()

                        if result:
                            # Update the existing row
                            update_query = text(f"""
                                UPDATE {self.backup_table_name}
                                SET last_updated = :last_updated, variables = :variables
                                WHERE strategy_id = :strategy_id
                            """)
                            connection.execute(update_query, {
                                'last_updated': now,
                                'variables': json_data_to_save,
                                'strategy_id': self._name
                            })
                        else:
                            # Insert a new row
                            insert_query = text(f"""
                                INSERT INTO {self.backup_table_name} (id, last_updated, variables, strategy_id)
                                VALUES (:id, :last_updated, :variables, :strategy_id)
                            """)
                            connection.execute(insert_query, {
                                'id': str(uuid.uuid4()),
                                'last_updated': now,
                                'variables': json_data_to_save,
                                'strategy_id': self._name
                            })

                self._last_backup_state = current_state
                logger.info("Variables backed up successfully")
            else:
                logger.info("No variables to back up")

        except Exception as e:
            logger.error(f"Error backing up variables to DB: {e}", exc_info=True)

    def load_variables_from_db(self):
        if self.is_backtesting:
            return

        if not hasattr(self, "db_connection_str") or self.db_connection_str is None or not self.should_backup_variables_to_database:
            return

        try:
            if not hasattr(self, 'db_engine') or not self.db_engine:
                self.db_engine = create_engine(self.db_connection_str)

            # Check if backup table exists
            inspector = inspect(self.db_engine)
            if not inspector.has_table(self.backup_table_name):
                logger.info(f"Backup for {self._name} does not exist in the database. Not restoring")
                return

             # Query the latest entry from the backup table
            query = text(
                f'SELECT * FROM {self.backup_table_name} WHERE strategy_id = :strategy_id ORDER BY last_updated DESC LIMIT 1')

            params = {'strategy_id': self._name}
            df = pd.read_sql_query(query, self.db_engine, params=params)

            if df.empty:
                logger.debug("No data found in the backup") 
            else:
                # Parse the JSON data
                json_data = df['variables'].iloc[0]
                # Decode any special types we stored using our SafeJSONEncoder
                data = json.loads(json_data, object_hook=lambda d: {
                    k: (
                        datetime.datetime.fromisoformat(v) if isinstance(v, str) and 'T' in v
                        else datetime.datetime.strptime(v, '%Y-%m-%d').date() if isinstance(v, str) and '-' in v
                        else v
                    ) for k, v in d.items()
                })

                # Update self.vars dictionary
                for key, value in data.items():
                    self.vars.set(key, value)

                current_state = json.dumps(self.vars.all(), sort_keys=True, cls=SafeJSONEncoder)
                self._last_backup_state = current_state

                logger.info("Variables loaded successfully from database")

        except Exception as e:
            logger.error(f"Error loading variables from database: {e}", exc_info=True)

    def calculate_returns(self):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Calculate the return over the past 24 hours, 7 days, and 30 days using the stats dataframe

        # Get the current time in New York
        ny_tz = LUMIBOT_DEFAULT_PYTZ

        # Get the datetime
        now = datetime.datetime.now(ny_tz)

        # Load the stats dataframe from the database
        stats_df = self.get_stats_from_database(STATS_TABLE_NAME)

        # Only keep the stats for this strategy ID
        stats_df = stats_df.loc[stats_df["strategy_id"] == self.strategy_id]

        # Convert the datetime column to a datetime
        stats_df["datetime"] = pd.to_datetime(stats_df["datetime"])  # , utc=True)

        # Check if the datetime column is timezone-aware
        if stats_df['datetime'].dt.tz is None:
            # If the datetime is timezone-naive, directly localize it to "America/New_York"
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(LUMIBOT_DEFAULT_PYTZ, ambiguous='infer')
        else:
            # If the datetime is already timezone-aware, first remove timezone and then localize
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(None)
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(LUMIBOT_DEFAULT_PYTZ, ambiguous='infer')

        # Get the stats
        stats_new = pd.DataFrame(
            {
                "id": str(uuid.uuid4()),
                "datetime": [now],
                "portfolio_value": [self.get_portfolio_value()],
                "cash": [self.get_cash()],
                "strategy_id": [self.strategy_id],
            }
        )

        # Set the index
        stats_new.set_index("id", inplace=True)

        # Add the new stats to the existing stats
        stats_df = pd.concat([stats_df, stats_new])

        # # Convert the datetime column to eastern time
        stats_df["datetime"] = stats_df["datetime"].dt.tz_convert(LUMIBOT_DEFAULT_PYTZ)

        # Remove any duplicate rows
        stats_df = stats_df[~stats_df["datetime"].duplicated(keep="last")]

        # Sort the stats by the datetime column
        stats_df = stats_df.sort_values("datetime")

        # Set the strategy ID column to be the strategy ID
        stats_df["strategy_id"] = self.strategy_id

        # Index should be a uuid, fill the index with uuids
        stats_df.loc[pd.isna(stats_df["id"]), "id"] = [
            str(uuid.uuid4()) for _ in range(len(stats_df.loc[pd.isna(stats_df["id"])]))
        ]

        # Set id as the index
        stats_df = stats_df.set_index("id")

        # Check that the stats dataframe has at least 1 row and contains the portfolio_value column
        if stats_df.shape[0] > 0 and "portfolio_value" in stats_df.columns:
            # Save the stats to the database
            self.to_sql(stats_new, STATS_TABLE_NAME, "append", index=True)

            # Get the current portfolio value
            portfolio_value = self.get_portfolio_value()

            # Initialize the results
            results_text = ""

            # Add results for the past 24 hours
            # Get the datetime 24 hours ago
            datetime_24_hours_ago = now - pd.Timedelta(days=1)
            # Get the df for the past 24 hours
            stats_past_24_hours = stats_df.loc[stats_df["datetime"] >= datetime_24_hours_ago]
            # Check if there are any stats for the past 24 hours
            if stats_past_24_hours.shape[0] > 0:
                # Get the portfolio value 24 hours ago
                portfolio_value_24_hours_ago = stats_past_24_hours.iloc[0]["portfolio_value"]
                if float(portfolio_value_24_hours_ago) != 0.0:
                    # Calculate the return over the past 24 hours
                    return_24_hours = ((portfolio_value / portfolio_value_24_hours_ago) - 1) * 100
                    # Add the return to the results
                    results_text += f"**24 hour Return:** {return_24_hours:,.2f}% (${(portfolio_value - portfolio_value_24_hours_ago):,.2f} change)\n"

            # Add results for the past 7 days
            # Get the datetime 7 days ago
            datetime_7_days_ago = now - pd.Timedelta(days=7)
            # First check if we have stats that are at least 7 days old
            if stats_df["datetime"].min() < datetime_7_days_ago:
                # Get the df for the past 7 days
                stats_past_7_days = stats_df.loc[stats_df["datetime"] >= datetime_7_days_ago]
                # Check if there are any stats for the past 7 days
                if stats_past_7_days.shape[0] > 0:
                    # Get the portfolio value 7 days ago
                    portfolio_value_7_days_ago = stats_past_7_days.iloc[0]["portfolio_value"]
                    return_7_days = None
                    if float(portfolio_value_7_days_ago) != 0.0:
                        # Calculate the return over the past 7 days
                        return_7_days = ((portfolio_value / portfolio_value_7_days_ago) - 1) * 100
                        # Add the return to the results
                        results_text += f"**7 day Return:** {return_7_days:,.2f}% (${(portfolio_value - portfolio_value_7_days_ago):,.2f} change)\n"

                    # If we are up more than pct_up_threshold over the past 7 days, send a message to Discord
                    PERCENT_UP_THRESHOLD = 3
                    if return_7_days and return_7_days > PERCENT_UP_THRESHOLD:
                        # Create a message to send to Discord
                        message = f"""
                                🚀 {self._name} is up {return_7_days:,.2f}% in 7 days.
                                """

                        # Remove any leading whitespace
                        # Remove the extra spaces at the beginning of each line
                        message = "\n".join(line.lstrip() for line in message.split("\n"))

                        # Send the message to Discord
                        self.send_discord_message(message, silent=False)

            # Add results for the past 30 days
            # Get the datetime 30 days ago
            datetime_30_days_ago = now - pd.Timedelta(days=30)
            # First check if we have stats that are at least 30 days old
            if stats_df["datetime"].min() < datetime_30_days_ago:
                # Get the df for the past 30 days
                stats_past_30_days = stats_df.loc[stats_df["datetime"] >= datetime_30_days_ago]
                # Check if there are any stats for the past 30 days
                if stats_past_30_days.shape[0] > 0:
                    # Get the portfolio value 30 days ago
                    portfolio_value_30_days_ago = stats_past_30_days.iloc[0]["portfolio_value"]
                    if float(portfolio_value_30_days_ago) != 0.0:
                        # Calculate the return over the past 30 days
                        return_30_days = ((portfolio_value / portfolio_value_30_days_ago) - 1) * 100
                        # Add the return to the results
                        results_text += f"**30 day Return:** {return_30_days:,.2f}% (${(portfolio_value - portfolio_value_30_days_ago):,.2f} change)\n"

            # Get inception date
            inception_date = stats_df["datetime"].min()

            # Inception date text
            inception_date_text = f"{inception_date.strftime('%b %d, %Y')}"

            # Add results since inception
            # Get the portfolio value at inception
            portfolio_value_inception = stats_df.iloc[0]["portfolio_value"]
            # Calculate the return since inception
            return_since_inception = ((portfolio_value / portfolio_value_inception) - 1) * 100
            # Add the return to the results
            results_text += f"**Since Inception ({inception_date_text}):** {return_since_inception:,.2f}% (started at ${portfolio_value_inception:,.2f}, now ${portfolio_value - portfolio_value_inception:,.2f} change)\n"

            return results_text, stats_df

        else:
            return "Not enough data to calculate returns", stats_df

    @property
    def cash(self):
        """Returns the current cash. This is the money that is not used for positions or
        orders (in other words, the money that is available to buy new assets, or cash).

        This property is updated whenever a transaction was filled by the broker or when dividends
        are paid.

        Crypto currencies are a form of cash. Therefore cash will always be zero.

        Returns
        -------
        cash : float
            The current cash.

        Example
        -------
        >>> # Get the current cash available in the account
        >>> self.log_message(self.cash)
        """

        self.update_broker_balances(force_update=False)

        cash_position = self.get_position(self._quote_asset)
        quantity = cash_position.quantity if cash_position else None

        # This is not really true:
        # if quantity is None:
        #     self._set_cash_position(0)
        #     quantity = 0

        if type(quantity) is Decimal:
            quantity = float(quantity)
        elif quantity is None: # Ensure we return a float if cash position doesn't exist
            quantity = 0.0

        return quantity
