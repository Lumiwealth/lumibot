from __future__ import annotations

import datetime
import io
import json
import math
import os
import random
import string
import time
import traceback
import uuid
from collections import deque
from decimal import Decimal
from importlib import import_module
from typing import TYPE_CHECKING, Dict, List, Union

from lumibot.tools.lumibot_logger import get_logger, get_strategy_logger

from ..entities import Asset, Order

if TYPE_CHECKING:
    from ..entities import CashEvent, Data

# Set the stats table name for when storing stats in a database, defined by db_connection_str
STATS_TABLE_NAME = "strategy_tracker"
_COMPAT_SENTINEL = object()
_BACKTESTING_IMPORTS_READY = False
_REQUESTS_MODULE = None
_SQLALCHEMY_IMPORTS = None
_POLARS_MODULE = None
_TOOL_FUNC_CACHE = {}
_PARQUET_UTILS = None
_STRATEGY_EXECUTOR_CLASS = None
_TRADER_CLASS = None
_LUMIBOT_DEFAULT_PYTZ = None
_CREDENTIALS_MODULE = None
_COLORED_FN = None
Trader = _COMPAT_SENTINEL
BROKER = _COMPAT_SENTINEL
DATA_SOURCE = _COMPAT_SENTINEL
STRATEGY_NAME = _COMPAT_SENTINEL
HIDE_POSITIONS = _COMPAT_SENTINEL
HIDE_TRADES = _COMPAT_SENTINEL
MARKET = _COMPAT_SENTINEL
LIVE_CONFIG = _COMPAT_SENTINEL
DISCORD_WEBHOOK_URL = _COMPAT_SENTINEL
DB_CONNECTION_STR = _COMPAT_SENTINEL
LUMIWEALTH_API_KEY = _COMPAT_SENTINEL
BACKTESTING_START = _COMPAT_SENTINEL
BACKTESTING_END = _COMPAT_SENTINEL
SHOW_PLOT = _COMPAT_SENTINEL
SHOW_TEARSHEET = _COMPAT_SENTINEL
SHOW_INDICATORS = _COMPAT_SENTINEL
POLYGON_API_KEY = _COMPAT_SENTINEL
THETADATA_CONFIG = _COMPAT_SENTINEL
BACKTESTING_SHOW_PROGRESS_BAR = _COMPAT_SENTINEL
POLYGON_MAX_MEMORY_BYTES = _COMPAT_SENTINEL
LOG_BACKTEST_PROGRESS_TO_FILE = _COMPAT_SENTINEL
AlpacaBacktesting = None
BacktestingBroker = None
CcxtBacktesting = None
DataBentoDataBacktesting = None
InteractiveBrokersRESTBacktesting = None
PolygonDataBacktesting = None
RoutedBacktestingPandas = None
ThetaDataBacktesting = None
ThetaDataBacktestingPandas = None
YahooDataBacktesting = None


def colored(*args, **kwargs):
    global _COLORED_FN
    if _COLORED_FN is None:
        from termcolor import colored as _termcolor_colored

        _COLORED_FN = _termcolor_colored
    return _COLORED_FN(*args, **kwargs)


def _strategy_executor_class():
    global _STRATEGY_EXECUTOR_CLASS
    if _STRATEGY_EXECUTOR_CLASS is None:
        from .strategy_executor import StrategyExecutor

        _STRATEGY_EXECUTOR_CLASS = StrategyExecutor
    return _STRATEGY_EXECUTOR_CLASS


def _trader_class():
    global _TRADER_CLASS, Trader
    if Trader is not _COMPAT_SENTINEL:
        return Trader
    if _TRADER_CLASS is None:
        from ..traders import Trader as _Trader

        _TRADER_CLASS = _Trader
    return _TRADER_CLASS


def _default_pytz():
    global _LUMIBOT_DEFAULT_PYTZ
    if _LUMIBOT_DEFAULT_PYTZ is None:
        from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

        _LUMIBOT_DEFAULT_PYTZ = LUMIBOT_DEFAULT_PYTZ
    return _LUMIBOT_DEFAULT_PYTZ


def _credential(name):
    override = globals().get(name, _COMPAT_SENTINEL)
    if override is not _COMPAT_SENTINEL:
        return override
    global _CREDENTIALS_MODULE
    if _CREDENTIALS_MODULE is None:
        from .. import credentials

        _CREDENTIALS_MODULE = credentials
    return getattr(_CREDENTIALS_MODULE, name)


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None

    def _load(self):
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        if name in {"_module_name", "_module"}:
            object.__setattr__(self, name, value)
        else:
            setattr(self._load(), name, value)

    def __delattr__(self, name):
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


pd = _LazyModule("pandas")


def _call_tool_func(name, *args, **kwargs):
    fn = _TOOL_FUNC_CACHE.get(name)
    if fn is None:
        tools = import_module("lumibot.tools")
        fn = getattr(tools, name)
        _TOOL_FUNC_CACHE[name] = fn
    return fn(*args, **kwargs)


def _get_parquet_utils():
    global _PARQUET_UTILS
    if _PARQUET_UTILS is None:
        from lumibot.tools.parquet_utils import (
            coerce_object_columns_to_json_strings,
            is_parquet_required,
            write_parquet_with_logging,
        )

        _PARQUET_UTILS = (
            coerce_object_columns_to_json_strings,
            is_parquet_required,
            write_parquet_with_logging,
        )
    return _PARQUET_UTILS


def cash_flow_adjusted_returns(*args, **kwargs):
    return _call_tool_func("cash_flow_adjusted_returns", *args, **kwargs)


def create_tearsheet(*args, **kwargs):
    return _call_tool_func("create_tearsheet", *args, **kwargs)


def cumulative_to_period_flows(*args, **kwargs):
    return _call_tool_func("cumulative_to_period_flows", *args, **kwargs)


def day_deduplicate(*args, **kwargs):
    return _call_tool_func("day_deduplicate", *args, **kwargs)


def get_symbol_returns(*args, **kwargs):
    return _call_tool_func("get_symbol_returns", *args, **kwargs)


def plot_indicators(*args, **kwargs):
    return _call_tool_func("plot_indicators", *args, **kwargs)


def plot_returns(*args, **kwargs):
    return _call_tool_func("plot_returns", *args, **kwargs)


def stats_summary(*args, **kwargs):
    return _call_tool_func("stats_summary", *args, **kwargs)


def to_datetime_aware(*args, **kwargs):
    return _call_tool_func("to_datetime_aware", *args, **kwargs)


def _get_requests_module():
    global _REQUESTS_MODULE
    if _REQUESTS_MODULE is None:
        import requests as _requests

        _REQUESTS_MODULE = _requests
    return _REQUESTS_MODULE


def _get_sqlalchemy_imports():
    global _SQLALCHEMY_IMPORTS
    if _SQLALCHEMY_IMPORTS is None:
        from sqlalchemy import create_engine, inspect, text
        from sqlalchemy.exc import OperationalError

        _SQLALCHEMY_IMPORTS = (create_engine, inspect, text, OperationalError)
    return _SQLALCHEMY_IMPORTS


def _get_polars_module():
    global _POLARS_MODULE
    if _POLARS_MODULE is None:
        import polars as _pl

        _POLARS_MODULE = _pl
    return _POLARS_MODULE


class _LazyAgentManager:
    """Import and construct AgentManager only when strategy code uses agents."""

    __slots__ = ("_strategy", "_manager")

    def __init__(self, strategy):
        self._strategy = strategy
        self._manager = None

    def _get_manager(self):
        manager = self._manager
        if manager is None:
            from ..components.agents import AgentManager

            manager = AgentManager(self._strategy)
            self._manager = manager
            try:
                self._strategy.agents = manager
            except Exception:
                pass
        return manager

    def __getattr__(self, name):
        return getattr(self._get_manager(), name)

    def __getitem__(self, key):
        return self._get_manager()[key]

    def __contains__(self, key):
        return key in self._get_manager()

    def __iter__(self):
        return iter(self._get_manager())

    def __len__(self):
        return len(self._get_manager())

    def __repr__(self):
        manager = self._manager
        if manager is None:
            return "<LazyAgentManager unloaded>"
        return repr(manager)


class _LazyIndicators:
    """Import and construct Indicators only when strategy code uses indicators."""

    __slots__ = ("_strategy", "_indicators")

    def __init__(self, strategy):
        self._strategy = strategy
        self._indicators = None

    def _get_indicators(self):
        indicators = self._indicators
        if indicators is None:
            from lumibot.indicators import Indicators

            indicators = Indicators(self._strategy)
            self._indicators = indicators
            try:
                self._strategy.indicators = indicators
            except Exception:
                pass
        return indicators

    def __getattr__(self, name):
        return getattr(self._get_indicators(), name)

    def __repr__(self):
        indicators = self._indicators
        if indicators is None:
            return "<LazyIndicators unloaded>"
        return repr(indicators)


def _ensure_backtesting_imports():
    global _BACKTESTING_IMPORTS_READY
    global AlpacaBacktesting, BacktestingBroker, CcxtBacktesting, DataBentoDataBacktesting
    global InteractiveBrokersRESTBacktesting, PolygonDataBacktesting, RoutedBacktestingPandas
    global ThetaDataBacktesting, ThetaDataBacktestingPandas, YahooDataBacktesting

    if _BACKTESTING_IMPORTS_READY:
        return

    backtesting = import_module("lumibot.backtesting")

    if AlpacaBacktesting is None:
        AlpacaBacktesting = backtesting.AlpacaBacktesting
    if BacktestingBroker is None:
        BacktestingBroker = backtesting.BacktestingBroker
    if CcxtBacktesting is None:
        CcxtBacktesting = backtesting.CcxtBacktesting
    if DataBentoDataBacktesting is None:
        DataBentoDataBacktesting = backtesting.DataBentoDataBacktesting
    if InteractiveBrokersRESTBacktesting is None:
        InteractiveBrokersRESTBacktesting = backtesting.InteractiveBrokersRESTBacktesting
    if PolygonDataBacktesting is None:
        PolygonDataBacktesting = backtesting.PolygonDataBacktesting
    if RoutedBacktestingPandas is None:
        RoutedBacktestingPandas = backtesting.RoutedBacktestingPandas
    if ThetaDataBacktesting is None:
        ThetaDataBacktesting = backtesting.ThetaDataBacktesting
    if ThetaDataBacktestingPandas is None:
        ThetaDataBacktestingPandas = backtesting.ThetaDataBacktestingPandas
    if YahooDataBacktesting is None:
        YahooDataBacktesting = backtesting.YahooDataBacktesting
    _BACKTESTING_IMPORTS_READY = True

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
    @staticmethod
    def _normalize_backtest_datetime(value):
        """Ensure backtest boundary datetimes are timezone-aware.

        Naive datetimes are localized to the LumiBot default timezone; timezone-aware
        inputs are returned unchanged so their original offsets are preserved.
        """
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            tzinfo = value.tzinfo
            if tzinfo is None or tzinfo.utcoffset(value) is None:
                return to_datetime_aware(value)
            if not hasattr(tzinfo, "zone"):
                return value.astimezone(_default_pytz())
        return value

    @property
    def is_backtesting(self) -> bool:
        """Boolean flag indicating whether the strategy is running in backtesting mode."""
        return getattr(self, "_is_backtesting", False)

    @is_backtesting.setter
    def is_backtesting(self, value: bool) -> None:
        self._is_backtesting = bool(value)

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
        buy_trading_slippages=[],
        sell_trading_slippages=[],
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
        buy_trading_slippages : list
            A list of TradingSlippage objects to use for buy fills in backtesting. Defaults to empty list.
        sell_trading_slippages : list
            A list of TradingSlippage objects to use for sell fills in backtesting. Defaults to empty list.
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
        self.buy_trading_slippages = buy_trading_slippages
        self.sell_trading_slippages = sell_trading_slippages
        self.save_logfile = save_logfile
        self.broker = broker
        if getattr(broker, "IS_BACKTESTING_BROKER", False):
            _ensure_backtesting_imports()

        # initialize cash variables
        self._position_value = None
        self._portfolio_value = None
        self._cash_deposits_total = 0.0
        self._cash_withdrawals_total = 0.0
        self._cash_adjustments_net_total = 0.0
        self._cash_financing_enabled = False
        self._cash_financing_account_mode = "margin"
        self._cash_financing_day_count_basis = 360
        self._cash_financing_missing_rate_policy = "carry_forward"
        self._cash_financing_credit_rate_annual = None
        self._cash_financing_debit_rate_annual = None
        self._cash_financing_last_valid_credit_rate_annual = None
        self._cash_financing_last_valid_debit_rate_annual = None
        self._cash_financing_last_accrual_date = None
        self._cash_financing_credit_total = 0.0
        self._cash_financing_debit_total = 0.0
        self._cash_financing_net_total = 0.0
        self._cash_financing_days_accrued = 0
        self._cash_financing_events = 0
        self._cash_financing_last_credit_rate_used = None
        self._cash_financing_last_debit_rate_used = None
        self._cash_event_poll_lookback_days = 7
        self._cash_event_poll_interval_seconds = 300
        self._cash_event_cloud_emit_limit = 50
        self._cash_event_fetch_limit = 100
        self._cash_event_dedupe_capacity = 1000
        self._cash_event_last_poll_at = None
        self._cash_event_pending_for_cloud = []
        self._cash_event_sent_ids = set()
        self._cash_event_sent_id_order = deque()

        # Only log one message about cloud API key being missing
        self._logged_missing_lumiwealth_api_key = False

        if name is not None:
            self._name = name

        elif _credential("STRATEGY_NAME") is not None:
            self._name = _credential("STRATEGY_NAME")

        else:
            self._name = self.__class__.__name__

        # Create an adapter with 'strategy_name' set to the instance's name
        if not hasattr(self, "logger") or self.logger is None:
            self.logger = get_strategy_logger(__name__, self._name)

        # Don't set log level here - let the logger hierarchy and quiet logs setting handle it
        # The StrategyLoggerAdapter will check BACKTESTING_QUIET_LOGS in its methods

        # Track which assets we've logged "Getting historical prices" for to reduce noise
        self._logged_get_historical_prices_assets = set()

        if self.broker == None:
            self.broker = _credential("BROKER")

        # Handle data source initialization
        self._data_source = data_source
        if self._data_source is None:
            self._data_source = _credential("DATA_SOURCE")

        # If we have a custom data source, attach it to the broker
        if self._data_source is not None and self.broker is not None:
            # Store the original data source for reference
            self._original_broker_data_source = self.broker.data_source

            # Set the custom data source
            self.broker.data_source = self._data_source

        self.hide_positions = _credential("HIDE_POSITIONS")
        self.hide_trades = _credential("HIDE_TRADES")
        self.include_cash_positions = include_cash_positions

        # If the MARKET env variable is set, use it as the market
        market = _credential("MARKET")
        if market:
            # Log the market being used
            colored_message = colored(f"Using market from environment variables: {market}", "green")
            self.logger.info(colored_message)
            self.set_market(market)

        self.live_config = _credential("LIVE_CONFIG")
        self.discord_webhook_url = discord_webhook_url if discord_webhook_url is not None else _credential("DISCORD_WEBHOOK_URL")

        if account_history_db_connection_str:
            self.db_connection_str = account_history_db_connection_str
            get_logger(__name__).warning("account_history_db_connection_str is deprecated and will be removed in future versions, please use db_connection_str instead")
        elif db_connection_str:
            self.db_connection_str = db_connection_str
        else:
            env_db_connection_str = _credential("DB_CONNECTION_STR")
            self.db_connection_str = env_db_connection_str if env_db_connection_str else None

        self.discord_account_summary_footer = discord_account_summary_footer
        self.backup_table_name="vars_backup"

        # Set the LumiWealth API key
        if lumiwealth_api_key:
            self.lumiwealth_api_key = lumiwealth_api_key
        else:
            self.lumiwealth_api_key = _credential("LUMIWEALTH_API_KEY")

        if strategy_id is None:
            self.strategy_id = self._name
        else:
            self.strategy_id = strategy_id

        # Check if self.broker is set before accessing its attributes
        if self.broker is None:
            error_message = (
                "No broker is set. This typically happens when:\n"
                "1. IS_BACKTESTING is not set to 'true' (so it defaults to live trading)\n"
                "2. No broker credentials are configured in your environment variables\n\n"
                "To fix this, you need to:\n"
                "1. Create a .env file in your project root directory\n"
                "2. Set IS_BACKTESTING=true for backtesting, OR\n"
                "3. Configure a broker by setting the appropriate environment variables\n\n"
                "For example, add to your .env file:\n"
                "IS_BACKTESTING=true\n"
                "BACKTESTING_START=2023-01-01\n"
                "BACKTESTING_END=2023-12-31\n\n"
                "OR for live trading, set broker credentials like:\n"
                "ALPACA_API_KEY=your_api_key\n"
                "ALPACA_API_SECRET=your_api_secret\n"
                "ALPACA_IS_PAPER=true\n\n"
                "For more information, see: http://lumibot.lumiwealth.com/deployment.html#secrets-configuration"
            )
            self.logger.error(colored(error_message, "red"))
            raise ValueError(error_message)

        self._quote_asset = quote_asset if self.broker.name != "bitunix" else Asset("USDT", Asset.AssetType.CRYPTO)

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

        # Initialize the chart OHLC list
        self._chart_ohlc_list = []

        # Hold the asset objects for strings for stocks only.
        self._asset_mapping = dict()

        # Setting the data provider
        if self.is_backtesting:
            if self.broker.data_source.SOURCE == "PANDAS":
                self.broker.data_source.load_data()

            # Create initial starting positions.
            self.starting_positions = starting_positions
            if self.starting_positions is not None and len(self.starting_positions) > 0:
                from ..entities import Position

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
            # Determine initial cash ("budget") for backtesting.
            # NOTE: In BotSpot/BotManager runs we often inject settings via environment variables.
            # If BACKTESTING_BUDGET is provided, prefer it (even if strategy code passed an explicit budget)
            # so the starting cash can be controlled per-run without forcing a code change.
            effective_budget = budget
            env_budget_raw = os.environ.get("BACKTESTING_BUDGET")
            if env_budget_raw is not None:
                trimmed = env_budget_raw.strip()
                if trimmed and trimmed.lower() not in ("none", "null"):
                    normalized = (
                        trimmed.replace("$", "")
                        .replace(",", "")
                        .replace("_", "")
                        .strip()
                    )
                    multiplier = 1.0
                    suffix = normalized[-1:].lower()
                    if suffix in ("k", "m", "b") and len(normalized) > 1:
                        normalized = normalized[:-1].strip()
                        if suffix == "k":
                            multiplier = 1_000.0
                        elif suffix == "m":
                            multiplier = 1_000_000.0
                        elif suffix == "b":
                            multiplier = 1_000_000_000.0
                    try:
                        parsed = float(normalized) * multiplier
                        if not math.isfinite(parsed) or parsed <= 0:
                            raise ValueError("budget must be a finite positive number")
                        effective_budget = parsed
                        self.logger.info(
                            colored(
                                f"Using BACKTESTING_BUDGET={effective_budget:g} as starting backtest cash",
                                "green",
                            )
                        )
                    except Exception:
                        self.logger.warning(
                            colored(
                                f"Invalid BACKTESTING_BUDGET value: {env_budget_raw!r}. "
                                "Expected a positive number like 500, 5000, 5k, 1_000_000, or $10,000. "
                                "Ignoring and falling back to budget/default.",
                                "yellow",
                            )
                        )

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
        self._executor = _strategy_executor_class()(self)
        self.broker._add_subscriber(self._executor)

        # Stats related variables
        self._stats_file = stats_file
        self._stats = None
        self._stats_list = []
        self._stats_dirty = False
        self._analysis = {}

        # Variable backup related variables
        self.should_backup_variables_to_database = should_backup_variables_to_database
        self.should_send_summary_to_discord = should_send_summary_to_discord
        self._last_backup_state = None
        self.vars = Vars()
        self.agents = _LazyAgentManager(self)

        self.indicators = _LazyIndicators(self)

        # Storing parameters for the initialize method
        if not hasattr(self, "parameters") or not isinstance(self.parameters, dict) or self.parameters is None:
            self.parameters = {}
        self.parameters = {**self.parameters, **kwargs}
        if parameters is not None and isinstance(self.parameters, dict):
            self.parameters = {**self.parameters, **parameters}

        # Apply BACKTESTING_PARAMETERS env var override (highest priority, wins over code-level params)
        from lumibot.credentials import BACKTESTING_PARAMETERS
        if BACKTESTING_PARAMETERS is not None and isinstance(BACKTESTING_PARAMETERS, dict):
            self.parameters = {**self.parameters, **BACKTESTING_PARAMETERS}
            self.logger.info(
                colored(
                    f"Applied BACKTESTING_PARAMETERS override: {list(BACKTESTING_PARAMETERS.keys())}",
                    "green",
                )
            )

        cash_financing_cfg = self.parameters.get("cash_financing")
        if isinstance(cash_financing_cfg, dict):
            self._configure_cash_financing(
                enabled=cash_financing_cfg.get("enabled", True),
                account_mode=cash_financing_cfg.get("account_mode", "margin"),
                day_count_basis=cash_financing_cfg.get("day_count_basis", 360),
                missing_rate_policy=cash_financing_cfg.get("missing_rate_policy", "carry_forward"),
            )
            self._set_cash_financing_rates(
                credit_rate_annual=cash_financing_cfg.get("credit_rate_annual"),
                debit_rate_annual=cash_financing_cfg.get("debit_rate_annual"),
            )

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
                "Order quantity cannot be None. Please provide a valid quantity value."
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
                self._cash_position = position
                return

        # If not in positions, create a new position for cash
        from ..entities import Position

        position = Position(
            self._name,
            self._quote_asset,
            Decimal(cash),
            orders=None,
            hold=0,
            available=Decimal(cash),
        )
        self.broker._filled_positions.append(position)
        self._cash_position = position

    def _get_cash_position(self):
        cash_position = getattr(self, "_cash_position", None)
        cash_asset = getattr(cash_position, "asset", None) if cash_position is not None else None
        if (
            cash_position is not None
            and (cash_asset is self._quote_asset or cash_asset == self._quote_asset)
            and getattr(cash_position, "strategy", None) == self._name
        ):
            return cash_position

        cash_position = self.broker.get_tracked_position(self._name, self._quote_asset)
        self._cash_position = cash_position
        return cash_position

    @staticmethod
    def _coerce_cash_rate(value, field_name):
        if value is None:
            return None
        try:
            rate = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{field_name} must be a finite non-negative number")
        if not math.isfinite(rate) or rate < 0:
            raise ValueError(f"{field_name} must be a finite non-negative number")
        return rate

    def _configure_cash_financing(
        self,
        *,
        enabled: bool | None = None,
        account_mode: str | None = None,
        day_count_basis: int | None = None,
        missing_rate_policy: str | None = None,
    ) -> None:
        if enabled is not None:
            self._cash_financing_enabled = bool(enabled)

        if account_mode is not None:
            normalized_account_mode = str(account_mode).strip().lower()
            if normalized_account_mode not in {"margin", "cash"}:
                raise ValueError("account_mode must be 'margin' or 'cash'")
            self._cash_financing_account_mode = normalized_account_mode

        if day_count_basis is not None:
            try:
                basis = int(day_count_basis)
            except (TypeError, ValueError):
                raise ValueError("day_count_basis must be a positive integer")
            if basis <= 0:
                raise ValueError("day_count_basis must be a positive integer")
            self._cash_financing_day_count_basis = basis

        if missing_rate_policy is not None:
            normalized_missing_policy = str(missing_rate_policy).strip().lower()
            if normalized_missing_policy not in {"carry_forward", "error"}:
                raise ValueError("missing_rate_policy must be 'carry_forward' or 'error'")
            self._cash_financing_missing_rate_policy = normalized_missing_policy

    def _set_cash_financing_rates(
        self,
        *,
        credit_rate_annual: float | None = None,
        debit_rate_annual: float | None = None,
    ) -> None:
        if credit_rate_annual is not None:
            normalized_credit_rate = self._coerce_cash_rate(credit_rate_annual, "credit_rate_annual")
            self._cash_financing_credit_rate_annual = normalized_credit_rate
            self._cash_financing_last_valid_credit_rate_annual = normalized_credit_rate

        if debit_rate_annual is not None:
            normalized_debit_rate = self._coerce_cash_rate(debit_rate_annual, "debit_rate_annual")
            self._cash_financing_debit_rate_annual = normalized_debit_rate
            self._cash_financing_last_valid_debit_rate_annual = normalized_debit_rate

    def _resolve_cash_financing_rate(self, *, side: str) -> float:
        if side == "credit":
            rate = self._cash_financing_credit_rate_annual
            fallback_rate = self._cash_financing_last_valid_credit_rate_annual
        elif side == "debit":
            rate = self._cash_financing_debit_rate_annual
            fallback_rate = self._cash_financing_last_valid_debit_rate_annual
        else:
            raise ValueError(f"Unknown financing side: {side}")

        if rate is not None:
            return float(rate)

        if self._cash_financing_missing_rate_policy == "carry_forward" and fallback_rate is not None:
            return float(fallback_rate)

        raise ValueError(
            f"Missing {side} financing rate for {self._name}. "
            "Set rates via set_cash_financing_rates()."
        )

    def _apply_cash_adjustment(
        self,
        *,
        delta_cash: float,
        reason: str,
        kind: str,
        allow_negative: bool | None = None,
    ) -> float:
        if not self.is_backtesting:
            raise RuntimeError("Cash adjustments are only supported in backtesting")

        try:
            delta = float(delta_cash)
        except (TypeError, ValueError):
            raise ValueError("delta_cash must be a finite number")

        if not math.isfinite(delta):
            raise ValueError("delta_cash must be a finite number")

        allow_negative_effective = (
            self._cash_financing_account_mode == "margin"
            if allow_negative is None
            else bool(allow_negative)
        )

        current_cash = float(self.cash or 0.0)
        updated_cash = current_cash + delta

        if (not allow_negative_effective) and updated_cash < 0:
            raise ValueError(
                f"Cash adjustment '{reason}' would create negative cash "
                f"({updated_cash:.2f}) while account_mode='cash'"
            )

        self._set_cash_position(updated_cash)

        if kind == "deposit":
            self._cash_deposits_total += abs(delta)
        elif kind == "withdrawal":
            self._cash_withdrawals_total += abs(delta)

        self._cash_adjustments_net_total += delta
        self._record_backtest_cash_event(
            event_type=kind if kind in {"deposit", "withdrawal", "adjustment"} else "adjustment",
            amount=delta,
            reason=reason,
            description=reason,
            is_external_cash_flow=True,
            raw_type=f"backtest_{kind}",
        )
        return updated_cash

    def _record_backtest_cash_event(
        self,
        *,
        event_type: str,
        amount: float,
        reason: str | None = None,
        description: str | None = None,
        is_external_cash_flow: bool = False,
        raw_type: str | None = None,
        raw_subtype: str | None = None,
    ) -> None:
        if not self.is_backtesting:
            return
        try:
            normalized_amount = float(amount)
        except (TypeError, ValueError):
            return
        if not math.isfinite(normalized_amount) or abs(normalized_amount) < 1e-12:
            return

        current_dt = self.get_datetime()
        if current_dt is None:
            return

        try:
            from ..entities import CashEvent

            cash_event = CashEvent(
                broker_name="backtesting",
                event_type=event_type,
                amount=normalized_amount,
                occurred_at=current_dt,
                raw_type=raw_type,
                raw_subtype=raw_subtype,
                description=description or reason,
                is_external_cash_flow=is_external_cash_flow,
            )
        except Exception:
            return

        broker = getattr(self, "broker", None)
        record_cash_event = getattr(broker, "record_cash_event", None)
        if callable(record_cash_event):
            record_cash_event(
                cash_event,
                strategy=getattr(self, "_name", None),
                reason=reason,
                occurred_at=current_dt,
            )

    def _apply_daily_cash_financing_if_needed(self) -> None:
        if not self.is_backtesting or not self._cash_financing_enabled:
            return

        current_dt = self.get_datetime()
        if current_dt is None:
            return
        current_date = current_dt.date() if hasattr(current_dt, "date") else current_dt

        if self._cash_financing_last_accrual_date == current_date:
            return

        if self._cash_financing_last_accrual_date is None:
            days_to_accrue = 1
        else:
            days_delta = (current_date - self._cash_financing_last_accrual_date).days
            if days_delta <= 0:
                return
            days_to_accrue = days_delta

        current_cash = float(self.cash or 0.0)
        if self._cash_financing_account_mode == "cash" and current_cash < 0:
            raise ValueError(
                "Negative cash is not permitted while account_mode='cash'. "
                "Switch to account_mode='margin' or prevent negative balances."
            )

        credit_rate = self._resolve_cash_financing_rate(side="credit")
        debit_rate = self._resolve_cash_financing_rate(side="debit")

        if current_cash >= 0:
            annual_rate = credit_rate
        else:
            annual_rate = debit_rate

        daily_rate = annual_rate / float(self._cash_financing_day_count_basis)
        cash_factor = (1.0 + daily_rate) ** days_to_accrue
        updated_cash = current_cash * cash_factor
        delta_cash = updated_cash - current_cash

        self._set_cash_position(updated_cash)

        if delta_cash >= 0:
            self._cash_financing_credit_total += delta_cash
        else:
            self._cash_financing_debit_total += abs(delta_cash)
        self._cash_financing_net_total += delta_cash
        self._cash_financing_days_accrued += days_to_accrue
        self._cash_financing_events += 1
        self._cash_financing_last_credit_rate_used = credit_rate
        self._cash_financing_last_debit_rate_used = debit_rate
        self._cash_financing_last_accrual_date = current_date

        if abs(delta_cash) >= 1e-12:
            financing_reason = "cash_financing_credit" if delta_cash >= 0 else "cash_financing_debit"
            financing_description = (
                f"Daily cash financing {'credit' if delta_cash >= 0 else 'debit'} "
                f"for {days_to_accrue} day(s)"
            )
            self._record_backtest_cash_event(
                event_type="interest",
                amount=delta_cash,
                reason=financing_reason,
                description=financing_description,
                is_external_cash_flow=False,
                raw_type=financing_reason,
                raw_subtype=f"annual_rate={annual_rate:.12f}",
            )

    def _sanitize_user_asset(self, asset):
        if isinstance(asset, Asset):
            return asset
        elif isinstance(asset, tuple):
            return asset
        elif isinstance(asset, str):
            # Make sure the asset is uppercase for consistency (and because some brokers require it)
            asset = asset.upper()
            cache = getattr(self, "_sanitized_string_asset_cache", None)
            if cache is None:
                cache = {}
                self._sanitized_string_asset_cache = cache
            cached = cache.get(asset)
            if cached is not None:
                return cached
            sanitized = Asset(symbol=asset)
            if len(cache) >= 256:
                cache.clear()
            cache[asset] = sanitized
            return sanitized
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
                self.logger.info(f"Error getting broker balances: {e}", exc_info=True)
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
                self.logger.warning(
                    "Unable to get balances (cash, portfolio value, etc) from broker. "
                    "Please check your broker and your broker configuration."
                )
                return False
        else:
            self.logger.debug("Balances already updated recently. Skipping update.")

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        # Live runs don't need to recalculate portfolio value here, as the broker sync should handle it
        if not self.is_backtesting:
            return

        filled_positions = getattr(self.broker, "_filled_positions", None)
        filled_positions_revision = getattr(filled_positions, "revision", 0)
        broker_datetime = getattr(self.broker, "datetime", None)
        cache_key = (broker_datetime, filled_positions_revision)
        if getattr(self, "_portfolio_value_cache_key", None) == cache_key:
            return getattr(self, "_portfolio_value_cache_value", self._portfolio_value)

        with self._executor.lock:
            # Initialize last known prices tracker for forward-fill fallback.
            # This is used when OHLC data is missing (common for illiquid options like LEAPS).
            if not hasattr(self, '_last_known_prices'):
                self._last_known_prices = {}

            # Option quotes are frequently sparse/unreliable outside regular session hours.
            # For backtests, avoid ingesting off-session option marks into portfolio valuation,
            # which can otherwise poison forward-fill with stale placeholder values.
            option_mark_time_local = None
            broker_dt = getattr(self.broker, "datetime", None)
            if isinstance(broker_dt, datetime.datetime):
                try:
                    if broker_dt.tzinfo is None:
                        option_mark_time_local = _default_pytz().localize(broker_dt)
                    else:
                        option_mark_time_local = broker_dt.astimezone(_default_pytz())
                except Exception:
                    option_mark_time_local = None

            option_marking_allowed = True
            if option_mark_time_local is not None:
                t_local = option_mark_time_local.time()
                option_marking_allowed = (t_local >= datetime.time(9, 30)) and (t_local <= datetime.time(16, 0))

            def _asset_type_key(asset_obj):
                cached_asset_type_key = getattr(asset_obj, "_cached_asset_type_key", None)
                if cached_asset_type_key is not None:
                    return cached_asset_type_key
                raw_asset_type = getattr(asset_obj, "asset_type", "")
                raw_asset_type = getattr(raw_asset_type, "value", raw_asset_type)
                return str(raw_asset_type).lower()

            # Used for traditional brokers, for crypto this could be 0
            portfolio_value = self.cash
            quote_asset = self._quote_asset
            data_source = self.broker.data_source
            option_source = self.broker.option_source
            positions = self.broker.get_tracked_positions(self._name)

            # Set the base currency for crypto valuations.

            prices = {}
            if not hasattr(self, "_forward_fill_warning_cache"):
                # Throttle repetitive backtest forward-fill warnings (asset, day) to keep
                # valuation logs informative without dominating runtime.
                self._forward_fill_warning_cache = set()
            for position in positions:
                asset = position.asset
                if asset != quote_asset:
                    asset_is_option = False
                    asset_type_key = _asset_type_key(asset)
                    if asset_type_key in {"crypto", "forex"}:
                        asset = (asset, quote_asset)
                    elif asset_type_key == "option":
                        asset_is_option = True

                    if option_source is not None and asset_is_option:
                        source = option_source
                    else:
                        source = data_source
                    prices[asset] = self._get_price_from_source(source, asset)

            for position in positions:
                position_asset = position.asset
                position_asset_type = _asset_type_key(position_asset)
                # Turn the asset into a tuple if it's a crypto asset
                asset = (
                    position_asset
                    if position_asset_type not in {"crypto", "forex"}
                    else (position_asset, quote_asset)
                )
                quantity = position.quantity
                price = prices.get(asset)
                is_option_asset = isinstance(asset, Asset) and position_asset_type == "option"

                # If the asset is the quote asset, then we already have included it from cash
                # Eg. if we have a position of USDT and USDT is the quote_asset then we already consider it as cash
                if quote_asset is not None:
                    if isinstance(asset, tuple) and asset == (
                        quote_asset,
                        quote_asset,
                    ):
                        continue
                    elif isinstance(asset, Asset) and asset == quote_asset:
                        continue

                # Normalize "missing" prices to None so forward-fill fallback can apply.
                # Some data sources return 0 or NaN for "no price" (common on non-trading timestamps).
                if price is not None:
                    try:
                        price_float = float(price)
                    except (TypeError, ValueError):
                        price = None
                    else:
                        if (not math.isfinite(price_float)) or price_float == 0:
                            price = None
                        else:
                            price = price_float

                # For backtests, ignore option marks outside regular options hours.
                if self.is_backtesting and is_option_asset and not option_marking_allowed:
                    price = None

                # Track valid prices for forward-fill fallback
                if price is not None:
                    self._last_known_prices[asset] = price

                if self.is_backtesting and price is None:
                    # Forward-fill fallback: use last known price when current price is unavailable.
                    # This is critical for illiquid options (LEAPS) that may not trade for days.
                    if asset in self._last_known_prices:
                        price = self._last_known_prices[asset]
                        base_asset = asset[0] if isinstance(asset, tuple) else asset
                        asset_symbol = getattr(base_asset, 'symbol', str(base_asset))
                        # Throttle noisy forward-fill warnings to once per contract/symbol per run.
                        # Daily option strategies can otherwise emit thousands of lines that materially
                        # slow long backtests and bloat logs.
                        warn_key = str(base_asset)
                        if warn_key not in self._forward_fill_warning_cache:
                            self._forward_fill_warning_cache.add(warn_key)
                            self.logger.warning(
                                "Using forward-filled price %.4f for %s at %s (no current price available).",
                                price, asset_symbol, self.broker.datetime,
                            )
                    else:
                        # No price history - must skip this position
                        if isinstance(asset, Asset):
                            asset_details = (
                                f"symbol: {asset.symbol}, type: {asset.asset_type}, right: {asset.right}, "
                                f"expiration: {asset.expiration}, strike: {asset.strike}"
                            )
                            self.logger.warning(
                                "Skipping valuation for asset (%s) because no price was available at %s.",
                                asset_details,
                                self.broker.datetime,
                            )
                        elif isinstance(asset, tuple):
                            base_asset = asset[0] if asset else None
                            if isinstance(base_asset, Asset):
                                asset_details = (
                                    f"symbol: {base_asset.symbol}, type: {base_asset.asset_type}, right: {base_asset.right}, "
                                    f"expiration: {base_asset.expiration}, strike: {base_asset.strike}"
                                )
                            else:
                                asset_details = str(asset)
                            self.logger.warning(
                                "Skipping valuation for pair (%s) because no price was available at %s.",
                                asset_details,
                                self.broker.datetime,
                            )
                        continue
                if isinstance(asset, tuple):
                    multiplier = 1
                else:
                    multiplier = asset.multiplier if position_asset_type in {"option", "future", "cont_future"} else 1

                # BACKTESTING ONLY: Special handling for futures portfolio value
                # In backtesting, cash has margin deducted, so we need to add it back
                # In live trading, brokers handle this internally
                if (
                    self.is_backtesting
                    and not isinstance(asset, tuple)
                    and position_asset_type in {"future", "cont_future"}
                ):
                    # Import here to avoid circular dependency
                    from lumibot.backtesting.backtesting_broker import get_futures_margin_requirement

                    # Add margin tied up in position (was deducted from cash)
                    margin_per_contract = get_futures_margin_requirement(asset)
                    total_margin = margin_per_contract * abs(float(quantity))
                    portfolio_value += total_margin

                    # Add unrealized P&L = (current_price - entry_price) × quantity × multiplier
                    entry_price = position.avg_fill_price if (hasattr(position, 'avg_fill_price') and position.avg_fill_price) else price
                    unrealized_pnl = (float(price) - float(entry_price)) * float(quantity) * multiplier
                    portfolio_value += unrealized_pnl
                else:
                    # All other cases (stocks, options, crypto, live trading)
                    position_value = float(quantity) * float(price) * multiplier
                    portfolio_value += position_value

            self._portfolio_value = portfolio_value
            self._portfolio_value_cache_key = cache_key
            self._portfolio_value_cache_value = portfolio_value
        return portfolio_value

    def _get_price_from_source(self, source, asset):
        """Return best available price from the provided data source."""
        if source is None:
            return None

        snapshot_price = None
        timestep_hint = None
        base_asset = asset[0] if isinstance(asset, tuple) else asset
        base_asset_type = getattr(base_asset, "_cached_asset_type_key", None)
        if base_asset_type is None:
            base_asset_type = getattr(base_asset, "asset_type", None)
            base_asset_type = getattr(base_asset_type, "value", base_asset_type)
        base_asset_type = str(base_asset_type).lower()
        is_option_asset = base_asset_type == "option"
        if self.is_backtesting and is_option_asset:
            _ensure_backtesting_imports()
        is_thetadata_option_backtest = (
            self.is_backtesting
            and is_option_asset
            and isinstance(source, ThetaDataBacktestingPandas)
        )

        def _thetadata_quote_mark(quote_obj):
            if quote_obj is None:
                return None
            bid = getattr(quote_obj, "bid", None)
            ask = getattr(quote_obj, "ask", None)
            price = getattr(quote_obj, "price", None)

            def _coerce(val):
                try:
                    numeric = float(val)
                except (TypeError, ValueError):
                    return None
                if math.isnan(numeric) or numeric <= 0:
                    return None
                return numeric

            bid_val = _coerce(bid)
            ask_val = _coerce(ask)
            if bid_val is not None and ask_val is not None:
                return (bid_val + ask_val) / 2
            if bid_val is not None:
                return bid_val
            if ask_val is not None:
                return ask_val
            return _coerce(price)

        # Determine if this strategy is effectively daily cadence.
        try:
            cadence_seconds = self._get_sleeptime_seconds()
            if cadence_seconds is not None and cadence_seconds >= 20 * 3600:
                timestep_hint = "day"
        except Exception:
            timestep_hint = None

        # ThetaData backtesting: for options, mark-to-market should be quote-driven (NBBO mark) and
        # extremely fast. Calling `get_price_snapshot()` first causes an extra `_update_pandas_data()`
        # pass per asset (and often still falls back to `get_quote()`), which is the dominant cost in
        # long, option-heavy intraday backtests.
        if is_thetadata_option_backtest:
            try:
                get_quote = getattr(source, "get_quote", None)
                if callable(get_quote):
                    quote_asset = getattr(self, "_quote_asset", None)
                    # ThetaData backtesting option MTM should be quote-driven when available.
                    # Prefer the normal quote path first (usually day/EOD for daily cadence),
                    # then fall back to a minimal intraday NBBO snapshot when day/EOD pricing
                    # is missing (ThetaData can return 472/no-data for option EOD history even
                    # when intraday quote history exists).
                    if quote_asset is not None:
                        quote = get_quote(base_asset, quote=quote_asset, timestep=timestep_hint or "minute")
                    else:
                        quote = get_quote(base_asset, timestep=timestep_hint or "minute")
                    quote_mark = _thetadata_quote_mark(quote)
                    day_quote_mark = quote_mark
                    if timestep_hint != "day":
                        if quote_mark is not None:
                            return quote_mark

                    # Daily-cadence fallback: intraday quote snapshots are the most robust source
                    # of option marks. Even when day quotes exist, they can be stale in some
                    # provider/cache states; prefer snapshot mark when available.
                    if timestep_hint == "day":
                        # Only attempt snapshot-only lookup when it's safe to do so.
                        #
                        # Some unit tests (and custom sources) override `get_quote()` at the class
                        # level and treat repeated calls as an error (or always return the same
                        # quote object regardless of timestep). For bound methods, only the real
                        # ThetaDataBacktestingPandas implementation is guaranteed to understand
                        # `snapshot_only`. For non-bound callables (e.g., instance-level stubs used
                        # by tests), allow the fallback.
                        can_try_snapshot = True
                        func = getattr(get_quote, "__func__", None)
                        if func is not None and func is not ThetaDataBacktestingPandas.get_quote:
                            can_try_snapshot = False
                        if can_try_snapshot:
                            quote_kwargs = {"timestep": "minute", "snapshot_only": True}
                            if quote_asset is not None:
                                quote = get_quote(base_asset, quote=quote_asset, **quote_kwargs)
                            else:
                                quote = get_quote(base_asset, **quote_kwargs)
                            quote_mark = _thetadata_quote_mark(quote)
                            if quote_mark is not None:
                                return quote_mark

                        # If snapshot probing failed, avoid forcing day-quote marks for established
                        # positions when we already have a prior valid mark to forward-fill from.
                        # This prevents stale day quotes from creating artificial intraday MTM cliffs.
                        has_last_known_price = False
                        try:
                            has_last_known_price = base_asset in getattr(self, "_last_known_prices", {})
                        except Exception:
                            has_last_known_price = False

                        if day_quote_mark is not None:
                            if has_last_known_price:
                                return None
                            return day_quote_mark
            except Exception as e:
                self.logger.debug("ThetaData quote-mark lookup failed for %s: %s", base_asset, e)
            return None

        if hasattr(source, "get_price_snapshot"):
            try:
                if timestep_hint:
                    snapshot = source.get_price_snapshot(asset, timestep=timestep_hint)
                else:
                    snapshot = source.get_price_snapshot(asset)
            except Exception:
                self.logger.exception(
                    "Error retrieving price snapshot for %s from %s; falling back to last trade.",
                    asset,
                    type(source).__name__,
                )
            else:
                # ThetaData backtests: options often have no prints, but NBBO quotes exist.
                # Portfolio mark-to-market should use mark (mid) when bid/ask are available.
                if is_thetadata_option_backtest:
                    snapshot_price = self._pick_thetadata_option_mark_price(base_asset, snapshot)
                else:
                    snapshot_price = self._pick_snapshot_price(asset, snapshot)

        if snapshot_price is not None:
            return snapshot_price

        get_last_price = getattr(source, "get_last_price", None)
        if callable(get_last_price):
            price = get_last_price(asset)
            if price is not None:
                return price

        # Quote fallback for options when OHLC is missing.
        # Options often have sparse OHLC data (LEAPS may not trade for days),
        # but bid/ask quotes from market makers are typically available.
        # This calls get_quote() which loads minute-level quote data.
        if hasattr(base_asset, 'asset_type') and base_asset.asset_type == 'option':
            try:
                get_quote = getattr(source, 'get_quote', None)
                if callable(get_quote):
                    quote = get_quote(base_asset, timestep=timestep_hint or "minute")
                    if quote is not None:
                        bid = getattr(quote, 'bid', None)
                        ask = getattr(quote, 'ask', None)
                        try:
                            bid_val = float(bid) if bid is not None else None
                            ask_val = float(ask) if ask is not None else None
                        except (TypeError, ValueError):
                            bid_val = None
                            ask_val = None

                        # IMPORTANT: Treat 0/negative bid/ask as "no actionable quote".
                        # Returning 0 here causes positions to be valued at $0 and breaks
                        # the forward-fill MTM fallback, producing sawtooth equity curves.
                        if bid_val is None or ask_val is None:
                            return None
                        if bid_val <= 0 or ask_val <= 0:
                            return None

                        mid_price = (bid_val + ask_val) / 2
                        if mid_price > 0:
                            self.logger.debug(
                                "Using quote mid-price %.4f for %s (bid=%.4f, ask=%.4f)",
                                mid_price, base_asset, bid_val, ask_val
                            )
                            return mid_price
            except Exception as e:
                self.logger.debug("Quote fallback failed for %s: %s", base_asset, e)

        self.logger.warning(
            "Data source %s for asset %s does not provide get_last_price; returning None.",
            type(source).__name__,
            asset,
        )
        return None

    def _pick_thetadata_option_mark_price(self, option_asset: Asset, snapshot):
        """ThetaData backtests: prefer mark (NBBO mid) for option MTM when available."""
        if not snapshot:
            return None

        def _positive(value):
            value = self._coerce_snapshot_price(value)
            if value is None:
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if math.isnan(numeric) or numeric <= 0:
                return None
            return numeric

        bid = _positive(snapshot.get("bid"))
        ask = _positive(snapshot.get("ask"))
        close = _positive(snapshot.get("close"))

        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
        if bid is not None:
            return bid
        if ask is not None:
            return ask
        if close is not None:
            return close

        expiry = getattr(option_asset, "expiration", None)
        now_dt = getattr(self.broker, "datetime", None)
        if expiry is not None and now_dt is not None:
            try:
                if now_dt.date() >= expiry:
                    return 0.0
            except Exception:
                pass

        return None

    def _pick_snapshot_price(self, asset, snapshot):
        """Decide which figure to use from a Theta snapshot."""
        if not snapshot:
            return None

        close_price = self._coerce_snapshot_price(snapshot.get("close"))
        bid_price = self._coerce_snapshot_price(snapshot.get("bid"))
        ask_price = self._coerce_snapshot_price(snapshot.get("ask"))
        threshold = self._snapshot_stale_threshold_seconds()

        now = self._normalize_snapshot_datetime(getattr(self.broker, "datetime", None))
        if now is None:
            now = self._normalize_snapshot_datetime(datetime.datetime.now(_default_pytz()))

        trade_time = self._normalize_snapshot_datetime(snapshot.get("last_trade_time"))
        bid_time = self._normalize_snapshot_datetime(snapshot.get("last_bid_time"))
        ask_time = self._normalize_snapshot_datetime(snapshot.get("last_ask_time"))

        def _is_fresh(ts):
            if ts is None or now is None:
                return False
            return (now - ts).total_seconds() <= threshold

        if close_price is not None and _is_fresh(trade_time):
            return close_price

        bid_fresh = bid_price is not None and _is_fresh(bid_time)
        ask_fresh = ask_price is not None and _is_fresh(ask_time)

        if bid_fresh and ask_fresh:
            mid_price = (bid_price + ask_price) / 2.0
            self.logger.debug(
                "Using bid/ask mid price for %s because last trade at %s is older than %ss.",
                asset,
                trade_time.isoformat() if trade_time else "unknown",
                threshold,
            )
            return mid_price
        if bid_fresh:
            self.logger.debug(
                "Using bid price for %s because last trade at %s is older than %ss.",
                asset,
                trade_time.isoformat() if trade_time else "unknown",
                threshold,
            )
            return bid_price
        if ask_fresh:
            self.logger.debug(
                "Using ask price for %s because last trade at %s is older than %ss.",
                asset,
                trade_time.isoformat() if trade_time else "unknown",
                threshold,
            )
            return ask_price

        if close_price is not None:
            # Use DEBUG - this is expected behavior in backtesting where historical data
            # may not have fresh bid/ask timestamps. WARNING here creates excessive noise.
            self.logger.debug(
                "Using stale trade price for %s; last trade=%s, last bid=%s, last ask=%s (threshold=%ss).",
                asset,
                trade_time.isoformat() if trade_time else "unknown",
                bid_time.isoformat() if bid_time else "unknown",
                ask_time.isoformat() if ask_time else "unknown",
                threshold,
            )
            return close_price

        return None

    @staticmethod
    def _coerce_snapshot_price(value):
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(numeric):
            return None
        return numeric

    def _normalize_snapshot_datetime(self, dt_value):
        if dt_value is None:
            return None
        if isinstance(dt_value, pd.Timestamp):
            dt_value = dt_value.to_pydatetime()
        elif isinstance(dt_value, str):
            try:
                dt_value = pd.to_datetime(dt_value).to_pydatetime()
            except (TypeError, ValueError):
                return None
        if isinstance(dt_value, datetime.datetime):
            if dt_value.tzinfo is None:
                try:
                    return _default_pytz().localize(dt_value)
                except ValueError:
                    return dt_value.replace(tzinfo=_default_pytz())
            return dt_value.astimezone(_default_pytz())
        return None

    @staticmethod
    def _snapshot_stale_threshold_seconds():
        try:
            return int(os.environ.get("THETADATA_MTM_STALE_SECONDS", "120"))
        except (TypeError, ValueError):
            return 120

    @staticmethod
    def _is_buy_side(side):
        if side is None:
            return False
        if isinstance(side, Order.OrderSide):
            normalized = side.value.lower()
        else:
            normalized = str(side).lower()
        return normalized in ("buy", "buy_to_open", "buy_to_cover", "buy_to_close")

    @staticmethod
    def _is_sell_side(side):
        if side is None:
            return False
        if isinstance(side, Order.OrderSide):
            normalized = side.value.lower()
        else:
            normalized = str(side).lower()
        return normalized in ("sell", "sell_short", "sell_to_close", "sell_to_open")

    def _update_cash(self, order_or_side, quantity, price, multiplier):
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

            order_obj = order_or_side if isinstance(order_or_side, Order) else None
            side = getattr(order_obj, "side", order_or_side)

            is_buy = order_obj.is_buy_order() if order_obj is not None else self._is_buy_side(side)
            is_sell = order_obj.is_sell_order() if order_obj is not None else self._is_sell_side(side)

            if is_buy:
                current_cash -= quantity_dec * price_dec * multiplier_dec
            if is_sell:
                current_cash += quantity_dec * price_dec * multiplier_dec

            self._set_cash_position(float(current_cash)) # _set_cash_position expects float

            # Todo also update the cash asset in positions?

            return self.cash # Return the updated cash by calling the property again

    def _update_cash_with_dividends(self):
        with self._executor.lock:
            # IDEMPOTENCY CHECK: Track which (date, asset) combinations have already had dividends applied.
            # This prevents double/multiple dividend application when this method is called multiple times
            # per day from different locations in strategy_executor.py.
            if not hasattr(self, '_dividends_applied_tracker'):
                self._dividends_applied_tracker = set()

            current_date = self.get_datetime().date() if hasattr(self.get_datetime(), 'date') else self.get_datetime()

            positions = self.broker.get_tracked_positions(self._name)

            assets = []
            for position in positions:
                if position.asset != self._quote_asset and position.asset.asset_type != "option":
                    assets.append(position.asset)

            # Early return if no assets - avoid expensive dividend API calls
            if not assets:
                return self.cash

            dividends_per_share = self.get_yesterday_dividends(assets)
            cash_position = self._get_cash_position()
            cash = cash_position.quantity if cash_position is not None else 0.0
            cash_delta = 0.0
            cash_updated = False

            for position in positions:
                asset = position.asset
                quantity = position.quantity
                dividend_per_share = 0 if dividends_per_share is None else dividends_per_share.get(asset, 0)

                # Skip if no dividend or already applied for this (date, asset) combination
                if dividend_per_share == 0:
                    continue

                tracker_key = (current_date, getattr(asset, 'symbol', str(asset)))
                if tracker_key in self._dividends_applied_tracker:
                    continue  # Already applied dividend for this asset on this date

                dividend_amount = dividend_per_share * float(quantity)
                cash_delta += dividend_amount
                cash_updated = True
                self._record_backtest_cash_event(
                    event_type="dividend",
                    amount=dividend_amount,
                    reason="dividend",
                    description=(
                        f"{getattr(asset, 'symbol', str(asset))} dividend "
                        f"{float(dividend_per_share):.6f} x {float(quantity):.6f}"
                    ),
                    raw_type="dividend",
                    raw_subtype=getattr(asset, "symbol", None),
                )

                # Mark as applied
                self._dividends_applied_tracker.add(tracker_key)

            if cash_updated:
                updated_cash = cash + cash_delta
                self._set_cash_position(updated_cash)
                return updated_cash

            return cash

    # =============Stats functions=====================

    def _append_row(self, row):
        self._stats_list.append(row)
        self._stats_dirty = True

    def _format_stats(self):
        if not self._stats_dirty and self._stats is not None:
            return self._stats

        self._stats = pd.DataFrame(self._stats_list)
        if "datetime" in self._stats.columns:
            self._stats = self._stats.set_index("datetime")
            self._stats = self._stats.sort_index()

        cumulative_period_columns = (
            ("cash_deposits_total", "cash_deposits_period"),
            ("cash_withdrawals_total", "cash_withdrawals_period"),
            ("cash_adjustments_net_total", "cash_adjustments_net_period"),
            ("cash_financing_credit_total", "cash_financing_credit_period"),
            ("cash_financing_debit_total", "cash_financing_debit_period"),
            ("cash_financing_net_total", "cash_financing_net_period"),
        )

        for total_col, period_col in cumulative_period_columns:
            if total_col in self._stats.columns:
                self._stats[period_col] = cumulative_to_period_flows(self._stats[total_col])

        external_flow_totals = (
            self._stats["cash_adjustments_net_total"]
            if "cash_adjustments_net_total" in self._stats.columns
            else None
        )
        self._stats["return"] = cash_flow_adjusted_returns(
            self._stats["portfolio_value"],
            external_flow_totals,
        )
        if "portfolio_value" in self._stats.columns:
            adjusted_base = float(self._stats["portfolio_value"].iloc[0])
            if external_flow_totals is not None:
                adjusted_base -= float(external_flow_totals.iloc[0])
            self._stats["cash_adjusted_portfolio_value"] = (
                (1.0 + self._stats["return"].fillna(0.0)).cumprod() * adjusted_base
            )
        self._stats_dirty = False

        return self._stats

    def _dump_stats(self):
        # Don't change logger levels - respect the configured quiet logs setting
        if len(self._stats_list) > 0:
            self._format_stats()
            if self._stats_file:
                # Get the directory name from the stats file path
                stats_directory = os.path.dirname(self._stats_file)

                # Check if the directory exists
                if not os.path.exists(stats_directory):
                    os.makedirs(stats_directory)

                self._stats.to_csv(self._stats_file)
                stats_parquet_file = (
                    self._stats_file[:-4] + ".parquet" if self._stats_file.lower().endswith(".csv") else self._stats_file + ".parquet"
                )
                (
                    coerce_object_columns_to_json_strings,
                    is_parquet_required,
                    write_parquet_with_logging,
                ) = _get_parquet_utils()
                required = bool(self.is_backtesting) and is_parquet_required()
                write_parquet_with_logging(
                    df=self._stats,
                    path=stats_parquet_file,
                    artifact="stats",
                    logger=self.logger,
                    index=True,
                    required=required,
                    compression="zstd",
                    sanitizer=coerce_object_columns_to_json_strings,
                )

            self._strategy_returns_df = day_deduplicate(self._stats)

            self._analysis = stats_summary(self._strategy_returns_df, self.risk_free_rate)

            # Get performance for the benchmark asset
            self._dump_benchmark_stats()


    def _dump_benchmark_stats(self):
        if not self.is_backtesting or not self._benchmark_asset:
            return
        if self._backtesting_start is not None and self._backtesting_end is not None:
            _ensure_backtesting_imports()

            # Need to adjust the backtesting end date because the data from Yahoo
            # is at the start of the day, so the graph cuts short. This may be needed
            # for other timeframes as well
            backtesting_end_adjusted = self._backtesting_end
            try:
                from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
            except Exception:
                RoutedBacktestingPandas = None  # type: ignore[misc,assignment]

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
                if hasattr(df, 'select'):  # Polars DataFrame
                    pl = _get_polars_module()
                    df = df.with_columns(pl.col("close").pct_change().alias("return"))
                    # Add the symbol_cumprod column for polars
                    df = df.with_columns((1 + pl.col("return")).cum_prod().alias("symbol_cumprod"))
                else:  # Pandas DataFrame
                    df["return"] = df["close"].pct_change(fill_method=None)
                    # Add the symbol_cumprod column for pandas
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

            # IBKR backtests:
            # - For crypto benchmarks, prefer the IBKR data source (Yahoo crypto tickers are inconsistent).
            # - For equity benchmarks (e.g., SPY), prefer Yahoo to avoid IBKR history flakiness impacting
            #   tearsheet generation (benchmark is cosmetic; strategy stats are authoritative).
            elif str(getattr(self.broker.data_source, "SOURCE", "") or "").upper() == "INTERACTIVEBROKERSREST":
                def _fallback_benchmark() -> None:
                    """Avoid benchmark contamination by leaving benchmark empty on fetch failure."""
                    self.logger.warning(
                        "IBKR benchmark bars unavailable; leaving benchmark empty (no strategy-equity fallback)."
                    )
                    self._benchmark_returns_df = None
                    return

                benchmark_asset = self._benchmark_asset
                if isinstance(benchmark_asset, str):
                    parts = [p.strip() for p in benchmark_asset.split("/") if p.strip()]
                    if len(parts) == 2:
                        benchmark_asset = (
                            Asset(symbol=parts[0], asset_type="crypto"),
                            Asset(symbol=parts[1], asset_type="forex"),
                        )
                    else:
                        try:
                            self._benchmark_returns_df = get_symbol_returns(
                                benchmark_asset,
                                self._backtesting_start,
                                backtesting_end_adjusted,
                            )
                        except Exception:
                            _fallback_benchmark()
                        return
                elif isinstance(benchmark_asset, Asset) and str(getattr(benchmark_asset, "asset_type", "")).lower() == "stock":
                    try:
                        self._benchmark_returns_df = get_symbol_returns(
                            benchmark_asset.symbol,
                            self._backtesting_start,
                            backtesting_end_adjusted,
                        )
                    except Exception:
                        _fallback_benchmark()
                    return

                timestep = "minute"
                if "D" in str(self._sleeptime):
                    timestep = "day"

                bars = self.broker.data_source.get_historical_prices_between_dates(
                    benchmark_asset,
                    timestep,
                    start_date=self._backtesting_start,
                    end_date=backtesting_end_adjusted,
                    quote=self._quote_asset,
                )
                if bars is None or getattr(bars, "df", None) is None:
                    self.logger.error(f"Couldn't get benchmark bars from IBKR data source: {benchmark_asset}")
                    _fallback_benchmark()
                    return
                df = bars.df
                if df is None or df.empty or "close" not in df.columns:
                    self.logger.error(f"IBKR benchmark bars empty/invalid: {benchmark_asset}")
                    _fallback_benchmark()
                    return
                df = df.copy()
                df["return"] = df["close"].pct_change(fill_method=None)
                df["symbol_cumprod"] = (1 + df["return"]).cumprod()
                self._benchmark_returns_df = df

            # Router backtests (prod-like Theta+IBKR routing):
            # Prefer the routed data source over Yahoo so benchmarks remain cacheable and don't
            # require external network access (Yahoo can be rate-limited and slow).
            elif RoutedBacktestingPandas is not None and isinstance(self.broker.data_source, RoutedBacktestingPandas):
                def _fallback_benchmark(local_benchmark_asset) -> None:
                    """Fallback for router benchmark failures without using strategy-equity returns.

                    Why:
                    - Router benchmark fetches can fail transiently for symbols like SPY.
                    - Using strategy-equity as a benchmark contaminates tearsheet metrics.
                    - A Yahoo stock benchmark fallback keeps benchmark reporting available while
                      preserving strategy metric integrity.
                    """
                    fallback_symbol = None
                    if isinstance(local_benchmark_asset, str):
                        parts = [p.strip() for p in local_benchmark_asset.split("/") if p.strip()]
                        if len(parts) == 1:
                            fallback_symbol = parts[0]
                    elif (
                        isinstance(local_benchmark_asset, Asset)
                        and str(getattr(local_benchmark_asset, "asset_type", "")).lower() == "stock"
                    ):
                        fallback_symbol = local_benchmark_asset.symbol

                    if fallback_symbol:
                        try:
                            self._benchmark_returns_df = get_symbol_returns(
                                fallback_symbol,
                                self._backtesting_start,
                                backtesting_end_adjusted,
                            )
                            if self._benchmark_returns_df is not None and not self._benchmark_returns_df.empty:
                                self.logger.warning(
                                    "Router benchmark bars unavailable for %s; using Yahoo fallback.",
                                    fallback_symbol,
                                )
                                return
                        except Exception:
                            pass

                    self.logger.warning(
                        "Router benchmark bars unavailable; leaving benchmark empty (no strategy-equity fallback)."
                    )
                    self._benchmark_returns_df = None
                    return

                benchmark_asset = self._benchmark_asset
                if isinstance(benchmark_asset, str):
                    parts = [p.strip() for p in benchmark_asset.split("/") if p.strip()]
                    if len(parts) == 2:
                        benchmark_asset = (
                            Asset(symbol=parts[0], asset_type="crypto"),
                            Asset(symbol=parts[1], asset_type="forex"),
                        )
                    else:
                        # Keep behavior consistent with Yahoo benchmarks: use daily series.
                        benchmark_asset = Asset(symbol=benchmark_asset, asset_type="stock")

                # Use daily bars for benchmark across intraday strategies to keep tearsheet cost bounded.
                timestep = "day"

                try:
                    bars = self.broker.data_source.get_historical_prices_between_dates(
                        benchmark_asset,
                        timestep,
                        start_date=self._backtesting_start,
                        end_date=backtesting_end_adjusted,
                        quote=self._quote_asset,
                    )
                except Exception:
                    bars = None

                if bars is None or getattr(bars, "df", None) is None:
                    self.logger.error(f"Couldn't get benchmark bars from Router data source: {benchmark_asset}")
                    _fallback_benchmark(benchmark_asset)
                    return
                df = bars.df
                if df is None or df.empty or "close" not in df.columns:
                    self.logger.error(f"Router benchmark bars empty/invalid: {benchmark_asset}")
                    _fallback_benchmark(benchmark_asset)
                    return
                df = df.copy()
                df["return"] = df["close"].pct_change(fill_method=None)
                df["symbol_cumprod"] = (1 + df["return"]).cumprod()
                self._benchmark_returns_df = df

            elif type(self.broker.data_source) == AlpacaBacktesting:
                benchmark_asset = self._benchmark_asset

                df = self.broker.data_source.get_historical_prices_between_dates(
                    base_asset=benchmark_asset
                )

                if df is None or df.empty:
                    self.logger.error(f"Couldn't get_historical_prices_between_dates: {benchmark_asset}")
                    return
                df = df.loc[self._backtesting_start:self._backtesting_end].copy()
                if hasattr(df, 'select'):  # Polars DataFrame
                    pl = _get_polars_module()
                    df = df.with_columns(pl.col("close").pct_change().alias("return"))
                    df = df.with_columns((1 + pl.col("return")).cumprod().alias("symbol_cumprod"))
                else:  # Pandas DataFrame
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
        trades_file=None,
        trades_df=None,
        show_plot=True,
    ):
        if self._strategy_returns_df is None:
            self.logger.warning("Cannot plot returns because the strategy returns are missing")
        elif self._benchmark_returns_df is None:
            self.logger.warning("Cannot plot returns because the benchmark returns are missing")
        else:
            plot_returns(
                self._strategy_returns_df,
                f"{self._log_strat_name()}Strategy",
                self._benchmark_returns_df,
                str(self._benchmark_asset),
                plot_file_html=plot_file_html,
                trades_file=trades_file,
                trades_df=trades_df,
                show_plot=show_plot,
                initial_budget=self._initial_budget,
            )

    @staticmethod
    def _extract_returns_series(frame, returns_col: str = "return", value_col: str | None = None) -> pd.Series:
        """Extract a clean returns series from a strategy/benchmark dataframe."""
        if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
            return pd.Series(dtype=float)

        if returns_col in frame.columns:
            series = pd.to_numeric(frame[returns_col], errors="coerce")
        elif value_col and value_col in frame.columns:
            values = pd.to_numeric(frame[value_col], errors="coerce")
            series = values.pct_change(fill_method=None)
        else:
            return pd.Series(dtype=float)

        series = series.dropna()
        if series.empty:
            return pd.Series(dtype=float)

        try:
            idx = pd.to_datetime(series.index)
            if getattr(idx, "tz", None) is not None:
                idx = idx.tz_localize(None)
            series.index = idx
        except Exception:
            pass

        return series

    @staticmethod
    def _build_drawdown_inputs(strategy_returns: pd.Series) -> tuple[pd.Series, pd.DataFrame]:
        """Build drawdown series/details passed to custom tearsheet metric hooks."""
        if strategy_returns is None or strategy_returns.empty:
            return pd.Series(dtype=float), pd.DataFrame()

        growth = (1.0 + strategy_returns.fillna(0.0)).cumprod()
        high_water_mark = growth.cummax()
        drawdown = (growth / high_water_mark) - 1.0
        drawdown.name = "drawdown"

        drawdown_details = pd.DataFrame()
        try:
            import quantstats_lumi as _qs

            drawdown_details = _qs.stats.drawdown_details(drawdown)
        except Exception:
            drawdown_details = pd.DataFrame()

        return drawdown, drawdown_details

    def _default_cash_tearsheet_metrics(self) -> dict:
        metrics = {
            "Cash Deposits Total": float(getattr(self, "_cash_deposits_total", 0.0)),
            "Cash Withdrawals Total": float(getattr(self, "_cash_withdrawals_total", 0.0)),
            "Cash Adjustments Net Total": float(getattr(self, "_cash_adjustments_net_total", 0.0)),
            "Cash Financing Credit Total": float(getattr(self, "_cash_financing_credit_total", 0.0)),
            "Cash Financing Debit Total": float(getattr(self, "_cash_financing_debit_total", 0.0)),
            "Cash Financing Net Total": float(getattr(self, "_cash_financing_net_total", 0.0)),
            "Cash Financing Days Accrued": int(getattr(self, "_cash_financing_days_accrued", 0)),
            "Cash Financing Events": int(getattr(self, "_cash_financing_events", 0)),
        }

        has_non_zero_flow = any(
            abs(float(metrics[key])) > 0.0
            for key in (
                "Cash Deposits Total",
                "Cash Withdrawals Total",
                "Cash Adjustments Net Total",
                "Cash Financing Credit Total",
                "Cash Financing Debit Total",
                "Cash Financing Net Total",
            )
        )
        has_financing_config = bool(getattr(self, "_cash_financing_enabled", False))
        has_financing_activity = bool(metrics["Cash Financing Days Accrued"] or metrics["Cash Financing Events"])

        if not (has_non_zero_flow or has_financing_config or has_financing_activity):
            return {}

        return metrics

    def _collect_custom_tearsheet_metrics(self) -> dict:
        """Invoke Strategy.tearsheet_custom_metrics() if implemented."""
        base_metrics = self._default_cash_tearsheet_metrics()
        hook = getattr(self, "tearsheet_custom_metrics", None)
        if not callable(hook):
            return base_metrics

        stats_df = self._stats.copy(deep=True) if isinstance(self._stats, pd.DataFrame) else None
        strategy_returns = self._extract_returns_series(
            self._strategy_returns_df,
            returns_col="return",
            value_col="portfolio_value",
        )
        benchmark_returns = self._extract_returns_series(
            self._benchmark_returns_df,
            returns_col="return",
            value_col="symbol_cumprod",
        )
        drawdown, drawdown_details = self._build_drawdown_inputs(strategy_returns)

        try:
            custom_metrics = hook(
                stats_df=stats_df,
                strategy_returns=strategy_returns,
                benchmark_returns=benchmark_returns if not benchmark_returns.empty else None,
                drawdown=drawdown,
                drawdown_details=drawdown_details,
                risk_free_rate=self.risk_free_rate,
            )
        except Exception as exc:
            self.logger.warning("tearsheet_custom_metrics() failed; continuing without custom metrics: %s", exc)
            return base_metrics

        if custom_metrics is None:
            return base_metrics
        if not isinstance(custom_metrics, dict):
            self.logger.warning(
                "tearsheet_custom_metrics() must return a dict, got %s; ignoring custom metrics.",
                type(custom_metrics).__name__,
            )
            return base_metrics

        return {**base_metrics, **custom_metrics}

    def tearsheet(
        self,
        save_tearsheet=True,
        tearsheet_file=None,
        show_tearsheet=True,
        tearsheet_metrics_file=None,
    ):
        if not save_tearsheet and not show_tearsheet:
            return None

        if show_tearsheet:
            save_tearsheet = True

        if self._strategy_returns_df is None:
            self.logger.warning("Cannot create a tearsheet because the strategy returns are missing")
        else:
            # Get the strategy parameters
            strategy_parameters = dict(self.parameters) if isinstance(self.parameters, dict) else {}

            # Remove pandas_data from the strategy parameters if it exists
            if "pandas_data" in strategy_parameters:
                del strategy_parameters["pandas_data"]

            # Always include backtest context in the QuantStats "Parameters Used" table.
            # This keeps reports self-describing (especially important when comparing sources).
            try:
                if self.is_backtesting:
                    strategy_parameters.setdefault(
                        "BACKTESTING_DATA_SOURCE",
                        os.environ.get("BACKTESTING_DATA_SOURCE") or type(self.broker.data_source).__name__,
                    )
                    if getattr(self.broker, "option_source", None) is not None:
                        strategy_parameters.setdefault(
                            "OPTION_DATA_SOURCE",
                            type(self.broker.option_source).__name__,
                        )
            except Exception:
                # Never fail tearsheet generation due to metadata/diagnostics.
                pass

            strat_name = self._name if self._name is not None else "Strategy"

            lumibot_version = None
            backtesting_data_sources = None
            backtest_time_seconds = None

            try:
                if self.is_backtesting:
                    try:
                        import lumibot as _lumibot

                        lumibot_version = getattr(_lumibot, "__version__", None)
                    except Exception:
                        lumibot_version = None

                    try:
                        backtesting_data_sources = (
                            os.environ.get("BACKTESTING_DATA_SOURCES")
                            or os.environ.get("BACKTESTING_DATA_SOURCE")
                            or type(self.broker.data_source).__name__
                        )
                    except Exception:
                        backtesting_data_sources = os.environ.get("BACKTESTING_DATA_SOURCE")

                    backtest_time_seconds = getattr(self, "_backtest_time_seconds", None)
                    if backtest_time_seconds is None:
                        start_ts = getattr(self, "_backtest_time_start_monotonic", None)
                        if start_ts is not None:
                            backtest_time_seconds = time.monotonic() - float(start_ts)
            except Exception:
                pass

            custom_metrics = self._collect_custom_tearsheet_metrics()

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
                lumibot_version=lumibot_version,
                backtesting_data_sources=backtesting_data_sources,
                backtest_time_seconds=backtest_time_seconds,
                tearsheet_metrics_file=tearsheet_metrics_file,
                custom_metrics=custom_metrics,
            )

            return result

    @classmethod
    def run_backtest(
        self,
        datasource_class=None,
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
        tearsheet_metrics_file = None,
        save_tearsheet = True,
        show_tearsheet = None,
        parameters = {},
        buy_trading_fees = [],
        sell_trading_fees = [],
        buy_trading_slippages = [],
        sell_trading_slippages = [],
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
        trader_class = None,
        include_cash_positions=False,
        save_stats_file = True,
        **kwargs,
    ):
        """Backtest a strategy.

        Parameters
        ----------
        datasource_class : class, optional
            The datasource class to use. For example, if you want to use the yahoo finance datasource,
            then you would pass YahooDataBacktesting as the datasource_class. When BACKTESTING_DATA_SOURCE
            is configured in the environment, you may leave this as None and let the runtime resolve the
            effective backtesting data source from the environment instead.
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
        buy_trading_slippages : list of TradingSlippage objects
            Slippage amounts to apply to buy SMART_LIMIT fills when no per-order slippage is provided.
        sell_trading_slippages : list of TradingSlippage objects
            Slippage amounts to apply to sell SMART_LIMIT fills when no per-order slippage is provided.
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
        >>>     datasource_class=None,
        >>>     backtesting_start=backtesting_start,
        >>>     backtesting_end=backtesting_end,
        >>>     benchmark_asset=benchmark_asset,
        >>> )
        """

        if name is None:
            name = self.__name__
        if trader_class is None:
            trader_class = _trader_class()

        self._name = name
        self._analyze_backtest = analyze_backtest

        # Set backtesting_start: priority 1 - passed argument, 2 - BACKTESTING_START env var, 3 - default to 1 year ago
        if backtesting_start is not None:
            pass
        elif _credential("BACKTESTING_START") is not None:
            backtesting_start = _credential("BACKTESTING_START")
        else:
            backtesting_start = datetime.datetime.now() - datetime.timedelta(days=365)
            get_logger(__name__).warning(
            colored(
                "backtesting_start is set to one year ago by default. You can set it to a specific date by passing in the backtesting_start parameter or by setting the BACKTESTING_START environment variable.",
                "yellow"
            )
            )

        # Set backtesting_end: priority 1 - passed argument, 2 - BACKTESTING_END env var, 3 - default to yesterday
        if backtesting_end is not None:
            pass
        elif _credential("BACKTESTING_END") is not None:
            backtesting_end = _credential("BACKTESTING_END")
        else:
            backtesting_end = datetime.datetime.now() - datetime.timedelta(days=1)
            get_logger(__name__).warning(
            colored(
                "backtesting_end is set to the current date by default. You can set it to a specific date by passing in the backtesting_end parameter or by setting the BACKTESTING_END environment variable.",
                "yellow"
            )
            )

        # Create an adapter with 'strategy_name' set to the instance's name
        if not hasattr(self, "logger") or self.logger is None:
            self.logger = get_strategy_logger(__name__, self._name)

        # If show_plot is None, then set it to True
        if show_plot is None:
            show_plot = _credential("SHOW_PLOT")

        # If show_tearsheet is None, then set it to True
        if show_tearsheet is None:
            show_tearsheet = _credential("SHOW_TEARSHEET")

        # If show_indicators is None, then set it to True
        if show_indicators is None:
            show_indicators = _credential("SHOW_INDICATORS")

        from lumibot.credentials import BACKTESTING_DATA_SOURCE as _DEFAULT_BACKTESTING_DATA_SOURCE
        _ensure_backtesting_imports()

        # Determine whether an environment override exists. When BACKTESTING_DATA_SOURCE
        # is set (and not blank/\"none\"), it should take precedence even if a
        # datasource_class argument was provided.
        env_override_raw = os.environ.get("BACKTESTING_DATA_SOURCE")
        env_override_name = None
        env_override_routing = None

        if env_override_raw is not None:
            trimmed = env_override_raw.strip()
            if trimmed and trimmed.lower() != "none":
                if trimmed.startswith("{") and trimmed.endswith("}"):
                    try:
                        parsed = json.loads(trimmed)
                    except Exception:
                        parsed = None
                    if isinstance(parsed, dict):
                        env_override_name = "router"
                        env_override_routing = parsed
                    else:
                        env_override_name = trimmed.lower()
                else:
                    env_override_name = trimmed.lower()
        elif datasource_class is None:
            # No override provided and no class in code – fall back to the default
            # configured in credentials (ThetaData unless the project overrides it).
            env_override_name = _DEFAULT_BACKTESTING_DATA_SOURCE.lower()

        env_override_label = env_override_raw or _DEFAULT_BACKTESTING_DATA_SOURCE
        if env_override_routing is not None:
            env_override_label = "router"
        elif isinstance(env_override_label, str):
            stripped_label = env_override_label.strip()
            if stripped_label.startswith("{") and stripped_label.endswith("}"):
                env_override_label = "<redacted BACKTESTING_DATA_SOURCE>"

        if env_override_name is not None:
            datasource_map = {
                "polygon": PolygonDataBacktesting,
                "thetadata": ThetaDataBacktesting,
                "yahoo": YahooDataBacktesting,
                "alpaca": AlpacaBacktesting,
                "ccxt": CcxtBacktesting,
                "databento": DataBentoDataBacktesting,
                "ibkr": InteractiveBrokersRESTBacktesting,
                "interactivebrokersrest": InteractiveBrokersRESTBacktesting,
                "interactive_brokers_rest": InteractiveBrokersRESTBacktesting,
                "router": RoutedBacktestingPandas,
                "thetadata_ibkr": RoutedBacktestingPandas,
                "theta_ibkr": RoutedBacktestingPandas,
            }

            if env_override_name not in datasource_map:
                raise ValueError(
                    f"Unknown BACKTESTING_DATA_SOURCE: '{env_override_label}'. "
                    f"Valid options: {list(datasource_map.keys())}"
                )

            datasource_class = datasource_map[env_override_name]

            if env_override_routing is not None:
                if config is None:
                    config = {}
                if isinstance(config, dict):
                    merged = dict(config)
                    merged["backtesting_data_routing"] = env_override_routing
                    config = merged
                else:
                    try:
                        setattr(config, "backtesting_data_routing", env_override_routing)
                    except Exception:
                        pass

            if quiet_logs:
                get_logger(__name__).debug(
                    colored(
                        f"Using BACKTESTING_DATA_SOURCE setting for backtest data: {env_override_label}",
                        "green"
                    )
                )
            else:
                get_logger(__name__).info(
                    colored(
                        f"Using BACKTESTING_DATA_SOURCE setting for backtest data: {env_override_label}",
                        "green"
                    )
                )
        elif datasource_class is None:
            raise ValueError(
                "No backtesting data source provided. Set BACKTESTING_DATA_SOURCE in the environment "
                "or pass datasource_class when calling backtest()."
            )

        # Make sure polygon_api_key is set if using PolygonDataBacktesting
        polygon_api_key = polygon_api_key if polygon_api_key is not None else _credential("POLYGON_API_KEY")
        if getattr(datasource_class, "__name__", None) == 'PolygonDataBacktesting' and polygon_api_key is None:
            raise ValueError(
                "Please set `POLYGON_API_KEY` to your API key from polygon.io as an environment variable if "
                "you are using PolygonDataBacktesting. If you don't have one, you can get a free API key "
                "from https://polygon.io/."
            )

        # Make sure thetadata_username and thetadata_password are set if using ThetaDataBacktesting
        if thetadata_username is None or thetadata_password is None:
            # Try getting the Theta Data credentials from credentials
            thetadata_config = _credential("THETADATA_CONFIG")
            if isinstance(thetadata_config, dict):
                if thetadata_username is None:
                    thetadata_username = thetadata_config.get('THETADATA_USERNAME')
                if thetadata_password is None:
                    thetadata_password = thetadata_config.get('THETADATA_PASSWORD')

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

        theta_credentials_missing = thetadata_username is None or thetadata_password is None
        uses_theta_data = (
            getattr(datasource_class, "__name__", None) == 'ThetaDataBacktesting'
            or getattr(optionsource_class, "__name__", None) == 'ThetaDataBacktesting'
        )
        if uses_theta_data and theta_credentials_missing:
            raise ValueError(
                "Please set `thetadata_username` and `thetadata_password` in the backtest() function if "
                "you are using ThetaDataBacktesting. If you don't have one, you can do registeration "
                "from https://www.thetadata.net/."
            )

        # Make a string with 6 random numbers/letters (upper and lowercase) to avoid overwriting
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=6))

        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        base_filename = f"{name + '_' if name is not None else ''}{datestring}_{random_string}"

        logdir = "logs"
        env_save_logfile = os.environ.get("SAVE_LOGFILE")
        if env_save_logfile is not None:
            normalized = env_save_logfile.strip().lower()
            if normalized in ("true", "1", "yes", "y"):
                save_logfile = True
            elif normalized in ("false", "0", "no", "n"):
                save_logfile = False
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

        try:
            backtesting_start = self._normalize_backtest_datetime(backtesting_start)
            backtesting_end = self._normalize_backtest_datetime(backtesting_end)
        except AttributeError:
            get_logger(__name__).error(
                "`backtesting_start` and `backtesting_end` must be datetime objects. \n"
                "You are receiving this error most likely because you are using \n"
                "the original positional arguments for backtesting. \n\n"
            )
            return None

        backtesting_start, backtesting_end = self.verify_backtest_inputs(backtesting_start, backtesting_end)

        quiet_logs_env = os.environ.get("BACKTESTING_QUIET_LOGS")
        if quiet_logs_env is not None:
            quiet_logs = quiet_logs_env.strip().lower() in ("true", "1", "yes", "on")

        show_progress_env = os.environ.get("BACKTESTING_SHOW_PROGRESS_BAR")
        if show_progress_env is not None:
            show_progress_bar = _credential("BACKTESTING_SHOW_PROGRESS_BAR")

        previous_backtesting_env = {
            "IS_BACKTESTING": os.environ.get("IS_BACKTESTING"),
            "BACKTESTING_QUIET_LOGS": os.environ.get("BACKTESTING_QUIET_LOGS"),
            "BACKTESTING_SHOW_PROGRESS_BAR": os.environ.get("BACKTESTING_SHOW_PROGRESS_BAR"),
        }
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "true" if quiet_logs else "false"
        os.environ["BACKTESTING_SHOW_PROGRESS_BAR"] = "true" if show_progress_bar else "false"

        try:
            logger = get_logger(__name__)
            logger.info("Backtest start = %s", backtesting_start)
            logger.info("Backtest end = %s", backtesting_end)

            if not self.IS_BACKTESTABLE:
                logger.warning(f"Strategy {name + ' ' if name is not None else ''}cannot be " f"backtested at the moment")
                return None

            self._trader = trader_class(logfile=logfile, backtest=True, quiet_logs=quiet_logs)

            if datasource_class.__name__ == 'PolygonDataBacktesting':
                data_source = datasource_class(
                    backtesting_start,
                    backtesting_end,
                    config=config,
                    auto_adjust=auto_adjust,
                    api_key=polygon_api_key,
                    pandas_data=pandas_data,
                    show_progress_bar=show_progress_bar,
                    max_memory=_credential("POLYGON_MAX_MEMORY_BYTES"),
                    log_backtest_progress_to_file=_credential("LOG_BACKTEST_PROGRESS_TO_FILE"),
                    **kwargs,
                )
            elif issubclass(datasource_class, ThetaDataBacktestingPandas) or (
                optionsource_class and issubclass(optionsource_class, ThetaDataBacktestingPandas)
            ):
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
                    log_backtest_progress_to_file=_credential("LOG_BACKTEST_PROGRESS_TO_FILE"),
                    **kwargs,
                )
            elif datasource_class == InteractiveBrokersRESTBacktesting:
                data_source = datasource_class(
                    backtesting_start,
                    backtesting_end,
                    config=config,
                    auto_adjust=auto_adjust,
                    pandas_data=pandas_data,
                    show_progress_bar=show_progress_bar,
                    log_backtest_progress_to_file=_credential("LOG_BACKTEST_PROGRESS_TO_FILE"),
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
                    log_backtest_progress_to_file=_credential("LOG_BACKTEST_PROGRESS_TO_FILE"),
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
                buy_trading_slippages=buy_trading_slippages,
                sell_trading_slippages=sell_trading_slippages,
                save_logfile=save_logfile,
                include_cash_positions=include_cash_positions,
                **kwargs,
            )
            self._trader.add_strategy(strategy)

            self.logger.info("Starting backtest...")
            try:
                strategy._backtest_time_start_monotonic = time.monotonic()
            except Exception:
                pass
            start = datetime.datetime.now()

            result = self._trader.run_all(
                show_plot=show_plot,
                show_tearsheet=show_tearsheet,
                save_tearsheet=save_tearsheet,
                show_indicators=show_indicators,
                plot_file_html=plot_file_html,
                trades_file=trades_file,
                settings_file=settings_file,
                indicators_file=indicators_file,
                tearsheet_file=tearsheet_file,
                tearsheet_metrics_file=tearsheet_metrics_file,
                base_filename=base_filename,
            )

            end = datetime.datetime.now()
            backtesting_length = backtesting_end - backtesting_start
            backtesting_run_time = end - start
            self.logger.info(
                f"Backtest took {backtesting_run_time} for a speed of {backtesting_run_time / backtesting_length:,.3f}"
            )

            return result[name], strategy
        finally:
            for key, previous_value in previous_backtesting_env.items():
                if previous_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = previous_value

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
        trade_events_file=None,
        settings_file=None,
        indicators_file=None,
        tearsheet_csv_file=None,
        tearsheet_metrics_file=None,
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
        if not trade_events_file:
            # Full trade-event export (includes optional `audit.*` telemetry when LUMIBOT_BACKTEST_AUDIT=1).
            # `plot_returns()` intentionally writes a simplified `_trades.csv` for UI/quick review, so
            # we keep the full event stream in a separate artifact for investigations.
            trade_events_file = f"{logdir}/{base_filename}_trade_events.csv"
        if not tearsheet_file:
            tearsheet_file = f"{logdir}/{base_filename}_tearsheet.html"
        if not settings_file:
            settings_file = f"{logdir}/{base_filename}_settings.json"
        if not indicators_file:
            indicators_file = f"{logdir}/{base_filename}_indicators.html"
        if not tearsheet_csv_file:
            tearsheet_csv_file = f"{logdir}/{base_filename}_tearsheet.csv"
        if not tearsheet_metrics_file:
            tearsheet_metrics_file = f"{logdir}/{base_filename}_tearsheet_metrics.json"

        try:
            start_ts = getattr(self, "_backtest_time_start_monotonic", None)
            if start_ts is not None:
                self._backtest_time_seconds = time.monotonic() - float(start_ts)
        except Exception:
            # Never fail analysis due to timing metadata.
            pass

        self.write_backtest_settings(settings_file)

        backtesting_broker = self.broker
        backtesting_broker.export_trade_events_to_csv(trade_events_file)
        # Legacy fallback: export trade events to the trades CSV path as well when plots
        # are disabled, in case downstream tooling reads the events-style format.
        # Note: plot_returns() now always writes the simplified trades CSV/parquet
        # regardless of show_plot, so this is a secondary export for compatibility.
        if not show_plot:
            backtesting_broker.export_trade_events_to_csv(trades_file)
        self.plot_returns_vs_benchmark(
            plot_file_html=plot_file_html,
            trades_file=trades_file,
            trades_df=backtesting_broker._trade_event_log_df,
            show_plot=show_plot,
        )
        # Create chart lines dataframe
        chart_lines_df = pd.DataFrame(self._chart_lines_list)
        # Create chart OHLC dataframe
        chart_ohlc_df = pd.DataFrame(getattr(self, "_chart_ohlc_list", []))
        # Create chart markers dataframe
        chart_markers_df = pd.DataFrame(self._chart_markers_list)

        # Check if we have at least one indicator to plot
        if chart_markers_df is not None and chart_lines_df is not None:
            plot_indicators(
                indicators_file,
                chart_markers_df,
                chart_lines_df,
                chart_ohlc_df,
                f"{self._log_strat_name()}Strategy Indicators",
                show_indicators=show_indicators,
            )

        tearsheet_result = self.tearsheet(
            save_tearsheet=save_tearsheet,
            tearsheet_file=tearsheet_file,
            show_tearsheet=show_tearsheet,
            tearsheet_metrics_file=tearsheet_metrics_file,
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

        Returns
        -------
        tuple[datetime.datetime, datetime.datetime]
            Normalized (timezone-aware) and validated start/end datetimes. If the provided
            end datetime is in the future, it is clamped to the current time.
        """
        # Check backtesting_start and backtesting_end
        if not isinstance(backtesting_start, datetime.datetime):
            raise ValueError(f"`backtesting_start` must be a datetime object. You passed in {backtesting_start}")

        if not isinstance(backtesting_end, datetime.datetime):
            raise ValueError(f"`backtesting_end` must be a datetime object. You passed in {backtesting_end}")

        start_dt = cls._normalize_backtest_datetime(backtesting_start)
        end_dt = cls._normalize_backtest_datetime(backtesting_end)

        # Check that backtesting end is after backtesting start
        if end_dt <= start_dt:
            raise ValueError(
                f"`backtesting_end` must be after `backtesting_start`. You passed in "
                f"{end_dt} and {start_dt}"
            )

        # If backtesting_end is in the future, clamp it to now. This avoids hard failures when
        # callers specify a "future" end date (e.g., tomorrow) and expect the backtest to stop
        # at the most recent available data.
        now = datetime.datetime.now(end_dt.tzinfo) if end_dt.tzinfo else datetime.datetime.now()
        if end_dt > now:
            get_logger(__name__).warning(
                "`backtesting_end` is in the future (%s > %s). Clamping to %s.",
                end_dt,
                now,
                now,
            )
            end_dt = now

        # After clamping, ensure end is still after start.
        if end_dt <= start_dt:
            raise ValueError(
                f"`backtesting_end` must be after `backtesting_start`. You passed in "
                f"{end_dt} and {start_dt}"
            )

        return start_dt, end_dt

    @staticmethod
    def _remember_sent_cash_event_id(self, event_id: str) -> None:
        if not event_id:
            return

        sent_ids = getattr(self, "_cash_event_sent_ids", None)
        sent_queue = getattr(self, "_cash_event_sent_id_order", None)
        if sent_ids is None or sent_queue is None:
            return

        if event_id in sent_ids:
            return

        dedupe_capacity = int(getattr(self, "_cash_event_dedupe_capacity", 1000) or 1000)
        while len(sent_queue) >= dedupe_capacity:
            expired_event_id = sent_queue.popleft()
            sent_ids.discard(expired_event_id)

        sent_queue.append(event_id)
        sent_ids.add(event_id)

    @staticmethod
    def _collect_cash_events_for_cloud(self) -> list[CashEvent]:
        pending_events = list(getattr(self, "_cash_event_pending_for_cloud", []) or [])
        emit_limit = int(getattr(self, "_cash_event_cloud_emit_limit", 50) or 50)
        if pending_events:
            return pending_events[:emit_limit]

        broker = getattr(self, "broker", None)
        get_cash_events = getattr(broker, "get_cash_events", None)
        if not callable(get_cash_events):
            return []

        last_poll_at = getattr(self, "_cash_event_last_poll_at", None)
        poll_interval_seconds = int(getattr(self, "_cash_event_poll_interval_seconds", 300) or 300)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if (
            isinstance(last_poll_at, datetime.datetime)
            and (now_utc - last_poll_at).total_seconds() < poll_interval_seconds
        ):
            return []

        lookback_days = int(getattr(self, "_cash_event_poll_lookback_days", 7) or 7)
        fetch_limit = int(getattr(self, "_cash_event_fetch_limit", 100) or 100)
        fetch_since = now_utc - datetime.timedelta(days=lookback_days)
        self._cash_event_last_poll_at = now_utc

        try:
            fetched_events = get_cash_events(since=fetch_since, limit=fetch_limit)
        except Exception as exc:
            broker_name = getattr(broker, "name", None) or broker.__class__.__name__
            self.logger.warning(
                f"Failed to load broker cash events from {broker_name} "
                f"({broker.__class__.__name__}): {exc}"
            )
            self.logger.debug(traceback.format_exc())
            return []

        sent_ids = getattr(self, "_cash_event_sent_ids", set())
        pending_ids = {
            getattr(event, "event_id", None)
            for event in getattr(self, "_cash_event_pending_for_cloud", []) or []
        }

        normalized_events = []
        from ..entities import CashEvent

        for event in fetched_events or []:
            if not isinstance(event, CashEvent):
                continue
            if event.event_id in sent_ids or event.event_id in pending_ids:
                continue
            normalized_events.append(event)

        normalized_events.sort(key=lambda event: (event.occurred_at, event.event_id))
        if normalized_events:
            self.logger.debug(
                "Loaded %s new cash events from broker '%s'",
                len(normalized_events),
                getattr(broker, "name", "unknown"),
            )
            self._cash_event_pending_for_cloud.extend(normalized_events)

        pending_events = list(getattr(self, "_cash_event_pending_for_cloud", []) or [])
        return pending_events[:emit_limit]

    @staticmethod
    def _mark_cash_events_sent(self, emitted_events: list[CashEvent]) -> None:
        if not emitted_events:
            return

        emitted_event_ids = {event.event_id for event in emitted_events if getattr(event, "event_id", None)}
        if not emitted_event_ids:
            return

        remaining_pending_events = []
        for event in getattr(self, "_cash_event_pending_for_cloud", []) or []:
            if getattr(event, "event_id", None) in emitted_event_ids:
                _Strategy._remember_sent_cash_event_id(self, event.event_id)
            else:
                remaining_pending_events.append(event)

        self._cash_event_pending_for_cloud = remaining_pending_events

    def send_update_to_cloud(self):
        """
        Sends an update to the LumiWealth cloud server with the current portfolio value, cash, positions, and any outstanding orders.
        There is an API Key that is required to send the update to the cloud.
        The API Key is stored in the environment variable LUMIWEALTH_API_KEY.
        """
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            self.logger.debug("Skipping cloud update - in backtesting mode")
            return

        # Check if self.lumiwealth_api_key has been set, if not, return
        if not hasattr(self, "lumiwealth_api_key") or self.lumiwealth_api_key is None or self.lumiwealth_api_key == "":
            # Log that we are not sending the update to the cloud
            if not self._logged_missing_lumiwealth_api_key:
                self.logger.warning("LUMIWEALTH_API_KEY not set. Not sending an update to the cloud because "
                                    "lumiwealth_api_key is not set. If you would like to be able to track your bot "
                                    "performance on www.botspot.trade, please set the lumiwealth_api_key parameter "
                                    "in the strategy initialization or the LUMIWEALTH_API_KEY environment variable.")
                self._logged_missing_lumiwealth_api_key = True
            return

        # Log that we're starting to send data
        self.logger.debug(f"Starting cloud update for strategy '{self._name}' with API key: {self.lumiwealth_api_key[:10]}...")

        # Get the current portfolio value
        try:
            portfolio_value = self.get_portfolio_value()
            self.logger.debug(f"Portfolio value: {portfolio_value}")
        except Exception as e:
            self.logger.error(f"Failed to get portfolio value: {e}")
            self.logger.error(traceback.format_exc())
            return False

        # Get the current cash
        try:
            cash = self.get_cash()
            self.logger.debug(f"Cash: {cash}")
        except Exception as e:
            self.logger.error(f"Failed to get cash: {e}")
            self.logger.error(traceback.format_exc())
            return False

        # Get the current positions
        try:
            positions = self.get_positions()
            self.logger.debug(f"Number of positions: {len(positions)}")
            # DEBUG: Log position details
            for pos in positions:
                self.logger.debug(f"[DEBUG] Position: {pos.symbol}, qty: {pos.quantity}, has_price: {hasattr(pos, 'current_price')}")
                if hasattr(pos, '__dict__'):
                    attrs = {k: v for k, v in pos.__dict__.items() if not k.startswith('_')}
                    self.logger.debug(f"[DEBUG] Position attrs for {pos.symbol}: {list(attrs.keys())}")
        except Exception as e:
            self.logger.error(f"Failed to get positions: {e}")
            self.logger.error(traceback.format_exc())
            return False

        # Get the current orders
        try:
            orders = self.get_orders()
            self.logger.debug(f"Number of orders: {len(orders)}")
        except Exception as e:
            self.logger.error(f"Failed to get orders: {e}")
            self.logger.error(traceback.format_exc())
            return False

        cash_events = _Strategy._collect_cash_events_for_cloud(self)
        self.logger.debug(f"Number of cash events: {len(cash_events)}")

        LUMIWEALTH_URL = "https://listener.lumiwealth.com/portfolio_events"

        headers = {
            "x-api-key": f"{self.lumiwealth_api_key}",
            "Content-Type": "application/json",
        }

        # Create the data to send to the cloud
        positions_data = [position.to_dict() for position in positions]

        data = {
            "data_type": "portfolio_event",
            "portfolio_value": portfolio_value,
            "cash": cash,
            "positions": positions_data,
            "orders": [order.to_dict() for order in orders],
            "cash_events": [event.to_dict() for event in cash_events],
            "strategy_name": self._name,
            "broker_name": self.broker.name,
        }

        self.logger.debug(
            f"Preparing to send portfolio update: value={portfolio_value}, cash={cash}, "
            f"positions={len(positions)}, orders={len(orders)}, cash_events={len(cash_events)}"
        )

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

        requests = _get_requests_module()
        try:
            # Send the data to the cloud
            json_data = json.dumps(data, default=str)
            data_size_kb = len(json_data.encode('utf-8')) / 1024
            self.logger.debug(f"Sending {data_size_kb:.2f} KB of data to {LUMIWEALTH_URL}")
            self.logger.debug(f"Request headers: {headers}")

            response = requests.post(LUMIWEALTH_URL, headers=headers, data=json_data)

            self.logger.debug(f"Cloud response: Status={response.status_code}, Headers={dict(response.headers)}")

        except requests.exceptions.ConnectionError as e:
            self.logger.info(f"Connection error when sending to cloud: {e}", exc_info=True)
            return False
        except requests.exceptions.Timeout as e:
            self.logger.info(f"Timeout error when sending to cloud: {e}", exc_info=True)
            return False
        except requests.exceptions.RequestException as e:
            self.logger.info(f"Request error when sending to cloud: {e}", exc_info=True)
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error when sending to cloud: {e}")
            self.logger.error(traceback.format_exc())
            return False

        # Check if the message was sent successfully
        if response.status_code == 200:
            _Strategy._mark_cash_events_sent(self, cash_events)
            self.logger.debug(f"Portfolio update sent successfully to cloud for strategy '{self._name}'")
            return True
        elif response.status_code == 401:
            self.logger.error(f"❌ Authentication failed - Invalid API key: {self.lumiwealth_api_key[:10]}...")
            self.logger.error(f"Response: {response.text}")
            return False
        elif response.status_code == 400:
            self.logger.error("❌ Bad request - Invalid data format")
            self.logger.error(f"Response: {response.text}")
            return False
        elif response.status_code == 413:
            self.logger.error(f"❌ Payload too large ({data_size_kb:.2f} KB)")
            self.logger.error(f"Response: {response.text}")
            return False
        else:
            self.logger.error(
                f"❌ Failed to send update to cloud. Status: {response.status_code}, Response: {response.text}"
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

        requests = _get_requests_module()
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
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import matplotlib.ticker as ticker

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
        formatter = ticker.FuncFormatter(lambda x, pos: f"${int(x):1,}")
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
        now = pd.Timestamp(datetime.datetime.now()).tz_localize(_default_pytz())

        # Get the returns
        returns_text, stats_df = self.calculate_returns()

        # Send a spark chart to Discord
        self.send_spark_chart_to_discord(stats_df, portfolio_value, now)

        # Send the results text to Discord
        self.send_result_text_to_discord(returns_text, portfolio_value, cash)

    def get_stats_from_database(self, stats_table_name, retries=5, delay=5):
        create_engine, inspect, text, OperationalError = _get_sqlalchemy_imports()
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
                    ny_tz = _default_pytz()
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
        create_engine, _inspect, _text, OperationalError = _get_sqlalchemy_imports()
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

        create_engine, inspect, text, _OperationalError = _get_sqlalchemy_imports()
        # Ensure we have a self.db_engine
        if not hasattr(self, 'db_engine') or not self.db_engine:
            self.db_engine = create_engine(self.db_connection_str)

        # Get the current time in New York
        ny_tz = _default_pytz()
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
                self.logger.info("Variables backed up successfully")
            else:
                self.logger.info("No variables to back up")

        except Exception as e:
            self.logger.error(f"Error backing up variables to DB: {e}", exc_info=True)

    def load_variables_from_db(self):
        if self.is_backtesting:
            return
    
        if not hasattr(self, "db_connection_str") or self.db_connection_str is None or not self.should_backup_variables_to_database:
            return
    
        try:
            create_engine, inspect, text, _OperationalError = _get_sqlalchemy_imports()
            if not hasattr(self, 'db_engine') or not self.db_engine:
                self.db_engine = create_engine(self.db_connection_str)
    
            # Check if backup table exists
            inspector = inspect(self.db_engine)
            if not inspector.has_table(self.backup_table_name):
                self.logger.info(f"Backup for {self._name} does not exist in the database. Not restoring")
                return
    
            # Query the latest entry from the backup table
            query = text(
                f'SELECT * FROM {self.backup_table_name} WHERE strategy_id = :strategy_id ORDER BY last_updated DESC LIMIT 1'
            )
    
            params = {'strategy_id': self._name}
            df = pd.read_sql_query(query, self.db_engine, params=params)
    
            if df.empty:
                self.logger.debug("No data found in the backup")
                return
    
            json_data = df['variables'].iloc[0]
    
            import re
    
            iso_dt_re = re.compile(r"^\d{4}-\d{2}-\d{2}T")      # datetime prefix
            iso_date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")    # date only
    
            def _coerce_value(v):
                if not isinstance(v, str):
                    return v
    
                # ISO datetime (support trailing Z)
                if iso_dt_re.match(v):
                    try:
                        v2 = v.replace("Z", "+00:00") if v.endswith("Z") else v
                        return datetime.datetime.fromisoformat(v2)
                    except Exception:
                        return v
    
                # ISO date (YYYY-MM-DD)
                if iso_date_re.match(v):
                    try:
                        return datetime.datetime.strptime(v, "%Y-%m-%d").date()
                    except Exception:
                        return v
    
                return v
    
            # Decode any special types we stored using our SafeJSONEncoder,
            # but only parse strings that actually look like ISO dates/datetimes.
            data = json.loads(json_data, object_hook=lambda d: {k: _coerce_value(v) for k, v in d.items()})
    
            # Update self.vars dictionary
            for key, value in data.items():
                self.vars.set(key, value)
    
            current_state = json.dumps(self.vars.all(), sort_keys=True, cls=SafeJSONEncoder)
            self._last_backup_state = current_state
    
            self.logger.info("Variables loaded successfully from database")
    
        except Exception as e:
            self.logger.error(f"Error loading variables from database: {e}", exc_info=True)

    def calculate_returns(self):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Calculate the return over the past 24 hours, 7 days, and 30 days using the stats dataframe

        # Get the current time in New York
        ny_tz = _default_pytz()

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
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(_default_pytz(), ambiguous='infer')
        else:
            # If the datetime is already timezone-aware, first remove timezone and then localize
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(None)
            stats_df["datetime"] = stats_df["datetime"].dt.tz_localize(_default_pytz(), ambiguous='infer')

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
        stats_df["datetime"] = stats_df["datetime"].dt.tz_convert(_default_pytz())

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
