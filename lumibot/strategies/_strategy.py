import datetime
import logging
import warnings
from asyncio.log import logger
from decimal import Decimal

import pandas as pd

from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
from lumibot.entities import Asset, Position
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


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        # Use an f-string for formatting
        return f'[{self.extra["strategy_name"]}] {msg}', kwargs

class _Strategy:
    IS_BACKTESTABLE = True

    def __init__(
        self,
        *args,
        broker=None,
        minutes_before_closing=1,
        minutes_before_opening=60,
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
        strategy_id=None,
        discord_account_summary_footer=None,
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
            and account summary to the discord channel at the end of each day (account_history_db_connection_str
            must be set for this to work). Defaults to None (no discord alerts).
            For instructions on how to create a discord webhook url, see this link:
            https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks
        discord_account_summary_footer : str
            The footer to use for the account summary sent to the discord channel if discord_webhook_url is set and the
            account_history_db_connection_str is set.
            Defaults to None (no footer).
        account_history_db_connection_str : str
            The connection string to use for the account history database. This is used to store the account history
            for the strategy. The account history is sent to the discord channel at the end of each day. The connection
            string should be in the format: "sqlite:///path/to/database.db". The database should have a table named
            "strategy_tracker". If that table does not exist, it will be created. Defaults to None (no account history).
        strategy_id : str
            The id of the strategy that will be used to identify the strategy in the account history database.
            Defaults to None (lumibot will use the name of the strategy as the id).
        """
        # Handling positional arguments.
        # If there is one positional argument, it is assumed to be `broker`.
        # If there are two positional arguments, they are assumed to be
        # `name` and `broker`.
        # If there are three positional arguments, they are assumed to be
        # `name`, `budget` and `broker`

        # TODO: Break up this function, too long!

        self.buy_trading_fees = buy_trading_fees
        self.sell_trading_fees = sell_trading_fees

        if len(args) == 1:
            if isinstance(args[0], str):
                self._name = args[0]
                self.broker = broker
            else:
                self.broker = args[0]
                self._name = kwargs.get("name", name)
        elif len(args) == 2:
            self._name = args[0]
            self.broker = args[1]
            logging.warning(
                "You are using the old style of initializing a Strategy. Only use \n"
                "the broker class as the first positional argument and the rest as keyword arguments. \n"
                "For example `MyStrategy(broker, name=strategy_name, budget=budget)`\n"
            )
        elif len(args) == 3:
            self._name = args[0]
            self.broker = args[2]
            logging.warning(
                "You are using the old style of initializing a Strategy. Only use \n"
                "the broker class as the first positional argument and the rest as keyword arguments. \n"
                "For example `MyStrategy(broker, name=strategy_name, budget=budget)`\n"
            )
        else:
            self.broker = broker
            self._name = name

        if self._name is None:
            self._name = self.__class__.__name__

        # Create an adapter with 'strategy_name' set to the instance's name
        self.logger = CustomLoggerAdapter(logger, {'strategy_name': self._name})

        self.discord_webhook_url = discord_webhook_url
        self.account_history_db_connection_str = account_history_db_connection_str
        self.discord_account_summary_footer = discord_account_summary_footer

        if strategy_id is None:
            self.strategy_id = self._name
        else:
            self.strategy_id = strategy_id

        self._quote_asset = quote_asset
        self.broker.quote_assets.add(self._quote_asset)

        # Setting the broker object
        self._is_backtesting = self.broker.IS_BACKTESTING_BROKER
        self._benchmark_asset = benchmark_asset

        # Get the backtesting start and end dates from the broker data source if we are backtesting
        if self._is_backtesting:
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
        if self._is_backtesting:
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
        if not self._is_backtesting:
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

            ### END
            ##############################################

        self._initial_budget = budget
        self._minutes_before_closing = minutes_before_closing
        self._minutes_before_opening = minutes_before_opening
        self._sleeptime = sleeptime
        self._executor = StrategyExecutor(self)
        self.broker._add_subscriber(self._executor)

        # Stats related variables
        self._stats_file = stats_file
        self._stats = None
        self._stats_list = []
        self._analysis = {}

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
                "_is_backtesting",
            ]:
                result[key[1:]] = self.__dict__[key]

        return result

    def _set_cash_position(self, cash: float):
        # Check if cash is in the list of positions yet
        for x in range(len(self.broker._filled_positions.get_list())):
            position = self.broker._filled_positions[x]
            if position.asset == self.quote_asset:
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
        if self._is_backtesting:
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
            broker_balances = self.broker._get_balances_at_broker(self.quote_asset)

            if broker_balances is not None:
                (
                    self._cash,
                    self._position_value,
                    self._portfolio_value,
                ) = broker_balances

                self.last_broker_balances_update = datetime.datetime.now()
                return True

            else:
                logger.error(
                    "Unable to get balances (cash, portfolio value, etc) from broker. "
                    "Please check your broker and your broker configuration."
                )
                return False
        else:
            logger.debug("Balances already updated recently. Skipping update.")

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        if not self._is_backtesting:
            broker_balances = self.broker._get_balances_at_broker(self.quote_asset)

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

            assets = []
            for asset in assets_original:
                if asset != self.quote_asset:
                    if asset.asset_type == "crypto" or asset.asset_type == "forex":
                        asset = (asset, self.quote_asset)
                    assets.append(asset)

            prices = self.broker.data_source.get_last_prices(assets)

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

                if self._is_backtesting and price is None:
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
                self._stats.to_csv(self._stats_file)

            self._strategy_returns_df = day_deduplicate(self._stats)

            self._analysis = stats_summary(self._strategy_returns_df, self.risk_free_rate)

            # Getting performance for the benchmark asset
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
                            benchmark_asset = Asset(symbol=benchmark_asset,asset_type="crypto")

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
                    self._benchmark_returns_df = get_symbol_returns(
                        self._benchmark_asset,
                        self._backtesting_start,
                        backtesting_end_adjusted,
                    )

        for handler in logger.handlers:
            if handler.__class__.__name__ == "StreamHandler":
                handler.setLevel(current_stream_handler_level)
        logger.setLevel(current_level)

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
            strat_name = self._name if self._name is not None else "Strategy"
            create_tearsheet(
                self._strategy_returns_df,
                strat_name,
                tearsheet_file,
                self._benchmark_returns_df,
                self._benchmark_asset,
                show_tearsheet,
                risk_free_rate=self.risk_free_rate,
            )

    @classmethod
    def run_backtest(
        cls,
        *args,
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
        show_plot=True,
        tearsheet_file=None,
        save_tearsheet=True,
        show_tearsheet=True,
        parameters={},
        buy_trading_fees=[],
        sell_trading_fees=[],
        api_key=None,
        polygon_api_key=None,
        polygon_has_paid_subscription=False,
        indicators_file=None,
        show_indicators=True,
        save_logfile=True,
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
        api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class.
        polygon_api_key: str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class. Deprecated, please use 'api_key' instead.
        polygon_has_paid_subscription : bool
            Whether you have a paid subscription to Polygon. Only required if you are using
            PolygonDataBacktesting as the datasource_class.
        indicators_file : str
            The file to write the indicators to.
        show_indicators : bool
            Whether to show the indicators plot.
        save_logfile : bool
            Whether to save the logfile. Defaults to True. If False, the logfile will not be saved.


        Returns
        -------
        Backtest
            The backtest object.

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

        warnings.warn(
            "The backtest() method will be depricated in future versions of Lumibot. Instead "
            "please use 'trader.run_all(backtest=True)' as this more closely models how your strategy "
            "will behave in Live Trading sessions.  <web link>",
            DeprecationWarning,
            stacklevel=2,
        )

        positional_args_error_message = (
            "Please do not use `name' or 'budget' as positional arguments. \n"
            "These have been changed to keyword arguments. For example, \n"
            "please create your `strategy.backtest` similar to below adding, \n"
            "if you need them, `name` and `budget` as keyword arguments. \n"
            "    strategy_class.backtest(\n"
            "        backtesting_datasource,\n"
            "        backtesting_start,\n"
            "        backtesting_end,\n"
            "        pandas_data=pandas_data,\n"
            "        stats_file=stats_file,\n"
            "        name='my_strategy_name',\n"
            "        budget=50000,\n"
            "        config=config,\n"
            "        logfile=logfile,\n"
            "        **kwargs,\n"
            "    )"
        )

        # Print start message
        print(f"Starting backtest for {cls.__name__}...")

        # Handling positional arguments.
        if len(args) == 3:
            datasource_class = args[0]
            backtesting_start = args[1]
            backtesting_end = args[2]
            name = kwargs.get("name", name)
            budget = kwargs.get("budget", budget)
        elif len(args) == 5:
            name = args[0]
            budget = args[1]
            datasource_class = args[2]
            backtesting_start = args[3]
            backtesting_end = args[4]
            logging.warning(
                f"You are using the old style of initializing a backtest object. \n" f"{positional_args_error_message}"
            )
        else:
            # Error message
            logging.error(
                "Unable to interpret positional arguments. Please ensure you have \n"
                "included `datasource_class`, `backtesting_start`,  and `backtesting_end` \n"
                "for your three positional arguments. \n"
            )

        if name is None:
            name = cls.__name__

        if not api_key and polygon_api_key:
            api_key = polygon_api_key

        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        basename = f"{name + '_' if name is not None else ''}{datestring}"
        logdir = "logs"
        if logfile is None and save_logfile:
            logfile = f"{logdir}/{basename}_logs.csv"
        if stats_file is None:
            stats_file = f"{logdir}/{basename}_stats.csv"

        # #############################################
        # Check the data types of the parameters
        # #############################################

        # Check datasource_class
        if not isinstance(datasource_class, type):
            raise ValueError(f"`datasource_class` must be a class. You passed in {datasource_class}")

        cls.verify_backtest_inputs(backtesting_start, backtesting_end)

        # Make sure polygon_api_key is set if using PolygonDataBacktesting
        if datasource_class == PolygonDataBacktesting and api_key is None:
            raise ValueError(
                "Please set `api_key` to your API key from polygon.io in the backtest() function if "
                "you are using PolygonDataBacktesting. If you don't have one, you can get a free API key "
                "from https://polygon.io/."
            )

        if not cls.IS_BACKTESTABLE:
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
                f"{positional_args_error_message}"
            )
            return None

        trader = Trader(logfile=logfile, backtest=True)
        data_source = datasource_class(
            backtesting_start,
            backtesting_end,
            config=config,
            auto_adjust=auto_adjust,
            api_key=api_key,
            pandas_data=pandas_data,
            **kwargs,
        )
        if hasattr(data_source, "has_paid_subscription"):
            data_source.has_paid_subscription = polygon_has_paid_subscription

        # if hasattr(data_source, 'pandas_data'):
        #     data_source.pandas_data = pandas_data

        backtesting_broker = BacktestingBroker(data_source)
        strategy = cls(
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
            **kwargs,
        )
        trader.add_strategy(strategy)

        logger = logging.getLogger("backtest_stats")
        logger.setLevel(logging.INFO)
        logger.info("Starting backtest...")
        start = datetime.datetime.now()

        result = trader.run_all(
            show_plot=show_plot,
            show_tearsheet=show_tearsheet,
            save_tearsheet=save_tearsheet,
            show_indicators=show_indicators,
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
    ):
        name = self._name

        # Filename defaults
        if not logdir:
            logdir = "logs"

        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        basename = f"{name + '_' if name is not None else ''}{datestring}"
        if plot_file_html is None:
            plot_file_html = f"{logdir}/{basename}_trades.html"
        if trades_file is None:
            trades_file = f"{logdir}/{basename}_trades.csv"
        if tearsheet_file is None:
            tearsheet_file = f"{logdir}/{basename}_tearsheet.html"
        if settings_file is None:
            settings_file = f"{logdir}/{basename}_settings.json"
        if indicators_file is None:
            indicators_file = f"{logdir}/{basename}_indicators.html"

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
        self.tearsheet(
            save_tearsheet=save_tearsheet,
            tearsheet_file=tearsheet_file,
            show_tearsheet=show_tearsheet,
        )

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
        cls,
        *args,
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
        show_plot=True,
        tearsheet_file=None,
        save_tearsheet=True,
        show_tearsheet=True,
        parameters={},
        buy_trading_fees=[],
        sell_trading_fees=[],
        api_key=None,
        polygon_api_key=None,
        polygon_has_paid_subscription=False,
        indicators_file=None,
        show_indicators=True,
        save_logfile=True,
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
        api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class.
        polygon_api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class. Depricated, please use 'api_key' instead.
        polygon_has_paid_subscription : bool
            Whether you have a paid subscription to Polygon. Only required if you are using
            PolygonDataBacktesting as the datasource_class.
        indicators_file : str
            The file to write the indicators to.
        show_indicators : bool
            Whether to show the indicators plot.
        save_logfile : bool
            Whether to save the logs to a file. If False, the logs will not be saved to a file. Default is True.

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
        results, strategy = cls.run_backtest(
            *args,
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
            api_key=api_key,
            polygon_api_key=polygon_api_key,
            polygon_has_paid_subscription=polygon_has_paid_subscription,
            indicators_file=indicators_file,
            show_indicators=show_indicators,
            save_logfile=save_logfile,
            **kwargs,
        )
        return results
