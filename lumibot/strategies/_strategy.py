import datetime
import logging
from asyncio.log import logger
from copy import deepcopy
from decimal import Decimal

import pandas as pd

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset, Position
from lumibot.tools import (
    create_tearsheet,
    day_deduplicate,
    get_risk_free_rate,
    get_symbol_returns,
    plot_returns,
    stats_summary,
    to_datetime_aware,
)
from lumibot.traders import Trader

from .strategy_executor import StrategyExecutor


class _Strategy:
    IS_BACKTESTABLE = True

    def __init__(
        self,
        *args,
        broker=None,
        data_source=None,
        minutes_before_closing=5,
        minutes_before_opening=60,
        sleeptime=1,
        stats_file=None,
        risk_free_rate=None,
        benchmark_asset="SPY",
        backtesting_start=None,
        backtesting_end=None,
        pandas_data=None,
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        starting_positions=None,
        filled_order_callback=None,
        name=None,
        budget=None,
        parameters={},
        **kwargs,
    ):
        # Handling positional arguments.
        # If there is one positional argument, it is assumed to be `broker`.
        # If there are two positional arguments, they are assumed to be
        # `name` and `broker`.
        # If there are three positional arguments, they are assumed to be
        # `name`, `budget` and `broker`

        # TODO: Break up this function, too long!

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
                f"You are using the old style of initializing a Strategy. Only use \n"
                f"the broker class as the first positional argument and the rest as keyword arguments. \n"
                f"For example `MyStrategy(broker, name=strategy_name, budget=budget)`\n"
            )
        elif len(args) == 3:
            self._name = args[0]
            self.broker = args[2]
            logging.warning(
                f"You are using the old style of initializing a Strategy. Only use \n"
                f"the broker class as the first positional argument and the rest as keyword arguments. \n"
                f"For example `MyStrategy(broker, name=strategy_name, budget=budget)`\n"
            )
        else:
            self.broker = broker
            self._name = name

        self.quote_asset = quote_asset

        # Setting the broker object
        self._is_backtesting = self.broker.IS_BACKTESTING_BROKER
        self._benchmark_asset = benchmark_asset
        self._backtesting_start = backtesting_start
        self._backtesting_end = backtesting_end

        # Hold the asset objects for strings for stocks only.
        self._asset_mapping = dict()

        # Setting the data provider
        if self._is_backtesting:
            self.data_source = self.broker._data_source
            if self.data_source.SOURCE == "PANDAS":
                try:
                    assert pandas_data != None
                except AssertionError:
                    raise ValueError(
                        f"Please add a pandas dataframe as an input parameter. "
                        f"Use the following: 'pandas_data': your_dataframe "
                    )
                self.broker._trading_days = self.data_source.load_data()

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

        elif data_source is None:
            self.data_source = self.broker
        else:
            self.data_source = data_source

        if risk_free_rate is None:
            # Get risk free rate from US Treasuries by default
            self._risk_free_rate = get_risk_free_rate()
        else:
            self._risk_free_rate = risk_free_rate

        # Setting execution parameters
        self._first_iteration = True
        self._last_on_trading_iteration_datetime = None
        self._initial_budget = budget
        if not self._is_backtesting:
            (
                self._cash,
                self._position_value,
                self._portfolio_value,
            ) = self.broker._get_balances_at_broker()
            # Set initial positions if live trading.
            self.broker._set_initial_positions(self._name)
        else:
            if budget is None:
                if self.cash is None:
                    # Default to $100,000 if no budget is set.
                    budget = 100000
                    self._set_cash_position(budget)
                else:
                    budget = self.cash
            else:
                logger.warning(
                    "You have set both a starting budget and a starting position for the quote asset. The starting position for the quote asset will be replaced with the budget, and the budget will be used as the cash value instead."
                )
                self._set_cash_position(budget)

            self._portfolio_value = self.cash

            store_assets = list(self.broker._data_source._data_store.keys())
            if len(store_assets) > 0:
                positions_value = 0
                for position in self.get_positions():
                    price = None
                    if position.asset == self.quote_asset:
                        # Don't include the quote asset since it's already included with cash
                        price = 0
                    else:
                        price = self.get_last_price(
                            position.asset, quote=self.quote_asset
                        )
                    value = float(position.quantity) * price
                    positions_value += value

                self._portfolio_value = self._portfolio_value + positions_value

            else:
                self._position_value = 0

        self._minutes_before_closing = minutes_before_closing
        self._minutes_before_opening = minutes_before_opening
        self._sleeptime = sleeptime
        self._executor = StrategyExecutor(self)
        self.broker._add_subscriber(self._executor)

        # Stats related variables
        self._stats_file = stats_file
        self._stats = pd.DataFrame()
        self._analysis = {}

        # Storing parameters for the initialize method
        self._parameters = dict(list(parameters.items()) + list(kwargs.items()))

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
                    result[key] = deepcopy(self.__dict__[key])
                except:
                    pass
                    # logging.warning(
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
                result[key[1:]] = deepcopy(self.__dict__[key])

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

    def _set_asset_mapping(self, asset):
        if isinstance(asset, Asset):
            return asset
        elif isinstance(asset, tuple):
            return asset
        elif isinstance(asset, str) and "/" not in asset:
            if asset not in self._asset_mapping:
                self._asset_mapping[asset] = Asset(symbol=asset)
            return self._asset_mapping[asset]
        elif (isinstance(asset, str) and "/" in asset) or (
            isinstance(asset, tuple) and len(asset) == 2
        ):
            asset_tuple = []
            if isinstance(asset, str):
                assets = asset.split("/")
            else:
                assets = asset
            for asset in assets:
                if isinstance(asset, str) and asset not in self._asset_mapping:
                    self._asset_mapping[asset] = Asset(symbol=asset)
                    asset_tuple.append(self._asset_mapping[asset])
                asset_tuple.append(asset)
            return tuple(asset_tuple)
        else:
            if self.broker.SOURCE != "CCXT":
                raise ValueError(
                    f"You must enter a symbol string or an asset object. You "
                    f"entered {asset}"
                )
            else:
                raise ValueError(
                    f"You must enter symbol string or an asset object. If you "
                    f"getting a quote, you may enter a string like `ETH/BTC` or "
                    f"asset objects in a tuple like (Asset(ETH), Asset(BTC))."
                )

    def _log_strat_name(self):
        """Returns the name of the strategy as a string if not default"""
        return f"{self._name} " if self._name != None else ""

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        if not self._is_backtesting:
            return self.broker._get_balances_at_broker()[2]

        with self._executor.lock:
            # Used for traditional brokers, for crypto this will likely be 0
            portfolio_value = self.cash

            positions = self.broker.get_tracked_positions(self._name)
            assets = [position.asset for position in positions]
            # Set the base currency for crypto valuations.
            if (
                len(assets) > 0
                and assets[0].asset_type == "crypto"
                and self.quote_asset is not None
            ):
                assets = [(asset, self.quote_asset) for asset in assets]

            prices = self.data_source.get_last_prices(assets)

            for position in positions:
                # Turn the asset into a tuple if it's a crypto asset
                asset = (
                    position.asset
                    if position.asset.asset_type != "crypto"
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
                            f"Please ensure data exist at "
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
                            f"Please ensure data exist at "
                            f"{self.broker.datetime} for the pair: {asset}"
                        )
                if isinstance(asset, tuple):
                    multiplier = 1
                else:
                    multiplier = (
                        asset.multiplier
                        if asset.asset_type in ["option", "future"]
                        else 1
                    )
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
            assets = [position.asset for position in positions]
            dividends_per_share = self.get_yesterday_dividends(assets)
            for position in positions:
                asset = position.asset
                quantity = position.quantity
                dividend_per_share = (
                    0
                    if dividends_per_share is None
                    else dividends_per_share.get(asset, 0)
                )
                cash = self.cash
                cash += dividend_per_share * float(quantity)
                self._set_cash_position(cash)
            return self.cash

    # =============Stats functions=====================

    def _append_row(self, row):
        self._stats = pd.concat([self._stats, pd.DataFrame(row, index=[0])])

    def _format_stats(self):
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
        if not self._stats.empty:
            self._format_stats()
            if self._stats_file:
                self._stats.to_csv(self._stats_file)

            # Getting the performance of the strategy
            self.log_message(f"--- {self._log_strat_name()}Strategy Performance  ---")

            self._strategy_returns_df = day_deduplicate(self._stats)
            self._analysis = stats_summary(
                self._strategy_returns_df, self._risk_free_rate
            )

            total_return = self._analysis["total_return"]
            self.log_message(f"Total Return: {total_return*100:,.2f}%")

            cagr_value = self._analysis["cagr"]
            self.log_message(f"CAGR {cagr_value*100:,.2f}%")

            volatility_value = self._analysis["volatility"]
            self.log_message(f"Volatility {volatility_value*100:,.2f}%")

            sharpe_value = self._analysis["sharpe"]
            self.log_message(f"Sharpe {sharpe_value:,.2f}")

            max_drawdown_result = self._analysis["max_drawdown"]
            max_drawdown_value = max_drawdown_result["drawdown"] * 100
            max_drawdown_date = max_drawdown_result["date"]
            self.log_message(
                f"Max Drawdown {max_drawdown_value:,.2f}% on {max_drawdown_date:%Y-%m-%d}"
            )

            romad_value = self._analysis["romad"]
            self.log_message(f"RoMaD {romad_value*100:,.2f}%")

            # Getting performance for the benchmark asset
            if (
                self._backtesting_start is not None
                and self._backtesting_end is not None
            ):
                self.log_message(
                    f"--- {self._benchmark_asset} Benchmark Performance ---"
                )

                # Need to adjust the backtesting end date because the data from Yahoo
                # is at the start of the day, so the graph cuts short. This may be needed
                # for other timeframes as well
                backtesting_end_adjusted = self._backtesting_end
                # if self.broker.market == "24/7":
                #     backtesting_end_adjusted = (
                #         self._backtesting_end + datetime.timedelta(days=1)
                #     )

                self._benchmark_returns_df = get_symbol_returns(
                    self._benchmark_asset,
                    self._backtesting_start,
                    backtesting_end_adjusted,
                )

                self._benchmark_analysis = stats_summary(
                    self._benchmark_returns_df, self._risk_free_rate
                )

                total_return = self._benchmark_analysis["total_return"]
                self.log_message(f"Total Return: {total_return*100:,.2f}%")

                cagr_value = self._benchmark_analysis["cagr"]
                self.log_message(f"{self._benchmark_asset} CAGR {cagr_value*100:,.2f}%")

                volatility_value = self._benchmark_analysis["volatility"]
                self.log_message(
                    f"{self._benchmark_asset} Volatility {volatility_value*100:,.2f}%"
                )

                sharpe_value = self._benchmark_analysis["sharpe"]
                self.log_message(f"{self._benchmark_asset} Sharpe {sharpe_value:,.2f}")

                max_drawdown_result = self._benchmark_analysis["max_drawdown"]
                self.log_message(
                    f"{self._benchmark_asset} Max Drawdown {max_drawdown_result['drawdown']*100:,.2f}% on {max_drawdown_result['date']:%Y-%m-%d}"
                )

                romad_value = self._benchmark_analysis["romad"]
                self.log_message(
                    f"{self._benchmark_asset} RoMaD {romad_value*100:,.2f}%"
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
            logging.warning(
                "Cannot plot returns because the strategy returns are missing"
            )
        elif self._benchmark_returns_df is None:
            logging.warning(
                "Cannot plot returns because the benchmark returns are missing"
            )
        else:
            self._strategy_returns_df.to_csv("df-strategy_returns.csv")
            self._benchmark_returns_df.to_csv("df-benchmark_returns.csv")
            plot_returns(
                self._strategy_returns_df,
                f"{self._log_strat_name()}Strategy",
                self._benchmark_returns_df,
                self._benchmark_asset,
                plot_file_html,
                trades_df,
                show_plot,
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
            logging.warning(
                "Cannot create a tearsheet because the strategy returns are missing"
            )
        else:
            strat_name = self._name if self._name != None else "Strategy"
            create_tearsheet(
                self._strategy_returns_df,
                strat_name,
                tearsheet_file,
                self._benchmark_returns_df,
                self._benchmark_asset,
                show_tearsheet,
            )

    @classmethod
    def backtest(
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
        pandas_data=None,
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        starting_positions=None,
        show_plot=True,
        tearsheet_file=None,
        save_tearsheet=True,
        show_tearsheet=True,
        **kwargs,
    ):
        """Backtest a strategy.

        Parameters
        ----------
        datasource_class : class
            The datasource class to use. For example, if you want to use the yahoo finance datasource, then you would pass YahooDataBacktesting as the datasource_class.
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
        benchmark_asset : str
            The benchmark asset to use for the backtest to compare to.
        plot_file_html : str
            The file to write the plot html to.
        trades_file : str
            The file to write the trades to.
        pandas_data : pandas.DataFrame
            The pandas data to use.
        quote_asset : Asset (crypto)
            An Asset object for the crypto currency that will get used
            as a valuation asset for measuring overall porfolio values.
            Usually USDT, USD, USDC.
        show_plot : bool
            Whether or not to show the plot.
        show_tearsheet : bool
            Whether or not to show the tearsheet.
        save_tearsheet : bool
            Whether or not to save the tearsheet.

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
        >>> budget = 10000
        >>> backtest = MyStrategy.backtest(
        >>>     datasource_class=YahooDataBacktesting,
        >>>     backtesting_start=backtesting_start,
        >>>     backtesting_end=backtesting_end,
        >>>     budget=budget,
        >>>     name="MyStrategy",
        >>>     benchmark_asset="QQQ",
        >>> )


        """
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
                f"You are using the old style of initializing a backtest object. \n"
                f"{positional_args_error_message}"
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

        # Filename defaults
        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if plot_file_html is None:
            plot_file_html = (
                f"logs/{name + '_' if name != None else ''}{datestring}.html"
            )
        if stats_file is None:
            stats_file = (
                f"logs/{name + '_' if name != None else ''}{datestring}_stats.csv"
            )
        if trades_file is None:
            trades_file = (
                f"logs/{name + '_' if name != None else ''}{datestring}_trades.csv"
            )
        if logfile is None:
            logfile = f"logs/{name + '_' if name != None else ''}{datestring}_logs.csv"
        if tearsheet_file is None:
            tearsheet_file = (
                f"logs/{name + '_' if name != None else ''}{datestring}_tearsheet.html"
            )
        if not cls.IS_BACKTESTABLE:
            logging.warning(
                f"Strategy {name + ' ' if name != None else ''}cannot be "
                f"backtested at the moment"
            )
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

        trader = Trader(logfile=logfile)
        data_source = datasource_class(
            backtesting_start,
            backtesting_end,
            config=config,
            auto_adjust=auto_adjust,
            pandas_data=pandas_data,
            **kwargs,
        )
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
            **kwargs,
        )
        trader.add_strategy(strategy)

        logger = logging.getLogger("backtest_stats")
        logger.setLevel(logging.INFO)
        logger.info("Starting backtest...")
        start = datetime.datetime.now()

        result = trader.run_all()

        end = datetime.datetime.now()
        backtesting_length = backtesting_end - backtesting_start
        backtesting_run_time = end - start
        logger.info(
            f"Backtest took {backtesting_run_time} for a speed of {backtesting_run_time/backtesting_length:,.3f}"
        )

        backtesting_broker.export_trade_events_to_csv(trades_file)

        strategy.plot_returns_vs_benchmark(
            plot_file_html,
            backtesting_broker._trade_event_log_df,
            show_plot=show_plot,
        )

        strategy.tearsheet(
            save_tearsheet=save_tearsheet,
            tearsheet_file=tearsheet_file,
            show_tearsheet=show_tearsheet,
        )

        return result
