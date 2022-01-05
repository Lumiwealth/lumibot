import datetime
import logging
from copy import deepcopy

import pandas as pd

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset
from lumibot.tools import (
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
        broker,
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
        filled_order_callback=None,
        name="StratName",
        budget=10000,
        parameters={},
        **kwargs,
    ):
        # Setting the broker object
        self._name = name
        self.broker = broker
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
                pd_asset_keys = dict()
                for asset, df in pandas_data.items():
                    new_asset = self._set_asset_mapping(asset)
                    pd_asset_keys[new_asset] = df
                self.broker._trading_days = self.data_source.load_data(pd_asset_keys)
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
            self._cash = budget
            self._position_value = 0
            self._portfolio_value = budget

        self._minutes_before_closing = minutes_before_closing
        self._minutes_before_opening = minutes_before_opening
        self._sleeptime = sleeptime
        self._executor = StrategyExecutor(self)
        broker._add_subscriber(self._executor)

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
                "_cash",
                "_portfolio_value",
                "_minutes_before_closing",
                "_minutes_before_opening",
                "_sleeptime",
                "_is_backtesting",
            ]:
                result[key[1:]] = deepcopy(self.__dict__[key])

        return result

    def _set_asset_mapping(self, asset):
        if isinstance(asset, Asset):
            return asset
        elif isinstance(asset, str):
            if asset not in self._asset_mapping:
                self._asset_mapping[asset] = Asset(symbol=asset)
            return self._asset_mapping[asset]
        else:
            raise ValueError(
                f"You must enter a symbol string or an asset object. You "
                f"entered {asset}"
            )

    def _log_strat_name(self):
        """Returns the name of the strategy as a string if not default"""
        return f"{self._name} " if self._name != "StratName" else ""

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        if not self.IS_BACKTESTABLE:
            return self.broker._get_balances_at_broker()[2]

        with self._executor.lock:
            portfolio_value = self._cash
            positions = self.broker.get_tracked_positions(self._name)
            assets = [position.asset for position in positions]
            prices = self.data_source.get_last_prices(assets)

            for position in positions:
                asset = position.asset
                quantity = position.quantity
                price = prices.get(asset, 0)
                if self._is_backtesting and price is None:
                    raise ValueError(
                        f"A security as returned a price of none will trying "
                        f"to set the portfolio value. This is usually due to "
                        f"a mixup in futures contract dates and when data is \n"
                        f"actually available. Please ensure data exist for "
                        f"{self.broker.datetime}. The security that has missing data is: \n"
                        f"symbol: {asset.symbol}, \n"
                        f"type: {asset.asset_type}, \n"
                        f"right: {asset.right}, \n"
                        f"expiration: {asset.expiration}, \n"
                        f"strike: {asset.strike}.\n"
                    )
                multiplier = (
                    asset.multiplier if asset.asset_type in ["option", "future"] else 1
                )
                portfolio_value += quantity * price * multiplier

            self._portfolio_value = portfolio_value

        return portfolio_value

    def _update_cash(self, side, quantity, price, multiplier):
        """update the self.cash"""
        with self._executor.lock:
            if side == "buy":
                self._cash -= quantity * price * multiplier
            if side == "sell":
                self._cash += quantity * price * multiplier
            return self._cash

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
                self._cash += dividend_per_share * quantity
            return self._cash

    # =============Stats functions=====================

    def _append_row(self, row):
        self._stats = self._stats.append(row, ignore_index=True)

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
            self.log_message(
                f"Max Drawdown {max_drawdown_result['drawdown']*100:,.2f}% on {max_drawdown_result['date']:%Y-%m-%d}"
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

                self._benchmark_returns_df = get_symbol_returns(
                    self._benchmark_asset,
                    self._backtesting_start,
                    self._backtesting_end,
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
        plot_file="backtest_result.jpg",
        plot_file_html="backtest_result.html",
        trades_df=None,
        show_plot=True,
    ):
        if self._strategy_returns_df is None:
            logging.warning(
                "Cannot plot returns because the strategy returns are missing"
            )
        elif self._benchmark_returns_df is None:
            logging.warning(
                "Cannot plot returns because the benchmark returns are missing"
            )
        else:
            plot_returns(
                self._strategy_returns_df,
                f"{self._log_strat_name()}Strategy",
                self._benchmark_returns_df,
                self._benchmark_asset,
                plot_file,
                plot_file_html,
                trades_df,
                show_plot,
            )

    @classmethod
    def backtest(
        cls,
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
        name="StratName",
        budget=10000,
        benchmark_asset="SPY",
        plot_file=None,
        plot_file_html=None,
        trades_file=None,
        pandas_data=None,
        show_plot=True,
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
            The config to use.
        auto_adjust : bool
            Whether or not to automatically adjust the strategy.
        name : str
            The name of the strategy.
        budget : float
            The initial budget to use for the backtest.
        benchmark_asset : str
            The benchmark asset to use for the backtest to compare to.
        plot_file : str
            The file to write the plot to.
        plot_file_html : str
            The file to write the plot html to.
        trades_file : str
            The file to write the trades to.
        pandas_data : pandas.DataFrame
            The pandas data to use.
        show_plot : bool
            Whether or not to show the plot.

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
        # Filename defaults
        datestring = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        if plot_file is None:
            plot_file = (
                f"logs/{name + '_' if name != 'StratName' else ''}{datestring}.jpg"
            )
        if plot_file_html is None:
            plot_file_html = (
                f"logs/{name + '_' if name != 'StratName' else ''}{datestring}.html"
            )
        if stats_file is None:
            stats_file = f"logs/{name + '_' if name != 'StratName' else ''}{datestring}_stats.csv"
        if trades_file is None:
            trades_file = f"logs/{name + '_' if name != 'StratName' else ''}{datestring}_trades.csv"
        if logfile is None:
            logfile = (
                f"logs/{name + '_' if name != 'StratName' else ''}{datestring}_logs.csv"
            )

        if not cls.IS_BACKTESTABLE:
            logging.warning(
                f"Strategy {name + ' ' if name != 'StratName' else ''}cannot be "
                f"backtested at the moment"
            )
            return None

        backtesting_start = to_datetime_aware(backtesting_start)
        backtesting_end = to_datetime_aware(backtesting_end)

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
            plot_file,
            plot_file_html,
            backtesting_broker._trade_event_log_df,
            show_plot=show_plot,
        )

        return result
