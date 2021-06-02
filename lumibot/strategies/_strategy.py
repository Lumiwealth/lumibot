import logging
from copy import deepcopy

import pandas as pd

from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset
from lumibot.tools import day_deduplicate, get_risk_free_rate, stats_summary
from lumibot.traders import Trader

from .strategy_executor import StrategyExecutor


class _Strategy:
    IS_BACKTESTABLE = True

    def __init__(
        self,
        name,
        budget,
        broker,
        data_source=None,
        minutes_before_closing=5,
        minutes_before_opening=60,
        sleeptime=1,
        stats_file=None,
        risk_free_rate=None,
        **kwargs,
    ):
        # Setting the broker object
        self._name = name
        self.broker = broker
        self._is_backtesting = self.broker.IS_BACKTESTING_BROKER
        if self._is_backtesting and not self.IS_BACKTESTABLE:
            raise Exception(
                "Strategy %s cannot be backtested for the moment" % self._name
            )

        # Setting the data provider
        if self._is_backtesting:
            self.data_source = self.broker._data_source
        elif data_source is None:
            self.data_source = self.broker
        else:
            self.data_source = data_source

        # Setting execution parameters
        self._first_iteration = True
        self._initial_budget = budget
        self._unspent_money = budget
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
        if risk_free_rate is None:
            # Get risk free rate from US Treasuries by default
            self._risk_free_rate = get_risk_free_rate()
        else:
            self._risk_free_rate = risk_free_rate

        # Storing parameters for the initialize method
        self._parameters = kwargs

        # Hold the asset objects for strings for stocks only.
        self._asset_mapping = dict()

    # =============Internal functions===================

    def _copy_dict(self):
        result = {}
        ignored_fields = ["broker", "data_source"]
        for key in self.__dict__:
            if key[0] != "_" and key not in ignored_fields:
                try:
                    result[key] = deepcopy(self.__dict__[key])
                except:
                    logging.warning(
                        "Cannot perform deepcopy on %r" % self.__dict__[key]
                    )
            elif key in [
                "_name",
                "_initial_budget",
                "_unspent_money",
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
                self._asset_mapping[asset] = Asset(asset)
            return self._asset_mapping[asset]
        else:
            raise ValueError(
                f"You must enter a symbol string or an asset object. You "
                f"entered {asset}"
            )

    # =============Auto updating functions=============

    def _update_portfolio_value(self):
        """updates self.portfolio_value"""
        with self._executor.lock:
            portfolio_value = self._unspent_money
            positions = self.broker.get_tracked_positions(self._name)
            assets = [position.asset for position in positions]
            prices = self.data_source.get_last_prices(assets)

            for position in positions:
                asset = position.asset
                quantity = position.quantity
                price = prices.get(asset, 0)
                portfolio_value += quantity * price

            self._portfolio_value = portfolio_value
            self.log_message(f"Porfolio value of {round(portfolio_value, 2)}")

        return portfolio_value

    def _update_unspent_money(self, side, quantity, price):
        """update the self.unspent_money"""
        with self._executor.lock:
            if side == "buy":
                self._unspent_money -= quantity * price
            if side == "sell":
                self._unspent_money += quantity * price
            self._unspent_money = self._unspent_money
            return self._unspent_money

    def _update_unspent_money_with_dividends(self):
        with self._executor.lock:
            positions = self.broker.get_tracked_positions(self._name)
            assets = [position.asset for position in positions]
            dividends_per_share = self.get_yesterday_dividends(assets)
            for position in positions:
                asset = position.asset
                quantity = position.quantity
                dividend_per_share = dividends_per_share.get(asset, 0)
                self._unspent_money += dividend_per_share * quantity
            return self._unspent_money

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
        logger.setLevel(logging.INFO)
        if not self._stats.empty:
            self._format_stats()
            if self._stats_file:
                self._stats.to_csv(self._stats_file)

            df_ = day_deduplicate(self._stats)
            self._analysis = stats_summary(df_, self._risk_free_rate)

            cagr_value = self._analysis["cagr"]
            self.log_message(f"CAGR {round(100 * cagr_value, 2)}%")

            volatility_value = self._analysis["volatility"]
            self.log_message(f"Volatility {round(100 * volatility_value, 2)}%")

            sharpe_value = self._analysis["sharpe"]
            self.log_message(f"Sharpe {round(sharpe_value, 2)}")

            max_drawdown_result = self._analysis["max_drawdown"]
            self.log_message(
                f"Max Drawdown {round(100 * max_drawdown_result['drawdown'], 2)}% on {max_drawdown_result['date']:%Y-%m-%d}"
            )

            romad_value = self._analysis["romad"]
            self.log_message(f"RoMaD {round(100 * romad_value, 2)}%")

        logger.setLevel(current_level)

    @classmethod
    def backtest(
        cls,
        name,
        budget,
        datasource_class,
        backtesting_start,
        backtesting_end,
        minutes_before_closing=5,
        minutes_before_opening=60,
        sleeptime=1,
        stats_file=None,
        risk_free_rate=None,
        logfile="logs/test.log",
        config=None,
        auto_adjust=False,
        **kwargs,
    ):
        trader = Trader(logfile=logfile)
        data_source = datasource_class(
            backtesting_start, backtesting_end, config=config, auto_adjust=auto_adjust
        )
        backtesting_broker = BacktestingBroker(data_source)
        strategy = cls(
            name,
            budget,
            backtesting_broker,
            minutes_before_closing=minutes_before_closing,
            minutes_before_opening=minutes_before_opening,
            sleeptime=sleeptime,
            risk_free_rate=risk_free_rate,
            stats_file=stats_file,
            **kwargs,
        )
        trader.add_strategy(strategy)
        result = trader.run_all()
        return result
