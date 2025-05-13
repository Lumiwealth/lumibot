import logging
from datetime import datetime as DateTime

from lumibot.backtesting import PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset

from tests.fixtures import pandas_data_fixture


logger = logging.getLogger(__name__)


class TestDefaultIndicatorStrategy(Strategy):
    """
    A strategy that adds the closing prices of each asset to a line.
    """
    __test__ = False

    def initialize(self):
        self.sleeptime = "1D"
        self.set_market("24/7")

        # Define the assets we want to track (SPY, TLT, GLD)
        self.assets = [
            Asset(symbol="SPY", asset_type="stock"),
            Asset(symbol="TLT", asset_type="stock"),
            Asset(symbol="GLD", asset_type="stock")
        ]


    def on_trading_iteration(self):
        # Get the current datetime
        dt = self.get_datetime()

        # For each asset, get the closing price and add it to a line
        for asset in self.assets:
            # Get the latest price data for the asset
            price_data = self.get_last_price(asset)

            # Add the closing price to a line
            if price_data is not None:
                close_price = price_data
                self.add_line(
                    name=f"{asset.symbol} Close",
                    value=close_price,
                    dt=dt
                )

                self.add_line(
                    name=f"{asset.symbol} Close price 2",
                    value=close_price,
                    dt=dt
                )

                # Add a green up triangle on Mondays
                if dt.weekday() == 0:  # Monday is 0
                    self.add_marker(
                        name="buy",
                        value=close_price,
                        color="green",
                        symbol="triangle-up",
                        dt=dt,
                    )

                # Add a red upside-down triangle on Fridays
                if dt.weekday() == 4:  # Friday is 4
                    self.add_marker(
                        name="sell",
                        value=close_price,
                        color="red",
                        symbol="triangle-down",
                        dt=dt,
                    )


class TestIndicatorStrategy(Strategy):
    """
    A strategy that adds the closing prices of each asset to a line.
    """
    __test__ = False

    def initialize(self):
        self.sleeptime = "1D"
        self.set_market("24/7")

        # Define the assets we want to track (SPY, TLT, GLD)
        self.assets = [
            Asset(symbol="SPY", asset_type="stock"),
            Asset(symbol="TLT", asset_type="stock"),
            Asset(symbol="GLD", asset_type="stock")
        ]

        self.colors = {
            "SPY": "lightblue",
            "TLT": "pink",
            "GLD": "yellow"
        }

    def on_trading_iteration(self):
        # Get the current datetime
        dt = self.get_datetime()

        # For each asset, get the closing price and add it to a line
        for asset in self.assets:
            # Get the latest price data for the asset
            price_data = self.get_last_price(asset)

            # Add the closing price to a line
            if price_data is not None:
                close_price = price_data
                self.add_line(
                    name=f"{asset.symbol} Close",
                    value=close_price,
                    color=self.colors[asset.symbol],
                    dt=dt,
                    plot_name=asset.symbol
                )

                self.add_line(
                    name=f"{asset.symbol} Close price 2",
                    value=close_price,
                    color=self.colors[asset.symbol],
                    dt=dt,
                    plot_name=f"{asset.symbol} line 2"
                )

                # Add a green up triangle on Mondays
                if dt.weekday() == 0:  # Monday is 0
                    self.add_marker(
                        name="buy",
                        value=close_price,
                        color="green",
                        symbol="triangle-up",
                        dt=dt,
                        plot_name=asset.symbol
                    )

                # Add a red upside-down triangle on Fridays
                if dt.weekday() == 4:  # Friday is 4
                    self.add_marker(
                        name="sell",
                        value=close_price,
                        color="red",
                        symbol="triangle-down",
                        dt=dt,
                        plot_name=asset.symbol
                    )


class TestIndicators:

    def test_default_lines(self, pandas_data_fixture):
        """Test the default behavior (unnamed lines)"""
        strategy_name = "TestDefaultIndicatorStrategy"
        strategy_class = TestDefaultIndicatorStrategy
        backtesting_start = DateTime(2019, 1, 2)
        backtesting_end = DateTime(2019, 3, 1)

        result = strategy_class.backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=True,  # This is set to True as per the requirement
            save_logfile=False,
            name=strategy_name,
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None

    def test_named_lines(self, pandas_data_fixture):
        """Test the named lines"""
        strategy_name = "TestIndicatorStrategy"
        strategy_class = TestIndicatorStrategy
        backtesting_start = DateTime(2019, 1, 2)
        backtesting_end = DateTime(2019, 3, 1)

        result = strategy_class.backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=True,  # This is set to True as per the requirement
            save_logfile=False,
            name=strategy_name,
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None


