import logging
from datetime import datetime as DateTime
from unittest.mock import MagicMock

import pandas as pd

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
            show_indicators=False,  # Disabled to prevent files from opening during tests
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
            show_indicators=False,  # Disabled to prevent files from opening during tests
            save_logfile=False,
            name=strategy_name,
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None


def test_plot_indicators_handles_nan_marker_size(tmp_path, monkeypatch):
    from lumibot.tools.indicators import plot_indicators

    # Build a marker DataFrame with NaN sizes to mirror the failing scenario
    marker_df = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-01 09:30", "2024-01-01 10:30"]),
            "value": [100, 101],
            "plot_name": ["default_plot", "default_plot"],
            "name": ["Test Marker", "Test Marker"],
            "symbol": ["circle", "circle"],
            "size": [float("nan"), float("nan")],
            "color": [None, None],
            "detail_text": [None, None],
        }
    )

    # Avoid opening the browser or writing actual files
    mock_write = MagicMock()
    monkeypatch.setattr("plotly.graph_objects.Figure.write_html", mock_write)

    # Should not raise even when marker size column is NaN-only
    plot_indicators(
        plot_file_html=str(tmp_path / "plot.html"),
        chart_markers_df=marker_df,
        chart_lines_df=None,
        strategy_name="Test",
        show_indicators=True,
    )

    mock_write.assert_called_once()


def _make_strategy_stub():
    strat = Strategy.__new__(Strategy)
    strat._chart_markers_list = []
    strat._chart_lines_list = []
    strat.logger = logging.getLogger("indicator_tests")
    strat.portfolio_value = 1_000
    strat.get_datetime = lambda: DateTime(2024, 1, 1)
    return strat


class TestAddMarkerAndLineGuards:

    def test_add_marker_rejects_nan(self, caplog):
        strat = _make_strategy_stub()
        with caplog.at_level(logging.WARNING):
            result = strat.add_marker("nan_marker", float("nan"))
        assert result is None
        assert strat._chart_markers_list == []
        assert "not finite" in caplog.text

    def test_add_marker_defaults_color(self, caplog):
        strat = _make_strategy_stub()
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            strat.add_marker("bad_color", 10.0, color="not-a-real-color")
        assert strat._chart_markers_list[0]["color"] == "blue"
        assert "Unsupported marker color" in caplog.text
        assert "defaulting to blue" in caplog.text

    def test_add_marker_accepts_css_color(self, caplog):
        strat = _make_strategy_stub()
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            strat.add_marker("css_color", 10.0, color="magenta")
        assert strat._chart_markers_list[0]["color"] == "magenta"
        assert "Unsupported marker color" not in caplog.text

    def test_add_line_rejects_nan(self, caplog):
        strat = _make_strategy_stub()
        with caplog.at_level(logging.WARNING):
            result = strat.add_line("nan_line", float("nan"))
        assert result is None
        assert strat._chart_lines_list == []
        assert "Skipping line" in caplog.text

    def test_add_line_defaults_color(self, caplog):
        strat = _make_strategy_stub()
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            strat.add_line("bad_color", 10.0, color="not-a-real-color")
        assert strat._chart_lines_list[0]["color"] == "blue"
        assert "Unsupported line color" in caplog.text

    def test_add_line_defaults_style(self, caplog):
        strat = _make_strategy_stub()
        with caplog.at_level(logging.WARNING):
            strat.add_line("bad_style", 10.0, color="lightblue", style="dot-dot")
        assert strat._chart_lines_list[0]["style"] == "solid"
        assert "Unsupported line style" in caplog.text
