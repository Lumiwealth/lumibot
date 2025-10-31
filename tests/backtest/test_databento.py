import datetime
import pytest
import pytz
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.databento_backtesting_pandas import (
    DataBentoDataBacktestingPandas,
)
from lumibot.backtesting.databento_backtesting_polars import (
    DataBentoDataBacktestingPolars,
)
from lumibot.tools.databento_helper import DataBentoAuthenticationError
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")




def test_databento_auth_failure_propagates(monkeypatch):
    start = datetime.datetime(2025, 1, 6, tzinfo=pytz.UTC)
    end = datetime.datetime(2025, 1, 7, tzinfo=pytz.UTC)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    def boom(*args, **kwargs):
        raise DataBentoAuthenticationError("401 auth_authentication_failed")

    monkeypatch.setattr(
        "lumibot.tools.databento_helper.get_price_data_from_databento",
        boom,
    )

    data_source = DataBentoDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        api_key="dummy",
        show_progress_bar=False,
    )

    with pytest.raises(DataBentoAuthenticationError):
        data_source.get_historical_prices(asset, length=1, timestep="minute")

class SimpleContinuousFutures(Strategy):
    """Simple strategy for testing continuous futures with minute-level data"""

    def initialize(self):
        self.sleeptime = "1M"  # Trade every minute
        self.set_market("us_futures")
        self.prices = []
        self.times = []

    def on_trading_iteration(self):
        # Create continuous futures asset
        asset = Asset(
            symbol="ES",
            asset_type="cont_future",
        )

        # Get current price and time
        price = self.get_last_price(asset)
        dt = self.get_datetime()

        self.prices.append(price)
        self.times.append(dt)

        # Only trade on first iteration
        if self.first_iteration:
            order = self.create_order(asset, 1, "buy")
            self.submit_order(order)


class TestDatabentoBacktestFull:
    """Test suite for Databento data source with continuous futures"""

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY,
        reason="This test requires a Databento API key"
    )
    @pytest.mark.skipif(
        DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_databento_continuous_futures_minute_data(self):
        """
        Test Databento with continuous futures (ES) using minute-level data.
        Tests a 2-day period in 2025 to verify minute-level cadence works correctly.
        """
        # Use timezone-aware datetimes for futures trading
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2025, 1, 2, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2025, 1, 3, 16, 0))

        data_source = DataBentoDataBacktestingPandas(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)

        strat_obj = SimpleContinuousFutures(
            broker=broker,
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(
            show_plot=False,
            show_tearsheet=False,
            show_indicators=False,
            save_tearsheet=False
        )

        # Verify results
        assert results is not None
        assert len(strat_obj.prices) > 0, "Expected to collect some prices"
        assert len(strat_obj.times) > 0, "Expected to collect some timestamps"

        # Verify minute-level cadence (should have many data points over 2 days)
        # With minute data from 9:30 to 16:00 (6.5 hours = 390 minutes per day)
        # Over 2 days we should have roughly 780 minutes of trading
        assert len(strat_obj.prices) > 100, f"Expected many minute-level data points, got {len(strat_obj.prices)}"

        # Verify all prices are valid numbers
        for price in strat_obj.prices:
            assert price is not None and price > 0, f"Expected valid price, got {price}"

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY,
        reason="This test requires a Databento API key"
    )
    @pytest.mark.skipif(
        DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_databento_continuous_futures_minute_data_polars(self):
        """
        Test Databento with Polars implementation - minute-level data.
        Should be significantly faster than pandas version.
        """
        # Use timezone-aware datetimes for futures trading
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2025, 1, 2, 9, 30))
        backtesting_end = tzinfo.localize(datetime.datetime(2025, 1, 3, 16, 0))

        data_source = DataBentoDataBacktestingPolars(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)

        strat_obj = SimpleContinuousFutures(
            broker=broker,
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(
            show_plot=False,
            show_tearsheet=False,
            show_indicators=False,
            save_tearsheet=False
        )

        # Verify results
        assert results is not None
        assert len(strat_obj.prices) > 0, "Expected to collect some prices"
        assert len(strat_obj.times) > 0, "Expected to collect some timestamps"

        # Verify minute-level cadence
        assert len(strat_obj.prices) > 100, f"Expected many minute-level data points, got {len(strat_obj.prices)}"

        # Verify all prices are valid numbers
        for price in strat_obj.prices:
            assert price is not None and price > 0, f"Expected valid price, got {price}"

    @pytest.mark.apitest
    @pytest.mark.skipif(
        not DATABENTO_API_KEY,
        reason="This test requires a Databento API key"
    )
    @pytest.mark.skipif(
        DATABENTO_API_KEY == '<your key here>',
        reason="This test requires a Databento API key"
    )
    def test_databento_daily_continuous_futures(self):
        """
        Test Databento with continuous futures using daily data over a longer period.
        This is similar to the profiling test but as a permanent test.
        """
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2025, 1, 2))
        backtesting_end = tzinfo.localize(datetime.datetime(2025, 3, 31))

        # Simple daily strategy
        class DailyContinuousFutures(Strategy):
            def initialize(self):
                self.sleeptime = "1D"
                self.set_market("us_futures")

            def on_trading_iteration(self):
                if self.first_iteration:
                    asset = Asset(symbol="ES", asset_type="cont_future")
                    order = self.create_order(asset, 1, "buy")
                    self.submit_order(order)

        data_source = DataBentoDataBacktestingPandas(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            api_key=DATABENTO_API_KEY,
        )

        broker = BacktestingBroker(data_source=data_source)
        strat_obj = DailyContinuousFutures(broker=broker)
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)

        results = trader.run_all(
            show_plot=False,
            show_tearsheet=False,
            show_indicators=False,
            save_tearsheet=False
        )

        # Verify results
        assert results is not None
        # Should have around 88 trading days
        assert strat_obj.broker.datetime == backtesting_end or \
               (backtesting_end - strat_obj.broker.datetime).days <= 1
