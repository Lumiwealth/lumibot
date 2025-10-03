"""
Test for BACKTESTING_DATA_SOURCE environment variable handling.
Ensures that datasource_class=None correctly auto-selects from the env var.
"""
import os
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from lumibot.strategies import Strategy
from lumibot.backtesting import (
    PolygonDataBacktesting,
    ThetaDataBacktesting,
    YahooDataBacktesting,
    AlpacaBacktesting,
)


class SimpleTestStrategy(Strategy):
    """Minimal strategy for testing datasource auto-selection."""

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if self.first_iteration:
            # Just buy one share to have some activity
            order = self.create_order("SPY", quantity=1, side="buy")
            self.submit_order(order)


class TestBacktestingDataSourceEnv:
    """Test BACKTESTING_DATA_SOURCE environment variable."""

    def test_auto_select_polygon_case_insensitive(self):
        """Test that BACKTESTING_DATA_SOURCE=polygon (lowercase) selects PolygonDataBacktesting."""
        with patch.dict(os.environ, {'BACKTESTING_DATA_SOURCE': 'polygon'}):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            # Mock the datasource to avoid actual data fetching
            with patch('lumibot.strategies._strategy.PolygonDataBacktesting') as MockPoly:
                mock_data_source = MagicMock()
                MockPoly.return_value = mock_data_source

                backtesting_start = datetime(2023, 1, 1)
                backtesting_end = datetime(2023, 1, 31)

                try:
                    SimpleTestStrategy.run_backtest(
                        None,  # Auto-select from env var
                        backtesting_start=backtesting_start,
                        backtesting_end=backtesting_end,
                        polygon_api_key="test_key",
                        show_plot=False,
                        show_tearsheet=False,
                        show_indicators=False,
                    )
                except:
                    pass  # We expect it to fail, we just want to verify the datasource was selected

                # Verify PolygonDataBacktesting was instantiated
                MockPoly.assert_called_once()

    def test_auto_select_thetadata_case_insensitive(self):
        """Test that BACKTESTING_DATA_SOURCE=THETADATA (uppercase) selects ThetaDataBacktesting."""
        with patch.dict(os.environ, {'BACKTESTING_DATA_SOURCE': 'THETADATA'}):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            # Mock the datasource to avoid actual data fetching
            with patch('lumibot.strategies._strategy.ThetaDataBacktesting') as MockTheta:
                mock_data_source = MagicMock()
                MockTheta.return_value = mock_data_source

                backtesting_start = datetime(2023, 1, 1)
                backtesting_end = datetime(2023, 1, 31)

                try:
                    SimpleTestStrategy.run_backtest(
                        None,  # Auto-select from env var
                        backtesting_start=backtesting_start,
                        backtesting_end=backtesting_end,
                        thetadata_username="test_user",
                        thetadata_password="test_pass",
                        show_plot=False,
                        show_tearsheet=False,
                        show_indicators=False,
                    )
                except:
                    pass  # We expect it to fail, we just want to verify the datasource was selected

                # Verify ThetaDataBacktesting was instantiated
                MockTheta.assert_called_once()

    def test_auto_select_yahoo(self):
        """Test that BACKTESTING_DATA_SOURCE=Yahoo selects YahooDataBacktesting."""
        with patch.dict(os.environ, {'BACKTESTING_DATA_SOURCE': 'Yahoo'}):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            # Mock the datasource to avoid actual data fetching
            with patch('lumibot.strategies._strategy.YahooDataBacktesting') as MockYahoo:
                mock_data_source = MagicMock()
                MockYahoo.return_value = mock_data_source

                backtesting_start = datetime(2023, 1, 1)
                backtesting_end = datetime(2023, 1, 31)

                try:
                    SimpleTestStrategy.run_backtest(
                        None,  # Auto-select from env var
                        backtesting_start=backtesting_start,
                        backtesting_end=backtesting_end,
                        show_plot=False,
                        show_tearsheet=False,
                        show_indicators=False,
                    )
                except:
                    pass  # We expect it to fail, we just want to verify the datasource was selected

                # Verify YahooDataBacktesting was instantiated
                MockYahoo.assert_called_once()

    def test_invalid_data_source_raises_error(self):
        """Test that invalid BACKTESTING_DATA_SOURCE raises ValueError."""
        with patch.dict(os.environ, {'BACKTESTING_DATA_SOURCE': 'InvalidSource'}):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            backtesting_start = datetime(2023, 1, 1)
            backtesting_end = datetime(2023, 1, 31)

            with pytest.raises(ValueError, match="Unknown BACKTESTING_DATA_SOURCE"):
                SimpleTestStrategy.run_backtest(
                    None,  # Auto-select from env var
                    backtesting_start=backtesting_start,
                    backtesting_end=backtesting_end,
                    show_plot=False,
                    show_tearsheet=False,
                    show_indicators=False,
                )

    def test_explicit_datasource_overrides_env(self):
        """Test that explicit datasource_class overrides BACKTESTING_DATA_SOURCE env var."""
        with patch.dict(os.environ, {'BACKTESTING_DATA_SOURCE': 'polygon'}):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            # Mock YahooDataBacktesting to verify it's used despite env var saying polygon
            with patch('lumibot.strategies._strategy.YahooDataBacktesting') as MockYahoo:
                mock_data_source = MagicMock()
                MockYahoo.return_value = mock_data_source

                backtesting_start = datetime(2023, 1, 1)
                backtesting_end = datetime(2023, 1, 31)

                try:
                    SimpleTestStrategy.run_backtest(
                        YahooDataBacktesting,  # Explicit override
                        backtesting_start=backtesting_start,
                        backtesting_end=backtesting_end,
                        show_plot=False,
                        show_tearsheet=False,
                        show_indicators=False,
                    )
                except:
                    pass

                # Verify YahooDataBacktesting was used (not Polygon)
                MockYahoo.assert_called_once()

    def test_default_thetadata_when_no_env_set(self):
        """Test that ThetaData is the default when BACKTESTING_DATA_SOURCE is not set."""
        # Remove BACKTESTING_DATA_SOURCE from env
        env_without_datasource = {k: v for k, v in os.environ.items() if k != 'BACKTESTING_DATA_SOURCE'}

        with patch.dict(os.environ, env_without_datasource, clear=True):
            # Re-import credentials to pick up env change
            from importlib import reload
            import lumibot.credentials
            reload(lumibot.credentials)

            # Mock the datasource to avoid actual data fetching
            with patch('lumibot.strategies._strategy.ThetaDataBacktesting') as MockTheta:
                mock_data_source = MagicMock()
                MockTheta.return_value = mock_data_source

                backtesting_start = datetime(2023, 1, 1)
                backtesting_end = datetime(2023, 1, 31)

                try:
                    SimpleTestStrategy.run_backtest(
                        None,  # Auto-select from env var (should default to ThetaData)
                        backtesting_start=backtesting_start,
                        backtesting_end=backtesting_end,
                        thetadata_username="test_user",
                        thetadata_password="test_pass",
                        show_plot=False,
                        show_tearsheet=False,
                        show_indicators=False,
                    )
                except:
                    pass

                # Verify ThetaDataBacktesting was instantiated (default)
                MockTheta.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
