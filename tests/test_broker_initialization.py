"""
Simple test cases for broker initialization error handling.
"""
import pytest
from unittest.mock import patch, MagicMock

from lumibot.strategies import Strategy
from lumibot.entities import Asset


class TestBrokerInitializationSimple:
    """Test cases for broker initialization and error handling."""
    
    def test_strategy_with_none_broker_raises_helpful_error(self):
        """
        Test that when broker is None, a helpful error message is provided
        that explains how to set up environment variables.
        """
        # Mock both the credentials imports in the strategy module
        with patch('lumibot.strategies._strategy.BROKER', None):
            with patch('lumibot.credentials.IS_BACKTESTING', False):
                # Create a minimal strategy class for testing
                class TestStrategy(Strategy):
                    def on_trading_iteration(self):
                        pass
                
                # Attempt to initialize the strategy with None broker
                with pytest.raises(ValueError) as exc_info:
                    TestStrategy(broker=None)
                
                # Check that the error message is helpful and contains key information
                error_message = str(exc_info.value)
                
                # Verify the error message contains helpful guidance
                assert "No broker is set" in error_message
                assert "IS_BACKTESTING" in error_message
                assert ".env file" in error_message
                assert "ALPACA_API_KEY" in error_message
                assert "lumibot.lumiwealth.com" in error_message
                assert "backtesting" in error_message.lower()
                assert "live trading" in error_message.lower()
    
    def test_strategy_with_valid_broker_does_not_raise_broker_error(self):
        """
        Test that when a valid broker is provided, the broker None error is not raised.
        """
        # Create a mock broker with required attributes
        mock_broker = MagicMock()
        mock_broker.name = "test_broker"
        mock_broker.quote_assets = set()
        mock_broker.IS_BACKTESTING_BROKER = True  # Set to True to avoid broker balance updates
        mock_broker.data_source = MagicMock()
        mock_broker.data_source.datetime_start = None
        mock_broker.data_source.datetime_end = None
        
        # Create a minimal strategy class for testing
        class TestStrategy(Strategy):
            def on_trading_iteration(self):
                pass
        
        # This should not raise the broker None error
        # (though it might raise other errors, we're only testing the broker None case)
        try:
            strategy = TestStrategy(broker=mock_broker)
            # If we get here, the broker None error was not raised
            assert strategy.broker == mock_broker
        except ValueError as e:
            # If a ValueError is raised, it should NOT be the broker None error
            error_message = str(e)
            assert "No broker is set" not in error_message, f"Unexpected broker None error: {error_message}"
            # If it's a different ValueError, we can let it pass for this test
        except Exception as e:
            # Other exceptions are acceptable for this test since we're only testing the broker None case
            pass
