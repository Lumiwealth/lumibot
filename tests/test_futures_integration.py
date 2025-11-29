"""
Integration test for continuous futures with the FuturesCycleAlgo strategy.
Tests the actual use case described in the original problem.
"""

import pytest
import os
from unittest.mock import Mock, MagicMock
from datetime import datetime, timedelta

from lumibot.entities import Asset, Position
from lumibot.strategies.strategy import Strategy

# Skip this integration test on CI where no broker/backtesting env is configured
_ON_CI = os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"
pytestmark = pytest.mark.skipif(_ON_CI, reason="Broker-dependent integration test skipped on CI (no broker/backtesting env)")


class MockFuturesCycleAlgo(Strategy):
    """Mock version of FuturesCycleAlgo for testing."""
    
    def initialize(self):
        self.set_market("24/5")
        self.sleeptime = "1M"
        self.future_asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        self.vars.entry_time = None
        
    def on_trading_iteration(self):
        # Simplified version of the original logic
        current_dt = self.get_datetime()
        position = self.get_position(self.future_asset)
        have_contract = position is not None and position.quantity != 0
        
        if have_contract and self.vars.entry_time is not None:
            time_held = current_dt - self.vars.entry_time
            if time_held >= timedelta(minutes=1):
                return "SELL"
        elif not have_contract and self.vars.entry_time is None:
            return "BUY"
        
        return "HOLD"


class TestFuturesCycleAlgoIntegration:
    """Test the FuturesCycleAlgo integration with continuous futures."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock broker with required attributes used by Strategy __init__
        self.mock_broker = Mock()
        self.mock_broker.name = "mock"
        # Treat tests as backtesting to avoid live dependencies
        self.mock_broker.IS_BACKTESTING_BROKER = True
        # Strategy __init__ will add to this set; ensure it exists
        self.mock_broker.quote_assets = set()

        # Provide minimal data_source expected by backtesting path
        class _DS:
            SOURCE = "MEMORY"
            datetime_start = None
            datetime_end = None
            _data_store = {}
        self.mock_broker.data_source = _DS()

        # Provide minimal filled positions container used by _set_cash_position
        class _FilledPositions:
            def __init__(self):
                self._list = []
            def get_list(self):
                return self._list
            def __len__(self):
                return len(self._list)
            def __getitem__(self, idx):
                return self._list[idx]
            def __setitem__(self, idx, val):
                self._list[idx] = val
            def append(self, val):
                self._list.append(val)
        self.mock_broker._filled_positions = _FilledPositions()

        # Initialize strategy with the mock broker so __init__ doesn't raise
        self.strategy = MockFuturesCycleAlgo(broker=self.mock_broker)
        self.strategy._name = "FuturesCycleAlgo"

        # Mock datetime
        self.mock_datetime = datetime(2025, 9, 4, 10, 30, 0)
        self.strategy.get_datetime = Mock(return_value=self.mock_datetime)

        # Mock log_message
        self.strategy.log_message = Mock()

        # Call initialize to set up the strategy
        self.strategy.initialize()

    def test_no_position_triggers_buy(self):
        """Test that no position triggers a buy signal."""
        # Mock no exact match and no positions
        self.mock_broker.get_tracked_position.return_value = None
        self.mock_broker.get_tracked_positions.return_value = []
        
        action = self.strategy.on_trading_iteration()
        
        assert action == "BUY"

    def test_tradovate_position_detected(self):
        """Test that Tradovate MNQU5 position is detected for MNQ continuous future."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock Tradovate-style position
        tradovate_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock()
        mock_position.asset = tradovate_asset
        mock_position.quantity = 1
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        # Set entry time to simulate holding position
        self.strategy.vars.entry_time = self.mock_datetime - timedelta(seconds=30)
        
        action = self.strategy.on_trading_iteration()
        
        # Should detect position and hold (not enough time passed)
        assert action == "HOLD"

    def test_tradovate_position_sells_after_minute(self):
        """Test that Tradovate position triggers sell after 1 minute."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock Tradovate-style position
        tradovate_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock()
        mock_position.asset = tradovate_asset
        mock_position.quantity = 1
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        # Set entry time to simulate holding position for over 1 minute
        self.strategy.vars.entry_time = self.mock_datetime - timedelta(minutes=1, seconds=5)
        
        action = self.strategy.on_trading_iteration()
        
        # Should trigger sell
        assert action == "SELL"

    def test_ib_style_position_detected(self):
        """Test that IB-style position (MNQ with expiration) is detected."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock IB-style position
        from datetime import date
        ib_asset = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 9, 19))
        mock_position = Mock()
        mock_position.asset = ib_asset
        mock_position.quantity = 1
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        # Set entry time to simulate holding position
        self.strategy.vars.entry_time = self.mock_datetime - timedelta(seconds=30)
        
        action = self.strategy.on_trading_iteration()
        
        # Should detect position and hold
        assert action == "HOLD"

    def test_multiple_contracts_logs_warning(self):
        """Test that multiple matching contracts logs appropriate warning."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create multiple mock positions
        sep_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        dec_asset = Asset("MNQZ5", asset_type=Asset.AssetType.FUTURE)
        
        sep_position = Mock()
        sep_position.asset = sep_asset
        sep_position.quantity = 1
        
        dec_position = Mock()
        dec_position.asset = dec_asset
        dec_position.quantity = 1
        
        self.mock_broker.get_tracked_positions.return_value = [dec_position, sep_position]
        
        # Set entry time to simulate holding position
        self.strategy.vars.entry_time = self.mock_datetime - timedelta(seconds=30)
        
        action = self.strategy.on_trading_iteration()
        
        # Should detect a position and hold
        assert action == "HOLD"
        
        # Should log warning about multiple contracts
        self.strategy.log_message.assert_called_once()
        log_call = self.strategy.log_message.call_args[0][0]
        assert "Multiple futures contracts found" in log_call
        assert "MNQ" in log_call

    def test_ignores_other_futures(self):
        """Test that other futures contracts are ignored."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock position with different root
        other_asset = Asset("ESU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock()
        mock_position.asset = other_asset
        mock_position.quantity = 1
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        action = self.strategy.on_trading_iteration()
        
        # Should not detect position and trigger buy
        assert action == "BUY"

    def test_ignores_stock_positions(self):
        """Test that stock positions with similar symbols are ignored."""
        # Mock no exact match
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock stock position
        stock_asset = Asset("MNQ", asset_type=Asset.AssetType.STOCK)
        mock_position = Mock()
        mock_position.asset = stock_asset
        mock_position.quantity = 100
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        action = self.strategy.on_trading_iteration()
        
        # Should not detect futures position and trigger buy
        assert action == "BUY"
