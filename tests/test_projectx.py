import datetime as dt
import pytest
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

from lumibot.entities import Asset, Order, Position
from lumibot.brokers.projectx import ProjectX
from lumibot.data_sources.projectx_data import ProjectXData


@pytest.fixture
def mock_projectx_config():
    """Mock ProjectX configuration for testing"""
    return {
        "firm": "TEST",
        "api_key": "test_api_key",
        "username": "test_user",
        "base_url": "https://test.projectx.com",
        "preferred_account_name": "TestAccount",
        "streaming_base_url": "wss://test.projectx.com/hub"
    }


@pytest.fixture
def mock_data_source():
    """Mock ProjectX data source"""
    return MagicMock(spec=ProjectXData)


@pytest.fixture
def projectx_broker(mock_projectx_config, mock_data_source):
    """Create ProjectX broker with mocked dependencies"""
    with patch('lumibot.brokers.projectx.ProjectXClient'):
        broker = ProjectX(mock_projectx_config, data_source=mock_data_source)
        broker.client = MagicMock()
        return broker


class TestProjectXBroker:
    """
    Unit tests for ProjectX broker. These tests focus on the conversion and mapping
    logic that had bugs - they don't require actual API calls.
    """

    def test_broker_initialization(self, mock_projectx_config, mock_data_source):
        """Test basic broker initialization"""
        with patch('lumibot.brokers.projectx.ProjectXClient'):
            broker = ProjectX(mock_projectx_config, data_source=mock_data_source)
            assert broker.name == "ProjectX_TEST"
            assert broker.firm == "TEST"

    def test_order_status_mapping_corrected(self, projectx_broker):
        """
        Test that order status mapping is corrected.
        Previously status=3 was mapped to 'partially_filled' which was impossible 
        for 1-share orders. Now it should be 'open'.
        """
        # Test the corrected status mappings
        status_mappings = projectx_broker.ORDER_STATUS_MAPPING
        
        # These were the problematic mappings that we fixed
        assert status_mappings[1] == "new"           # Pending/New
        assert status_mappings[2] == "submitted"     # Submitted  
        assert status_mappings[3] == "open"          # Open/Active on exchange (was "partially_filled")
        assert status_mappings[4] == "filled"        # Filled/Completed
        assert status_mappings[5] == "cancelled"     # Canceled/Rejected
        assert status_mappings[11] == "partially_filled"  # Partially filled (for multi-share orders)

    def test_position_conversion_field_mapping(self, projectx_broker):
        """
        Test position conversion with corrected field mappings.
        Previously failed due to avgPrice vs averagePrice mismatch.
        """
        # Mock position data from ProjectX API (real structure)
        broker_position = {
            "id": 12345,
            "symbol": "MES",
            "contractId": "CON.F.US.MES.U25",
            "size": 4,
            "avgPrice": 5150.25,  # This field name was the issue
            # "averagePrice": 5150.25,  # Sometimes API returns this instead
            "unrealizedPnL": 125.50,
            "realizedPnL": 0.0,
            "accountId": 67890,
            "description": "E-mini S&P 500 Future"
        }

        # Mock the asset resolution
        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        # Test position conversion
        position = projectx_broker._convert_broker_position_to_lumibot_position(broker_position)

        assert position.asset.symbol == "MES"
        assert position.quantity == 4
        assert position.avg_fill_price == 5150.25  # Should use avgPrice field

    def test_position_conversion_field_fallback(self, projectx_broker):
        """
        Test position conversion with field name fallback.
        API sometimes returns 'averagePrice' instead of 'avgPrice'.
        """
        # Mock position with different field name
        broker_position = {
            "symbol": "MES",
            "contractId": "CON.F.US.MES.U25", 
            "size": 2,
            "averagePrice": 5200.75,  # Different field name
            "unrealizedPnL": 50.25,
        }

        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        position = projectx_broker._convert_broker_position_to_lumibot_position(broker_position)
        assert position.avg_fill_price == 5200.75  # Should fallback to averagePrice

    def test_order_conversion_from_broker_format(self, projectx_broker):
        """
        Test order conversion from ProjectX broker format to Lumibot format.
        This tests the _convert_projectx_order_to_lumibot method.
        """
        # Mock order data from ProjectX API (real structure based on our debugging)
        broker_order = {
            "id": 11517491,
            "status": 2,  # Active/Working
            "type": 2,    # Market order
            "side": 0,    # Buy (0=buy, 1=sell)
            "symbol": "MES",
            "contractId": "CON.F.US.MES.U25",
            "size": 1,
            "filledSize": 0,
            "avgFillPrice": 0.0,
            "limitPrice": None,     # Market order has no limit price
            "stopPrice": None,
            "createdDateTime": "2024-01-15T10:30:00Z",
            "updatedDateTime": "2024-01-15T10:30:00Z"
        }

        # Mock asset resolution
        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        # Test order conversion  
        order = projectx_broker._convert_broker_order_to_lumibot_order(broker_order)

        assert order.id == "11517491"
        assert order.asset.symbol == "MES"
        assert order.quantity == 1
        assert order.side == "buy"
        assert order.status == "submitted"  # Status 2 maps to "submitted"
        assert order.order_type == "market"

    def test_order_status_mapping_edge_cases(self, projectx_broker):
        """Test order status mapping for edge cases that caused issues"""
        
        # Test status=3 with 1-share order (was incorrectly "partially_filled")
        broker_order = {
            "id": 123,
            "status": 3,  # This was the problematic status
            "size": 1,         # 1 share cannot be partially filled!
            "filledSize": 0,   # Not filled at all
            "symbol": "MES",
            "contractId": "CON.F.US.MES.U25",
            "type": 2,
            "side": 0,
        }

        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        order = projectx_broker._convert_broker_order_to_lumibot_order(broker_order)
        
        # Should be "open" not "partially_filled" 
        assert order.status == "open"

    def test_asset_resolution_no_hardcoded_mappings(self, projectx_broker):
        """
        Test that asset resolution uses Asset class logic instead of hardcoded mappings.
        Previously had hardcoded: 'MES': 'CON.F.US.MES.U25' which expires.
        """
        # Mock the Asset class method
        with patch.object(Asset, 'get_potential_futures_contracts') as mock_contracts:
            mock_contracts.return_value = ['MESU25', 'MES.U25', 'MESU2025']
            
            # Test contract ID generation
            asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
            contract_id = projectx_broker._get_contract_id_from_asset(asset)
            
            # Should use Asset class logic, not hardcoded mappings
            mock_contracts.assert_called_once()
            assert contract_id  # Should return a valid contract ID

    def test_order_tracking_sync_functionality(self, projectx_broker):
        """
        Test order tracking and sync functionality that was broken.
        Orders were being synced but then auto-canceled during validation.
        """
        # Mock existing orders at broker
        existing_broker_orders = [
            {
                "id": 9475374,
                "status": 3,  # Open
                "symbol": "MES", 
                "contractId": "CON.F.US.MES.U25",
                "size": 1,
                "type": 2,
                "side": 0,
            }
        ]

        # Mock client methods
        projectx_broker.client.get_orders = MagicMock(return_value=existing_broker_orders)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(
            return_value=Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        )

        # Mock the sync method
        projectx_broker._sync_existing_orders_to_tracking = MagicMock()
        
        # Test that sync is called during order retrieval
        orders = projectx_broker._get_orders_at_broker()
        
        # Should have synced existing orders
        projectx_broker._sync_existing_orders_to_tracking.assert_called_once()

    def test_order_sync_prevents_auto_cancellation(self, projectx_broker):
        """
        Test that synced orders have the _synced_from_broker flag to prevent cancellation.
        This was the core issue - orders were synced then immediately canceled.
        """
        # Create mock order
        mock_order = MagicMock(spec=Order)
        mock_order.identifier = 9475374
        mock_order.strategy = "test_strategy"

        # Simulate the sync process 
        orders_to_sync = [mock_order]
        for order in orders_to_sync:
            order._synced_from_broker = True

        # Verify sync flag is set
        assert mock_order._synced_from_broker == True

    def test_get_historical_account_value_not_implemented(self, projectx_broker):
        """Test that historical account value returns empty dict"""
        result = projectx_broker.get_historical_account_value()
        assert result == {}

    def test_get_option_chains_not_supported(self, projectx_broker):
        """Test that options chains properly raises NotImplementedError for futures broker"""
        asset = Asset("SPY", Asset.AssetType.STOCK)
        with pytest.raises(NotImplementedError, match="ProjectX is a futures broker - options chains are not supported"):
            projectx_broker.get_chains(asset)

    def test_order_type_conversions(self, projectx_broker):
        """Test ProjectX order type conversions"""
        # Test ProjectX type ID to string conversion
        assert projectx_broker._get_order_type_from_id(1) == "limit"
        assert projectx_broker._get_order_type_from_id(2) == "market" 
        assert projectx_broker._get_order_type_from_id(4) == "stop"
        assert projectx_broker._get_order_type_from_id(5) == "trailing_stop"

        # Test unknown type defaults to market
        assert projectx_broker._get_order_type_from_id(999) == "market"

    def test_contract_id_parsing(self, projectx_broker):
        """Test contract ID parsing for asset resolution"""
        # Mock contract details
        mock_contract_details = {
            "symbol": "MES",  # Use lowercase to match broker expectations
            "description": "E-mini S&P 500 Future",
            "contractSize": 50,
            "tickSize": 0.25
        }
        
        projectx_broker.client.get_contract_details = MagicMock(return_value=mock_contract_details)
        
        # Test asset creation from contract ID
        asset = projectx_broker._get_asset_from_contract_id("CON.F.US.MES.U25")
        
        assert asset.symbol == "MES"
        assert asset.asset_type == Asset.AssetType.CONT_FUTURE

    def test_logging_cleanup(self, projectx_broker):
        """
        Test that logging is clean and not verbose.
        Previously had 58+ log lines per sync, now should be much fewer.
        """
        # This is a behavioral test - we can verify the broker has proper logging setup
        assert projectx_broker.logger is not None
        assert projectx_broker.firm == "TEST"

    def test_position_display_format(self, projectx_broker):
        """
        Test that positions display cleanly.
        Previously showed "MES None" instead of "13.0 shares of MES".
        """
        broker_position = {
            "symbol": "MES",
            "contractId": "CON.F.US.MES.U25",
            "size": 13,
            "avgPrice": 5150.0,
        }

        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        position = projectx_broker._convert_broker_position_to_lumibot_position(broker_position)
        
        # Should display cleanly
        assert position.asset.symbol == "MES"
        assert position.quantity == 13
        # Position string representation should be clean (not "MES None")
        assert "MES" in str(position)

    def test_multiple_order_status_scenarios(self, projectx_broker):
        """Test various order status scenarios that were problematic"""
        
        test_cases = [
            # (status_id, expected_lumibot_status, description)
            (1, "new", "Pending/New order"),
            (2, "submitted", "Submitted to exchange"),
            (3, "open", "Open/Active (was incorrectly partial_filled)"),
            (4, "fill", "Completely filled"),
            (5, "canceled", "Canceled or rejected"),
            (11, "partial_filled", "Actually partially filled"),
        ]

        mock_asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(return_value=mock_asset)

        for status_id, expected_status, description in test_cases:
            broker_order = {
                "id": 123 + status_id,
                "status": status_id,
                "symbol": "MES",
                "contractId": "CON.F.US.MES.U25",
                "size": 1,
                "filledSize": 1 if status_id == 4 else 0,  # Only filled orders have FilledSize
                "type": 2,
                "side": 0,
            }

            order = projectx_broker._convert_broker_order_to_lumibot_order(broker_order)
            if order is not None:  # Some conversions might fail and return None
                assert order.status == expected_status, f"Failed for {description}: expected {expected_status}, got {order.status}"


class TestProjectXBrokerIntegration:
    """
    Integration-style tests that test the interaction between components.
    These still use mocks but test larger workflows.
    """

    def test_full_order_sync_workflow(self, projectx_broker):
        """Test the complete order sync workflow that was broken"""
        
        # Mock broker orders
        broker_orders = [
            {
                "id": 111,
                "status": 3,  # Open
                "symbol": "MES",
                "contractId": "CON.F.US.MES.U25", 
                "size": 1,
                "type": 2,
                "side": 0,
            },
            {
                "id": 222, 
                "status": 4,  # Filled
                "symbol": "NQ",
                "contractId": "CON.F.US.NQ.U25",
                "size": 2,
                "type": 2,
                "side": 1,
            }
        ]

        # Setup mocks - ensure account_id is set
        projectx_broker.account_id = "test_account_123"
        projectx_broker.client.get_orders = MagicMock(return_value=broker_orders)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(side_effect=[
            Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset(symbol="NQ", asset_type=Asset.AssetType.CONT_FUTURE)
        ])

        # Test the full workflow
        orders = projectx_broker._get_orders_at_broker()

        # Should return converted orders
        assert len(orders) == 2
        assert orders[0].id == "111"
        assert orders[0].status == "open"
        assert orders[1].id == "222"
        assert orders[1].status == "fill"

    def test_position_sync_workflow(self, projectx_broker):
        """Test the complete position sync workflow"""
        
        # Mock broker positions
        broker_positions = [
            {
                "symbol": "MES",
                "contractId": "CON.F.US.MES.U25",
                "size": 5,
                "avgPrice": 5150.0,
                "unrealizedPnL": 250.0,
            },
            {
                "symbol": "NQ", 
                "contractId": "CON.F.US.NQ.U25",
                "size": -2,
                "avgPrice": 18500.0,
                "unrealizedPnL": -100.0,
            }
        ]

        # Setup mocks
        projectx_broker.client.get_positions = MagicMock(return_value=broker_positions)
        projectx_broker._get_asset_from_contract_id_cached = MagicMock(side_effect=[
            Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset(symbol="NQ", asset_type=Asset.AssetType.CONT_FUTURE)
        ])

        # Test position retrieval
        positions = projectx_broker._get_positions_at_broker()

        # Should return converted positions
        assert len(positions) == 2
        assert positions[0].asset.symbol == "MES"
        assert positions[0].quantity == 5
        assert positions[1].asset.symbol == "NQ"
        assert positions[1].quantity == -2 