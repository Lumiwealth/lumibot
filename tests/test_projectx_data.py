import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
import datetime as dt

from lumibot.entities import Asset, Bars
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
def projectx_data_source(mock_projectx_config):
    """Create ProjectX data source with mocked dependencies"""
    with patch('lumibot.data_sources.projectx_data.ProjectXClient'):
        data_source = ProjectXData(mock_projectx_config)
        data_source.client = MagicMock()
        return data_source


class TestProjectXDataSource:
    """
    Unit tests for ProjectX data source. These focus on asset resolution
    and contract mapping issues that caused problems.
    """

    def test_data_source_initialization(self, mock_projectx_config):
        """Test basic data source initialization"""
        with patch('lumibot.data_sources.projectx_data.ProjectXClient'):
            data_source = ProjectXData(mock_projectx_config)
            assert data_source.name == "data_source"  # Inherited from parent DataSource class
            assert data_source.firm == "TEST"

    def test_asset_to_contract_id_no_hardcoded_mappings(self, projectx_data_source):
        """
        Test that asset resolution doesn't use hardcoded contract mappings.
        Previously had: 'MES': 'CON.F.US.MES.U25' which would expire.
        """
        # Mock the Asset class method for dynamic contract resolution
        with patch.object(Asset, 'get_potential_futures_contracts') as mock_contracts:
            mock_contracts.return_value = ['MESU25', 'MES.U25', 'MESU2025']

            # Test asset conversion
            asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
            contract_id = projectx_data_source._get_contract_id_from_asset(asset)

            # Should use Asset class logic, not hardcoded mappings
            mock_contracts.assert_called_once()
            assert contract_id  # Should return a valid contract ID

    def test_contract_id_generation_dynamic(self, projectx_data_source):
        """Test dynamic contract ID generation using Asset class"""

        # Mock client to return a contract ID
        projectx_data_source.client.find_contract_by_symbol = MagicMock(return_value="CON.F.US.MES.U25")

        # Test with continuous futures asset
        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)

        # Should use dynamic search, not hardcoded mappings
        contract_id = projectx_data_source._get_contract_id_from_asset(asset)
        assert contract_id == "CON.F.US.MES.U25"  # Should pick appropriate contract

    def test_timespan_parsing(self, projectx_data_source):
        """Test timespan parsing for ProjectX unit conversion"""

        # Test various timespan formats
        test_cases = [
            ("minute", 1, 1),    # Fixed: returns (unit_id, unit_number)
            ("1minute", 1, 1),
            ("5minute", 1, 5),
            ("15minute", 1, 15),
            ("hour", 2, 1),      # Fixed: returns (unit_id, unit_number)
            ("1hour", 2, 1),
            ("4hour", 2, 4),
            ("day", 3, 1),       # Fixed: returns (unit_id, unit_number)
            ("1day", 3, 1),
            ("week", 4, 1),      # Fixed: returns (unit_id, unit_number)
            ("month", 5, 1),     # Fixed: returns (unit_id, unit_number)
        ]

        for timespan, expected_unit, expected_number in test_cases:
            unit, unit_number = projectx_data_source._parse_timespan(timespan)
            assert unit == expected_unit, f"Failed for {timespan}: expected {expected_unit}, got {unit}"
            assert unit_number == expected_number, f"Failed for {timespan}: expected {expected_number}, got {unit_number}"

    def test_projectx_unit_mapping(self, projectx_data_source):
        """Test ProjectX unit mapping"""

        # Test unit name to ProjectX unit mapping - using correct attribute name
        unit_mappings = projectx_data_source.TIME_UNIT_MAPPING

        assert unit_mappings["minute"] == 1
        assert unit_mappings["hour"] == 2
        assert unit_mappings["day"] == 3
        assert unit_mappings["week"] == 4
        assert unit_mappings["month"] == 5

    def test_get_historical_prices_bars_conversion(self, projectx_data_source):
        """Test historical price retrieval and bars conversion"""

        # Mock the _get_contract_id_from_asset method
        projectx_data_source._get_contract_id_from_asset = MagicMock(return_value="CON.F.US.MES.U25")

        # Mock empty DataFrame return (simulate no data)
        projectx_data_source.client.history_retrieve_bars = MagicMock(return_value=pd.DataFrame())

        # Test historical prices retrieval
        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        bars = projectx_data_source.get_historical_prices(asset, 10, "minute")

        # Should return None when no data available
        assert bars is None

    def test_get_last_price(self, projectx_data_source):
        """Test last price retrieval"""

        # Mock get_bars to return bars with price data
        mock_df = pd.DataFrame({
            'close': [5150.25]
        })
        mock_bars = MagicMock()
        mock_bars.df = mock_df
        projectx_data_source.get_bars = MagicMock(return_value=mock_bars)

        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        price = projectx_data_source.get_last_price(asset)

        assert price == 5150.25
        projectx_data_source.get_bars.assert_called_once()

    def test_dividends_not_supported(self, projectx_data_source):
        """Test that dividends return 0 for futures broker"""
        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)

        # get_yesterday_dividend returns 0.0, doesn't raise exception
        dividend = projectx_data_source.get_yesterday_dividend(asset)
        assert dividend == 0.0

    def test_option_chains_not_supported(self, projectx_data_source):
        """Test that option chains are not supported for futures broker"""
        asset = Asset(symbol="SPY", asset_type=Asset.AssetType.STOCK)

        with pytest.raises(NotImplementedError, match="ProjectX is a futures data source - options chains are not supported"):
            projectx_data_source.get_chains(asset)

    def test_get_bars_with_contract_found(self, projectx_data_source):
        """Test get_bars when contract is found and data is available"""

        # Mock successful contract lookup
        projectx_data_source._get_contract_id_from_asset = MagicMock(return_value="CON.F.US.MES.U25")

        # Mock successful bars retrieval
        mock_df = pd.DataFrame({
            'time': pd.date_range('2024-01-15 09:30:00', periods=2, freq='1min'),
            'open': [5150.0, 5152.0],
            'high': [5155.0, 5157.0],
            'low': [5148.0, 5151.0],
            'close': [5152.0, 5156.0],
            'volume': [1000, 1200]
        })
        projectx_data_source.client.history_retrieve_bars = MagicMock(return_value=mock_df)

        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        bars = projectx_data_source.get_bars(asset, 2, "minute")

        # Should return Bars object
        assert isinstance(bars, Bars)
        assert len(bars.df) == 2
        assert bars.asset == asset
        assert bars.source == "PROJECTX"  # Source is uppercase

    def test_contract_search_multiple_formats(self, projectx_data_source):
        """
        Test contract search handles multiple potential contract formats.
        Asset class returns multiple formats: ['MESU25', 'MES.U25', 'MESU2025']
        """

        # Mock Asset class to return multiple potential formats
        with patch.object(Asset, 'get_potential_futures_contracts') as mock_contracts:
            mock_contracts.return_value = ['MESU25', 'MES.U25', 'MESU2025']

            # Mock client method to return valid contract
            projectx_data_source.client.find_contract_by_symbol = MagicMock(return_value="CON.F.US.MES.U25")

            asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
            contract_id = projectx_data_source._get_contract_id_from_asset(asset)

            # Should find contract using Asset class logic
            assert contract_id == "CON.F.US.MES.U25"
            mock_contracts.assert_called_once()

    def test_error_handling_no_contracts_found(self, projectx_data_source):
        """Test error handling when no contracts are found"""

        # Mock empty contract lookup - need to mock both Asset method and client method
        projectx_data_source.client.find_contract_by_symbol = MagicMock(return_value=None)

        with patch.object(Asset, 'get_potential_futures_contracts') as mock_contracts:
            mock_contracts.side_effect = Exception("Asset resolution failed")  # Force fallback to client

            asset = Asset(symbol="UNKNOWN", asset_type=Asset.AssetType.CONT_FUTURE)

            # Should return None when no contract found
            contract_id = projectx_data_source._get_contract_id_from_asset(asset)
            assert contract_id is None

    def test_get_contract_details(self, projectx_data_source):
        """Test contract details retrieval"""

        # Mock successful contract lookup
        projectx_data_source._get_contract_id_from_asset = MagicMock(return_value="CON.F.US.MES.U25")

        # Mock contract details response
        mock_response = {
            "success": True,
            "contract": {
                "ContractId": "CON.F.US.MES.U25",
                "Symbol": "MES", 
                "Description": "E-mini S&P 500 Future",
                "TickSize": 0.25
            }
        }
        projectx_data_source.client.contract_search_id = MagicMock(return_value=mock_response)

        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        details = projectx_data_source.get_contract_details(asset)

        assert details["Symbol"] == "MES"
        assert details["TickSize"] == 0.25

    def test_search_contracts(self, projectx_data_source):
        """Test contract search functionality"""

        # Mock contract search response
        mock_response = {
            "success": True,
            "contracts": [
                {
                    "ContractId": "CON.F.US.MES.U25",
                    "Symbol": "MES",
                    "Description": "E-mini S&P 500 Future"
                }
            ]
        }
        projectx_data_source.client.contract_search = MagicMock(return_value=mock_response)

        contracts = projectx_data_source.search_contracts("MES")

        assert len(contracts) == 1
        assert contracts[0]["Symbol"] == "MES"

    def test_get_quote(self, projectx_data_source):
        """Test quote retrieval"""
        from lumibot.entities import Quote

        # Mock last price
        projectx_data_source.get_last_price = MagicMock(return_value=5150.25)

        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        quote = projectx_data_source.get_quote(asset)

        assert isinstance(quote, Quote)
        assert quote.asset == asset
        assert quote.price == 5150.25
        assert quote.bid is not None
        assert quote.ask is not None
        assert quote.timestamp is not None

    def test_get_bars_from_datetime(self, projectx_data_source):
        """Test historical bars between specific datetime range"""

        # Mock successful contract lookup
        projectx_data_source._get_contract_id_from_asset = MagicMock(return_value="CON.F.US.MES.U25")

        # Mock successful bars retrieval
        mock_df = pd.DataFrame({
            'time': pd.date_range('2024-01-15 09:30:00', periods=1, freq='1min'),
            'open': [5150.0],
            'high': [5155.0],
            'low': [5148.0],
            'close': [5152.0],
            'volume': [1000]
        })
        projectx_data_source.client.history_retrieve_bars = MagicMock(return_value=mock_df)

        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        start_time = dt.datetime(2024, 1, 15, 9, 30)
        end_time = dt.datetime(2024, 1, 15, 10, 30)

        bars = projectx_data_source.get_bars_from_datetime(asset, start_time, end_time, "minute")

        # Should return Bars object
        assert isinstance(bars, Bars)
        assert len(bars.df) == 1
        assert bars.asset == asset


class TestProjectXDataSourceIntegration:
    """
    Integration tests for ProjectX data source workflows.
    """

    def test_full_historical_data_workflow(self, projectx_data_source):
        """Test complete historical data retrieval workflow"""

        # Mock successful contract lookup
        projectx_data_source._get_contract_id_from_asset = MagicMock(return_value="CON.F.US.MES.U25")

        # Mock historical data
        mock_df = pd.DataFrame({
            'time': pd.date_range('2024-01-15 09:30:00', periods=2, freq='1min'),
            'open': [5150.0, 5152.0],
            'high': [5155.0, 5157.0],
            'low': [5148.0, 5151.0],
            'close': [5152.0, 5156.0],
            'volume': [1000, 1200]
        })
        projectx_data_source.client.history_retrieve_bars = MagicMock(return_value=mock_df)

        # Test full workflow
        asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        bars = projectx_data_source.get_historical_prices(asset, 10, "minute")

        # Verify complete workflow
        assert isinstance(bars, Bars)
        assert len(bars.df) == 2
        assert bars.asset.symbol == "MES"
        assert bars.source == "PROJECTX"  # Source is uppercase

    def test_asset_resolution_workflow_with_fallbacks(self, projectx_data_source):
        """Test asset resolution workflow with multiple format fallbacks"""

        # Mock Asset class to return multiple formats
        with patch.object(Asset, 'get_potential_futures_contracts') as mock_contracts:
            mock_contracts.return_value = ['MESU25', 'MES.U25', 'MESU2025']

            # Mock client to eventually succeed
            projectx_data_source.client.find_contract_by_symbol = MagicMock(return_value="CON.F.US.MES.U25")

            asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
            contract_id = projectx_data_source._get_contract_id_from_asset(asset)

            # Should eventually find contract
            assert contract_id == "CON.F.US.MES.U25"
            mock_contracts.assert_called_once() 
