import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal

from lumibot.brokers.bitunix import Bitunix
from lumibot.entities import Asset, Order, Position
from lumibot.tools.bitunix_helpers import BitUnixClient
from lumibot.brokers.broker import LumibotBrokerAPIError

class TestBitunixBroker(unittest.TestCase):
    def setUp(self):
        self.config = {
            "API_KEY": "test_api_key",
            "API_SECRET": "test_api_secret",
            "TRADING_MODE": "FUTURES",
        }
        # Mock the BitUnixClient to prevent actual API calls
        self.mock_bitunix_client = MagicMock(spec=BitUnixClient)

    @patch("lumibot.brokers.bitunix.BitUnixClient")
    @patch("lumibot.brokers.bitunix.BitunixData")
    def test_initialization_success(self, MockBitunixData, MockBitUnixClientInstance):
        MockBitUnixClientInstance.return_value = self.mock_bitunix_client
        mock_data_source = MockBitunixData.return_value
        mock_data_source.client_symbols = set()

        broker = Bitunix(self.config)
        self.assertIsNotNone(broker.api)
        self.assertEqual(broker.api, self.mock_bitunix_client)
        MockBitUnixClientInstance.assert_called_once_with(api_key="test_api_key", secret_key="test_api_secret")

    @patch("lumibot.brokers.bitunix.BitUnixClient")
    @patch("lumibot.brokers.bitunix.BitunixData")
    def test_initialization_missing_keys(self, MockBitunixData, MockBitUnixClientInstance):
        with self.assertRaises(ValueError):
            Bitunix({"TRADING_MODE": "FUTURES"})

    @patch("lumibot.brokers.bitunix.BitUnixClient")
    @patch("lumibot.brokers.bitunix.BitunixData")
    def test_pull_positions_success(self, MockBitunixData, MockBitUnixClientInstance):
        MockBitUnixClientInstance.return_value = self.mock_bitunix_client
        mock_data_source = MockBitunixData.return_value
        mock_data_source.client_symbols = set()

        broker = Bitunix(self.config)
        mock_strategy = MagicMock()
        mock_strategy.name = "test_strategy"

        self.mock_bitunix_client.get_positions.return_value = {
            "code": 0,
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "qty": "0.5",
                    "side": "BUY",
                    "avgOpenPrice": "51000",
                },
                {
                    "symbol": "ETHUSDT",
                    "qty": "2.0",
                    "side": "SELL",
                    "avgOpenPrice": "3000",
                },
            ],
        }
        positions = broker._pull_positions(mock_strategy)
        self.assertEqual(len(positions), 2)
        
        btc_pos = next(p for p in positions if p.asset.symbol == "BTCUSDT")
        eth_pos = next(p for p in positions if p.asset.symbol == "ETHUSDT")

        self.assertEqual(btc_pos.quantity, Decimal("0.5"))
        self.assertEqual(btc_pos.avg_fill_price, Decimal("51000"))
        self.assertEqual(eth_pos.quantity, Decimal("-2.0"))
        self.assertEqual(eth_pos.avg_fill_price, Decimal("3000"))

    @patch("lumibot.brokers.bitunix.BitUnixClient")
    @patch("lumibot.brokers.bitunix.BitunixData")
    def test_get_balances_at_broker(self, MockBitunixData, MockBitUnixClientInstance):
        MockBitUnixClientInstance.return_value = self.mock_bitunix_client
        mock_data_source = MockBitunixData.return_value
        mock_data_source.client_symbols = set()

        broker = Bitunix(self.config)
        mock_strategy = MagicMock()
        mock_strategy.name = "test_strategy"

        self.mock_bitunix_client.get_account.return_value = {
            "code": 0,
            "data": {
                "available": "10000.00",
                "frozen": "500.00",
                "margin": "2000.00",
                "crossUnrealizedPNL": "100.00",
                "isolationUnrealizedPNL": "50.00",
            }
        }
        # Mock _pull_positions as it's called by _get_balances_at_broker
        broker._pull_positions = MagicMock(return_value=[
            Position("test_strategy", Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE), Decimal("0.1"), avg_fill_price=Decimal("50000"))
        ])

        cash, positions_value, net_liquidation = broker._get_balances_at_broker(Asset("USDT", Asset.AssetType.CRYPTO), mock_strategy)

        self.assertEqual(cash, 10000.00)
        self.assertEqual(positions_value, 5000.0) # 0.1 * 50000
        self.assertEqual(net_liquidation, 12650.00) # 10000 + 500 + 2000 + 100 + 50

    def test_map_status_from_bitunix(self):
        broker = Bitunix(self.config, connect_stream=False) # No need for full init
        self.assertEqual(broker._map_status_from_bitunix("NEW"), Order.OrderStatus.SUBMITTED)
        self.assertEqual(broker._map_status_from_bitunix("PARTIALLY_FILLED"), Order.OrderStatus.PARTIALLY_FILLED)
        self.assertEqual(broker._map_status_from_bitunix("FILLED"), Order.OrderStatus.FILLED)
        self.assertEqual(broker._map_status_from_bitunix("CANCELED"), Order.OrderStatus.CANCELED)
        self.assertEqual(broker._map_status_from_bitunix("REJECTED"), Order.OrderStatus.ERROR)
        self.assertEqual(broker._map_status_from_bitunix("EXPIRED"), Order.OrderStatus.CANCELED)
        self.assertEqual(broker._map_status_from_bitunix("PENDING_CANCEL"), Order.OrderStatus.CANCELED)
        self.assertEqual(broker._map_status_from_bitunix("UNKNOWN_STATUS"), Order.OrderStatus.ERROR) # Test default

    @patch("lumibot.brokers.bitunix.BitUnixClient")
    @patch("lumibot.brokers.bitunix.BitunixData")
    def test_parse_broker_order(self, MockBitunixData, MockBitUnixClientInstance):
        MockBitUnixClientInstance.return_value = self.mock_bitunix_client
        mock_data_source = MockBitunixData.return_value
        mock_data_source.client_symbols = set()
        
        broker = Bitunix(self.config)
        
        raw_order_data = {
            "orderId": "98765",
            "symbol": "ETHUSDT",
            "status": "FILLED",
            "side": "SELL",
            "orderType": "LIMIT",
            "qty": "1.5",
            "tradeQty": "1.5",
            "price": "3000.00",
            "avgPrice": "3005.50",
            "leverage": "5",
            "time": 1678886400000 # Example timestamp
        }
        
        parsed_order = broker._parse_broker_order(raw_order_data, "test_strategy")
        
        self.assertIsNotNone(parsed_order)
        self.assertEqual(parsed_order.identifier, "98765")
        self.assertEqual(parsed_order.asset.symbol, "ETHUSDT")
        self.assertEqual(parsed_order.asset.asset_type, Asset.AssetType.CRYPTO_FUTURE)
        self.assertEqual(parsed_order.status, Order.OrderStatus.FILLED)
        self.assertEqual(parsed_order.side, Order.OrderSide.SELL)
        self.assertEqual(parsed_order.order_type, Order.OrderType.LIMIT)
        self.assertEqual(parsed_order.quantity, Decimal("1.5"))
        self.assertEqual(parsed_order.filled_quantity, Decimal("1.5"))
        self.assertEqual(parsed_order.limit_price, Decimal("3000.00"))
        self.assertEqual(parsed_order.avg_fill_price, Decimal("3005.50"))
        self.assertIsNotNone(parsed_order.broker_create_date)

    @patch("lumibot.data_sources.bitunix_data.BitunixData")
    def test_parse_source_timestep(self, MockBitunixData):
        mock_data_source = MockBitunixData.return_value
        mock_data_source.client_symbols = set()

        broker = Bitunix(self.config)

        # Test all supported timesteps
        self.assertEqual(broker._parse_source_timestep("1m"), "1m")
        self.assertEqual(broker._parse_source_timestep("minute"), "1m")
        self.assertEqual(broker._parse_source_timestep("3m"), "3m")
        self.assertEqual(broker._parse_source_timestep("5m"), "5m")
        self.assertEqual(broker._parse_source_timestep("15m"), "15m")
        self.assertEqual(broker._parse_source_timestep("30m"), "30m")
        self.assertEqual(broker._parse_source_timestep("1h"), "1h")
        self.assertEqual(broker._parse_source_timestep("2h"), "2h")
        self.assertEqual(broker._parse_source_timestep("4h"), "4h")
        self.assertEqual(broker._parse_source_timestep("1d"), "1d")
        # Test fallback/default
        self.assertEqual(broker._parse_source_timestep("unknown"), "1m")

if __name__ == "__main__":
    unittest.main()
