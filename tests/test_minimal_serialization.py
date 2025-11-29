"""
Tests for entity to_minimal_dict() methods and ThetaData download status tracking.

These tests cover the minimal serialization methods added to Asset, Position, and Order
entities for lightweight progress logging, as well as the ThetaData download status
tracking functionality.
"""
import unittest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from lumibot.entities import Asset, Position, Order


class TestAssetMinimalDict(unittest.TestCase):
    """Test Asset.to_minimal_dict() method."""

    def test_stock_minimal_dict(self):
        """Test stock asset returns minimal dict with symbol and type."""
        asset = Asset(symbol="AAPL")
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["type"], "stock")
        # Should only have these 2 fields for stocks
        self.assertEqual(set(result.keys()), {"symbol", "type"})

    def test_stock_explicit_type_minimal_dict(self):
        """Test explicitly typed stock returns correct minimal dict."""
        asset = Asset(symbol="MSFT", asset_type=Asset.AssetType.STOCK)
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "MSFT")
        self.assertEqual(result["type"], "stock")

    def test_option_minimal_dict(self):
        """Test option asset returns minimal dict with option-specific fields."""
        asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            strike=150.0,
            expiration=date(2024, 12, 20),
            right="CALL",
            multiplier=100
        )
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "AAPL")
        self.assertEqual(result["type"], "option")
        self.assertEqual(result["strike"], 150.0)
        self.assertEqual(result["exp"], "2024-12-20")
        self.assertEqual(result["right"], "CALL")
        self.assertEqual(result["mult"], 100)

    def test_option_put_minimal_dict(self):
        """Test put option asset returns correct right value."""
        asset = Asset(
            symbol="SPY",
            asset_type="option",
            strike=450.0,
            expiration=date(2024, 6, 15),
            right=Asset.OptionRight.PUT,
            multiplier=100
        )
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "SPY")
        self.assertEqual(result["type"], "option")
        self.assertEqual(result["right"], "PUT")

    def test_future_minimal_dict(self):
        """Test future asset returns minimal dict with expiration."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=date(2024, 12, 20),
            multiplier=50
        )
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "ES")
        self.assertEqual(result["type"], "future")
        self.assertEqual(result["exp"], "2024-12-20")
        self.assertEqual(result["mult"], 50)

    def test_future_no_multiplier_if_default(self):
        """Test future with multiplier=1 doesn't include mult field."""
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=date(2024, 12, 20),
            multiplier=1
        )
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "MES")
        self.assertEqual(result["type"], "future")
        self.assertNotIn("mult", result)

    def test_cont_future_minimal_dict(self):
        """Test continuous future asset returns correct type."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.CONT_FUTURE
        )
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "ES")
        self.assertEqual(result["type"], "cont_future")

    def test_crypto_minimal_dict(self):
        """Test crypto asset returns minimal dict."""
        asset = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "BTC")
        self.assertEqual(result["type"], "crypto")
        # Crypto should only have symbol and type
        self.assertEqual(set(result.keys()), {"symbol", "type"})

    def test_forex_minimal_dict(self):
        """Test forex asset returns minimal dict."""
        asset = Asset(symbol="EUR", asset_type=Asset.AssetType.FOREX)
        result = asset.to_minimal_dict()

        self.assertEqual(result["symbol"], "EUR")
        self.assertEqual(result["type"], "forex")


class TestPositionMinimalDict(unittest.TestCase):
    """Test Position.to_minimal_dict() method."""

    def test_basic_position_minimal_dict(self):
        """Test position returns minimal dict with all required fields."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)

        result = position.to_minimal_dict()

        self.assertIn("asset", result)
        self.assertEqual(result["asset"]["symbol"], "AAPL")
        self.assertEqual(result["qty"], 100.0)
        self.assertIn("val", result)
        self.assertIn("pnl", result)

    def test_position_with_market_value(self):
        """Test position with market value returns correct val."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)
        position.market_value = 15000.50

        result = position.to_minimal_dict()

        self.assertEqual(result["val"], 15000.50)

    def test_position_with_pnl(self):
        """Test position with P&L returns correct pnl."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)
        position.pnl = 500.25

        result = position.to_minimal_dict()

        self.assertEqual(result["pnl"], 500.25)

    def test_position_rounds_values(self):
        """Test position rounds val and pnl to 2 decimal places."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)
        position.market_value = 15000.12345
        position.pnl = 500.98765

        result = position.to_minimal_dict()

        self.assertEqual(result["val"], 15000.12)
        self.assertEqual(result["pnl"], 500.99)

    def test_position_with_option_asset(self):
        """Test position with option asset includes full asset info."""
        asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            strike=150.0,
            expiration=date(2024, 12, 20),
            right="CALL",
            multiplier=100
        )
        position = Position(strategy="TestStrategy", asset=asset, quantity=10)

        result = position.to_minimal_dict()

        self.assertEqual(result["asset"]["symbol"], "AAPL")
        self.assertEqual(result["asset"]["type"], "option")
        self.assertEqual(result["asset"]["strike"], 150.0)
        self.assertEqual(result["asset"]["right"], "CALL")

    def test_position_without_market_value(self):
        """Test position without market value returns 0."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)
        # Don't set market_value

        result = position.to_minimal_dict()

        self.assertEqual(result["val"], 0.0)

    def test_position_without_pnl(self):
        """Test position without pnl returns 0."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=100)
        # Don't set pnl

        result = position.to_minimal_dict()

        self.assertEqual(result["pnl"], 0.0)

    def test_position_negative_quantity(self):
        """Test position with negative quantity (short position)."""
        asset = Asset(symbol="AAPL")
        position = Position(strategy="TestStrategy", asset=asset, quantity=-50)

        result = position.to_minimal_dict()

        self.assertEqual(result["qty"], -50.0)


class TestOrderMinimalDict(unittest.TestCase):
    """Test Order.to_minimal_dict() method."""

    def test_market_order_minimal_dict(self):
        """Test market order returns minimal dict."""
        asset = Asset(symbol="AAPL")
        order = Order(strategy="TestStrategy", asset=asset, quantity=100, side="buy")

        result = order.to_minimal_dict()

        self.assertEqual(result["asset"]["symbol"], "AAPL")
        self.assertEqual(result["side"], "buy")
        self.assertEqual(result["qty"], 100.0)
        self.assertEqual(result["type"], "market")
        self.assertIn("status", result)
        # Market orders shouldn't have limit or stop
        self.assertNotIn("limit", result)
        self.assertNotIn("stop", result)

    def test_limit_order_minimal_dict(self):
        """Test limit order includes limit price."""
        asset = Asset(symbol="AAPL")
        order = Order(
            strategy="TestStrategy",
            asset=asset,
            quantity=100,
            side="buy",
            order_type="limit",
            limit_price=150.00
        )

        result = order.to_minimal_dict()

        self.assertEqual(result["type"], "limit")
        self.assertEqual(result["limit"], 150.00)
        self.assertNotIn("stop", result)

    def test_stop_order_minimal_dict(self):
        """Test stop order includes stop price."""
        asset = Asset(symbol="AAPL")
        order = Order(
            strategy="TestStrategy",
            asset=asset,
            quantity=100,
            side="sell",
            order_type="stop",
            stop_price=140.00
        )

        result = order.to_minimal_dict()

        self.assertEqual(result["type"], "stop")
        self.assertEqual(result["stop"], 140.00)
        self.assertNotIn("limit", result)

    def test_stop_limit_order_minimal_dict(self):
        """Test stop-limit order includes both prices."""
        asset = Asset(symbol="AAPL")
        order = Order(
            strategy="TestStrategy",
            asset=asset,
            quantity=100,
            side="buy",
            order_type="stop_limit",
            limit_price=152.00,
            stop_price=150.00
        )

        result = order.to_minimal_dict()

        self.assertEqual(result["type"], "stop_limit")
        self.assertEqual(result["limit"], 152.00)
        self.assertEqual(result["stop"], 150.00)

    def test_sell_order_minimal_dict(self):
        """Test sell order returns correct side."""
        asset = Asset(symbol="AAPL")
        order = Order(strategy="TestStrategy", asset=asset, quantity=100, side="sell")

        result = order.to_minimal_dict()

        self.assertEqual(result["side"], "sell")

    def test_order_with_option_asset(self):
        """Test order with option asset includes full asset info."""
        asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            strike=150.0,
            expiration=date(2024, 12, 20),
            right="CALL",
            multiplier=100
        )
        order = Order(strategy="TestStrategy", asset=asset, quantity=5, side="buy")

        result = order.to_minimal_dict()

        self.assertEqual(result["asset"]["symbol"], "AAPL")
        self.assertEqual(result["asset"]["type"], "option")
        self.assertEqual(result["asset"]["strike"], 150.0)

    def test_order_status_in_minimal_dict(self):
        """Test order status is included in minimal dict."""
        asset = Asset(symbol="AAPL")
        order = Order(strategy="TestStrategy", asset=asset, quantity=100, side="buy")

        result = order.to_minimal_dict()

        self.assertIn("status", result)


class TestDownloadStatusTracking(unittest.TestCase):
    """Test ThetaData download status tracking functions."""

    def setUp(self):
        """Clear download status before each test."""
        from lumibot.tools.thetadata_helper import clear_download_status
        clear_download_status()

    def tearDown(self):
        """Clear download status after each test."""
        from lumibot.tools.thetadata_helper import clear_download_status
        clear_download_status()

    def test_get_download_status_initial(self):
        """Test initial download status is inactive."""
        from lumibot.tools.thetadata_helper import get_download_status

        status = get_download_status()

        self.assertFalse(status["active"])
        self.assertIsNone(status["asset"])
        self.assertIsNone(status["quote"])
        self.assertEqual(status["progress"], 0)

    def test_set_download_status(self):
        """Test setting download status."""
        from lumibot.tools.thetadata_helper import get_download_status, set_download_status

        asset = Asset(symbol="AAPL")
        set_download_status(
            asset=asset,
            quote_asset="USD",
            data_type="ohlc",
            timespan="minute",
            current=5,
            total=10
        )

        status = get_download_status()

        self.assertTrue(status["active"])
        self.assertEqual(status["asset"]["symbol"], "AAPL")
        self.assertEqual(status["quote"], "USD")
        self.assertEqual(status["data_type"], "ohlc")
        self.assertEqual(status["timespan"], "minute")
        self.assertEqual(status["progress"], 50)
        self.assertEqual(status["current"], 5)
        self.assertEqual(status["total"], 10)

    def test_clear_download_status(self):
        """Test clearing download status."""
        from lumibot.tools.thetadata_helper import (
            get_download_status, set_download_status, clear_download_status
        )

        asset = Asset(symbol="AAPL")
        set_download_status(asset, "USD", "ohlc", "minute", 5, 10)
        clear_download_status()

        status = get_download_status()

        self.assertFalse(status["active"])
        self.assertIsNone(status["asset"])
        self.assertEqual(status["progress"], 0)

    def test_download_status_progress_calculation(self):
        """Test progress percentage calculation."""
        from lumibot.tools.thetadata_helper import get_download_status, set_download_status

        asset = Asset(symbol="SPY")

        # Test 0%
        set_download_status(asset, "USD", "ohlc", "minute", 0, 10)
        self.assertEqual(get_download_status()["progress"], 0)

        # Test 25%
        set_download_status(asset, "USD", "ohlc", "minute", 25, 100)
        self.assertEqual(get_download_status()["progress"], 25)

        # Test 100%
        set_download_status(asset, "USD", "ohlc", "minute", 10, 10)
        self.assertEqual(get_download_status()["progress"], 100)

    def test_download_status_thread_safety(self):
        """Test download status operations are thread-safe."""
        import threading
        from lumibot.tools.thetadata_helper import (
            get_download_status, set_download_status, clear_download_status
        )

        errors = []
        iterations = 100

        def writer_thread():
            try:
                for i in range(iterations):
                    asset = Asset(symbol=f"TEST{i}")
                    set_download_status(asset, "USD", "ohlc", "minute", i, iterations)
            except Exception as e:
                errors.append(e)

        def reader_thread():
            try:
                for _ in range(iterations):
                    status = get_download_status()
                    # Just access the fields to ensure no race conditions
                    _ = status["active"]
                    _ = status["progress"]
            except Exception as e:
                errors.append(e)

        def clearer_thread():
            try:
                for _ in range(iterations // 10):
                    clear_download_status()
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer_thread),
            threading.Thread(target=reader_thread),
            threading.Thread(target=clearer_thread),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Thread safety errors: {errors}")

    def test_download_status_with_option_asset(self):
        """Test download status with option asset."""
        from lumibot.tools.thetadata_helper import get_download_status, set_download_status

        asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            strike=150.0,
            expiration=date(2024, 12, 20),
            right="CALL"
        )
        set_download_status(asset, "USD", "ohlc", "minute", 1, 5)

        status = get_download_status()

        self.assertEqual(status["asset"]["symbol"], "AAPL")
        self.assertEqual(status["asset"]["type"], "option")
        self.assertEqual(status["asset"]["strike"], 150.0)


if __name__ == "__main__":
    unittest.main()
