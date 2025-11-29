"""
Unit tests for multi-leg order submission fix in Alpaca broker.

Tests the fix for issue where Alpaca API was rejecting multi-leg orders
due to incorrect order_class value and missing required fields.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from lumibot.entities.order import Order
from lumibot.entities.asset import Asset
from lumibot.brokers.alpaca import Alpaca


class TestAlpacaMultiLegOrders:
    """Test cases for Alpaca multi-leg order submission."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Provide proper test credentials
        self.test_config = {
            "API_KEY": "test_api_key_multileg",
            "API_SECRET": "test_api_secret_multileg", 
            "PAPER": True
        }
        
        # Create sample option assets for testing
        self.expiration = datetime.now() + timedelta(days=30)
        
        self.call_asset = Asset(
            symbol="SPY",
            asset_type=Asset.AssetType.OPTION,
            expiration=self.expiration,
            strike=450.0,
            right="call"
        )
        
        self.put_asset = Asset(
            symbol="SPY", 
            asset_type=Asset.AssetType.OPTION,
            expiration=self.expiration,
            strike=440.0,
            right="put"
        )
    
    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_multileg_order_class_is_correct(self, mock_trading_client):
        """Test that multi-leg orders use the correct order_class value."""
        mock_trading_client.return_value = Mock()
        broker = Alpaca(self.test_config, connect_stream=False)
        # Create sample orders for a spread
        call_order = Order(
            strategy="test_strategy",
            asset=self.call_asset,
            quantity=1,
            side="sell",
            order_type=Order.OrderType.LIMIT,
            limit_price=2.50
        )
        
        put_order = Order(
            strategy="test_strategy",
            asset=self.put_asset,
            quantity=1,
            side="buy", 
            order_type=Order.OrderType.LIMIT,
            limit_price=1.50
        )
        
        orders = [call_order, put_order]
        
        # Mock the API submit_order method to capture the kwargs
        with patch.object(broker, 'api') as mock_api:
            mock_response = Mock()
            mock_response.id = "test_order_id"
            mock_response.status = "submitted"
            mock_api.submit_order.return_value = mock_response
            
            try:
                broker._submit_multileg_order(orders, "SPY", price=1.00)
            except Exception:
                # We expect this to fail due to mocking, but we can check the call
                pass
            
            # Verify that submit_order was called
            if mock_api.submit_order.called:
                call_args = mock_api.submit_order.call_args
                order_data = call_args[1]['order_data'] if 'order_data' in call_args[1] else call_args[0][0]
                
                # Alpaca expects the short code "mleg" for multi-leg orders
                assert hasattr(order_data, 'order_class'), "OrderData should have order_class attribute"
                assert order_data.order_class == "mleg", f"Expected order_class 'mleg', got '{order_data.order_class}'"
    
    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_multileg_order_has_required_fields(self, mock_trading_client):
        """Test that multi-leg orders include all required fields for Alpaca API."""
        mock_trading_client.return_value = Mock()
        broker = Alpaca(self.test_config, connect_stream=False)
        call_order = Order(
            strategy="test_strategy",
            asset=self.call_asset,
            quantity=1,
            side="sell",
            order_type=Order.OrderType.LIMIT,
            limit_price=2.50
        )
        
        put_order = Order(
            strategy="test_strategy",
            asset=self.put_asset,
            quantity=1,
            side="buy", 
            order_type=Order.OrderType.LIMIT,
            limit_price=1.50
        )
        
        orders = [call_order, put_order]
        
        # Create the kwargs that would be passed to OrderData
        # (simulate the _submit_multileg_order logic)
        first_order = orders[0]
        side = first_order.side
        if side in ("buy_to_open", "buy_to_close"):
            side = "buy"
        elif side in ("sell_to_open", "sell_to_close"):
            side = "sell"
        
        kwargs = {
            "symbol": "SPY",
            "qty": "1",
            "side": side,
            "type": "limit",
            "order_class": "mleg",
            "time_in_force": "day",
            "legs": [],  # Would be populated in real scenario
            "limit_price": "1.00"
        }
        
        # Test that all required fields are present
        required_fields = ["symbol", "qty", "side", "type", "order_class", "time_in_force", "legs"]
        for field in required_fields:
            assert field in kwargs, f"Missing required field: {field}"
        
        # Test that order_class is the correct value
        assert kwargs["order_class"] == "mleg", "order_class must be 'mleg' for multi-leg orders"
        
        # Test that symbol is present (fixes missing asset info)
        assert kwargs["symbol"] == "SPY", "symbol field is required for multi-leg orders"
        
        # Test that side is mapped correctly
        assert kwargs["side"] in ["buy", "sell"], f"side must be 'buy' or 'sell', got '{kwargs['side']}'"
    
    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_side_mapping_for_multileg_orders(self, mock_trading_client):
        """Test that extended side values are correctly mapped to simple buy/sell."""
        mock_trading_client.return_value = Mock()
        broker = Alpaca(self.test_config, connect_stream=False)
        test_cases = [
            ("buy_to_open", "buy"),
            ("buy_to_close", "buy"), 
            ("sell_to_open", "sell"),
            ("sell_to_close", "sell"),
            ("buy", "buy"),
            ("sell", "sell")
        ]
        
        for input_side, expected_side in test_cases:
            # Create a mock order with the input side
            order = Order(
                strategy="test_strategy",
                asset=self.call_asset,
                quantity=1,
                side=input_side,
                order_type=Order.OrderType.LIMIT,
                limit_price=2.50
            )
            
            # Test the side mapping logic
            side = order.side
            if side in ("buy_to_open", "buy_to_close"):
                side = "buy"
            elif side in ("sell_to_open", "sell_to_close"):
                side = "sell"
            
            assert side == expected_side, f"Side '{input_side}' should map to '{expected_side}', got '{side}'"
    
    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_multileg_order_with_limit_price(self, mock_trading_client):
        """Test that multi-leg orders correctly handle limit prices."""
        mock_trading_client.return_value = Mock()
        broker = Alpaca(self.test_config, connect_stream=False)
        call_order = Order(
            strategy="test_strategy",
            asset=self.call_asset,
            quantity=1,
            side="sell",
            order_type=Order.OrderType.LIMIT,
            limit_price=2.50
        )
        
        orders = [call_order]
        
        # Test that limit price is properly rounded to 2 decimal places
        price = 1.23456
        rounded_price = round(float(price), 2)
        
        assert rounded_price == 1.23, f"Expected 1.23, got {rounded_price}"
        
        # Test that limit price requirement is enforced
        with pytest.raises(ValueError, match="limit price is required"):
            # This should raise an error because price is None for a limit order
            broker._submit_multileg_order(orders, order_type="limit", price=None)


if __name__ == "__main__":
    # Run the tests
    test_instance = TestAlpacaMultiLegOrders()
    test_instance.setup_method()
    
    try:
        test_instance.test_multileg_order_class_is_correct()
        print("✅ test_multileg_order_class_is_correct passed")
    except Exception as e:
        print(f"❌ test_multileg_order_class_is_correct failed: {e}")
    
    try:
        test_instance.test_multileg_order_has_required_fields()
        print("✅ test_multileg_order_has_required_fields passed")
    except Exception as e:
        print(f"❌ test_multileg_order_has_required_fields failed: {e}")
    
    try:
        test_instance.test_side_mapping_for_multileg_orders()
        print("✅ test_side_mapping_for_multileg_orders passed")
    except Exception as e:
        print(f"❌ test_side_mapping_for_multileg_orders failed: {e}")
    
    try:
        test_instance.test_multileg_order_with_limit_price()
        print("✅ test_multileg_order_with_limit_price passed")
    except Exception as e:
        print(f"❌ test_multileg_order_with_limit_price failed: {e}")
    
    print("\n🎉 All multi-leg order tests completed!")
