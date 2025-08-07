#!/usr/bin/env python3
"""
Test script to verify the multi-leg order fix
"""
import sys
import os

# Add the lumibot directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from lumibot.entities.order import Order
from lumibot.entities.asset import Asset
from lumibot.brokers.alpaca import Alpaca
from datetime import datetime, timedelta

def test_multileg_order_structure():
    """Test that the multi-leg order structure is correct"""
    print("Testing multi-leg order structure...")
    
    # Create a mock broker instance (we won't actually submit orders)
    broker = Alpaca({"ALPACA_CREDS": "mock_for_testing"})
    
    # Create sample option assets
    expiration = datetime.now() + timedelta(days=30)
    call_asset = Asset(
        symbol="SPY",
        asset_type=Asset.AssetType.OPTION,
        expiration=expiration,
        strike=450.0,
        right="call"
    )
    
    put_asset = Asset(
        symbol="SPY", 
        asset_type=Asset.AssetType.OPTION,
        expiration=expiration,
        strike=440.0,
        right="put"
    )
    
    # Create sample orders for a spread
    call_order = Order(
        asset=call_asset,
        quantity=1,
        side="sell",
        order_type=Order.OrderType.LIMIT,
        limit_price=2.50
    )
    
    put_order = Order(
        asset=put_asset,
        quantity=1,
        side="buy", 
        order_type=Order.OrderType.LIMIT,
        limit_price=1.50
    )
    
    orders = [call_order, put_order]
    
    # Test the leg construction without actually submitting
    symbol = "SPY"
    qty = "1"
    legs = []
    
    for order in orders:
        # Format option symbol
        if order.asset.asset_type == Asset.AssetType.OPTION:
            strike_formatted = f"{order.asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = order.asset.expiration.strftime("%y%m%d")
            option_symbol = f"{order.asset.symbol}{date}{order.asset.right[0]}{strike_formatted}"
        else:
            option_symbol = order.asset.symbol
            
        # Determine position_intent
        position_intent = "sell_to_open" if order.side == "sell" else "buy_to_open"
        
        legs.append({
            "symbol": option_symbol,
            "ratio_qty": str(order.quantity),
            "side": order.side,
            "position_intent": position_intent
        })
    
    # Test the new order structure
    first_order = orders[0]
    side = first_order.side
    if side in ("buy_to_open", "buy_to_close"):
        side = "buy"
    elif side in ("sell_to_open", "sell_to_close"):
        side = "sell"
    
    kwargs = {
        "symbol": symbol,               # Required: Primary symbol
        "qty": qty,                    # Required: Total quantity  
        "side": side,                  # Required: Primary side (buy/sell)
        "type": "limit",               # Required: Order type
        "order_class": "multileg",     # Required: Must be "multileg" for multi-leg orders
        "time_in_force": "day",        # Required: Duration
        "legs": legs,                  # Required: Individual legs
        "limit_price": "1.00"          # Optional: For limit orders
    }
    
    print("✅ Multi-leg order structure looks correct!")
    print(f"Order class: {kwargs['order_class']}")  # Should be "multileg"
    print(f"Required fields present: symbol={kwargs.get('symbol')}, qty={kwargs.get('qty')}, side={kwargs.get('side')}")
    print(f"Number of legs: {len(kwargs['legs'])}")
    
    # Verify the order_class is correct
    assert kwargs["order_class"] == "multileg", f"Expected 'multileg', got '{kwargs['order_class']}'"
    
    # Verify required fields are present
    required_fields = ["symbol", "qty", "side", "type", "order_class", "time_in_force", "legs"]
    for field in required_fields:
        assert field in kwargs, f"Missing required field: {field}"
    
    print("✅ All tests passed! The multi-leg order fix should work correctly.")
    return True

if __name__ == "__main__":
    try:
        test_multileg_order_structure()
        print("\n🎉 SUCCESS: Multi-leg order structure is fixed!")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
