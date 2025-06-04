import unittest
from decimal import Decimal
from datetime import datetime, timedelta

from lumibot.strategies import Strategy
from lumibot.entities import Asset, Order
from lumibot.backtesting import YahooDataBacktesting, BacktestingBroker


class CashTestStrategy(Strategy):
    """Test strategy for cash functionality testing"""
    
    def initialize(self):
        self.sleeptime = "1D"
        self.tqqq_asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
        
    def on_trading_iteration(self):
        # Simulate the exact code pattern that was causing TypeError
        current_price = 150.0
        
        # This is the line that was failing with TypeError before the fix
        available_cash = self.get_cash()
        shares = int(available_cash // current_price)
        
        # Store results for testing
        self.last_available_cash = available_cash
        self.last_shares = shares


class TestCash(unittest.TestCase):
    """Comprehensive test suite for cash functionality in Lumibot strategies"""
    
    def setUp(self):
        """Set up test environment before each test"""
        self.start_date = datetime(2024, 1, 1)
        self.end_date = datetime(2024, 1, 31)
        
        self.data_source = YahooDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        self.broker = BacktestingBroker(self.data_source)
        
        # Create strategy
        self.strategy = CashTestStrategy(
            broker=self.broker,
            budget=100000,
            name="CashTestStrategy"
        )
    
    # Basic Cash Consistency Tests
    def test_cash_property_never_none(self):
        """Test that self.cash property never returns None"""
        cash_value = self.strategy.cash
        self.assertIsNotNone(cash_value, "self.cash should never return None")
        self.assertIsInstance(cash_value, (int, float), "self.cash should return a numeric value")
    
    def test_get_cash_method_never_none(self):
        """Test that get_cash() method never returns None"""
        cash_value = self.strategy.get_cash()
        self.assertIsNotNone(cash_value, "get_cash() should never return None")
        self.assertIsInstance(cash_value, (int, float), "get_cash() should return a numeric value")
    
    def test_cash_property_and_method_consistency(self):
        """Test that self.cash and self.get_cash() always return the same value"""
        property_value = self.strategy.cash
        method_value = self.strategy.get_cash()
        
        self.assertEqual(property_value, method_value, 
                        f"self.cash ({property_value}) and self.get_cash() ({method_value}) should return the same value")
    
    def test_initial_cash_equals_budget(self):
        """Test that initial cash equals the budget provided"""
        expected_budget = 100000
        
        property_cash = self.strategy.cash
        method_cash = self.strategy.get_cash()
        
        self.assertEqual(property_cash, expected_budget, 
                        f"Initial self.cash should equal budget: {expected_budget}")
        self.assertEqual(method_cash, expected_budget, 
                        f"Initial get_cash() should equal budget: {expected_budget}")
    
    def test_no_private_cash_variable(self):
        """Test that we no longer have a problematic _cash variable"""
        # This test ensures we don't reintroduce the _cash variable
        self.assertFalse(hasattr(self.strategy, '_cash'),
                        "Strategy should not have a _cash private variable to avoid sync issues")
    
    # Cash Position Updates
    def test_cash_after_position_creation(self):
        """Test cash consistency after creating a cash position"""
        new_cash_amount = 50000
        
        # Set new cash position
        self.strategy._set_cash_position(new_cash_amount)
        
        # Both methods should return the new amount
        property_cash = self.strategy.cash
        method_cash = self.strategy.get_cash()
        
        self.assertEqual(property_cash, new_cash_amount,
                        f"self.cash should equal {new_cash_amount} after _set_cash_position")
        self.assertEqual(method_cash, new_cash_amount,
                        f"get_cash() should equal {new_cash_amount} after _set_cash_position")
        self.assertEqual(property_cash, method_cash,
                        "self.cash and get_cash() should remain consistent after position change")
    
    def test_cash_after_trade_simulation(self):
        """Test cash consistency after simulating a trade"""
        initial_cash = self.strategy.cash
        
        # Simulate buying something (reduce cash)
        trade_amount = 10000
        new_cash = initial_cash - trade_amount
        
        self.strategy._set_cash_position(new_cash)
        
        # Check consistency
        property_cash = self.strategy.cash
        method_cash = self.strategy.get_cash()
        
        self.assertEqual(property_cash, new_cash,
                        f"self.cash should equal {new_cash} after simulated trade")
        self.assertEqual(method_cash, new_cash,
                        f"get_cash() should equal {new_cash} after simulated trade")
        self.assertEqual(property_cash, method_cash,
                        "self.cash and get_cash() should remain consistent after trade")
    
    def test_cash_multiple_position_updates(self):
        """Test cash consistency after multiple position updates"""
        test_amounts = [100000, 75000, 50000, 25000, 0, 10000]
        
        for amount in test_amounts:
            with self.subTest(amount=amount):
                self.strategy._set_cash_position(amount)
                
                property_cash = self.strategy.cash
                method_cash = self.strategy.get_cash()
                
                self.assertEqual(property_cash, amount,
                                f"self.cash should equal {amount}")
                self.assertEqual(method_cash, amount,
                                f"get_cash() should equal {amount}")
                self.assertEqual(property_cash, method_cash,
                                f"Both methods should be consistent for amount {amount}")
    
    # Edge Cases
    def test_cash_with_zero_value(self):
        """Test cash consistency when cash is zero"""
        self.strategy._set_cash_position(0)
        
        property_cash = self.strategy.cash
        method_cash = self.strategy.get_cash()
        
        self.assertEqual(property_cash, 0, "self.cash should handle zero value correctly")
        self.assertEqual(method_cash, 0, "get_cash() should handle zero value correctly")
        self.assertEqual(property_cash, method_cash, "Both should return 0 consistently")
    
    def test_cash_with_decimal_values(self):
        """Test cash consistency with decimal values"""
        decimal_amount = 12345.67
        
        self.strategy._set_cash_position(decimal_amount)
        
        property_cash = self.strategy.cash
        method_cash = self.strategy.get_cash()
        
        self.assertAlmostEqual(property_cash, decimal_amount, places=2,
                              msg="self.cash should handle decimal values correctly")
        self.assertAlmostEqual(method_cash, decimal_amount, places=2,
                              msg="get_cash() should handle decimal values correctly")
        self.assertEqual(property_cash, method_cash,
                        "Both should return same decimal value consistently")
    
    # Mathematical Operations (TypeError Prevention)
    def test_cash_division_operation(self):
        """Test that cash can be used in division operations (prevents TypeError: NoneType division)"""
        current_price = 150.0
        
        # Test with property
        available_cash_property = self.strategy.cash
        self.assertIsNotNone(available_cash_property, "Cash property should not be None for division")
        
        # This should not raise a TypeError
        try:
            shares_property = int(available_cash_property // current_price)
            self.assertIsInstance(shares_property, int, "Division should produce integer shares")
        except TypeError as e:
            self.fail(f"self.cash division failed: {e}")
        
        # Test with method
        available_cash_method = self.strategy.get_cash()
        self.assertIsNotNone(available_cash_method, "get_cash() should not be None for division")
        
        # This should not raise a TypeError
        try:
            shares_method = int(available_cash_method // current_price)
            self.assertIsInstance(shares_method, int, "Division should produce integer shares")
        except TypeError as e:
            self.fail(f"get_cash() division failed: {e}")
        
        # Both should calculate the same number of shares
        self.assertEqual(shares_property, shares_method,
                        "Both cash methods should calculate same number of shares")
    
    def test_division_with_different_prices(self):
        """Test the division operation with various stock prices"""
        test_prices = [1.0, 10.0, 50.0, 100.0, 150.0, 500.0, 1000.0]
        
        for price in test_prices:
            with self.subTest(price=price):
                try:
                    available_cash = self.strategy.get_cash()
                    shares = int(available_cash // price)
                    
                    self.assertIsInstance(shares, int, f"Shares calculation failed for price {price}")
                    self.assertGreaterEqual(shares, 0, f"Shares should be non-negative for price {price}")
                    
                    # Validate the math
                    expected_shares = int(available_cash // price)
                    self.assertEqual(shares, expected_shares, 
                                   f"Share calculation incorrect for price {price}")
                    
                except TypeError as e:
                    self.fail(f"Division failed for price {price}: {e}")
    
    def test_strategy_mathematical_operations(self):
        """Test that strategy can perform the exact mathematical operations that were failing"""
        current_price = 150.0
        
        try:
            # Run one iteration of the strategy which does the problematic operation
            self.strategy.on_trading_iteration()
            
            # Check that the strategy stored the values correctly
            self.assertIsNotNone(self.strategy.last_available_cash, 
                                "Strategy should have stored available_cash")
            self.assertIsInstance(self.strategy.last_shares, int, 
                                "Strategy should have calculated integer shares")
            
        except Exception as e:
            self.fail(f"Strategy mathematical operations failed with error: {e}")
    
    # Multiple Calls and Consistency
    def test_multiple_get_cash_calls(self):
        """Test multiple consecutive get_cash() calls return consistent values"""
        # Call get_cash multiple times in succession
        cash_values = []
        for i in range(10):
            cash_value = self.strategy.get_cash()
            cash_values.append(cash_value)
            self.assertIsNotNone(cash_value, f"get_cash() call {i+1} returned None")
        
        # All values should be the same
        first_value = cash_values[0]
        for i, value in enumerate(cash_values):
            self.assertEqual(value, first_value, 
                           f"get_cash() call {i+1} returned different value: {value} vs {first_value}")
    
    def test_cash_with_property_and_method_interleaved(self):
        """Test alternating between self.cash property and get_cash() method"""
        for i in range(5):
            property_value = self.strategy.cash
            method_value = self.strategy.get_cash()
            
            self.assertIsNotNone(property_value, f"Iteration {i+1}: self.cash returned None")
            self.assertIsNotNone(method_value, f"Iteration {i+1}: get_cash() returned None")
            self.assertEqual(property_value, method_value, 
                           f"Iteration {i+1}: values differ - property: {property_value}, method: {method_value}")
    
    # Backtest Lifecycle
    def test_cash_consistency_during_backtest_lifecycle(self):
        """Test cash consistency during different phases of backtest lifecycle"""
        # Test at strategy creation
        initial_cash = self.strategy.get_cash()
        self.assertIsNotNone(initial_cash, "Cash should not be None at creation")
        
        # Test during initialize
        try:
            self.strategy.initialize()
            post_init_cash = self.strategy.get_cash()
            self.assertIsNotNone(post_init_cash, "Cash should not be None after initialize")
            self.assertEqual(initial_cash, post_init_cash, "Cash should remain consistent")
        except Exception as e:
            self.fail(f"Initialize phase failed: {e}")
        
        # Test during trading iteration
        try:
            self.strategy.on_trading_iteration()
            post_iteration_cash = self.strategy.get_cash()
            self.assertIsNotNone(post_iteration_cash, "Cash should not be None after iteration")
        except Exception as e:
            self.fail(f"Trading iteration phase failed: {e}")
    
    def test_backtest_setup_cash_handling(self):
        """Test cash handling during different backtest setups"""
        try:
            start_date = datetime(2024, 3, 1)
            end_date = datetime(2025, 5, 29)
            
            data_source = YahooDataBacktesting(
                datetime_start=start_date,
                datetime_end=end_date
            )
            
            broker = BacktestingBroker(data_source)
            
            strategy = CashTestStrategy(
                broker=broker,
                budget=100000,
                name="BacktestCashTest"
            )
            
            # Test that cash operations work immediately after creation
            cash_value = strategy.get_cash()
            self.assertIsNotNone(cash_value, "Cash should not be None in backtest setup")
            self.assertEqual(cash_value, 100000, "Cash should equal initial budget")
            
            # Test the problematic operation
            current_price = 150.0
            shares = int(cash_value // current_price)
            self.assertIsInstance(shares, int, "Share calculation should work")
            
        except Exception as e:
            self.fail(f"Backtest setup cash handling failed: {e}")


class TestCashWithDifferentBudgets(unittest.TestCase):
    """Test cash consistency with different initial budgets"""
    
    def test_various_budget_amounts(self):
        """Test cash consistency with various budget amounts"""
        budgets = [10000, 50000, 100000, 250000, 1000000]
        
        for budget in budgets:
            with self.subTest(budget=budget):
                # Create data source and broker
                start_date = datetime(2024, 1, 1)
                end_date = datetime(2024, 1, 31)
                
                data_source = YahooDataBacktesting(
                    datetime_start=start_date,
                    datetime_end=end_date
                )
                
                broker = BacktestingBroker(data_source)
                
                # Create strategy with specific budget
                strategy = CashTestStrategy(
                    broker=broker,
                    budget=budget,
                    name=f"CashTestStrategy_{budget}"
                )
                
                # Test consistency
                property_cash = strategy.cash
                method_cash = strategy.get_cash()
                
                self.assertEqual(property_cash, budget,
                                f"self.cash should equal budget {budget}")
                self.assertEqual(method_cash, budget,
                                f"get_cash() should equal budget {budget}")
                self.assertEqual(property_cash, method_cash,
                                f"Both methods should be consistent for budget {budget}")


if __name__ == '__main__':
    unittest.main() 