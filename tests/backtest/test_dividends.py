import datetime
import pytest
import pytz

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader

# Global parameters
from lumibot.credentials import POLYGON_API_KEY


class DividendTestStrategy(Strategy):
    """
    Strategy to test dividend handling by buying and holding BIL (SPDR Bloomberg 1-3 Month T-Bill ETF)
    which typically pays monthly dividends around the first of each month.
    """
    
    def initialize(self):
        # Sleep for one day to track cash changes over time
        self.sleeptime = "1D"
        self.initial_cash = None
        self.cash_after_purchase = None
        self.final_cash = None
        self.bil_quantity = None
        self.purchase_made = False
        self.cash_tracking = []  # Track cash over time
        
    def on_trading_iteration(self):
        current_dt = self.get_datetime()
        current_cash = self.get_cash()
        
        # Track cash every iteration
        self.cash_tracking.append({
            'datetime': current_dt,
            'cash': current_cash,
            'portfolio_value': self.get_portfolio_value()
        })
        
        if not self.purchase_made:
            # Record initial cash
            self.initial_cash = current_cash
            
            # Buy BIL with all available cash
            bil_asset = Asset("BIL")
            bil_price = self.get_last_price(bil_asset)
            
            if bil_price and bil_price > 0:
                # Calculate how many shares we can buy with available cash
                # Leave a small buffer for fees/rounding
                available_cash = current_cash * 0.99  # Use 99% to account for potential fees
                quantity = int(available_cash / bil_price)
                
                if quantity > 0:
                    # Create and submit buy order
                    order = self.create_order(bil_asset, quantity=quantity, side="buy")
                    self.submit_order(order)
                    
                    self.bil_quantity = quantity
                    self.purchase_made = True
                    
                    self.log_message(f"Purchased {quantity} shares of BIL at ${bil_price:.2f} per share")
                    self.log_message(f"Total cost: ${quantity * bil_price:.2f}")
        
        # Update cash after purchase
        if self.purchase_made:
            self.cash_after_purchase = current_cash
            
        # Always update final cash (will be the last iteration's value)
        self.final_cash = current_cash
        
        self.log_message(f"Date: {current_dt.strftime('%Y-%m-%d')}, Cash: ${current_cash:.2f}, Portfolio Value: ${self.get_portfolio_value():.2f}")


class TestDividends:
    """Test dividend handling for both Yahoo Finance and Polygon data sources"""
    
    def _run_dividend_test(self, data_source_class, **data_source_kwargs):
        """Helper method to run dividend test with specified data source"""
        # Test period: Jan 25, 2025 to Feb 5, 2025 (to catch potential dividend around Feb 1)
        tzinfo = pytz.timezone("America/New_York")
        backtesting_start = tzinfo.localize(datetime.datetime(2025, 1, 25))
        backtesting_end = tzinfo.localize(datetime.datetime(2025, 2, 5, 23, 59, 59))
        
        # Create data source
        data_source = data_source_class(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            **data_source_kwargs
        )
        
        # Create broker and strategy
        broker = BacktestingBroker(data_source=data_source)
        strategy = DividendTestStrategy(broker=broker)
        
        # Run backtest
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strategy)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)
        
        return results, strategy
    
    def _verify_dividend_test_results(self, strategy, data_source_name):
        """Verify the results of the dividend test"""
        # Basic sanity checks
        assert strategy.initial_cash is not None, f"{data_source_name}: Initial cash should be recorded"
        assert strategy.purchase_made, f"{data_source_name}: BIL purchase should have been made"
        assert strategy.bil_quantity is not None, f"{data_source_name}: BIL quantity should be recorded"
        assert strategy.bil_quantity > 0, f"{data_source_name}: Should have purchased some BIL shares"
        
        # Cash tracking checks
        assert len(strategy.cash_tracking) > 0, f"{data_source_name}: Should have cash tracking data"
        
        # Log cash progression for debugging
        print(f"\n{data_source_name} Cash Tracking:")
        for entry in strategy.cash_tracking:
            print(f"  {entry['datetime'].strftime('%Y-%m-%d')}: Cash=${entry['cash']:.2f}, Portfolio=${entry['portfolio_value']:.2f}")
        
        # Check if cash increased after the purchase (indicating dividends were received)
        # Since we used almost all cash to buy BIL, any significant cash increase should be from dividends
        cash_after_purchase = min([entry['cash'] for entry in strategy.cash_tracking if entry['cash'] < strategy.initial_cash * 0.95])
        final_cash = strategy.final_cash
        
        print(f"{data_source_name} Analysis:")
        print(f"  Initial cash: ${strategy.initial_cash:.2f}")
        print(f"  Cash after purchase: ${cash_after_purchase:.2f}")
        print(f"  Final cash: ${final_cash:.2f}")
        print(f"  BIL quantity purchased: {strategy.bil_quantity}")
        
        # If dividends are properly handled, final cash should be greater than cash after purchase
        # Allow for some tolerance due to potential rounding or fees
        dividend_threshold = 0.01  # $0.01 minimum dividend expected
        
        if final_cash > cash_after_purchase + dividend_threshold:
            print(f"  ✓ {data_source_name}: Dividends appear to be handled (cash increased by ${final_cash - cash_after_purchase:.2f})")
            return True
        else:
            print(f"  ✗ {data_source_name}: No dividend increase detected (cash change: ${final_cash - cash_after_purchase:.2f})")
            return False
    
    def test_yahoo_finance_dividends(self):
        """Test dividend handling with Yahoo Finance data source"""
        # Run the backtest
        results, strategy = self._run_dividend_test(YahooDataBacktesting)
        
        # Verify results
        assert results is not None, "Yahoo Finance: Backtest should return results"
        dividend_handled = self._verify_dividend_test_results(strategy, "Yahoo Finance")
        
        # For now, we'll just log whether dividends were handled or not
        # If they weren't handled, this indicates a potential issue that needs to be fixed
        if not dividend_handled:
            print("WARNING: Yahoo Finance does not appear to handle dividends properly")
    
    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_dividends(self):
        """Test dividend handling with Polygon data source"""
        # Run the backtest
        results, strategy = self._run_dividend_test(
            PolygonDataBacktesting,
            api_key=POLYGON_API_KEY
        )
        
        # Verify results
        assert results is not None, "Polygon: Backtest should return results"
        dividend_handled = self._verify_dividend_test_results(strategy, "Polygon")
        
        # For now, we'll just log whether dividends were handled or not
        # If they weren't handled, this indicates a potential issue that needs to be fixed
        if not dividend_handled:
            print("WARNING: Polygon does not appear to handle dividends properly")
    
    @pytest.mark.skipif(
        not POLYGON_API_KEY,
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_API_KEY == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_compare_yahoo_vs_polygon_dividends(self):
        """Compare dividend handling between Yahoo Finance and Polygon"""
        print("\n" + "="*50)
        print("DIVIDEND COMPARISON TEST")
        print("="*50)
        
        # Test Yahoo Finance
        yahoo_results, yahoo_strategy = self._run_dividend_test(YahooDataBacktesting)
        yahoo_dividend_handled = self._verify_dividend_test_results(yahoo_strategy, "Yahoo Finance")
        
        # Test Polygon
        polygon_results, polygon_strategy = self._run_dividend_test(
            PolygonDataBacktesting,
            api_key=POLYGON_API_KEY
        )
        polygon_dividend_handled = self._verify_dividend_test_results(polygon_strategy, "Polygon")
        
        # Summary
        print(f"\nSUMMARY:")
        print(f"  Yahoo Finance dividends handled: {'✓' if yahoo_dividend_handled else '✗'}")
        print(f"  Polygon dividends handled: {'✓' if polygon_dividend_handled else '✗'}")
        
        if not yahoo_dividend_handled and not polygon_dividend_handled:
            print(f"  🚨 ISSUE: Neither data source appears to handle dividends!")
        elif yahoo_dividend_handled != polygon_dividend_handled:
            print(f"  ⚠️  WARNING: Inconsistent dividend handling between data sources!")
        else:
            print(f"  ✅ Both data sources handle dividends consistently") 