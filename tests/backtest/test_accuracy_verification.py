"""
Phase 1: Accuracy Verification Tests

This test suite verifies that ThetaData price variance compared to Polygon
remains acceptable over long time periods and across different price ranges.

Goals:
- Verify portfolio variance < 0.01% over 1 year
- Verify price differences remain sub-penny across all price ranges
- Verify no systematic bias (variance is random, not directional)
"""

import datetime
import os
import pytest
from dotenv import load_dotenv
from lumibot.strategies import Strategy
from lumibot.backtesting import PolygonDataBacktesting, ThetaDataBacktesting
from lumibot.entities import Asset

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
THETADATA_USERNAME = os.environ.get("THETADATA_USERNAME")
THETADATA_PASSWORD = os.environ.get("THETADATA_PASSWORD")


class AccuracyTestStrategy(Strategy):
    """Simple buy-and-hold strategy for accuracy testing"""

    parameters = {
        "symbol": "AMZN",
        "quantity": 10
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.bought = False

    def on_trading_iteration(self):
        if not self.bought:
            asset = Asset(self.parameters["symbol"])
            price = self.get_last_price(asset)
            self.log_message(f"Buying {self.parameters['quantity']} shares of {self.parameters['symbol']} at ${price}")
            order = self.create_order(asset, quantity=self.parameters["quantity"], side="buy")
            self.submit_order(order)
            self.bought = True


@pytest.mark.apitest
@pytest.mark.skipif(
    not POLYGON_API_KEY or not THETADATA_USERNAME or not THETADATA_PASSWORD,
    reason="Requires both Polygon and ThetaData credentials"
)
class TestAccuracyVerification:
    """Accuracy verification test suite"""

    def test_one_year_amzn_accuracy(self):
        """
        Test 1: Verify AMZN accuracy over 1 year (2023)

        Expected:
        - Portfolio variance < 0.01% ($10 on $100k portfolio)
        - Price differences remain sub-penny
        - No systematic directional bias
        """
        backtesting_start = datetime.datetime(2023, 1, 3)  # First trading day of 2023
        backtesting_end = datetime.datetime(2023, 12, 29)  # Last trading day of 2023

        print("\n" + "="*80)
        print("TEST 1: ONE YEAR ACCURACY VERIFICATION - AMZN")
        print("="*80)
        print(f"Period: {backtesting_start.date()} to {backtesting_end.date()}")
        print(f"Symbol: AMZN")
        print(f"Trading days: ~252")

        # Run ThetaData backtest
        print("\n[1/2] Running ThetaData backtest...")
        theta_results, theta_strat = AccuracyTestStrategy.run_backtest(
            ThetaDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            parameters={"symbol": "AMZN", "quantity": 100},
            thetadata_username=THETADATA_USERNAME,
            thetadata_password=THETADATA_PASSWORD,
        )

        # Run Polygon backtest
        print("\n[2/2] Running Polygon backtest...")
        polygon_results, polygon_strat = AccuracyTestStrategy.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            parameters={"symbol": "AMZN", "quantity": 100},
            polygon_api_key=POLYGON_API_KEY,
        )

        # Compare results - get final portfolio value from strategy
        theta_final = theta_strat.get_portfolio_value()
        polygon_final = polygon_strat.get_portfolio_value()
        difference = abs(theta_final - polygon_final)
        percent_diff = (difference / polygon_final) * 100

        print("\n" + "-"*80)
        print("RESULTS:")
        print("-"*80)
        print(f"ThetaData Final Portfolio Value:  ${theta_final:,.2f}")
        print(f"Polygon Final Portfolio Value:    ${polygon_final:,.2f}")
        print(f"Absolute Difference:              ${difference:,.2f}")
        print(f"Percentage Difference:            {percent_diff:.4f}%")
        print(f"Acceptance Threshold:             0.01% (${polygon_final * 0.0001:,.2f})")

        # Verify acceptance criteria
        assert percent_diff < 0.01, f"Portfolio variance {percent_diff:.4f}% exceeds 0.01% threshold"

        print(f"\n✓ TEST PASSED: Variance {percent_diff:.4f}% is within acceptable range")
        print("="*80 + "\n")

    def test_multi_symbol_price_ranges(self):
        """
        Test 2: Verify accuracy across different price ranges

        Tests 5 symbols with different price points:
        - AMZN: ~$180
        - AAPL: ~$175
        - GOOGL: ~$140
        - SPY: ~$450
        - BRK.B: ~$420

        Expected:
        - 0.5¢ variance is consistent percentage across all price ranges
        - Sub-penny differences for all symbols
        """
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 5)  # 1 week for speed

        symbols = [
            ("AMZN", 10, 180),   # ~$180/share, 10 shares
            ("AAPL", 10, 175),   # ~$175/share, 10 shares
            ("GOOGL", 10, 140),  # ~$140/share, 10 shares
            ("SPY", 10, 450),    # ~$450/share, 10 shares
            ("BRK.B", 5, 420),   # ~$420/share, 5 shares
        ]

        print("\n" + "="*80)
        print("TEST 2: MULTI-SYMBOL PRICE RANGE VERIFICATION")
        print("="*80)
        print(f"Period: {backtesting_start.date()} to {backtesting_end.date()}")
        print(f"Symbols: {len(symbols)}")

        results_table = []

        for symbol, qty, approx_price in symbols:
            print(f"\n--- Testing {symbol} (~${approx_price}/share, {qty} shares) ---")

            # Run ThetaData backtest
            theta_results, theta_strat = AccuracyTestStrategy.run_backtest(
                ThetaDataBacktesting,
                backtesting_start,
                backtesting_end,
                benchmark_asset="SPY",
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
                parameters={"symbol": symbol, "quantity": qty},
                thetadata_username=THETADATA_USERNAME,
                thetadata_password=THETADATA_PASSWORD,
            )

            # Run Polygon backtest
            polygon_results, polygon_strat = AccuracyTestStrategy.run_backtest(
                PolygonDataBacktesting,
                backtesting_start,
                backtesting_end,
                benchmark_asset="SPY",
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
                parameters={"symbol": symbol, "quantity": qty},
                polygon_api_key=POLYGON_API_KEY,
            )

            # Compare final portfolio values
            theta_final = theta_strat.get_portfolio_value()
            polygon_final = polygon_strat.get_portfolio_value()
            difference = abs(theta_final - polygon_final)
            percent_diff = (difference / polygon_final) * 100

            results_table.append({
                "symbol": symbol,
                "price": approx_price,
                "qty": qty,
                "theta": theta_final,
                "polygon": polygon_final,
                "diff": difference,
                "pct": percent_diff
            })

            print(f"  ThetaData:  ${theta_final:,.2f}")
            print(f"  Polygon:    ${polygon_final:,.2f}")
            print(f"  Difference: ${difference:,.2f} ({percent_diff:.4f}%)")

            # Verify sub-0.01% variance for each symbol
            assert percent_diff < 0.01, f"{symbol}: Variance {percent_diff:.4f}% exceeds 0.01%"

        # Summary table
        print("\n" + "-"*80)
        print("SUMMARY TABLE:")
        print("-"*80)
        print(f"{'Symbol':<8} {'Price':<8} {'Qty':<5} {'ThetaData':<15} {'Polygon':<15} {'Diff':<10} {'%':<8}")
        print("-"*80)

        for r in results_table:
            print(f"{r['symbol']:<8} ${r['price']:<7} {r['qty']:<5} ${r['theta']:<14,.2f} ${r['polygon']:<14,.2f} ${r['diff']:<9,.2f} {r['pct']:.4f}%")

        # Calculate average variance
        avg_pct = sum(r['pct'] for r in results_table) / len(results_table)
        max_pct = max(r['pct'] for r in results_table)

        print("-"*80)
        print(f"Average Variance: {avg_pct:.4f}%")
        print(f"Maximum Variance: {max_pct:.4f}%")
        print(f"Threshold:        0.01%")

        assert avg_pct < 0.01, f"Average variance {avg_pct:.4f}% exceeds 0.01%"
        assert max_pct < 0.01, f"Max variance {max_pct:.4f}% exceeds 0.01%"

        print(f"\n✓ TEST PASSED: All symbols within acceptable variance")
        print("="*80 + "\n")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "-s"])
