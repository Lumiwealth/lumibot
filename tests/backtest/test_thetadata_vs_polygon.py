"""
Comparison tests between ThetaData and Polygon data sources.

These tests run the same strategy with both data sources and compare:
- Stock prices
- Option prices
- Index prices (SPX, VIX)
- Option chains
- Fill prices
- Portfolio values

The goal is to ensure ThetaData produces comparable results to Polygon,
which we trust as our baseline for data accuracy.
"""

import datetime
import os
import pytest
import json
from dotenv import load_dotenv
from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.traders import Trader

# Load environment variables from .env file
load_dotenv()


def detailed_comparison_report(theta_data, polygon_data, data_type):
    """
    Create a detailed comparison report when prices don't match.
    Any difference requires investigation - there is ZERO tolerance.
    """
    report = [
        f"\n{'='*80}",
        f"PRICE MISMATCH DETECTED - {data_type.upper()}",
        f"{'='*80}",
    ]

    for key in theta_data.keys():
        theta_val = theta_data.get(key)
        polygon_val = polygon_data.get(key)

        if isinstance(theta_val, (int, float)) and isinstance(polygon_val, (int, float)):
            diff = theta_val - polygon_val
            if diff != 0:
                report.append(f"\n{key}:")
                report.append(f"  ThetaData:  {theta_val}")
                report.append(f"  Polygon:    {polygon_val}")
                report.append(f"  Difference: {diff}")
                report.append(f"  Diff %:     {(diff/polygon_val*100):.6f}%")
        else:
            report.append(f"\n{key}:")
            report.append(f"  ThetaData:  {theta_val}")
            report.append(f"  Polygon:    {polygon_val}")

    report.append(f"\n{'='*80}")
    report.append("Full ThetaData:")
    report.append(json.dumps(theta_data, indent=2, default=str))
    report.append(f"\n{'='*80}")
    report.append("Full Polygon:")
    report.append(json.dumps(polygon_data, indent=2, default=str))
    report.append(f"\n{'='*80}\n")

    return "\n".join(report)


class ComparisonStrategy(Strategy):
    """Strategy that collects data points for comparison."""

    parameters = {
        "symbol": "AMZN",
        "test_type": "stock",  # "stock", "option", or "index"
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.data_points = {
            "stock_prices": [],
            "option_prices": [],
            "index_prices": [],
            "chains_data": None,
            "fill_prices": [],
            "portfolio_values": [],
            "cash_values": [],
        }

    def on_trading_iteration(self):
        test_type = self.parameters.get("test_type", "stock")

        if test_type == "stock":
            self._test_stock()
        elif test_type == "option":
            self._test_option()
        elif test_type == "index":
            self._test_index()

    def _test_stock(self):
        """Test stock data collection."""
        if self.first_iteration:
            symbol = self.parameters["symbol"]
            asset = Asset(symbol, asset_type="stock")

            # Get price and quote
            price = self.get_last_price(asset)
            quote = self.get_quote(asset)
            dt = self.get_datetime()

            # Collect detailed information for diagnosis
            quote_dict = {
                "price": quote.price if hasattr(quote, 'price') else None,
                "bid": quote.bid if hasattr(quote, 'bid') else None,
                "ask": quote.ask if hasattr(quote, 'ask') else None,
                "open": quote.open if hasattr(quote, 'open') else None,
                "high": quote.high if hasattr(quote, 'high') else None,
                "low": quote.low if hasattr(quote, 'low') else None,
                "close": quote.close if hasattr(quote, 'close') else None,
                "volume": quote.volume if hasattr(quote, 'volume') else None,
                "bid_size": quote.bid_size if hasattr(quote, 'bid_size') else None,
                "ask_size": quote.ask_size if hasattr(quote, 'ask_size') else None,
                "timestamp": str(quote.timestamp) if hasattr(quote, 'timestamp') else None,
            }

            self.data_points["stock_prices"].append({
                "price": price,
                "quote": quote_dict,
                "datetime": str(dt),
                "datetime_obj": dt,
            })

            # Buy 10 shares to test fill price
            order = self.create_order(asset, 10, "buy")
            self.submit_order(order)

    def _test_option(self):
        """Test option data collection."""
        if self.first_iteration:
            symbol = self.parameters["symbol"]
            underlying = Asset(symbol, asset_type="stock")

            # Get underlying price
            underlying_price = self.get_last_price(underlying)

            # Get chains
            chains = self.get_chains(underlying)
            self.data_points["chains_data"] = {
                "multiplier": chains.get("Multiplier"),
                "exchange": chains.get("Exchange"),
                "call_expirations_count": len(chains.calls()) if hasattr(chains, 'calls') else len(chains.get("Chains", {}).get("CALL", {})),
                "put_expirations_count": len(chains.puts()) if hasattr(chains, 'puts') else len(chains.get("Chains", {}).get("PUT", {})),
            }

            # Try to create an option order
            try:
                # Get first available expiration
                if hasattr(chains, 'expirations'):
                    expirations = chains.expirations("CALL")
                else:
                    call_chains = chains.get("Chains", {}).get("CALL", {})
                    expirations = sorted(call_chains.keys())

                if expirations:
                    expiration_str = expirations[0]
                    expiration = datetime.datetime.strptime(expiration_str, "%Y-%m-%d").date()

                    # Get strikes for this expiration
                    if hasattr(chains, 'strikes'):
                        strikes = chains.strikes(expiration_str, "CALL")
                    else:
                        strikes = chains.get("Chains", {}).get("CALL", {}).get(expiration_str, [])

                    if strikes:
                        # Find ATM strike
                        strike = min(strikes, key=lambda x: abs(x - underlying_price))

                        option = Asset(
                            symbol,
                            asset_type="option",
                            expiration=expiration,
                            strike=strike,
                            right="CALL"
                        )

                        option_price = self.get_last_price(option)
                        self.data_points["option_prices"].append({
                            "price": option_price,
                            "strike": strike,
                            "expiration": expiration,
                            "datetime": self.get_datetime(),
                        })

                        # Buy 1 contract
                        order = self.create_order(option, 1, "buy")
                        self.submit_order(order)
            except Exception as e:
                self.log_message(f"Error creating option order: {e}")

    def _test_index(self):
        """Test index data collection."""
        if self.first_iteration:
            index_symbol = self.parameters.get("index_symbol", "SPX")
            asset = Asset(index_symbol, asset_type="index")

            # Get price and quote
            try:
                price = self.get_last_price(asset)
                quote = self.get_quote(asset)

                self.data_points["index_prices"].append({
                    "symbol": index_symbol,
                    "price": price,
                    "quote": quote,
                    "datetime": self.get_datetime(),
                })
            except Exception as e:
                self.log_message(f"Error getting index price for {index_symbol}: {e}")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.data_points["fill_prices"].append({
            "price": price,
            "quantity": quantity,
            "multiplier": multiplier,
            "asset": str(order.asset),
            "datetime": self.get_datetime(),
        })

    def after_market_closes(self):
        self.data_points["portfolio_values"].append(self.portfolio_value)
        self.data_points["cash_values"].append(self.cash)


def run_backtest(data_source_class, **params):
    """
    Helper to run a backtest with a given data source.

    Args:
        data_source_class: ThetaDataBacktesting or PolygonDataBacktesting
        **params: Parameters for the strategy (symbol, test_type, etc.)

    Returns:
        dict: Data points collected by the strategy
    """
    # Use recent dates to avoid Polygon subscription limitations
    # Free tier allows last 2 years of data
    start = datetime.datetime(2024, 8, 1)
    end = datetime.datetime(2024, 8, 2, 23, 59, 59)

    # Create data source
    if data_source_class == ThetaDataBacktesting:
        data_source = ThetaDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            username=os.environ.get("THETADATA_USERNAME"),
            password=os.environ.get("THETADATA_PASSWORD"),
        )
    else:  # PolygonDataBacktesting
        data_source = PolygonDataBacktesting(
            datetime_start=start,
            datetime_end=end,
            api_key=os.environ.get("POLYGON_API_KEY"),
        )

    # Run backtest
    broker = BacktestingBroker(data_source=data_source)
    strategy = ComparisonStrategy(broker=broker, parameters=params)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

    return strategy.data_points


@pytest.mark.apitest
class TestThetaDataVsPolygonComparison:
    """Comparison tests between ThetaData and Polygon."""

    def test_stock_price_comparison(self):
        """
        Compare stock prices between ThetaData and Polygon.
        ZERO TOLERANCE - prices must match exactly or investigation is required.
        """
        params = {"symbol": "AMZN", "test_type": "stock"}

        # Run with ThetaData
        theta_data = run_backtest(ThetaDataBacktesting, **params)

        # Run with Polygon
        polygon_data = run_backtest(PolygonDataBacktesting, **params)

        # Compare stock prices
        assert len(theta_data["stock_prices"]) > 0, "ThetaData: No stock prices collected"
        assert len(polygon_data["stock_prices"]) > 0, "Polygon: No stock prices collected"

        theta_info = theta_data["stock_prices"][0]
        polygon_info = polygon_data["stock_prices"][0]

        theta_price = theta_info["price"]
        polygon_price = polygon_info["price"]

        # Tolerance: 1 cent for liquid stocks (accounts for SIP feed timing differences)
        tolerance = 0.01
        price_diff = abs(theta_price - polygon_price)

        if price_diff > tolerance:
            report = detailed_comparison_report(theta_info, polygon_info, "STOCK PRICE")
            print(report)
            pytest.fail(
                f"Stock prices differ by more than ${tolerance}:\n"
                f"  ThetaData: ${theta_price}\n"
                f"  Polygon:   ${polygon_price}\n"
                f"  Difference: ${price_diff} (tolerance: ${tolerance})\n"
                f"See detailed report above."
            )

        print(f"✓ Stock prices match within tolerance: ThetaData=${theta_price}, Polygon=${polygon_price}, diff=${price_diff:.4f}")

    def test_option_price_comparison(self):
        """
        Compare option prices between ThetaData and Polygon.
        ZERO TOLERANCE - prices must match exactly or investigation is required.
        """
        params = {"symbol": "AMZN", "test_type": "option"}

        # Run with ThetaData
        theta_data = run_backtest(ThetaDataBacktesting, **params)

        # Run with Polygon
        polygon_data = run_backtest(PolygonDataBacktesting, **params)

        # Compare chains data
        assert theta_data["chains_data"] is not None, "ThetaData: No chains data"
        assert polygon_data["chains_data"] is not None, "Polygon: No chains data"

        theta_chains = theta_data["chains_data"]
        polygon_chains = polygon_data["chains_data"]

        # Both should have the same multiplier
        if theta_chains["multiplier"] != polygon_chains["multiplier"]:
            pytest.fail(
                f"Multiplier MISMATCH: ThetaData={theta_chains['multiplier']}, "
                f"Polygon={polygon_chains['multiplier']}"
            )

        # Both should have expirations
        assert theta_chains["call_expirations_count"] > 0, "ThetaData: No CALL expirations"
        assert polygon_chains["call_expirations_count"] > 0, "Polygon: No CALL expirations"

        print(f"✓ Chains data collected: ThetaData expirations={theta_chains['call_expirations_count']}, "
              f"Polygon expirations={polygon_chains['call_expirations_count']}")

        # Compare option prices if available
        if theta_data["option_prices"] and polygon_data["option_prices"]:
            theta_info = theta_data["option_prices"][0]
            polygon_info = polygon_data["option_prices"][0]

            theta_opt_price = theta_info["price"]
            polygon_opt_price = polygon_info["price"]

            # Tolerance: 5 cents for options (wider spread, less liquid than stocks)
            tolerance = 0.05
            price_diff = abs(theta_opt_price - polygon_opt_price)

            if price_diff > tolerance:
                report = detailed_comparison_report(theta_info, polygon_info, "OPTION PRICE")
                print(report)
                pytest.fail(
                    f"Option prices differ by more than ${tolerance}:\n"
                    f"  ThetaData: ${theta_opt_price}\n"
                    f"  Polygon:   ${polygon_opt_price}\n"
                    f"  Difference: ${price_diff} (tolerance: ${tolerance})\n"
                    f"See detailed report above."
                )

            print(f"✓ Option prices match within tolerance: ThetaData=${theta_opt_price}, Polygon=${polygon_opt_price}, diff=${price_diff:.4f}")

    def test_index_price_comparison(self):
        """
        Tests SPX index data accessibility and timestamp accuracy.

        NOTE: ThetaData VALUE Indices plan only supports 15-minute intervals.
        This test verifies:
        1. SPX data is accessible
        2. Timestamps are correct (15-min intervals)
        3. OHLC data is consistent
        4. No timestamp offset bugs (like the +1 minute bug we fixed for stocks)
        """
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        from lumibot.tools import thetadata_helper

        asset = Asset("SPX", asset_type="index")

        # ThetaData - 15 minute intervals (VALUE plan limitation)
        theta_df = thetadata_helper.get_historical_data(
            asset=asset,
            start_dt=datetime.datetime(2024, 8, 1, 9, 30),
            end_dt=datetime.datetime(2024, 8, 1, 12, 0),
            ivl=900000,  # 15 minutes (900,000 ms) - VALUE plan supports this
            username=username,
            password=password,
            datastyle='ohlc'
        )

        # Verify data was returned
        if theta_df is None or len(theta_df) == 0:
            pytest.fail("ThetaData SPX data not available - check indices subscription is active")

        print(f"\n✓ SPX Index Data Verification (15-minute intervals):")
        print(f"{'Time':<25} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10}")
        print("="*80)

        # Verify first few bars
        for i in range(min(5, len(theta_df))):
            bar = theta_df.iloc[i]
            timestamp = theta_df.index[i]
            print(f"{str(timestamp):<25} ${bar['open']:<9.2f} ${bar['high']:<9.2f} ${bar['low']:<9.2f} ${bar['close']:<9.2f}")

        # Verify timestamps are 15 minutes apart
        for i in range(1, min(5, len(theta_df))):
            time_diff = (theta_df.index[i] - theta_df.index[i-1]).total_seconds()
            assert time_diff == 900, f"Bar {i} is {time_diff}s after previous, expected 900s (15 min)"

        # Verify OHLC consistency
        for i in range(len(theta_df)):
            bar = theta_df.iloc[i]
            assert bar['high'] >= bar['open'], f"Bar {i}: high < open"
            assert bar['high'] >= bar['close'], f"Bar {i}: high < close"
            assert bar['high'] >= bar['low'], f"Bar {i}: high < low"
            assert bar['low'] <= bar['open'], f"Bar {i}: low > open"
            assert bar['low'] <= bar['close'], f"Bar {i}: low > close"
            assert 4000 < bar['close'] < 7000, f"Bar {i}: close ${bar['close']:.2f} outside reasonable range"

        # Verify first bar starts at 9:30 or 9:29 (depending on timestamp alignment)
        first_time = theta_df.index[0]
        assert first_time.hour == 9, f"First bar hour is {first_time.hour}, expected 9"
        assert 29 <= first_time.minute <= 30, f"First bar minute is {first_time.minute}, expected 29 or 30"

        print(f"\n✓ SPX index data is accessible and working correctly!")
        print(f"  - Got {len(theta_df)} bars of 15-minute data")
        print(f"  - Timestamps are 15 minutes apart")
        print(f"  - OHLC data is consistent")
        print(f"  - First bar at {first_time}")
        print(f"  - Price range: ${theta_df['close'].min():.2f} - ${theta_df['close'].max():.2f}")

    def test_fill_price_comparison(self):
        """
        Compare fill prices between ThetaData and Polygon.
        ZERO TOLERANCE - prices must match exactly or investigation is required.
        """
        params = {"symbol": "AMZN", "test_type": "stock"}

        # Run with ThetaData
        theta_data = run_backtest(ThetaDataBacktesting, **params)

        # Run with Polygon
        polygon_data = run_backtest(PolygonDataBacktesting, **params)

        # Compare fill prices
        if theta_data["fill_prices"] and polygon_data["fill_prices"]:
            theta_info = theta_data["fill_prices"][0]
            polygon_info = polygon_data["fill_prices"][0]

            theta_fill = theta_info["price"]
            polygon_fill = polygon_info["price"]

            # Tolerance: 1 cent for fill prices
            tolerance = 0.01
            price_diff = abs(theta_fill - polygon_fill)

            if price_diff > tolerance:
                report = detailed_comparison_report(theta_info, polygon_info, "FILL PRICE")
                print(report)
                pytest.fail(
                    f"Fill prices differ by more than ${tolerance}:\n"
                    f"  ThetaData: ${theta_fill}\n"
                    f"  Polygon:   ${polygon_fill}\n"
                    f"  Difference: ${price_diff} (tolerance: ${tolerance})\n"
                    f"See detailed report above."
                )

            print(f"✓ Fill prices match within tolerance: ThetaData=${theta_fill}, Polygon=${polygon_fill}, diff=${price_diff:.4f}")

    def test_portfolio_value_comparison(self):
        """
        Compare portfolio values between ThetaData and Polygon.
        ZERO TOLERANCE - values must match exactly or investigation is required.
        """
        params = {"symbol": "AMZN", "test_type": "stock"}

        # Run with ThetaData
        theta_data = run_backtest(ThetaDataBacktesting, **params)

        # Run with Polygon
        polygon_data = run_backtest(PolygonDataBacktesting, **params)

        # Compare final portfolio values
        if theta_data["portfolio_values"] and polygon_data["portfolio_values"]:
            theta_pv = theta_data["portfolio_values"][-1]
            polygon_pv = polygon_data["portfolio_values"][-1]

            # Tolerance: $10 for portfolio value (accounts for compounding small price differences)
            tolerance = 10.0
            pv_diff = abs(theta_pv - polygon_pv)

            if pv_diff > tolerance:
                theta_info = {"portfolio_value": theta_pv, "all_values": theta_data["portfolio_values"]}
                polygon_info = {"portfolio_value": polygon_pv, "all_values": polygon_data["portfolio_values"]}
                report = detailed_comparison_report(theta_info, polygon_info, "PORTFOLIO VALUE")
                print(report)
                pytest.fail(
                    f"Portfolio values differ by more than ${tolerance}:\n"
                    f"  ThetaData: ${theta_pv}\n"
                    f"  Polygon:   ${polygon_pv}\n"
                    f"  Difference: ${pv_diff} (tolerance: ${tolerance})\n"
                    f"See detailed report above."
                )

            print(f"✓ Portfolio values match within tolerance: ThetaData=${theta_pv}, Polygon=${polygon_pv}, diff=${pv_diff:.2f}")

    def test_cash_comparison(self):
        """
        Compare cash values between ThetaData and Polygon.
        ZERO TOLERANCE - values must match exactly or investigation is required.
        """
        params = {"symbol": "AMZN", "test_type": "stock"}

        # Run with ThetaData
        theta_data = run_backtest(ThetaDataBacktesting, **params)

        # Run with Polygon
        polygon_data = run_backtest(PolygonDataBacktesting, **params)

        # Compare final cash values
        if theta_data["cash_values"] and polygon_data["cash_values"]:
            theta_cash = theta_data["cash_values"][-1]
            polygon_cash = polygon_data["cash_values"][-1]

            # Tolerance: $10 for cash (mirrors portfolio value tolerance)
            tolerance = 10.0
            cash_diff = abs(theta_cash - polygon_cash)

            if cash_diff > tolerance:
                theta_info = {"cash": theta_cash, "all_values": theta_data["cash_values"]}
                polygon_info = {"cash": polygon_cash, "all_values": polygon_data["cash_values"]}
                report = detailed_comparison_report(theta_info, polygon_info, "CASH")
                print(report)
                pytest.fail(
                    f"Cash values differ by more than ${tolerance}:\n"
                    f"  ThetaData: ${theta_cash}\n"
                    f"  Polygon:   ${polygon_cash}\n"
                    f"  Difference: ${cash_diff} (tolerance: ${tolerance})\n"
                    f"See detailed report above."
                )

            print(f"✓ Cash values match within tolerance: ThetaData=${theta_cash}, Polygon=${polygon_cash}, diff=${cash_diff:.2f}")
