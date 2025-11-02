#!/usr/bin/env python3
"""Tests covering OptionsHelper behaviours and chain normalization."""

import unittest
from unittest.mock import Mock, MagicMock
from datetime import date, timedelta, datetime
import sys
import os
import pytest

# Add the lumibot path
sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.components.options_helper import OptionsHelper, OptionMarketEvaluation
from lumibot.entities import Asset
from lumibot.entities.chains import OptionsDataFormatError, normalize_option_chains
from lumibot.brokers.broker import Broker


class _StubDataSource:
    def __init__(self, payload):
        self.payload = payload
        self.datetime_start = None
        self.datetime_end = None
        self.SOURCE = "STUB"

    def get_chains(self, asset):
        return self.payload


class _StubBroker(Broker):
    IS_BACKTESTING_BROKER = True

    def __init__(self, data_source):
        super().__init__(data_source=data_source)

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

    def cancel_order(self, order):
        return None

    def _modify_order(self, order, limit_price=None, stop_price=None):
        return None

    def _submit_order(self, order):
        return None

    def _get_balances_at_broker(self, quote_asset, strategy):
        return 0, 0, 0

    def get_historical_account_value(self):
        return {}

    def _pull_positions(self, strategy):
        return []

    def _pull_position(self, strategy, asset):
        return None

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        return None

    def _pull_broker_order(self, identifier):
        return None

    def _pull_broker_all_orders(self):
        return []

class TestOptionsHelper(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_strategy = Mock()
        self.mock_strategy.log_message = Mock()
        self.mock_strategy.get_last_price = Mock(return_value=5.0)
        self.mock_strategy.get_quote = Mock(return_value=None)
        data_source = Mock()
        data_source.option_quote_fallback_allowed = False
        broker = Mock()
        broker.data_source = data_source
        self.mock_strategy.broker = broker
        
        # Mock get_greeks with realistic delta values
        def mock_get_greeks(option, underlying_price=None):
            strike = option.strike
            if option.right.lower() == "put":
                if strike < underlying_price * 0.95:  # OTM put
                    return {"delta": -0.15}
                elif strike < underlying_price * 1.05:  # Near ATM put
                    return {"delta": -0.45}
                else:  # ITM put
                    return {"delta": -0.75}
            else:  # call
                if strike > underlying_price * 1.05:  # OTM call
                    return {"delta": 0.25}
                elif strike > underlying_price * 0.95:  # Near ATM call
                    return {"delta": 0.55}
                else:  # ITM call
                    return {"delta": 0.85}
        
        self.mock_strategy.get_greeks = Mock(side_effect=mock_get_greeks)
        self.options_helper = OptionsHelper(self.mock_strategy)
    
    def test_normal_strike_calculation(self):
        """Test normal strike calculation for a typical stock"""
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Should find a reasonable strike
        self.assertIsNotNone(result)
        self.assertGreater(result, 150)  # Should be reasonable for $200 stock
        self.assertLess(result, 250)
        
        # Should have logged the search parameters
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("STRIKE SEARCH" in msg for msg in log_calls))
        self.assertTrue(any("underlying_price=$200" in msg for msg in log_calls))
    
    def test_invalid_underlying_price(self):
        """Test handling of invalid underlying price"""
        underlying_asset = Asset("TEST", asset_type="stock")
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # Test with negative price
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=-10.0,  # Invalid
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        self.assertIsNone(result)
        
        # Should have logged an error
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("ERROR: Invalid underlying price" in msg for msg in log_calls))
    
    def test_invalid_delta(self):
        """Test handling of invalid delta values"""
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # Test with delta > 1
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=1.5,  # Invalid
            expiry=expiry,
            right=right
        )
        
        self.assertIsNone(result)
        
        # Should have logged an error
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("ERROR: Invalid target delta" in msg for msg in log_calls))
    
    def test_warning_for_unrealistic_strike(self):
        """Test that warnings are generated for unrealistic strikes"""
        # Mock a scenario where we get an unrealistically low strike
        def mock_get_greeks_low_strike(option, underlying_price=None):
            # Always return a delta that would make very low strikes look good
            return {"delta": -0.3}
        
        self.mock_strategy.get_greeks = Mock(side_effect=mock_get_greeks_low_strike)
        
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0  # High stock price
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # This should find a low strike due to our mocked greeks
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Should have found something (mocked to return low strike)
        self.assertIsNotNone(result)
        
        # Should have warned about the unrealistic strike
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        if result and result < 10:  # If we got an unrealistically low strike
            self.assertTrue(any("WARNING" in msg and "too low" in msg for msg in log_calls))
    
    def test_enhanced_logging_format(self):
        """Test that the enhanced logging includes emoji and detailed information"""
        underlying_asset = Asset("LULU", asset_type="stock")
        underlying_price = 200.0
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Check for enhanced logging format
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        
        # Should have emoji in logs
        self.assertTrue(any("🎯" in msg for msg in log_calls))  # Target emoji
        self.assertTrue(any("🔍" in msg for msg in log_calls))  # Search emoji
        
        # Should show the search range
        self.assertTrue(any("Search range: strikes" in msg for msg in log_calls))
        
        # Should show individual strike attempts
        self.assertTrue(any("Trying strike" in msg for msg in log_calls))

    def test_missing_chains_returns_none(self):
        """Ensure missing option-chain structures do not crash and return None."""
        target_dt = datetime(2024, 1, 2)

        result = self.options_helper.get_expiration_on_or_after_date(target_dt, {}, "call")

        self.assertIsNone(result)
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("option chains" in msg.lower() for msg in log_calls))

    def test_normalize_option_chains_adds_missing_keys(self):
        normalized_empty = normalize_option_chains({})
        self.assertIn("Chains", normalized_empty)
        self.assertEqual(normalized_empty["Chains"]["CALL"], {})
        self.assertFalse(normalized_empty)

        normalized_partial = normalize_option_chains({"Chains": {"CALL": {"2024-01-02": [100.0, 101.0]}}})
        self.assertIn("PUT", normalized_partial["Chains"])
        self.assertIn("2024-01-02", normalized_partial["Chains"]["CALL"])
        self.assertEqual(normalized_partial["Chains"]["CALL"]["2024-01-02"], [100.0, 101.0])
        self.assertTrue(normalized_partial)

    def test_normalize_option_chains_invalid_expiry(self):
        with self.assertRaisesRegex(OptionsDataFormatError, "Could not parse option expiry value"):
            normalize_option_chains({"Chains": {"CALL": {"02-01-2024": [100.0]}}})

    def test_options_expiry_to_datetime_date_accepts_strings(self):
        """Test that options_expiry_to_datetime_date accepts various string formats."""
        from lumibot.strategies import Strategy

        # Test the method directly without creating a full Strategy instance
        # This avoids needing broker/data source setup
        strategy_class = Strategy

        # Test YYYY-MM-DD format (Polygon)
        result = strategy_class.options_expiry_to_datetime_date(None, "2024-01-15")
        self.assertEqual(result, date(2024, 1, 15))

        # Test YYYYMMDD format (IB legacy)
        result = strategy_class.options_expiry_to_datetime_date(None, "20240115")
        self.assertEqual(result, date(2024, 1, 15))

        # Test date object passthrough
        test_date = date(2024, 1, 15)
        result = strategy_class.options_expiry_to_datetime_date(None, test_date)
        self.assertEqual(result, test_date)

        # Test datetime object conversion
        test_datetime = datetime(2024, 1, 15, 10, 30)
        result = strategy_class.options_expiry_to_datetime_date(None, test_datetime)
        self.assertEqual(result, date(2024, 1, 15))

        # Test invalid string format raises error
        with self.assertRaises(ValueError):
            strategy_class.options_expiry_to_datetime_date(None, "01-15-2024")

    def test_get_expiration_on_or_after_date_returns_future(self):
        from datetime import date as _date

        expiries = {
            "Chains": {
                "CALL": {
                    "2024-01-02": [100.0],
                    "2024-01-09": [101.0],
                }
            }
        }

        target = _date(2024, 1, 3)
        result = self.options_helper.get_expiration_on_or_after_date(target, expiries, "call")
        self.assertEqual(result, _date(2024, 1, 9))

    def test_get_expiration_on_or_after_date_returns_none_when_no_future_available(self):
        from datetime import date as _date

        expiries = {
            "Chains": {
                "CALL": {
                    "2024-01-02": [100.0],
                    "2024-01-09": [101.0],
                }
            }
        }

        target = _date(2024, 2, 1)
        result = self.options_helper.get_expiration_on_or_after_date(target, expiries, "call")
        self.assertIsNone(result)

    def test_chains_backward_compatibility_string_access(self):
        """Test that existing code using string keys still works."""
        chains = normalize_option_chains({
            "Chains": {
                "CALL": {"2024-01-02": [100.0, 101.0]},
                "PUT": {"2024-01-02": [95.0, 96.0]}
            },
            "Multiplier": 100
        })

        # String access should work (backward compatibility)
        self.assertEqual(chains["Chains"]["CALL"]["2024-01-02"], [100.0, 101.0])
        self.assertEqual(chains["Chains"]["PUT"]["2024-01-02"], [95.0, 96.0])

        # Helper methods should also work with strings
        self.assertIn("2024-01-02", chains.expirations())
        self.assertEqual(chains.strikes("2024-01-02"), [100.0, 101.0])

    def test_chains_date_helper_methods(self):
        """Test new date-based internal helper methods."""
        chains = normalize_option_chains({
            "Chains": {
                "CALL": {"2024-01-02": [100.0, 101.0], "2024-01-09": [102.0]},
                "PUT": {"2024-01-02": [95.0, 96.0]}
            }
        })

        # Test expirations_as_dates
        expiry_dates = chains.expirations_as_dates()
        self.assertEqual(len(expiry_dates), 2)
        self.assertEqual(expiry_dates[0], date(2024, 1, 2))
        self.assertEqual(expiry_dates[1], date(2024, 1, 9))

        # Test get_option_chain_by_date
        strikes = chains.get_option_chain_by_date(date(2024, 1, 2))
        self.assertEqual(strikes, [100.0, 101.0])

    def test_chains_strikes_accepts_both_string_and_date(self):
        """Test that strikes() method accepts both string and date parameters."""
        chains = normalize_option_chains({
            "Chains": {
                "CALL": {"2024-01-02": [100.0, 101.0]}
            }
        })

        # Should work with string
        self.assertEqual(chains.strikes("2024-01-02"), [100.0, 101.0])

        # Should also work with date
        self.assertEqual(chains.strikes(date(2024, 1, 2)), [100.0, 101.0])

        # Should also work with datetime
        self.assertEqual(chains.strikes(datetime(2024, 1, 2, 10, 30)), [100.0, 101.0])

    def test_broker_get_chains_handles_missing_payload(self):
        asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)

        broker_empty = _StubBroker(data_source=_StubDataSource({}))
        chains_empty = broker_empty.get_chains(asset)
        self.assertFalse(chains_empty)
        self.assertIn("CALL", chains_empty["Chains"])
        self.assertIn("PUT", chains_empty["Chains"])

        payload_partial = {"Chains": {"CALL": {"2024-01-02": [100.0]}}}
        broker_partial = _StubBroker(data_source=_StubDataSource(payload_partial))
        chains_partial = broker_partial.get_chains(asset)
        self.assertTrue(chains_partial)
        self.assertEqual(chains_partial["Chains"]["CALL"]["2024-01-02"], [100.0])
        self.assertIn("PUT", chains_partial["Chains"])

    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Requires ThetaData Terminal (not available in CI)"
    )
    def test_find_next_valid_option_checks_quote_first(self):
        """Test that find_next_valid_option checks quote before last_price using REAL ThetaData"""
        import os
        from dotenv import load_dotenv
        from lumibot.backtesting import ThetaDataBacktesting, BacktestingBroker
        from lumibot.strategies import Strategy
        from lumibot.traders import Trader

        load_dotenv()

        # Get real ThetaData credentials
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        if not username or username.lower() in {"", "uname"}:
            self.skipTest("ThetaData username not configured")
        if not password or password.lower() in {"", "pwd"}:
            self.skipTest("ThetaData password not configured")

        # Create a simple strategy that uses OptionsHelper with REAL data
        class TestStrategy(Strategy):
            def initialize(self):
                self.sleeptime = "1D"
                self.option_found = None

            def on_trading_iteration(self):
                from lumibot.components.options_helper import OptionsHelper
                options_helper = OptionsHelper(self)

                # Use SPY as underlying (guaranteed to have options data)
                underlying_asset = Asset("SPY", asset_type="stock")
                current_price = self.get_last_price(underlying_asset)

                # Get chains to find a valid expiration
                chains = self.get_chains(underlying_asset)
                if not chains or not chains.expirations("CALL"):
                    self.log_message("No chains available")
                    return

                # Get the first available expiration
                expiry_str = chains.expirations("CALL")[0]
                expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()

                # Try to find next valid option
                self.option_found = options_helper.find_next_valid_option(
                    underlying_asset=underlying_asset,
                    rounded_underlying_price=round(current_price),
                    expiry=expiry,
                    put_or_call="call"
                )

        # Run backtest for September 2-3, 2025
        backtesting_start = datetime(2025, 9, 2)
        backtesting_end = datetime(2025, 9, 3)

        data_source = ThetaDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            username=username,
            password=password
        )

        broker = BacktestingBroker(data_source=data_source)
        strategy = TestStrategy(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end
        )

        trader = Trader(backtest=True)
        trader.add_strategy(strategy)
        trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

        # Verify that an option was found using real data
        self.assertIsNotNone(strategy.option_found, "Should find valid option using real ThetaData")

    @pytest.mark.skipif(
        os.environ.get("CI") == "true",
        reason="Requires ThetaData Terminal (not available in CI)"
    )
    def test_find_next_valid_option_falls_back_to_last_price(self):
        """Test fallback to last_price when quote has no bid/ask using REAL ThetaData"""
        import os
        from dotenv import load_dotenv
        from lumibot.backtesting import ThetaDataBacktesting, BacktestingBroker
        from lumibot.strategies import Strategy
        from lumibot.traders import Trader

        load_dotenv()

        # Get real ThetaData credentials
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        if not username or username.lower() in {"", "uname"}:
            self.skipTest("ThetaData username not configured")
        if not password or password.lower() in {"", "pwd"}:
            self.skipTest("ThetaData password not configured")

        # Create a simple strategy that uses OptionsHelper with REAL data
        class TestStrategy(Strategy):
            def initialize(self):
                self.sleeptime = "1D"
                self.option_found = None
                self.quote_checked = False
                self.last_price_checked = False

            def on_trading_iteration(self):
                from lumibot.components.options_helper import OptionsHelper
                options_helper = OptionsHelper(self)

                # Use SPY as underlying (guaranteed to have options data)
                underlying_asset = Asset("SPY", asset_type="stock")
                current_price = self.get_last_price(underlying_asset)

                # Get chains to find a valid expiration
                chains = self.get_chains(underlying_asset)
                if not chains or not chains.expirations("PUT"):
                    self.log_message("No chains available")
                    return

                # Get the first available expiration
                expiry_str = chains.expirations("PUT")[0]
                expiry = datetime.strptime(expiry_str, "%Y-%m-%d").date()

                # Try to find next valid option (PUT this time)
                self.option_found = options_helper.find_next_valid_option(
                    underlying_asset=underlying_asset,
                    rounded_underlying_price=round(current_price),
                    expiry=expiry,
                    put_or_call="put"
                )

                # Verify both quote and last_price were used
                if self.option_found:
                    # Check that we can get quote and last_price for the found option
                    quote = self.get_quote(self.option_found)
                    last_price = self.get_last_price(self.option_found)
                    self.quote_checked = quote is not None
                    self.last_price_checked = last_price is not None

        # Run backtest for September 2-3, 2025
        backtesting_start = datetime(2025, 9, 2)
        backtesting_end = datetime(2025, 9, 3)

        data_source = ThetaDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            username=username,
            password=password
        )

        broker = BacktestingBroker(data_source=data_source)
        strategy = TestStrategy(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end
        )

        trader = Trader(backtest=True)
        trader.add_strategy(strategy)
        trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

        # Verify that an option was found and both methods were available
        self.assertIsNotNone(strategy.option_found, "Should find valid option using real ThetaData")
        # Note: We can't guarantee which method was used (quote vs last_price), but we verify the option works
        self.assertTrue(strategy.quote_checked or strategy.last_price_checked, "Should be able to get data for option")

    def test_get_expiration_validates_data_when_underlying_provided(self):
        """Test that get_expiration_on_or_after_date validates data exists when underlying provided"""
        underlying_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

        chains = {
            "Chains": {
                "CALL": {
                    "2024-01-02": [100.0, 105.0],
                    "2024-01-09": [100.0, 105.0],
                    "2024-01-16": [100.0, 105.0],
                }
            }
        }

        # Mock first expiry has no data, second has quote data
        def mock_get_quote(option):
            if option.expiration == date(2024, 1, 2):
                # First expiry has no quote
                return None
            else:
                # Other expiries have valid quote
                mock_quote = Mock()
                mock_quote.bid = 2.0
                mock_quote.ask = 2.5
                return mock_quote

        self.mock_strategy.get_quote = Mock(side_effect=mock_get_quote)
        self.mock_strategy.get_last_price = Mock(return_value=None)

        target = date(2024, 1, 1)
        result = self.options_helper.get_expiration_on_or_after_date(
            target, chains, "call", underlying_asset=underlying_asset
        )

        # Should skip Jan 2 (no data) and return Jan 9 (has quote)
        self.assertEqual(result, date(2024, 1, 9))

        # Check it tried to validate options
        self.mock_strategy.get_quote.assert_called()

    def test_evaluate_option_market_with_quotes(self):
        """evaluate_option_market returns actionable prices when quotes exist."""
        option_asset = Asset(
            "TEST",
            asset_type=Asset.AssetType.OPTION,
            expiration=date.today() + timedelta(days=7),
            strike=200,
            right="call",
            underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        )

        self.mock_strategy.get_quote.return_value = Mock(bid=1.0, ask=1.2)
        self.mock_strategy.get_last_price.return_value = 1.1

        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.5)

        self.assertTrue(evaluation.has_bid_ask)
        self.assertFalse(evaluation.spread_too_wide)
        self.assertEqual(evaluation.buy_price, 1.2)
        self.assertEqual(evaluation.sell_price, 1.0)
        self.assertFalse(evaluation.used_last_price_fallback)

    def test_evaluate_option_market_fallback_allowed(self):
        """Missing quotes use last price when the data source allows fallback."""
        option_asset = Asset(
            "TEST",
            asset_type=Asset.AssetType.OPTION,
            expiration=date.today() + timedelta(days=7),
            strike=200,
            right="call",
            underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        )

        self.mock_strategy.get_quote.return_value = Mock(bid=None, ask=None)
        self.mock_strategy.get_last_price.return_value = 2.5
        self.mock_strategy.broker.data_source.option_quote_fallback_allowed = True

        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.25)

        self.assertTrue(evaluation.missing_bid_ask)
        self.assertTrue(evaluation.used_last_price_fallback)
        self.assertEqual(evaluation.buy_price, 2.5)
        self.assertEqual(evaluation.sell_price, 2.5)
        self.assertFalse(evaluation.spread_too_wide)

    def test_evaluate_option_market_fallback_blocked(self):
        """If fallback is not allowed missing quotes produce no price anchors."""
        option_asset = Asset(
            "TEST",
            asset_type=Asset.AssetType.OPTION,
            expiration=date.today() + timedelta(days=7),
            strike=200,
            right="call",
            underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        )

        self.mock_strategy.get_quote.return_value = Mock(bid=None, ask=None)
        self.mock_strategy.get_last_price.return_value = 3.1
        self.mock_strategy.broker.data_source.option_quote_fallback_allowed = False

        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.25)

        self.assertTrue(evaluation.missing_bid_ask)
        self.assertIsNone(evaluation.buy_price)
        self.assertIsNone(evaluation.sell_price)
        self.assertFalse(evaluation.used_last_price_fallback)

    def test_evaluate_option_market_rejects_non_finite_quotes(self):
        """NaN or infinite quotes are treated as missing to prevent crashes."""
        option_asset = Asset(
            "TEST",
            asset_type=Asset.AssetType.OPTION,
            expiration=date.today() + timedelta(days=7),
            strike=200,
            right="call",
            underlying_asset=Asset("TEST", asset_type=Asset.AssetType.STOCK),
        )

        self.mock_strategy.get_quote.return_value = Mock(bid=float("nan"), ask=float("inf"))
        self.mock_strategy.get_last_price.return_value = float("nan")
        self.mock_strategy.broker.data_source.option_quote_fallback_allowed = True

        evaluation = self.options_helper.evaluate_option_market(option_asset, max_spread_pct=0.25)

        self.assertIsNone(evaluation.buy_price)
        self.assertIn("bid_non_finite", evaluation.data_quality_flags)
        self.assertIn("ask_non_finite", evaluation.data_quality_flags)
        self.assertTrue(evaluation.missing_bid_ask)
        self.assertFalse(OptionsHelper.has_actionable_price(evaluation))

    def test_has_actionable_price_requires_positive_finite_value(self):
        """has_actionable_price returns False for zero or negative values."""
        evaluation = OptionMarketEvaluation(
            bid=0.5,
            ask=0.6,
            last_price=0.55,
            spread_pct=0.18,
            has_bid_ask=True,
            spread_too_wide=False,
            missing_bid_ask=False,
            missing_last_price=False,
            buy_price=0.0,
            sell_price=0.4,
            used_last_price_fallback=False,
            max_spread_pct=0.25,
            data_quality_flags=["buy_price_non_positive"],
        )
        self.assertFalse(OptionsHelper.has_actionable_price(evaluation))

if __name__ == "__main__":
    print("🧪 Running enhanced options helper tests...")
    unittest.main(verbosity=2)
