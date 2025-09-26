#!/usr/bin/env python3
"""
Test that our OptionsHelper fixes work for the SPY weekly strategy scenario
"""
import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from datetime import datetime, timedelta

class TestOptionsStrategy(Strategy):
    parameters = {
        "underlying_symbol": "SPY",
        "min_dte": 14,
        "max_dte": 28,
        "target_dte": 21,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.options_helper = OptionsHelper(self)
        if not hasattr(self.vars, "tested_dates"):
            self.vars.tested_dates = []
        self.log_message("Testing OptionsHelper fixes", color="blue")

    def on_trading_iteration(self):
        dt = self.get_datetime()
        weekday = dt.weekday()

        # Only test on Thursdays
        if weekday == 3:
            today_date = self.to_default_timezone(dt).date()

            # Check if we already tested today
            if today_date in self.vars.tested_dates:
                return

            self.vars.tested_dates.append(today_date)

            # Get SPY chains
            underlying_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
            chains = self.get_chains(underlying_asset)

            if not chains:
                self.log_message("No chains available", color="red")
                return

            # Try to find a valid expiry using our improved method
            target_date = today_date + timedelta(days=21)

            self.log_message(f"\nTesting on {today_date} (Thursday)", color="green")
            self.log_message(f"Looking for expiry on/after {target_date} (21 DTE)", color="blue")

            # Test the fixed method with underlying asset for validation
            valid_expiry = self.options_helper.get_expiration_on_or_after_date(
                dt=target_date,
                chains=chains,
                call_or_put="call",
                underlying_asset=underlying_asset
            )

            if valid_expiry:
                dte = (valid_expiry - today_date).days
                self.log_message(f"✓ Found valid expiry: {valid_expiry} ({dte} DTE)", color="green")

                # Now try to get a valid option
                spy_price = self.get_last_price(underlying_asset)
                if spy_price:
                    option = self.options_helper.find_next_valid_option(
                        underlying_asset=underlying_asset,
                        rounded_underlying_price=round(spy_price),
                        expiry=valid_expiry,
                        put_or_call="call"
                    )

                    if option:
                        # Verify it has data
                        quote = self.get_quote(option)
                        last_price = self.get_last_price(option)

                        has_quote = quote and (quote.bid is not None or quote.ask is not None)
                        has_price = last_price is not None

                        if has_quote:
                            self.log_message(f"✓ Option has quote data: bid={quote.bid}, ask={quote.ask}", color="green")
                        elif has_price:
                            self.log_message(f"✓ Option has price data: {last_price}", color="green")
                        else:
                            self.log_message("✗ Option has NO data (this shouldn't happen!)", color="red")
                    else:
                        self.log_message("✗ Could not find valid option", color="red")
            else:
                self.log_message("✗ Could not find valid expiry", color="red")


if __name__ == "__main__":
    print("Testing OptionsHelper fixes...")
    print("This will verify that get_expiration_on_or_after_date finds valid expiries")
    print("and that find_next_valid_option properly checks quote data\n")

    trading_fee = TradingFee(flat_fee=0.65)

    results = TestOptionsStrategy.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 30),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )