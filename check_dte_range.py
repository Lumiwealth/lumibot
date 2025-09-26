#!/usr/bin/env python3
"""
Check data availability for options in the 14-28 DTE range
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta

class CheckDTERange(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.checked = False

    def on_trading_iteration(self):
        if self.checked:
            return

        dt = self.get_datetime()
        current_date = self.to_default_timezone(dt).date()

        # Only run on first Thursday we encounter
        if dt.weekday() != 3:
            return

        print(f"\n{'='*80}")
        print(f"CHECKING DTE RANGE 14-28 - {current_date} at {dt.time()}")
        print(f"{'='*80}\n")

        # Get SPY price and chains
        spy_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        spy_price = self.get_last_price(spy_asset)
        print(f"SPY Price: ${spy_price:.2f}\n")

        chains_res = self.get_chains(spy_asset)
        if not chains_res:
            print("No chains available")
            return

        call_chains = chains_res.get("Chains", {}).get("CALL", {})

        # Get all available expiries and calculate DTEs
        expiry_data = []
        for exp_str in call_chains.keys():
            exp_date = self.options_expiry_to_datetime_date(exp_str)
            dte = (exp_date - current_date).days
            expiry_data.append((exp_str, exp_date, dte))

        # Sort by DTE
        expiry_data.sort(key=lambda x: x[2])

        # Filter to 10-30 DTE range (a bit wider to see the pattern)
        target_expiries = [e for e in expiry_data if 10 <= e[2] <= 30]

        print(f"Found {len(target_expiries)} expiries in 10-30 DTE range\n")
        print(f"{'DTE':<5} {'Expiry':<12} {'ATM Strike':<12} {'Has Data?':<12} {'Data Type'}")
        print("-" * 60)

        for exp_str, exp_date, dte in target_expiries:
            strikes = call_chains[exp_str]
            strikes_list = sorted([float(s) for s in strikes])

            # Find ATM strike
            atm_strike = min(strikes_list, key=lambda s: abs(s - spy_price))

            # Check ATM option
            option = Asset(
                "SPY",
                asset_type=Asset.AssetType.OPTION,
                expiration=exp_date,
                strike=atm_strike,
                right=Asset.OptionRight.CALL,
            )

            quote = self.get_quote(option)
            last_price = self.get_last_price(option)

            # Check what data we have
            has_bid = quote and hasattr(quote, 'bid') and quote.bid is not None
            has_ask = quote and hasattr(quote, 'ask') and quote.ask is not None
            has_last = last_price is not None and last_price > 0

            if has_bid and has_ask:
                status = "✓ YES"
                data_type = f"Bid/Ask (${quote.bid:.2f}/${quote.ask:.2f})"
            elif has_last:
                status = "✓ YES"
                data_type = f"Last only (${last_price:.2f})"
            else:
                status = "✗ NO"
                data_type = "None"

            # Mark the target DTEs
            target_marker = " <--" if 14 <= dte <= 28 else ""
            print(f"{dte:<5} {exp_str:<12} ${atm_strike:<11.0f} {status:<12} {data_type}{target_marker}")

        # Now do a detailed check on a few strikes for each DTE in target range
        print(f"\n{'='*60}")
        print("DETAILED CHECK FOR TARGET RANGE (14-28 DTE)")
        print("Checking multiple strikes for each expiry...")
        print("-" * 60)

        target_range_expiries = [e for e in expiry_data if 14 <= e[2] <= 28]

        for exp_str, exp_date, dte in target_range_expiries:
            print(f"\nDTE {dte} ({exp_str}):")

            strikes = call_chains[exp_str]
            strikes_list = sorted([float(s) for s in strikes])

            # Find ATM and check 5 strikes around it
            atm_strike = min(strikes_list, key=lambda s: abs(s - spy_price))
            atm_index = strikes_list.index(atm_strike)

            start_idx = max(0, atm_index - 2)
            end_idx = min(len(strikes_list), atm_index + 3)

            strikes_with_data = 0
            for strike in strikes_list[start_idx:end_idx]:
                option = Asset(
                    "SPY",
                    asset_type=Asset.AssetType.OPTION,
                    expiration=exp_date,
                    strike=strike,
                    right=Asset.OptionRight.CALL,
                )

                quote = self.get_quote(option)
                last_price = self.get_last_price(option)

                has_bid = quote and hasattr(quote, 'bid') and quote.bid is not None
                has_ask = quote and hasattr(quote, 'ask') and quote.ask is not None
                has_last = last_price is not None and last_price > 0

                if has_bid and has_ask:
                    strikes_with_data += 1
                    print(f"  ✓ ${strike:.0f}: Bid=${quote.bid:.2f}, Ask=${quote.ask:.2f}")
                elif has_last:
                    strikes_with_data += 1
                    print(f"  ✓ ${strike:.0f}: Last=${last_price:.2f} (no quotes)")
                else:
                    print(f"  ✗ ${strike:.0f}: No data")

            print(f"  Summary: {strikes_with_data}/{end_idx-start_idx} strikes have data")

        self.checked = True
        print(f"\n{'='*80}")
        print("END OF DTE RANGE ANALYSIS")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    print("Checking data availability for 14-28 DTE range...")
    print("This is the target range for the strategy\n")

    trading_fee = TradingFee(flat_fee=0.65)

    results = CheckDTERange.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 10),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        budget=100000,
    )