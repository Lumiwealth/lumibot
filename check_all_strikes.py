#!/usr/bin/env python3
"""
Check ALL strikes to see if ANY have quote data
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta

class CheckAllStrikes(Strategy):
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
        print(f"CHECKING ALL STRIKES - {current_date} at {dt.time()}")
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

        # Get all available expiries
        all_expiries = sorted(call_chains.keys())

        print(f"Found {len(all_expiries)} expiration dates\n")

        # Check first 5 expiries
        for exp_str in all_expiries[:5]:
            exp_date = self.options_expiry_to_datetime_date(exp_str)
            dte = (exp_date - current_date).days

            print(f"\nExpiry: {exp_str} (DTE: {dte})")
            print("-" * 40)

            strikes = call_chains[exp_str]
            strikes_list = sorted([float(s) for s in strikes])

            # Find ATM and nearby strikes
            atm_strike = min(strikes_list, key=lambda s: abs(s - spy_price))
            atm_index = strikes_list.index(atm_strike)

            # Check 10 strikes around ATM
            start_idx = max(0, atm_index - 5)
            end_idx = min(len(strikes_list), atm_index + 6)

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

                # Check what data we have
                has_bid = quote and hasattr(quote, 'bid') and quote.bid is not None
                has_ask = quote and hasattr(quote, 'ask') and quote.ask is not None
                has_last = last_price is not None and last_price > 0

                if has_bid or has_ask or has_last:
                    strikes_with_data += 1
                    status = "✓ DATA"
                    details = []
                    if has_bid:
                        details.append(f"bid=${quote.bid:.2f}")
                    if has_ask:
                        details.append(f"ask=${quote.ask:.2f}")
                    if has_last:
                        details.append(f"last=${last_price:.2f}")
                    detail_str = ", ".join(details)
                else:
                    status = "✗ NONE"
                    detail_str = "No bid, ask, or last price"

                atm_marker = " <-- ATM" if strike == atm_strike else ""
                print(f"  {status} Strike ${strike:.0f}: {detail_str}{atm_marker}")

            print(f"\nSummary: {strikes_with_data}/{end_idx-start_idx} strikes have ANY data")

        # Also check some deep ITM and OTM strikes
        print(f"\n{'='*40}")
        print("EXTREME STRIKES TEST (June expiry if available)")
        print("-" * 40)

        if all_expiries:
            # Use first available expiry
            exp_str = all_expiries[0]
            exp_date = self.options_expiry_to_datetime_date(exp_str)
            strikes = call_chains[exp_str]
            strikes_list = sorted([float(s) for s in strikes])

            # Check very deep ITM (low strikes)
            print("\nDeep ITM (low strikes):")
            for strike in strikes_list[:3]:
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
                has_last = last_price is not None

                if has_bid or has_ask or has_last:
                    print(f"  ✓ Strike ${strike:.0f} has data!")
                else:
                    print(f"  ✗ Strike ${strike:.0f}: No data")

            # Check very deep OTM (high strikes)
            print("\nDeep OTM (high strikes):")
            for strike in strikes_list[-3:]:
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
                has_last = last_price is not None

                if has_bid or has_ask or has_last:
                    print(f"  ✓ Strike ${strike:.0f} has data!")
                else:
                    print(f"  ✗ Strike ${strike:.0f}: No data")

        self.checked = True
        print(f"\n{'='*80}")
        print("END OF STRIKE ANALYSIS")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    print("Checking ALL strikes to see if ANY have quote data...")
    print("This will test multiple expiries and strike prices\n")

    trading_fee = TradingFee(flat_fee=0.65)

    results = CheckAllStrikes.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 10),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        budget=100000,
    )