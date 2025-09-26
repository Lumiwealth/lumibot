#!/usr/bin/env python3
"""
Full DTE spectrum analysis with days of week
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta

class FullDTESpectrum(Strategy):
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

        print(f"\n{'='*90}")
        print(f"FULL DTE SPECTRUM ANALYSIS - {current_date} at {dt.time()}")
        print(f"Current day: {current_date.strftime('%A')} ({current_date})")
        print(f"{'='*90}\n")

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

        # Filter to 0-30 DTE range
        target_expiries = [e for e in expiry_data if 0 <= e[2] <= 30]

        print(f"Checking {len(target_expiries)} expiries from 0-30 DTE\n")
        print(f"{'DTE':<4} {'Day of Week':<11} {'Expiry Date':<12} {'Has Data?':<10} {'# Strikes':<10} {'Notes'}")
        print("-" * 90)

        # Track patterns
        weekly_data = {0: [], 1: [], 2: [], 3: [], 4: []}  # Mon-Fri

        for exp_str, exp_date, dte in target_expiries:
            # Get day of week for expiry
            day_of_week = exp_date.weekday()
            day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][day_of_week]

            # Check data availability for ATM and nearby strikes
            strikes = call_chains[exp_str]
            strikes_list = sorted([float(s) for s in strikes])

            # Find ATM strike
            atm_strike = min(strikes_list, key=lambda s: abs(s - spy_price))
            atm_index = strikes_list.index(atm_strike)

            # Check 5 strikes around ATM
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

                if has_bid or has_ask or has_last:
                    strikes_with_data += 1

            # Determine status
            if strikes_with_data > 0:
                status = "✓ YES"
                data_pct = f"{strikes_with_data}/{end_idx-start_idx}"
            else:
                status = "✗ NO"
                data_pct = f"0/{end_idx-start_idx}"

            # Mark special dates
            notes = ""
            if 14 <= dte <= 28:
                notes += "TARGET "
            if dte == 21:
                notes += "<-- Strategy picks this"
            elif dte == 0:
                notes += "Same day"
            elif dte == 1:
                notes += "Next day"

            print(f"{dte:<4} {day_name:<11} {exp_str:<12} {status:<10} {data_pct:<10} {notes}")

            # Track for pattern analysis
            if day_of_week <= 4:  # Weekday
                weekly_data[day_of_week].append((dte, strikes_with_data > 0))

        # Analyze patterns
        print(f"\n{'='*90}")
        print("PATTERN ANALYSIS BY DAY OF WEEK")
        print("-" * 90)

        for dow in range(5):
            day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'][dow]
            entries = weekly_data[dow]
            if entries:
                with_data = sum(1 for _, has_data in entries if has_data)
                total = len(entries)
                pct = (with_data / total) * 100
                dtes_with_data = [dte for dte, has_data in entries if has_data]
                dtes_without_data = [dte for dte, has_data in entries if not has_data]

                print(f"\n{day_name} expiries:")
                print(f"  Total: {total}, With data: {with_data} ({pct:.0f}%)")
                if dtes_with_data:
                    print(f"  DTEs WITH data: {sorted(dtes_with_data)}")
                if dtes_without_data:
                    print(f"  DTEs WITHOUT data: {sorted(dtes_without_data)}")

        self.checked = True
        print(f"\n{'='*90}")
        print("END OF SPECTRUM ANALYSIS")
        print(f"{'='*90}\n")


if __name__ == "__main__":
    print("Analyzing full DTE spectrum with day-of-week patterns...")
    print("This will show which DTEs have data and what day of week they expire\n")

    trading_fee = TradingFee(flat_fee=0.65)

    results = FullDTESpectrum.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 10),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        budget=100000,
    )