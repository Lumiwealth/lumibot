#!/usr/bin/env python3
"""
Deep Quote Analysis - Understanding when and why quote data is missing
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta, time

class DeepQuoteAnalysis(Strategy):
    parameters = {
        "underlying_symbol": "SPY",
        "buy_day_of_week": 3,  # Thursday
        "min_dte": 14,
        "max_dte": 28,
        "target_dte": 21,
    }

    def initialize(self):
        self.sleeptime = "1D"
        if not hasattr(self.vars, "last_analysis_date"):
            self.vars.last_analysis_date = None
        self.log_message("DEEP QUOTE ANALYSIS - Understanding timing issues", color="blue")

    def _pick_expiration_and_strike(self, dt):
        params = self.get_parameters()
        underlying_symbol = params.get("underlying_symbol", "SPY")
        min_dte = int(params.get("min_dte", 14))
        max_dte = int(params.get("max_dte", 28))
        target_dte = int(params.get("target_dte", 21))

        underlying_asset = Asset(underlying_symbol, asset_type=Asset.AssetType.STOCK)
        last_price = self.get_last_price(underlying_asset)

        if last_price is None:
            return None, None

        chains_res = self.get_chains(underlying_asset)
        if not chains_res:
            return None, None

        call_chains = chains_res.get("Chains", {}).get("CALL")
        if not call_chains:
            return None, None

        expiries = []
        today_date = self.to_default_timezone(dt).date()

        for exp_str in list(call_chains.keys()):
            try:
                exp_date = self.options_expiry_to_datetime_date(exp_str)
                dte = (exp_date - today_date).days
                if min_dte <= dte <= max_dte:
                    expiries.append((exp_date, dte))
            except Exception:
                continue

        if not expiries:
            return None, None

        expiry_date, dte = min(expiries, key=lambda x: abs(x[1] - target_dte))
        expiry_str = expiry_date.strftime("%Y-%m-%d")

        strikes = call_chains.get(expiry_str)
        if strikes is None or len(strikes) == 0:
            return None, None

        strikes_list = [float(s) for s in list(strikes)]
        atm_strike = min(strikes_list, key=lambda s: abs(s - last_price))

        return expiry_date, atm_strike

    def _deep_analyze_quotes(self, option_asset, dt):
        """Extremely detailed analysis of quote availability and timing"""

        current_dt = self.get_datetime()
        current_time = current_dt.time()
        current_date = self.to_default_timezone(current_dt).date()

        print(f"\n{'='*80}")
        print(f"DEEP QUOTE ANALYSIS - {current_date} {current_time}")
        print(f"{'='*80}")

        # 1. TIME AND MARKET STATUS
        print(f"\n1. TIME AND MARKET STATUS:")
        print(f"   Current DateTime (raw): {current_dt}")
        print(f"   Current DateTime (localized): {self.to_default_timezone(current_dt)}")
        print(f"   Day of Week: {current_dt.strftime('%A')} ({current_dt.weekday()})")
        print(f"   Time of Day: {current_time}")

        # Check if it's a weekend
        is_weekend = current_dt.weekday() >= 5
        print(f"   Is Weekend: {is_weekend}")

        # Market hours check
        market_open = time(9, 30)
        market_close = time(16, 0)
        is_market_hours = market_open <= current_time <= market_close
        print(f"   Market Hours (9:30-16:00 ET): {is_market_hours}")

        # Pre/Post market
        pre_market_start = time(4, 0)
        pre_market_end = time(9, 30)
        is_premarket = pre_market_start <= current_time < pre_market_end

        post_market_start = time(16, 0)
        post_market_end = time(20, 0)
        is_postmarket = post_market_start < current_time <= post_market_end

        print(f"   Pre-market (4:00-9:30): {is_premarket}")
        print(f"   Post-market (16:00-20:00): {is_postmarket}")
        print(f"   After hours: {current_time > post_market_end or current_time < pre_market_start}")

        # 2. OPTION DETAILS
        print(f"\n2. OPTION DETAILS:")
        print(f"   Symbol: SPY")
        print(f"   Strike: {option_asset.strike}")
        print(f"   Expiration: {option_asset.expiration}")
        print(f"   DTE: {(option_asset.expiration - current_date).days}")
        print(f"   Type: CALL")

        # 3. CURRENT QUOTE ATTEMPT
        print(f"\n3. CURRENT QUOTE DATA:")
        quote = self.get_quote(option_asset)

        if quote is None:
            print(f"   ❌ Quote is None")
        else:
            print(f"   ✓ Quote object exists")
            print(f"   Type: {type(quote)}")

            # Check all possible price fields
            fields_to_check = ['bid', 'ask', 'bid_price', 'ask_price', 'mid_price', 'price',
                             'last', 'last_price', 'close', 'open', 'high', 'low']

            for field in fields_to_check:
                if hasattr(quote, field):
                    value = getattr(quote, field)
                    if value is not None:
                        print(f"   ✓ {field}: {value}")
                    else:
                        print(f"   ✗ {field}: None")
                else:
                    print(f"   - {field}: not present")

            # Check volume and other fields
            if hasattr(quote, 'volume'):
                print(f"   Volume: {quote.volume}")
            if hasattr(quote, 'timestamp'):
                print(f"   Timestamp: {quote.timestamp}")
            if hasattr(quote, 'bid_size'):
                print(f"   Bid Size: {quote.bid_size}")
            if hasattr(quote, 'ask_size'):
                print(f"   Ask Size: {quote.ask_size}")

        # 4. LAST PRICE CHECK
        print(f"\n4. LAST PRICE DATA:")
        last_price = self.get_last_price(option_asset)
        if last_price is not None:
            print(f"   ✓ Last Price: {last_price}")
        else:
            print(f"   ✗ Last Price: None")

        # 5. HISTORICAL QUOTES - LOOK BACK IN TIME
        print(f"\n5. LOOKING BACK FOR HISTORICAL QUOTES:")

        # Try minute bars at different intervals
        lookback_intervals = [1, 5, 10, 30, 60, 120, 390]  # 390 minutes = full trading day

        for minutes in lookback_intervals:
            print(f"\n   Looking back {minutes} minutes:")
            try:
                bars = self.get_historical_prices(option_asset, minutes, "minute")
                if bars and hasattr(bars, 'df') and bars.df is not None:
                    df = bars.df
                    if len(df) > 0:
                        print(f"   ✓ Found {len(df)} minute bars")

                        # Show first and last bars
                        first_bar = df.iloc[0]
                        last_bar = df.iloc[-1]

                        print(f"      First bar: {df.index[0]}")
                        print(f"         Close: {first_bar['close']}, Volume: {first_bar['volume']}")

                        print(f"      Last bar: {df.index[-1]}")
                        print(f"         Close: {last_bar['close']}, Volume: {last_bar['volume']}")

                        # Check how many bars have actual price data
                        bars_with_prices = df[df['close'].notna() & (df['close'] > 0)]
                        bars_with_volume = df[df['volume'] > 0]

                        print(f"      Bars with valid prices: {len(bars_with_prices)}/{len(df)}")
                        print(f"      Bars with volume > 0: {len(bars_with_volume)}/{len(df)}")

                        # Find the most recent bar with valid data
                        if len(bars_with_prices) > 0:
                            most_recent_valid = bars_with_prices.iloc[-1]
                            time_since_valid = current_dt - most_recent_valid.name
                            print(f"      Most recent valid price: {most_recent_valid.name}")
                            print(f"         Price: {most_recent_valid['close']}")
                            print(f"         Time ago: {time_since_valid}")
                    else:
                        print(f"   ✗ DataFrame is empty")
                else:
                    print(f"   ✗ No data returned")
            except Exception as e:
                print(f"   ✗ Error: {e}")

        # 6. TRY DAILY BARS
        print(f"\n6. DAILY BAR HISTORY:")
        for days in [1, 5, 10]:
            print(f"\n   Looking back {days} days:")
            try:
                bars = self.get_historical_prices(option_asset, days, "day")
                if bars and hasattr(bars, 'df') and bars.df is not None:
                    df = bars.df
                    if len(df) > 0:
                        print(f"   ✓ Found {len(df)} daily bars")
                        for i in range(min(3, len(df))):
                            bar = df.iloc[i]
                            print(f"      {df.index[i].date()}: O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} V:{bar['volume']}")
                    else:
                        print(f"   ✗ DataFrame is empty")
                else:
                    print(f"   ✗ No data returned")
            except Exception as e:
                print(f"   ✗ Error: {e}")

        # 7. CHECK NEARBY STRIKES
        print(f"\n7. CHECKING NEARBY STRIKES (same expiry):")
        test_offsets = [-10, -5, -2, -1, 0, 1, 2, 5, 10]

        for offset in test_offsets:
            test_strike = option_asset.strike + offset
            test_option = Asset(
                "SPY",
                asset_type=Asset.AssetType.OPTION,
                expiration=option_asset.expiration,
                strike=test_strike,
                right=Asset.OptionRight.CALL,
            )

            test_quote = self.get_quote(test_option)

            if test_quote:
                has_bid = hasattr(test_quote, 'bid') and test_quote.bid is not None
                has_ask = hasattr(test_quote, 'ask') and test_quote.ask is not None

                if has_bid or has_ask:
                    bid_str = f"{test_quote.bid:.2f}" if has_bid else "None"
                    ask_str = f"{test_quote.ask:.2f}" if has_ask else "None"
                    print(f"   ✓ Strike {test_strike:6.0f} ({offset:+3d}): bid={bid_str}, ask={ask_str}")
                else:
                    print(f"   ✗ Strike {test_strike:6.0f} ({offset:+3d}): Quote exists but no bid/ask")
            else:
                print(f"   ✗ Strike {test_strike:6.0f} ({offset:+3d}): No quote")

        # 8. CHECK DIFFERENT EXPIRIES
        print(f"\n8. CHECKING DIFFERENT EXPIRIES (same strike):")
        chains_res = self.get_chains(Asset("SPY", asset_type=Asset.AssetType.STOCK))
        if chains_res:
            call_chains = chains_res.get("Chains", {}).get("CALL", {})
            sorted_expiries = sorted(call_chains.keys())[:10]  # Check first 10 expiries

            for exp_str in sorted_expiries:
                exp_date = self.options_expiry_to_datetime_date(exp_str)
                dte = (exp_date - current_date).days

                if option_asset.strike in call_chains[exp_str]:
                    test_option = Asset(
                        "SPY",
                        asset_type=Asset.AssetType.OPTION,
                        expiration=exp_date,
                        strike=option_asset.strike,
                        right=Asset.OptionRight.CALL,
                    )

                    test_quote = self.get_quote(test_option)

                    if test_quote:
                        has_bid = hasattr(test_quote, 'bid') and test_quote.bid is not None
                        has_ask = hasattr(test_quote, 'ask') and test_quote.ask is not None

                        if has_bid or has_ask:
                            bid_str = f"{test_quote.bid:.2f}" if has_bid else "None"
                            ask_str = f"{test_quote.ask:.2f}" if has_ask else "None"
                            print(f"   ✓ Exp {exp_str} (DTE:{dte:3d}): bid={bid_str}, ask={ask_str}")
                        else:
                            print(f"   ✗ Exp {exp_str} (DTE:{dte:3d}): Quote exists but no bid/ask")
                    else:
                        print(f"   ✗ Exp {exp_str} (DTE:{dte:3d}): No quote")

        print(f"\n{'='*80}")
        print(f"END DEEP ANALYSIS")
        print(f"{'='*80}\n")

    def on_trading_iteration(self):
        dt = self.get_datetime()
        weekday = dt.weekday()

        # Run analysis on Thursdays
        if weekday == 3:  # Thursday
            today_date = self.to_default_timezone(dt).date()

            # Check if we already analyzed today
            if self.vars.last_analysis_date == today_date:
                return

            # Pick option
            expiry_date, strike = self._pick_expiration_and_strike(dt)

            if expiry_date and strike:
                option_asset = Asset(
                    "SPY",
                    asset_type=Asset.AssetType.OPTION,
                    expiration=expiry_date,
                    strike=float(strike),
                    right=Asset.OptionRight.CALL,
                )

                # Deep analysis
                self._deep_analyze_quotes(option_asset, dt)

                # Mark that we analyzed today
                self.vars.last_analysis_date = today_date


if __name__ == "__main__":
    print("Starting DEEP quote timing analysis...")
    print("This will show exactly when quotes are requested and what data is available")
    print("Looking at market hours, historical data availability, and more\n")

    trading_fee = TradingFee(flat_fee=0.65)

    # Run for June and July to see patterns
    results = DeepQuoteAnalysis.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 7, 31),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )