#!/usr/bin/env python3
"""
Time Progression Analysis - Check quote availability at different times after market open
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta, time

class TimeProgressionAnalysis(Strategy):
    parameters = {
        "underlying_symbol": "SPY",
        "min_dte": 14,
        "max_dte": 28,
        "target_dte": 21,
    }

    def initialize(self):
        self.sleeptime = "1M"  # Check every minute
        if not hasattr(self.vars, "thursday_option"):
            self.vars.thursday_option = None
        if not hasattr(self.vars, "last_check_date"):
            self.vars.last_check_date = None
        self.log_message("TIME PROGRESSION ANALYSIS - Checking quotes at different times", color="blue")

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

    def _check_quote_at_time(self, option_asset, check_time_str):
        """Check if quote has bid/ask at a specific time"""
        current_dt = self.get_datetime()
        current_time = current_dt.time()

        quote = self.get_quote(option_asset)

        has_bid = False
        has_ask = False
        bid_val = None
        ask_val = None
        last_val = None

        if quote is not None:
            has_bid = hasattr(quote, 'bid') and quote.bid is not None and quote.bid > 0
            has_ask = hasattr(quote, 'ask') and quote.ask is not None and quote.ask > 0

            if has_bid:
                bid_val = quote.bid
            if has_ask:
                ask_val = quote.ask

        last_price = self.get_last_price(option_asset)
        if last_price is not None:
            last_val = last_price

        return {
            'time': check_time_str,
            'actual_time': current_time,
            'has_quote': quote is not None,
            'has_bid': has_bid,
            'has_ask': has_ask,
            'bid': bid_val,
            'ask': ask_val,
            'last': last_val
        }

    def on_trading_iteration(self):
        dt = self.get_datetime()
        current_time = dt.time()
        current_date = self.to_default_timezone(dt).date()
        weekday = dt.weekday()

        # On Thursday at 9:30, pick the option to monitor
        if weekday == 3 and current_time >= time(9, 30) and current_time < time(9, 31):
            if self.vars.thursday_option is None or self.vars.last_check_date != current_date:
                expiry_date, strike = self._pick_expiration_and_strike(dt)

                if expiry_date and strike:
                    self.vars.thursday_option = {
                        'expiry': expiry_date,
                        'strike': strike,
                        'date': current_date
                    }
                    self.vars.last_check_date = current_date

                    print(f"\n{'='*80}")
                    print(f"THURSDAY OPTION SELECTED - {current_date}")
                    print(f"Strike: {strike}, Expiry: {expiry_date}")
                    print(f"Will monitor quotes at: 9:30, 9:31, 9:32, 9:35, 9:40, 9:45, 10:00, 10:30, 11:00")
                    print(f"{'='*80}")

        # If we have a Thursday option for today, check quotes at specific times
        if self.vars.thursday_option and self.vars.thursday_option['date'] == current_date:

            # Define check times and tolerance
            check_times = [
                (time(9, 30), "9:30 AM"),
                (time(9, 31), "9:31 AM"),
                (time(9, 32), "9:32 AM"),
                (time(9, 35), "9:35 AM"),
                (time(9, 40), "9:40 AM"),
                (time(9, 45), "9:45 AM"),
                (time(10, 0), "10:00 AM"),
                (time(10, 30), "10:30 AM"),
                (time(11, 0), "11:00 AM"),
                (time(11, 30), "11:30 AM"),
                (time(12, 0), "12:00 PM"),
                (time(13, 0), "1:00 PM"),
                (time(14, 0), "2:00 PM"),
                (time(15, 0), "3:00 PM"),
                (time(15, 30), "3:30 PM"),
                (time(15, 45), "3:45 PM"),
                (time(15, 55), "3:55 PM"),
            ]

            for target_time, time_label in check_times:
                # Check if we're within 1 minute of the target time
                if (current_time >= target_time and
                    current_time < (datetime.combine(current_date, target_time) + timedelta(minutes=1)).time()):

                    option_asset = Asset(
                        "SPY",
                        asset_type=Asset.AssetType.OPTION,
                        expiration=self.vars.thursday_option['expiry'],
                        strike=float(self.vars.thursday_option['strike']),
                        right=Asset.OptionRight.CALL,
                    )

                    result = self._check_quote_at_time(option_asset, time_label)

                    # Format output
                    bid_str = f"${result['bid']:.2f}" if result['bid'] else "None"
                    ask_str = f"${result['ask']:.2f}" if result['ask'] else "None"
                    last_str = f"${result['last']:.2f}" if result['last'] else "None"

                    status = "✓" if (result['has_bid'] and result['has_ask']) else "✗"

                    print(f"{status} {time_label:10s} | Bid: {bid_str:8s} | Ask: {ask_str:8s} | Last: {last_str:8s}")

                    # Special detailed check at 9:40 AM
                    if target_time == time(9, 40):
                        print(f"\n  DETAILED CHECK AT 9:40 AM:")

                        # Check historical minute bars
                        print(f"  Historical minute bars (last 10 minutes):")
                        try:
                            bars = self.get_historical_prices(option_asset, 10, "minute")
                            if bars and hasattr(bars, 'df') and bars.df is not None:
                                df = bars.df
                                if len(df) > 0:
                                    print(f"    Found {len(df)} bars")
                                    for i in range(min(5, len(df))):
                                        bar = df.iloc[-(i+1)]  # Show most recent first
                                        print(f"    {df.index[-(i+1)]}: Close=${bar['close']:.2f}, Vol={bar['volume']}")
                                else:
                                    print(f"    No minute bars found")
                            else:
                                print(f"    No data returned")
                        except Exception as e:
                            print(f"    Error: {e}")

                        # Check a few nearby strikes
                        print(f"\n  Nearby strikes at 9:40 AM:")
                        for offset in [-2, -1, 0, 1, 2]:
                            test_strike = self.vars.thursday_option['strike'] + offset
                            test_option = Asset(
                                "SPY",
                                asset_type=Asset.AssetType.OPTION,
                                expiration=self.vars.thursday_option['expiry'],
                                strike=test_strike,
                                right=Asset.OptionRight.CALL,
                            )
                            test_result = self._check_quote_at_time(test_option, "")

                            status = "✓" if (test_result['has_bid'] and test_result['has_ask']) else "✗"
                            bid_str = f"${test_result['bid']:.2f}" if test_result['bid'] else "None"
                            ask_str = f"${test_result['ask']:.2f}" if test_result['ask'] else "None"

                            print(f"    {status} Strike {test_strike}: Bid={bid_str}, Ask={ask_str}")

                        print("")  # Extra line for readability


if __name__ == "__main__":
    print("Starting TIME PROGRESSION analysis...")
    print("This will check quote availability at multiple times throughout the trading day")
    print("Focus on: 9:30, 9:31, 9:32, 9:35, 9:40, 9:45, 10:00, etc.\n")

    trading_fee = TradingFee(flat_fee=0.65)

    # Run for June to see the pattern
    results = TimeProgressionAnalysis.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 30),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )