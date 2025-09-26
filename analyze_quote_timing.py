#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee, Bars
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta, time

class AnalyzeQuoteTiming(Strategy):
    parameters = {
        "underlying_symbol": "SPY",
        "allocation_pct": 0.07,
        "buy_day_of_week": 3,  # Thursday
        "min_dte": 14,
        "max_dte": 28,
        "target_dte": 21,
    }

    def initialize(self):
        self.sleeptime = "1D"
        if not hasattr(self.vars, "last_buy_date"):
            self.vars.last_buy_date = None
        self.log_message("Quote Timing Analysis Strategy", color="blue")

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
            except Exception as e:
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

    def _analyze_data_availability(self, option_asset, dt):
        """Comprehensive analysis of data availability at different times"""

        current_dt = self.get_datetime()
        current_time = current_dt.time()
        current_date = self.to_default_timezone(current_dt).date()

        print(f"\n{'='*70}")
        print(f"DATA AVAILABILITY ANALYSIS")
        print(f"{'='*70}")
        print(f"Current DateTime: {current_dt}")
        print(f"Current Time: {current_time}")
        print(f"Current Date: {current_date}")
        print(f"Day of Week: {current_dt.strftime('%A')}")
        print(f"Option: SPY {option_asset.strike} CALL exp:{option_asset.expiration}")
        print(f"DTE: {(option_asset.expiration - current_date).days}")

        # Check if market is open
        market_open = time(9, 30)
        market_close = time(16, 0)
        is_market_hours = market_open <= current_time <= market_close
        is_weekday = current_dt.weekday() < 5

        print(f"\nMarket Status:")
        print(f"  Is Weekday: {is_weekday}")
        print(f"  Is Market Hours (9:30-16:00): {is_market_hours}")

        # 1. Current quote
        print(f"\n1. CURRENT QUOTE (at {current_time}):")
        quote = self.get_quote(option_asset)
        self._print_quote_details(quote)

        # 2. Current last price
        print(f"\n2. CURRENT LAST PRICE:")
        last_price = self.get_last_price(option_asset)
        print(f"   Last Price: {last_price}")

        # 3. Try to get historical data (minute bars)
        print(f"\n3. HISTORICAL MINUTE BARS (last 60 minutes):")
        try:
            minute_bars = self.get_historical_prices(option_asset, 60, "minute")
            if minute_bars and hasattr(minute_bars, 'df') and minute_bars.df is not None:
                df = minute_bars.df
                print(f"   Found {len(df)} minute bars")
                if len(df) > 0:
                    print(f"   Latest bar: {df.index[-1]} - Close: {df['close'].iloc[-1]}")
                    print(f"   First bar: {df.index[0]} - Close: {df['close'].iloc[0]}")
                    # Check for any bars with volume
                    bars_with_volume = df[df['volume'] > 0]
                    print(f"   Bars with volume > 0: {len(bars_with_volume)}")
                    if len(bars_with_volume) > 0:
                        print(f"   Last bar with volume: {bars_with_volume.index[-1]}")
            else:
                print(f"   No minute bars available")
        except Exception as e:
            print(f"   Error getting minute bars: {e}")

        # 4. Try to get daily bars
        print(f"\n4. HISTORICAL DAILY BARS (last 5 days):")
        try:
            daily_bars = self.get_historical_prices(option_asset, 5, "day")
            if daily_bars and hasattr(daily_bars, 'df') and daily_bars.df is not None:
                df = daily_bars.df
                print(f"   Found {len(df)} daily bars")
                if len(df) > 0:
                    for i in range(min(3, len(df))):
                        print(f"   {df.index[i].date()}: O:{df['open'].iloc[i]:.2f} H:{df['high'].iloc[i]:.2f} L:{df['low'].iloc[i]:.2f} C:{df['close'].iloc[i]:.2f} V:{df['volume'].iloc[i]}")
            else:
                print(f"   No daily bars available")
        except Exception as e:
            print(f"   Error getting daily bars: {e}")

        # 5. Check different strikes for the same expiry
        print(f"\n5. OTHER STRIKES FOR SAME EXPIRY:")
        spy_price = self.get_last_price(Asset("SPY", asset_type=Asset.AssetType.STOCK))
        if spy_price:
            # Check strikes around ATM
            test_strikes = [
                option_asset.strike - 5,
                option_asset.strike - 2,
                option_asset.strike,
                option_asset.strike + 2,
                option_asset.strike + 5,
            ]

            for test_strike in test_strikes:
                test_option = Asset(
                    "SPY",
                    asset_type=Asset.AssetType.OPTION,
                    expiration=option_asset.expiration,
                    strike=test_strike,
                    right=Asset.OptionRight.CALL,
                )
                test_quote = self.get_quote(test_option)
                test_last = self.get_last_price(test_option)

                has_bid = test_quote and test_quote.bid is not None
                has_ask = test_quote and test_quote.ask is not None

                status = "✓" if (has_bid or has_ask or test_last) else "✗"
                print(f"   {status} Strike {test_strike}: Quote bid:{test_quote.bid if test_quote else None} ask:{test_quote.ask if test_quote else None} last:{test_last}")

        # 6. Check different expiries
        print(f"\n6. DIFFERENT EXPIRIES (same strike):")
        chains_res = self.get_chains(Asset("SPY", asset_type=Asset.AssetType.STOCK))
        if chains_res:
            call_chains = chains_res.get("Chains", {}).get("CALL", {})
            sorted_expiries = sorted(call_chains.keys())[:5]  # Check first 5 expiries

            for exp_str in sorted_expiries:
                exp_date = self.options_expiry_to_datetime_date(exp_str)
                if option_asset.strike in call_chains[exp_str]:
                    test_option = Asset(
                        "SPY",
                        asset_type=Asset.AssetType.OPTION,
                        expiration=exp_date,
                        strike=option_asset.strike,
                        right=Asset.OptionRight.CALL,
                    )
                    test_quote = self.get_quote(test_option)
                    test_last = self.get_last_price(test_option)

                    has_data = (test_quote and (test_quote.bid or test_quote.ask)) or test_last
                    status = "✓" if has_data else "✗"
                    dte = (exp_date - current_date).days
                    print(f"   {status} Exp {exp_str} (DTE:{dte}): Quote exists:{test_quote is not None} Last:{test_last}")

        return quote, last_price

    def _print_quote_details(self, quote):
        if quote:
            print(f"   Quote exists: Yes")
            print(f"   bid: {quote.bid}")
            print(f"   ask: {quote.ask}")
            print(f"   bid_size: {quote.bid_size if hasattr(quote, 'bid_size') else 'N/A'}")
            print(f"   ask_size: {quote.ask_size if hasattr(quote, 'ask_size') else 'N/A'}")
            print(f"   mid_price: {quote.mid_price}")
            print(f"   price: {quote.price}")
            print(f"   volume: {quote.volume}")
            print(f"   timestamp: {quote.timestamp if hasattr(quote, 'timestamp') else 'N/A'}")

            # Calculate mid if we have bid/ask
            if quote.bid is not None and quote.ask is not None:
                calculated_mid = (quote.bid + quote.ask) / 2
                print(f"   calculated mid: {calculated_mid}")
        else:
            print(f"   Quote exists: No")

    def on_trading_iteration(self):
        dt = self.get_datetime()
        weekday = dt.weekday()

        # Only process Thursdays
        if weekday == 3:  # Thursday
            today_date = self.to_default_timezone(dt).date()

            # Check if we already analyzed today
            if self.vars.last_buy_date == today_date:
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

                # Detailed analysis
                quote, last_price = self._analyze_data_availability(option_asset, dt)

                # Mark that we analyzed today
                self.vars.last_buy_date = today_date


if __name__ == "__main__":
    print("Starting comprehensive quote timing analysis...")
    print("This will examine data availability at different times and for different options\n")

    trading_fee = TradingFee(flat_fee=0.65)

    # Run for just June to keep output manageable
    results = AnalyzeQuoteTiming.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 30),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )