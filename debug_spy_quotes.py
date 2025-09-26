#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta

class DebugSpyQuotes(Strategy):
    parameters = {
        "underlying_symbol": "SPY",
        "allocation_pct": 0.07,
        "buy_day_of_week": 3,  # Thursday
        "min_dte": 14,
        "max_dte": 28,
        "target_dte": 21,
        "hold_days": 7,
    }

    def initialize(self):
        self.sleeptime = "1D"
        if not hasattr(self.vars, "last_buy_date"):
            self.vars.last_buy_date = None
        if not hasattr(self.vars, "entries"):
            self.vars.entries = {}
        self.log_message("DEBUG Quote Analysis Strategy", color="blue")

    def _should_buy_today(self, dt) -> bool:
        params = self.get_parameters()
        target_dow = int(params.get("buy_day_of_week", 3))
        is_thursday = (dt.weekday() == target_dow)

        if not is_thursday:
            return False

        today = self.to_default_timezone(dt).date()
        if self.vars.last_buy_date == today:
            return False

        return True

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

        # Convert expirations to dates
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

        # Choose expiry closest to target
        expiry_date, dte = min(expiries, key=lambda x: abs(x[1] - target_dte))
        expiry_str = expiry_date.strftime("%Y-%m-%d")

        # Get strikes for chosen expiry
        strikes = call_chains.get(expiry_str)
        if strikes is None or len(strikes) == 0:
            return None, None

        strikes_list = [float(s) for s in list(strikes)]
        atm_strike = min(strikes_list, key=lambda s: abs(s - last_price))

        return expiry_date, atm_strike

    def _analyze_quote(self, option_asset, dt):
        """Detailed analysis of quote data"""
        today_date = self.to_default_timezone(dt).date()

        print(f"\n[QUOTE ANALYSIS] {today_date}")
        print(f"  Option: SPY {option_asset.strike} CALL exp:{option_asset.expiration}")

        # Get quote
        quote = self.get_quote(option_asset)
        print(f"  get_quote() returned: {quote}")

        if quote is not None:
            print(f"    Type: {type(quote)}")
            print(f"    Has bid: {hasattr(quote, 'bid')}")
            print(f"    Has ask: {hasattr(quote, 'ask')}")
            print(f"    Has bid_price: {hasattr(quote, 'bid_price')}")
            print(f"    Has ask_price: {hasattr(quote, 'ask_price')}")
            print(f"    Has mid_price: {hasattr(quote, 'mid_price')}")

            if hasattr(quote, 'bid'):
                print(f"    Bid: {quote.bid}")
            if hasattr(quote, 'ask'):
                print(f"    Ask: {quote.ask}")
            if hasattr(quote, 'bid_price'):
                print(f"    Bid Price: {quote.bid_price}")
            if hasattr(quote, 'ask_price'):
                print(f"    Ask Price: {quote.ask_price}")
            if hasattr(quote, 'mid_price'):
                print(f"    Mid Price: {quote.mid_price}")

            # Try to calculate mid if we have bid/ask
            calculated_mid = None
            if hasattr(quote, 'bid') and hasattr(quote, 'ask'):
                if quote.bid is not None and quote.ask is not None:
                    calculated_mid = (quote.bid + quote.ask) / 2
                    print(f"    Calculated mid from bid/ask: {calculated_mid}")

            if hasattr(quote, 'bid_price') and hasattr(quote, 'ask_price'):
                if quote.bid_price is not None and quote.ask_price is not None:
                    calculated_mid = (quote.bid_price + quote.ask_price) / 2
                    print(f"    Calculated mid from bid_price/ask_price: {calculated_mid}")

            # List all attributes
            print(f"    All attributes: {dir(quote)}")

            # Try to access as dict if possible
            if hasattr(quote, '__dict__'):
                print(f"    Quote.__dict__: {quote.__dict__}")
        else:
            print(f"  get_quote() returned None")

        # Get last price
        last_price = self.get_last_price(option_asset)
        print(f"  get_last_price() returned: {last_price}")

        return quote, last_price

    def on_trading_iteration(self):
        dt = self.get_datetime()
        today_date = self.to_default_timezone(dt).date()
        weekday = dt.weekday()
        weekday_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][weekday]

        # Only process Thursdays
        if weekday == 3 and self._should_buy_today(dt):
            print(f"\n{'='*60}")
            print(f"[THURSDAY] {today_date} - {weekday_name}")
            print(f"{'='*60}")

            # Get SPY price
            spy_asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
            spy_price = self.get_last_price(spy_asset)
            print(f"  SPY Price: {spy_price}")

            # Pick option
            expiry_date, strike = self._pick_expiration_and_strike(dt)

            if expiry_date and strike:
                print(f"  Selected Option: {strike} strike, expires {expiry_date}")

                option_asset = Asset(
                    "SPY",
                    asset_type=Asset.AssetType.OPTION,
                    expiration=expiry_date,
                    strike=float(strike),
                    right=Asset.OptionRight.CALL,
                )

                # Analyze quote in detail
                quote, last_price = self._analyze_quote(option_asset, dt)

                # Mark that we "bought" today to prevent duplicate analysis
                self.vars.last_buy_date = today_date
            else:
                print(f"  Could not find valid option")


if __name__ == "__main__":
    print("Starting detailed quote analysis...")
    print("This will examine what data is actually returned from get_quote()\n")

    trading_fee = TradingFee(flat_fee=0.65)

    # Run for just June to keep output manageable
    results = DebugSpyQuotes.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 6, 30),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )