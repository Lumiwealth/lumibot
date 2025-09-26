#!/usr/bin/env python3
"""
Fixed SPY Weekly ATM Calls Strategy
------------------------------------
This version properly handles missing quote data by skipping trades
when bid/ask data is unavailable from the data source.
"""

import os
import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee, Order
from lumibot.backtesting import PolygonDataBacktesting
from datetime import datetime, timedelta
import math

class SpyWeeklyATMCallsFixed(Strategy):
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
        self.log_message("Fixed SPY ATM Calls Strategy - Validates quote data", color="blue")

    def _asset_key(self, option_asset: Asset) -> str:
        exp = option_asset.expiration.isoformat() if option_asset.expiration else "NA"
        right = getattr(option_asset, "right", "NA")
        return f"{option_asset.symbol}-{right}-{option_asset.strike}-{exp}"

    def _pick_expiration_and_strike(self, dt):
        params = self.get_parameters()
        underlying_symbol = params.get("underlying_symbol", "SPY")
        min_dte = int(params.get("min_dte", 14))
        max_dte = int(params.get("max_dte", 28))
        target_dte = int(params.get("target_dte", 21))

        underlying_asset = Asset(underlying_symbol, asset_type=Asset.AssetType.STOCK)
        last_price = self.get_last_price(underlying_asset)

        if last_price is None:
            self.log_message("SPY price unavailable; skipping selection.", color="red")
            return None, None

        chains_res = self.get_chains(underlying_asset)
        if not chains_res:
            self.log_message("Option chains unavailable; skipping.", color="red")
            return None, None

        call_chains = chains_res.get("Chains", {}).get("CALL")
        if not call_chains:
            self.log_message("CALL chains unavailable; skipping.", color="red")
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
            self.log_message("No expirations within 14–28 days; skipping this iteration.", color="yellow")
            return None, None

        expiry_date, _ = min(expiries, key=lambda x: abs(x[1] - target_dte))
        expiry_str = expiry_date.strftime("%Y-%m-%d")

        strikes = call_chains.get(expiry_str)
        if strikes is None or len(strikes) == 0:
            self.log_message(f"No strikes for chosen expiry {expiry_str}; skipping.", color="red")
            return None, None

        strikes_list = [float(s) for s in list(strikes)]
        atm_strike = min(strikes_list, key=lambda s: abs(s - last_price))

        return expiry_date, atm_strike

    def _should_buy_today(self, dt) -> bool:
        params = self.get_parameters()
        target_dow = int(params.get("buy_day_of_week", 3))
        is_target_day = (dt.weekday() == target_dow)

        if not is_target_day:
            return False

        today = self.to_default_timezone(dt).date()
        if self.vars.last_buy_date == today:
            return False

        return True

    def _has_valid_quote(self, option_asset: Asset) -> bool:
        """Check if we have valid bid/ask quote data for the option."""
        quote = self.get_quote(option_asset)

        if quote is None:
            self.log_message(f"No quote available for {self._asset_key(option_asset)}", color="yellow")
            return False

        # Check if we have valid bid and ask prices
        has_bid = hasattr(quote, 'bid') and quote.bid is not None and quote.bid > 0
        has_ask = hasattr(quote, 'ask') and quote.ask is not None and quote.ask > 0

        if not (has_bid and has_ask):
            self.log_message(
                f"Invalid quote for {self._asset_key(option_asset)}: bid={quote.bid if hasattr(quote, 'bid') else 'N/A'}, ask={quote.ask if hasattr(quote, 'ask') else 'N/A'}",
                color="yellow"
            )
            return False

        # Sanity check: bid should be less than ask
        if quote.bid >= quote.ask:
            self.log_message(
                f"Invalid bid/ask spread for {self._asset_key(option_asset)}: bid={quote.bid}, ask={quote.ask}",
                color="red"
            )
            return False

        return True

    def _place_buy_order(self, option_asset: Asset):
        params = self.get_parameters()
        allocation_pct = float(params.get("allocation_pct", 0.07))

        # First check if we have valid quote data
        if not self._has_valid_quote(option_asset):
            self.log_message(
                f"Skipping trade - no valid quote data available for {self._asset_key(option_asset)}",
                color="yellow"
            )
            return None

        # Get the quote (we know it's valid now)
        quote = self.get_quote(option_asset)
        mid_price = (quote.bid + quote.ask) / 2

        portfolio_value = float(self.get_portfolio_value())
        target_notional = portfolio_value * allocation_pct
        cost_per_contract = mid_price * 100.0
        contracts = int(math.floor(target_notional / cost_per_contract))

        if contracts <= 0:
            self.log_message(
                f"Not enough budget for 1 contract at ~${mid_price:.2f}; skipping buy.",
                color="yellow"
            )
            return None

        # Place a limit order at the mid-price
        order = self.create_order(
            option_asset,
            contracts,
            Order.OrderSide.BUY,
            order_type=Order.OrderType.LIMIT,
            limit_price=mid_price,
            order_class=Order.OrderClass.SIMPLE,
        )

        self.log_message(
            f"Submitting LIMIT buy for {contracts}x {self._asset_key(option_asset)} @ ${mid_price:.2f} (bid=${quote.bid:.2f}, ask=${quote.ask:.2f})",
            color="green",
        )

        submitted = self.submit_order(order)

        if submitted is not None:
            last = self.get_last_price(Asset("SPY", asset_type=Asset.AssetType.STOCK))
            if last is not None and math.isfinite(last):
                self.add_marker(
                    name="Buy ATM Call",
                    value=float(last),
                    color="green",
                    symbol="arrow-up",
                    size=10,
                    detail_text=f"{contracts}x {self._asset_key(option_asset)}",
                )

        return submitted

    def _place_sell_order(self, option_asset: Asset, quantity: int):
        # For exits, we can use market orders since we're just closing positions
        order = self.create_order(
            option_asset,
            quantity,
            Order.OrderSide.SELL,
            order_type=Order.OrderType.MARKET,
            order_class=Order.OrderClass.SIMPLE,
        )

        self.log_message(
            f"Submitting MARKET sell for {quantity}x {self._asset_key(option_asset)} (7-day exit)",
            color="yellow",
        )

        submitted = self.submit_order(order)

        if submitted is not None:
            last = self.get_last_price(Asset("SPY", asset_type=Asset.AssetType.STOCK))
            if last is not None and math.isfinite(last):
                self.add_marker(
                    name="Sell ATM Call",
                    value=float(last),
                    color="red",
                    symbol="arrow-down",
                    size=10,
                    detail_text=f"Exit {self._asset_key(option_asset)}",
                )

        return submitted

    def on_trading_iteration(self):
        dt = self.get_datetime()

        # Manage existing positions: exit any SPY call options held for >= 7 days
        hold_days = int(self.parameters.get("hold_days", 7))
        now_dt = self.to_default_timezone(dt)
        positions = self.get_positions()

        for pos in positions:
            if pos.asset.asset_type == Asset.AssetType.FOREX and pos.asset.symbol == "USD":
                continue
            if pos.asset.asset_type != Asset.AssetType.OPTION:
                continue
            if pos.asset.symbol != self.parameters.get("underlying_symbol", "SPY"):
                continue
            if getattr(pos.asset, "right", None) != Asset.OptionRight.CALL:
                continue

            key = self._asset_key(pos.asset)

            if key not in self.vars.entries:
                self.vars.entries[key] = now_dt
                self.log_message(f"Tracking entry time for {key}.", color="white")

            entry_dt = self.vars.entries.get(key, now_dt)
            days_held = (now_dt - entry_dt).days

            if days_held >= hold_days:
                qty = int(pos.quantity)
                if qty > 0:
                    self._place_sell_order(pos.asset, qty)
                    if key in self.vars.entries:
                        del self.vars.entries[key]

        # If it's Thursday and we haven't bought yet today, try to place a new buy
        if self._should_buy_today(dt):
            expiry_date, strike = self._pick_expiration_and_strike(dt)

            if expiry_date is None or strike is None:
                self.log_message("Could not find a valid expiry/strike today.", color="yellow")
                return

            option_asset = Asset(
                self.parameters.get("underlying_symbol", "SPY"),
                asset_type=Asset.AssetType.OPTION,
                expiration=expiry_date,
                strike=float(strike),
                right=Asset.OptionRight.CALL,
            )

            submitted = self._place_buy_order(option_asset)

            if submitted is not None:
                self.vars.last_buy_date = self.to_default_timezone(dt).date()
                key = self._asset_key(option_asset)
                if key not in self.vars.entries:
                    self.vars.entries[key] = self.to_default_timezone(dt)

    def on_filled_order(self, position, order, price, quantity, multiplier):
        side = order.side if hasattr(order, "side") else ""
        try:
            q = float(quantity)
        except Exception:
            q = quantity
        self.log_message(f"Filled {side} {q} of {self._asset_key(order.asset)} @ {price}.", color="white")


if __name__ == "__main__":
    print("Running fixed strategy that validates quote data...")
    print("This strategy will skip trades when bid/ask quotes are unavailable\n")

    trading_fee = TradingFee(flat_fee=0.65)

    results = SpyWeeklyATMCallsFixed.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=datetime(2025, 6, 1),
        backtesting_end=datetime(2025, 7, 31),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=None,
        budget=100000,
    )