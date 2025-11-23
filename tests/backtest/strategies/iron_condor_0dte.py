from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, Order, TradingFee
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.credentials import IS_BACKTESTING

from lumibot.components.options_helper import OptionsHelper
from lumibot.components.vix_helper import VixHelper

from datetime import time, timedelta
import math
import os

class IronCondor0DTE(Strategy):
    """
    0DTE SPX Iron Condor Strategy
    ---------------------------------
    - Sells 25-delta iron condors on same-day (0DTE) SPX options and buys wings one strike further out.
    - Optional entry filters based on VIX change and underlying (SPX) intraday percent change.

    This code was generated based on the user prompt: 'make a bot that trades 25 delta iron condors 0dte spx long wings one strike out and backtest if its better to use filters such as vix change or underlying change % etc'
    """

    # Strategy parameters you can tune in backtests to compare performance across filters
    parameters = {
        # Target delta for short legs
        "delta_target": 0.25,
        # How many strikes away for the long wings (1 = one strike out)
        "wing_steps": 1,
        # Time window to enter and exit (HH:MM in exchange local time)
        "enter_time": "10:00",
        "exit_time": "15:45",
        # Max acceptable bid/ask spread percentage for each option leg (skip trade if exceeded)
        "max_spread_pct": 0.25,
        # Take profit: buy back when value is this fraction of original credit (e.g., 0.5 = 50% of credit)
        "tp_buyback_pct_of_credit": 0.5,
        # Stop loss: buy back when value reaches this multiple of original credit (e.g., 2.0 = 200% of credit)
        "sl_buyback_multiple_of_credit": 2.0,
        # Risk-based sizing: use up to this fraction of cash for the theoretical max risk of the condor
        "risk_per_trade_pct": 0.10,
        # Avoid entries too close to expiration (minutes)
        "min_minutes_to_expiry": 60,
        # Filters you can toggle on/off to compare results in backtests
        "vix_change_filter_enabled": True,
        # Do not enter if VIX has risen more than this % from today's reference (e.g., open or first read) in absolute terms
        "vix_change_max_abs_pct": 10.0,
        "underlying_change_filter_enabled": True,
        # Do not enter if SPX absolute change from today's reference exceeds this %
        "underlying_change_max_abs_pct": 1.0,
        # Only one new position per day (typical for 0DTE style)
        "max_positions_per_day": 1,
    }

    def initialize(self):
        # Run the bot every 5 minutes during the trading day
        self.sleeptime = "5M"

        # Helpers for options selection and VIX-based filters
        self.options_helper = OptionsHelper(self)
        self.vix_helper = VixHelper(self)

        # Use persistent storage for state that must survive restarts and across lifecycle hooks
        self.vars.underlying_asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)  # SPX index as the underlying for SPX options
        self.vars.opened_today = 0  # how many positions opened today
        self.vars.underlying_ref_price = None  # reference price for SPX to compute daily % change filter
        self.vars.vix_ref_price = None  # reference price for VIX to compute daily % change filter
        self.vars.open_trade = None  # track the currently open condor details for exits

        # Friendly label used only in logs (do not assign to self.name)
        self.vars.strategy_label = "IC-0DTE-25D"

    def before_market_opens(self):
        # Each trading day, reset state and let the strategy look for a fresh setup
        self.vars.opened_today = 0
        self.vars.open_trade = None
        self.vars.underlying_ref_price = None
        self.vars.vix_ref_price = None
        self.log_message("Daily reset complete. Waiting for market data and entry window.", color="blue")

    # ----------------------------
    # Utility helpers (simple and trader-friendly comments)
    # ----------------------------
    def _to_time(self, hhmm: str) -> time:
        # Convert "HH:MM" strings into a time object
        parts = hhmm.split(":")
        return time(int(parts[0]), int(parts[1]))

    def _minutes_to(self, dt, end_dt) -> int:
        # Helper to compute minutes between two datetimes
        return int((end_dt - dt).total_seconds() // 60)

    def _get_today_expiry(self, chains, call_or_put: str):
        # Get the expiration that is today (0DTE). If not available, return None.
        dt = self.get_datetime()
        target_date = dt.date()
        expiry = self.options_helper.get_expiration_on_or_after_date(target_date, chains, call_or_put, underlying_asset=self.vars.underlying_asset)
        if expiry and expiry == target_date:
            return expiry
        return None

    def _find_wing_strike(self, strikes_list, short_strike, steps, is_call_side):
        # For calls: wing is further OTM -> next higher strikes; for puts: wing is further OTM -> next lower strikes
        if not strikes_list:
            return None
        sorted_strikes = sorted(strikes_list)
        # Find index of the short strike in the available strikes; use nearest if exact not found
        idx = min(range(len(sorted_strikes)), key=lambda i: abs(sorted_strikes[i] - short_strike))
        if is_call_side:
            wing_idx = idx + steps
        else:
            wing_idx = idx - steps
        if 0 <= wing_idx < len(sorted_strikes):
            return sorted_strikes[wing_idx]
        return None

    def _build_condor_orders(self, chains, expiry, quantity):
        # This function builds the four legs of an iron condor:
        # short call (≈25Δ), long call wing (next strike above), short put (≈25Δ), long put wing (next strike below)
        params = self.get_parameters()
        target_delta = float(params.get("delta_target", 0.25))
        wing_steps = int(params.get("wing_steps", 1))
        underlying_price = self.get_last_price(self.vars.underlying_asset)
        if underlying_price is None:
            self.log_message("No SPX price available to build condor.", color="red")
            return None

        # Find 25-delta strikes on both sides using the helper
        short_call_strike = self.options_helper.find_strike_for_delta(
            self.vars.underlying_asset, underlying_price, target_delta, expiry, right="call"
        )
        short_put_strike = self.options_helper.find_strike_for_delta(
            self.vars.underlying_asset, underlying_price, target_delta, expiry, right="put"
        )
        if short_call_strike is None or short_put_strike is None:
            self.log_message("Could not find valid 25-delta strikes. Skipping entry.", color="red")
            return None

        # Get available strikes list for each side so we can select the wing one step out
        # Use the Chains convenience methods if present
        try:
            call_strikes_list = chains.strikes(expiry, "CALL")
            put_strikes_list = chains.strikes(expiry, "PUT")
        except Exception:
            # Fallback to raw dict access if convenience methods aren't available
            chains_dict = chains.get("Chains", {})
            call_strikes_list = chains_dict.get("CALL", {}).get(expiry.strftime("%Y-%m-%d"), [])
            put_strikes_list = chains_dict.get("PUT", {}).get(expiry.strftime("%Y-%m-%d"), [])

        long_call_strike = self._find_wing_strike(call_strikes_list, short_call_strike, wing_steps, is_call_side=True)
        long_put_strike = self._find_wing_strike(put_strikes_list, short_put_strike, wing_steps, is_call_side=False)

        if long_call_strike is None or long_put_strike is None:
            self.log_message("Wing strikes not available one step out. Skipping entry.", color="red")
            return None

        # Build the vertical spreads (short call spread + short put spread)
        # For call spread (short): lower_strike = short_call, upper_strike = long_call
        call_spread_orders = self.options_helper.build_call_vertical_spread_orders(
            self.vars.underlying_asset, expiry, lower_strike=short_call_strike, upper_strike=long_call_strike, quantity=quantity
        )
        # For put spread (short): upper_strike = short_put, lower_strike = long_put
        put_spread_orders = self.options_helper.build_put_vertical_spread_orders(
            self.vars.underlying_asset, expiry, upper_strike=short_put_strike, lower_strike=long_put_strike, quantity=quantity
        )

        if not call_spread_orders or not put_spread_orders:
            self.log_message("Failed to build condor orders.", color="red")
            return None

        all_orders = call_spread_orders + put_spread_orders
        details = {
            "short_call": short_call_strike,
            "long_call": long_call_strike,
            "short_put": short_put_strike,
            "long_put": long_put_strike,
        }
        return all_orders, details

    def _estimate_total_credit(self, open_orders):
        # Use helper to compute an approximate combined credit using mid prices
        credit = self.options_helper.calculate_multileg_limit_price(open_orders, limit_type="mid")
        if credit is None:
            return None
        # For a short condor, the limit price mid should be a credit value. Use absolute to be safe.
        return abs(float(credit))

    def _reverse_side(self, side):
        # Flip buy/sell to generate closing orders from opening legs
        if side in (Order.OrderSide.BUY, Order.OrderSide.BUY_TO_OPEN, Order.OrderSide.BUY_TO_COVER):
            return Order.OrderSide.SELL
        return Order.OrderSide.BUY

    def _build_close_orders_from_open(self, open_orders):
        close_orders = []
        for o in open_orders:
            # Create a mirror order on the same asset with the opposite side
            # We don't set limit prices here; helper will compute mid later when submitting
            close_orders.append(
                self.create_order(
                    o.asset, o.quantity, self._reverse_side(o.side), order_type=Order.OrderType.MARKET
                )
            )
        return close_orders

    def _filters_ok(self):
        # Apply optional filters using VIX change and SPX change to avoid entering on wild days
        params = self.get_parameters()
        use_vix = bool(params.get("vix_change_filter_enabled", True))
        use_under = bool(params.get("underlying_change_filter_enabled", True))
        vix_ok = True
        under_ok = True

        # Get current reference if not set
        if self.vars.vix_ref_price is None:
            v = self.vix_helper.get_vix_value()
            if v is not None:
                self.vars.vix_ref_price = float(v)
        if self.vars.underlying_ref_price is None:
            p = self.get_last_price(self.vars.underlying_asset)
            if p is not None:
                self.vars.underlying_ref_price = float(p)

        # VIX filter
        if use_vix and self.vars.vix_ref_price is not None:
            cur_v = self.vix_helper.get_vix_value()
            if cur_v is not None and cur_v > 0:
                vix_change = 100.0 * (float(cur_v) - self.vars.vix_ref_price) / self.vars.vix_ref_price
                self.log_message(f"VIX change vs ref: {vix_change:.2f}%", color="white")
                if abs(vix_change) > float(params.get("vix_change_max_abs_pct", 10.0)):
                    vix_ok = False
                    self.log_message("VIX change filter failed; skipping entry.", color="yellow")

        # Underlying filter
        if use_under and self.vars.underlying_ref_price is not None:
            cur_p = self.get_last_price(self.vars.underlying_asset)
            if cur_p is not None and self.vars.underlying_ref_price > 0:
                under_change = 100.0 * (float(cur_p) - self.vars.underlying_ref_price) / self.vars.underlying_ref_price
                self.log_message(f"SPX change vs ref: {under_change:.2f}%", color="white")
                if abs(under_change) > float(params.get("underlying_change_max_abs_pct", 1.0)):
                    under_ok = False
                    self.log_message("Underlying change filter failed; skipping entry.", color="yellow")

        return vix_ok and under_ok

    def _manage_open_trade(self):
        # If we have an open condor, check take-profit/stop/exit-time conditions to close it
        if not self.vars.open_trade:
            return
        trade = self.vars.open_trade
        params = self.get_parameters()
        dt = self.get_datetime()

        # Compute the estimated debit to close using mid prices
        close_orders = trade.get("close_orders")
        if not close_orders:
            return
        est_close_debit = self.options_helper.calculate_multileg_limit_price(close_orders, limit_type="mid")
        if est_close_debit is None:
            self.log_message("Unable to estimate close price. Will check again next iteration.", color="yellow")
            return
        est_close_debit = float(est_close_debit)
        open_credit = float(trade.get("open_credit", 0.0))
        if open_credit <= 0:
            return

        tp_buyback = float(params.get("tp_buyback_pct_of_credit", 0.5)) * open_credit
        sl_buyback = float(params.get("sl_buyback_multiple_of_credit", 2.0)) * open_credit

        self.log_message(
            f"Open credit ~ ${open_credit:.2f}, est. close debit ~ ${est_close_debit:.2f} | TP@${tp_buyback:.2f}, SL@${sl_buyback:.2f}",
            color="white",
        )

        # Time-based exit near end of day
        exit_t = self._to_time(params.get("exit_time", "15:45"))
        if dt.time() >= exit_t:
            self.log_message("Time-based exit triggered; closing condor.", color="yellow")
            self.options_helper.execute_orders(close_orders, limit_type="mid")
            self.add_marker("Time Exit", value=None, color="blue", symbol="star", detail_text="Closing at exit time")
            self.vars.open_trade = None
            return

        # Profit target: buy back at fraction of credit
        if est_close_debit <= tp_buyback:
            self.log_message("Take-profit hit; closing condor.", color="green")
            self.options_helper.execute_orders(close_orders, limit_type="mid")
            self.add_marker("TP", value=None, color="green", symbol="star", detail_text="TP reached")
            self.vars.open_trade = None
            return

        # Stop loss: buy back if cost explodes above threshold
        if est_close_debit >= sl_buyback:
            self.log_message("Stop-loss hit; closing condor.", color="red")
            self.options_helper.execute_orders(close_orders, limit_type="mid")
            self.add_marker("SL", value=None, color="red", symbol="star", detail_text="SL reached")
            self.vars.open_trade = None
            return

    def on_trading_iteration(self):
        params = self.get_parameters()
        dt = self.get_datetime()

        # Plot a simple line for the SPX price so we can visually follow along
        spx_price = self.get_last_price(self.vars.underlying_asset)
        if spx_price is not None:
            self.add_line("SPX", float(spx_price), color="black", width=2, detail_text="SPX Last Price")

        # Also plot VIX when available to understand regime changes
        vix_val = self.vix_helper.get_vix_value()
        if vix_val is not None:
            self.add_line("VIX", float(vix_val), color="orange", width=1, detail_text="VIX")

        # Establish reference prices once per day when first data comes in
        if self.vars.underlying_ref_price is None and spx_price is not None:
            self.vars.underlying_ref_price = float(spx_price)
            self.log_message(f"Set SPX reference price: {self.vars.underlying_ref_price:.2f}", color="blue")
        if self.vars.vix_ref_price is None and vix_val is not None:
            self.vars.vix_ref_price = float(vix_val)
            self.log_message(f"Set VIX reference price: {self.vars.vix_ref_price:.2f}", color="blue")

        # Always manage an existing position first (take-profit / stop / scheduled exit)
        self._manage_open_trade()

        # If we already opened our daily allocation, do nothing more today
        if self.vars.opened_today >= int(params.get("max_positions_per_day", 1)):
            return

        # Only try to enter during our entry window
        enter_t = self._to_time(params.get("enter_time", "10:00"))
        exit_t = self._to_time(params.get("exit_time", "15:45"))
        if not (enter_t <= dt.time() < exit_t):
            return

        # Filters: do not enter if the day is too volatile by our rules
        if not self._filters_ok():
            return

        # Retrieve options chains for SPX and find today's expiration (0DTE)
        chains = self.get_chains(self.vars.underlying_asset)
        if not chains:
            self.log_message("Options chains unavailable; cannot trade now.", color="red")
            return

        # Try to get 0DTE expiry for both call and put (some data providers need one side passed)
        expiry = self._get_today_expiry(chains, "call")
        if not expiry:
            expiry = self._get_today_expiry(chains, "put")
        if not expiry:
            self.log_message("No same-day expiration found; skipping.", color="yellow")
            return

        # Avoid entries too close to expiration
        # We approximate day end at 16:00 local exchange time
        approx_close_dt = dt.replace(hour=16, minute=0, second=0, microsecond=0)
        minutes_left = self._minutes_to(dt, approx_close_dt)
        if minutes_left < int(params.get("min_minutes_to_expiry", 60)):
            self.log_message("Too close to expiration; skipping entry.", color="yellow")
            return

        # Determine sizing based on theoretical max risk per condor (width * 100)
        # We'll compute wing width from the planned legs; to do so, we first build the condor with quantity=1
        built = self._build_condor_orders(chains, expiry, quantity=1)
        if not built:
            return
        open_orders, leg_details = built
        call_width = abs(float(leg_details["long_call"]) - float(leg_details["short_call"]))
        put_width = abs(float(leg_details["short_put"]) - float(leg_details["long_put"]))
        width = max(call_width, put_width)
        if width <= 0:
            self.log_message("Invalid wing width; skipping.", color="red")
            return

        max_risk_per_spread = width * 100.0  # ignoring credit for conservative sizing
        cash = self.get_cash()
        risk_pct = float(params.get("risk_per_trade_pct", 0.10))
        max_alloc = cash * risk_pct
        qty = max(1, int(max_alloc // max_risk_per_spread))
        if qty < 1:
            self.log_message("Insufficient cash to open even 1 spread.", color="yellow")
            return

        # Rebuild the condor orders at final quantity
        built_final = self._build_condor_orders(chains, expiry, quantity=qty)
        if not built_final:
            return
        open_orders, leg_details = built_final

        # Sanity check: avoid illiquid legs by ensuring option bid/ask are reasonable
        max_spread_pct = float(params.get("max_spread_pct", 0.25))
        for o in open_orders:
            evaluation = self.options_helper.evaluate_option_market(o.asset, max_spread_pct=max_spread_pct)
            # Log the evaluation so we can see if a leg is skipped due to wide spreads
            self.log_message(
                f"Leg {o.asset.symbol} {getattr(o.asset, 'right', None)} {getattr(o.asset, 'strike', None)} @ {evaluation}",
                color="white",
            )
            if evaluation.spread_too_wide:
                self.log_message("Leg spread too wide; skipping entry.", color="yellow")
                return

        # Estimate opening total credit so we can drive exits later
        est_credit = self._estimate_total_credit(open_orders)
        if est_credit is None:
            self.log_message("Could not estimate opening credit; skipping entry.", color="yellow")
            return

        # Submit the condor using mid pricing for a fair fill target
        submitted = self.options_helper.execute_orders(open_orders, limit_type="mid")
        if not submitted:
            self.log_message("Order submission failed; will try again later.", color="red")
            return

        # Build the corresponding closing orders template (reverse of open) for later exits
        close_orders = self._build_close_orders_from_open(open_orders)

        # Track this trade so we can manage exits
        self.vars.open_trade = {
            "open_dt": dt,
            "expiry": expiry,
            "open_orders": open_orders,
            "close_orders": close_orders,
            "open_credit": est_credit,
            "quantity": qty,
            "legs": leg_details,
        }
        self.vars.opened_today += 1

        # Add a marker to indicate entry (kept minimal to avoid clutter)
        self.add_marker(
            name="Entry",
            value=None,
            color="green",
            symbol="star",
            detail_text=f"{self.vars.strategy_label} Qty {qty} | Credit ~ ${est_credit:.2f} | Legs {leg_details}",
        )
        self.log_message(
            f"Entered {self.vars.strategy_label}: Qty {qty} | Est. credit ${est_credit:.2f} | Legs {leg_details}",
            color="green",
        )


if __name__ == "__main__":
    # Default parameters; you can tweak these for backtests or set via environment in your runner
    params = {
        "delta_target": 0.25,
        "wing_steps": 1,
        "enter_time": os.getenv("IC_ENTER_TIME", "10:00"),
        "exit_time": os.getenv("IC_EXIT_TIME", "15:45"),
        "max_spread_pct": float(os.getenv("IC_MAX_SPREAD_PCT", "0.25")),
        "tp_buyback_pct_of_credit": float(os.getenv("IC_TP_BUYBACK_PCT", "0.5")),
        "sl_buyback_multiple_of_credit": float(os.getenv("IC_SL_BUYBACK_MULT", "2.0")),
        "risk_per_trade_pct": float(os.getenv("IC_RISK_PCT", "0.10")),
        "min_minutes_to_expiry": int(os.getenv("IC_MIN_MINUTES_TO_EXPIRY", "60")),
        "vix_change_filter_enabled": os.getenv("IC_USE_VIX_FILTER", "true").lower() in ("1", "true", "yes"),
        "vix_change_max_abs_pct": float(os.getenv("IC_VIX_MAX_CHANGE_PCT", "10.0")),
        "underlying_change_filter_enabled": os.getenv("IC_USE_UNDER_FILTER", "true").lower() in ("1", "true", "yes"),
        "underlying_change_max_abs_pct": float(os.getenv("IC_UNDER_MAX_CHANGE_PCT", "1.0")),
        "max_positions_per_day": int(os.getenv("IC_MAX_POS_PER_DAY", "1")),
    }

    if IS_BACKTESTING:
        # -----------------------------
        # Backtesting with Polygon (needed for options data)
        # -----------------------------
        trading_fee = TradingFee(percent_fee=0.0005)  # small fee assumption

        results = IronCondor0DTE.backtest(
            datasource_class=PolygonDataBacktesting,
            backtesting_start=None,  # Set via environment when running backtests
            backtesting_end=None,    # Set via environment when running backtests
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            parameters=params,
            budget=float(os.getenv("IC_BUDGET", "100000")),
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
    else:
        # -----------------------------
        # Live trading path (broker is configured externally by environment)
        # -----------------------------
        trader = Trader()
        strategy = IronCondor0DTE(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            parameters=params,
        )
        trader.add_strategy(strategy)
        strategies = trader.run_all()
