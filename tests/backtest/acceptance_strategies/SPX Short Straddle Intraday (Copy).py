################################################################################
# Must Be Imported First If Run Locally
if True:
    import os
    import sys

    myPath = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")
    sys.path.insert(
        0,
        "/Users/robertgrzesik/Development/lumiwealth_tradier/",
    )
    sys.path.insert(0, "/Users/robertgrzesik/Development/quantstats_lumi/")
################################################################################

import math

import pytz  # (1) Added for explicit ET conversion

# NOTE: Minute-level strategy. For minute-level backtesting, use Polygon per framework guidance.
# Yahoo backtesting does not support options.
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.components.options_helper import OptionsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

"""
Strategy Description
--------------------
This strategy trades a 0DTE short straddle on SPXW at specific entry times with simple risk management.

User Query
----------
This code was refined based on the user prompt: 'Fix the backtest crash by ensuring the option assets use SPX as their underlying_asset, even if they were found via SPXW chains. The data provider requires SPX for settlement price lookups.'

Notes
-----
- Uses OptionsHelper to find expirations/options that actually have tradeable data during backtests.
- Uses quote.mid_price when possible (safer for options) and falls back to last trade if needed.
- Avoids assigning arbitrary attributes on self; persistent state stored in self.vars.
"""


class SPXShortStraddle(Strategy):
    # Keep parameters centralized (recommended pattern).
    parameters = {
        "entry_times": ["10:15", "10:59"],
        "eod_close_time": "15:55",
        "contracts_per_leg": 1,
        "stop_loss_pct": 0.20,  # 20% stop loss based on premium collected
        "max_spread_pct": 0.25,  # liquidity guard for option bid/ask spreads
    }

    def initialize(self):
        # Minute cadence
        self.sleeptime = "1M"

        # Options helper (REQUIRED for robust options selection in backtests)
        self.vars.options_helper = OptionsHelper(self)

        # (1) Timezone used for all time checks/logs
        self.vars.et_tz = pytz.timezone("America/New_York")

        # State variables
        self.vars.last_trade_date = None
        self.vars.last_processed_minute = None
        self.vars.entry_times = self.parameters.get("entry_times", ["10:15", "10:59"])
        self.vars.eod_close_time = self.parameters.get("eod_close_time", "15:55")
        self.vars.stop_loss_pct = float(self.parameters.get("stop_loss_pct", 0.20))
        self.vars.max_spread_pct = float(self.parameters.get("max_spread_pct", 0.25))
        self.vars.contracts_per_leg = int(self.parameters.get("contracts_per_leg", 1))

        # Track which entry slots we already used today
        self.vars.entries_taken_today = set()

        # Track current open straddle state
        # Stored as dict:
        # {
        #   "open": bool,
        #   "call_asset": Asset,
        #   "put_asset": Asset,
        #   "entry_premium": float,         # dollars (already includes multiplier)
        #   "stop_loss_threshold": float,   # dollars
        #   "entry_dt": datetime
        # }
        self.vars.current_straddle = {
            "open": False,
            "call_asset": None,
            "put_asset": None,
            "entry_premium": None,
            "stop_loss_threshold": None,
            "entry_dt": None,
        }

        # Underlying (index) used for chain + pricing
        # NOTE: Primary is SPXW per original intent; we will fallback to SPX in entry if SPXW chains are empty.
        self.vars.underlying_spxw = Asset("SPXW", asset_type=Asset.AssetType.INDEX)
        self.vars.underlying_spx = Asset("SPX", asset_type=Asset.AssetType.INDEX)

    # -------------------------
    # Helpers
    # -------------------------
    def _is_finite_positive(self, x):
        return x is not None and isinstance(x, (int, float)) and math.isfinite(x) and x > 0

    def _get_option_mark_price(self, option_asset: Asset):
        """Prefer quote.mid_price; fallback to last trade. Returns None if not actionable."""
        q = self.get_quote(option_asset)
        if (
            q is not None
            and self._is_finite_positive(q.bid)
            and self._is_finite_positive(q.ask)
            and self._is_finite_positive(q.mid_price)
        ):
            return float(q.mid_price)

        last = self.get_last_price(option_asset)
        if self._is_finite_positive(last):
            return float(last)

        return None

    def _close_straddle_if_open(self, reason: str):
        """Closes both legs if there is an open straddle. Uses buy_to_close."""
        if not self.vars.current_straddle.get("open", False):
            return

        call_asset = self.vars.current_straddle.get("call_asset")
        put_asset = self.vars.current_straddle.get("put_asset")
        qty = self.vars.contracts_per_leg

        if call_asset is None or put_asset is None:
            self.log_message(f"Close requested ({reason}) but straddle assets missing; clearing state.", color="red")
            self.vars.current_straddle = {
                "open": False,
                "call_asset": None,
                "put_asset": None,
                "entry_premium": None,
                "stop_loss_threshold": None,
                "entry_dt": None,
            }
            return

        self.log_message(
            f"Closing straddle ({reason}). BUY_TO_CLOSE {qty}x CALL {call_asset.symbol} {call_asset.expiration} {call_asset.strike} and "
            f"{qty}x PUT {put_asset.symbol} {put_asset.expiration} {put_asset.strike}.",
            color="yellow",
        )

        # Close both legs (market). Using create_order/submit_order to follow official signatures.
        call_close = self.create_order(call_asset, qty, Order.OrderSide.BUY_TO_CLOSE)
        put_close = self.create_order(put_asset, qty, Order.OrderSide.BUY_TO_CLOSE)
        self.submit_order([call_close, put_close])

        # State will be confirmed next iteration via positions, but we clear intent now.
        self.vars.current_straddle = {
            "open": False,
            "call_asset": None,
            "put_asset": None,
            "entry_premium": None,
            "stop_loss_threshold": None,
            "entry_dt": None,
        }

    def _check_stop_loss(self):
        """Checks stop loss for the currently tracked straddle."""
        if not self.vars.current_straddle.get("open", False):
            return

        call_asset = self.vars.current_straddle.get("call_asset")
        put_asset = self.vars.current_straddle.get("put_asset")
        entry_premium = self.vars.current_straddle.get("entry_premium")
        stop_thr = self.vars.current_straddle.get("stop_loss_threshold")

        if (
            call_asset is None
            or put_asset is None
            or not self._is_finite_positive(entry_premium)
            or not self._is_finite_positive(stop_thr)
        ):
            self.log_message("Stop-loss check skipped: missing straddle state.", color="red")
            return

        call_mark = self._get_option_mark_price(call_asset)
        put_mark = self._get_option_mark_price(put_asset)

        if not self._is_finite_positive(call_mark) or not self._is_finite_positive(put_mark):
            self.log_message(
                f"Stop-loss check: missing mark price(s). call_mark={call_mark}, put_mark={put_mark}. Holding.",
                color="yellow",
            )
            return

        # Options are quoted per contract; multiplier is typically 100 for SPX/SPXW
        multiplier = 100
        qty = self.vars.contracts_per_leg
        buyback_cost = (call_mark + put_mark) * multiplier * qty

        self.log_message(
            f"Stop-loss monitor: entry_premium=${entry_premium:.2f}, buyback_cost=${buyback_cost:.2f}, stop_thr=${stop_thr:.2f}.",
            color="white",
        )

        if buyback_cost >= stop_thr:
            self._close_straddle_if_open(reason=f"STOP LOSS hit (buyback ${buyback_cost:.2f} >= ${stop_thr:.2f})")

    def _enter_short_straddle(self, current_dt_et):
        """Builds and submits a short straddle (sell_to_open call + put) at ATM for 0DTE.

        current_dt_et MUST be Eastern Time (America/New_York).
        """
        # Only allow one open straddle at a time (keeps risk management simple and matches request)
        if self.vars.current_straddle.get("open", False):
            self.log_message("Entry skipped: already have an open straddle.", color="yellow")
            return

        # Verbose: show the ET datetime we are using for 0DTE comparisons
        self.log_message(
            f"[_enter_short_straddle] Entry attempt at ET={current_dt_et.strftime('%Y-%m-%d %H:%M:%S %Z%z')}",
            color="blue",
        )

        # Start with SPXW per original intent
        primary_underlying = self.vars.underlying_spxw
        fallback_underlying = self.vars.underlying_spx

        # 1) Get current underlying price (prefer primary, but fallback if missing)
        underlying_for_entry = primary_underlying
        underlying_price = self.get_last_price(underlying_for_entry)
        if not self._is_finite_positive(underlying_price):
            self.log_message(
                f"[_enter_short_straddle] {primary_underlying.symbol} price unavailable (got {underlying_price}). Trying {fallback_underlying.symbol} for price.",
                color="yellow",
            )
            underlying_for_entry = fallback_underlying
            underlying_price = self.get_last_price(underlying_for_entry)

        if not self._is_finite_positive(underlying_price):
            self.log_message(
                f"[_enter_short_straddle] Entry aborted: underlying price unavailable for both {primary_underlying.symbol} and {fallback_underlying.symbol}.",
                color="red",
            )
            return

        self.log_message(
            f"[_enter_short_straddle] Underlying price: {underlying_for_entry.symbol}={underlying_price:.2f}",
            color="blue",
        )

        # 2) Fetch option chain for SPXW, fallback to SPX if empty
        self.log_message(f"[_enter_short_straddle] Fetching chains for {primary_underlying.symbol}...", color="blue")
        chains = self.get_chains(primary_underlying)
        if not chains:
            self.log_message(
                f"[_enter_short_straddle] No chains for {primary_underlying.symbol}. Trying fallback {fallback_underlying.symbol} (per user request).",
                color="yellow",
            )
            chains = self.get_chains(fallback_underlying)
            if not chains:
                self.log_message(
                    f"[_enter_short_straddle] Entry aborted: could not fetch chains for {primary_underlying.symbol} OR {fallback_underlying.symbol}.",
                    color="red",
                )
                return

            # If SPX chains exist, use SPX for option selection.
            underlying_for_chain = fallback_underlying
            self.log_message(
                f"[_enter_short_straddle] Using {underlying_for_chain.symbol} chains for this entry.", color="blue"
            )
        else:
            underlying_for_chain = primary_underlying
            self.log_message(
                f"[_enter_short_straddle] Using {underlying_for_chain.symbol} chains for this entry.", color="blue"
            )

        # 3) Filter for 0DTE (expiration = today)
        today_et = current_dt_et.date()
        target_date = today_et  # 0DTE

        self.log_message(
            f"[_enter_short_straddle] Looking for 0DTE expiry for {underlying_for_chain.symbol}: today_et={today_et}",
            color="blue",
        )

        # CRITICAL: use OptionsHelper to find an expiration that exists *and has data*
        expiry = self.vars.options_helper.get_expiration_on_or_after_date(
            target_date, chains, "call", underlying_asset=underlying_for_chain
        )

        if expiry is None:
            self.log_message(
                f"[_enter_short_straddle] Entry aborted: OptionsHelper returned no valid expiration for target_date={target_date} on {underlying_for_chain.symbol}.",
                color="yellow",
            )
            return

        if expiry != today_et:
            self.log_message(
                f"[_enter_short_straddle] Entry aborted: nearest valid expiry is {expiry}, not 0DTE today_et={today_et} for {underlying_for_chain.symbol}.",
                color="yellow",
            )
            return

        self.log_message(
            f"[_enter_short_straddle] Found tradeable 0DTE expiry={expiry} for {underlying_for_chain.symbol}.",
            color="blue",
        )

        # 4) Find ATM call and put
        rounded_atm = round(float(underlying_price) / 5.0) * 5.0
        self.log_message(
            f"[_enter_short_straddle] Finding valid options near ATM strike {rounded_atm} for expiry {expiry} on {underlying_for_chain.symbol}.",
            color="blue",
        )

        call_asset = self.vars.options_helper.find_next_valid_option(
            underlying_for_chain, rounded_atm, expiry, put_or_call="call"
        )
        put_asset = self.vars.options_helper.find_next_valid_option(
            underlying_for_chain, rounded_atm, expiry, put_or_call="put"
        )

        if call_asset is None or put_asset is None:
            self.log_message(
                f"[_enter_short_straddle] Entry aborted: could not find valid ATM options. call_asset={call_asset}, put_asset={put_asset}",
                color="red",
            )
            return

        # (USER REQUEST / CRITICAL FIX)
        # Some data providers require SPX as the option's underlying_asset for settlement price lookups,
        # even when the chain/series is SPXW. Force the underlying_asset to SPX to prevent settlement crashes.
        call_asset.underlying_asset = self.vars.underlying_spx
        put_asset.underlying_asset = self.vars.underlying_spx
        self.log_message(
            f"[_enter_short_straddle] Adjusted option underlying_asset for settlement: "
            f"call_underlying={getattr(call_asset.underlying_asset, 'symbol', None)}, "
            f"put_underlying={getattr(put_asset.underlying_asset, 'symbol', None)}",
            color="blue",
        )

        # 5) Liquidity check + determine actionable prices
        call_eval = self.vars.options_helper.evaluate_option_market(call_asset, max_spread_pct=self.vars.max_spread_pct)
        put_eval = self.vars.options_helper.evaluate_option_market(put_asset, max_spread_pct=self.vars.max_spread_pct)

        # REQUIRED logging of evaluation
        self.log_message(f"Call market eval: {call_eval}", color="blue")
        self.log_message(f"Put market eval: {put_eval}", color="blue")

        if (not self.vars.options_helper.has_actionable_price(call_eval)) or (
            not self.vars.options_helper.has_actionable_price(put_eval)
        ):
            self.log_message(
                f"Entry skipped: non-actionable prices. Call flags={call_eval.data_quality_flags}, Put flags={put_eval.data_quality_flags}",
                color="yellow",
            )
            return

        # Premium collected (use sell_price if available, else fallback as set by helper)
        call_sell = float(call_eval.sell_price)
        put_sell = float(put_eval.sell_price)
        multiplier = 100
        qty = self.vars.contracts_per_leg
        premium_collected = (call_sell + put_sell) * multiplier * qty

        if not self._is_finite_positive(premium_collected):
            self.log_message("Entry skipped: computed premium collected invalid.", color="red")
            return

        stop_loss_threshold = premium_collected * (1.0 + self.vars.stop_loss_pct)

        self.log_message(
            f"Placing short straddle: SELL_TO_OPEN {qty}x CALL strike={call_asset.strike} @ ~{call_sell:.2f}, "
            f"SELL_TO_OPEN {qty}x PUT strike={put_asset.strike} @ ~{put_sell:.2f}. "
            f"UnderlyingUsedForChains={underlying_for_chain.symbol}. "
            f"ForcedSettlementUnderlying=SPX. Premium=${premium_collected:.2f}, StopLoss=${stop_loss_threshold:.2f}",
            color="yellow",
        )

        # Sell both legs (simple market orders)
        call_order = self.create_order(call_asset, qty, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.MARKET)
        put_order = self.create_order(put_asset, qty, Order.OrderSide.SELL_TO_OPEN, order_type=Order.OrderType.MARKET)
        self.submit_order([call_order, put_order])

        # Persist full Asset objects (required for options)
        self.vars.current_straddle = {
            "open": True,
            "call_asset": call_asset,
            "put_asset": put_asset,
            "entry_premium": premium_collected,
            "stop_loss_threshold": stop_loss_threshold,
            "entry_dt": current_dt_et,
        }

    # -------------------------
    # Main loop
    # -------------------------
    def on_trading_iteration(self):
        # Convert self.get_datetime() to America/New_York for ALL time checks
        current_dt_utc = self.get_datetime()
        current_dt_et = current_dt_utc.astimezone(self.vars.et_tz)
        current_time_et = current_dt_et.strftime("%H:%M")
        current_date_et = current_dt_et.date()

        # Log the converted time every 30 minutes (minute==0 or 30)
        if current_dt_et.minute in (0, 30):
            self.log_message(
                f"DEBUG TIME: utc={current_dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z%z')} | et={current_dt_et.strftime('%Y-%m-%d %H:%M:%S %Z%z')}",
                color="blue",
            )

        # Reset daily counters (use ET date)
        if self.vars.last_trade_date != current_date_et:
            self.vars.last_trade_date = current_date_et
            self.vars.entries_taken_today = set()
            self.log_message(f"New trading day detected (ET date {current_date_et}). Reset entry slots.", color="blue")

        # Guard: avoid duplicate processing within the same minute (use ET minute key)
        current_minute_key = current_dt_et.strftime("%Y-%m-%d %H:%M")
        if self.vars.last_processed_minute == current_minute_key:
            return
        self.vars.last_processed_minute = current_minute_key

        # End-of-day close at 15:55 ET
        if current_time_et == self.vars.eod_close_time:
            if self.vars.current_straddle.get("open", False):
                self._close_straddle_if_open(reason=f"EOD close time {self.vars.eod_close_time} ET")
            else:
                self.log_message(
                    f"EOD time {self.vars.eod_close_time} ET reached; no open straddle to close.", color="white"
                )
            return

        # Stop loss monitoring (every minute)
        self._check_stop_loss()

        # Entry at configured times (only once per slot per day)
        if current_time_et in self.vars.entry_times:
            if current_time_et in self.vars.entries_taken_today:
                self.log_message(f"Entry time {current_time_et} already used today; skipping.", color="white")
                return

            self.vars.entries_taken_today.add(current_time_et)
            self.log_message(f"Entry time matched ({current_time_et} ET). Attempting entry.", color="yellow")
            self._enter_short_straddle(current_dt_et)
        else:
            # Keep the log minimal (not every minute), only occasionally.
            if current_dt_et.minute in (0, 30):
                self.log_message(
                    f"No entry this minute (ET {current_time_et}). Next entry times: {self.vars.entry_times}.",
                    color="white",
                )


if __name__ == "__main__":
    if IS_BACKTESTING:
        # Backtesting (minute-level): Polygon is appropriate.
        trading_fee = TradingFee(percent_fee=0.0)

        results = SPXShortStraddle.backtest(
            datasource_class=PolygonDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=100000,
        )
    else:
        trader = Trader()
        strategy = SPXShortStraddle(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
        trader.add_strategy(strategy)
        trader.run_all()
