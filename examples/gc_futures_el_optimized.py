import os
from datetime import datetime, time, timedelta

import matplotlib
import pandas as pd
from zoneinfo import ZoneInfo

from lumibot.backtesting import DataBentoDataBacktesting
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

matplotlib.use("Agg")  # Use non-interactive backend for headless environments
import matplotlib.pyplot as plt
import numpy as np

"""
Futures One-Sided ATR Bracket Strategy (1-minute)
-------------------------------------------------
This code was generated based on the user prompt: 'Unified Spec: Port this strategy to LumiBot (Python)'

Notes for users:
- This strategy ports the core trading logic from the provided EasyLanguage source into LumiBot while normalizing execution and session rules.
- It is one-sided (long-only by default) and runs strictly on completed 1-minute bars, submitting a market entry the moment a signal is detected.
- It attaches a non-trailing ATR(20) profit target and stop loss using the decision bar's close as the baseline and respects Topstep-style session controls.
"""


class FuturesOneSidedATRStrategy(Strategy):
    # Class variable to store all collected data for plotting
    _collected_data = None

    # Parameters exposed to users. Keep names human-friendly and aligned with the canonical spec.
    parameters = {
        # Instrument & timeframe
        "symbol_root": "GC",  # Continuous futures root symbol (e.g., GC)
        "timestep": "minute",  # We use 1-minute bars only
        # Sizing (fixed contracts only)
        "fixed_contracts": 1,
        "broker_min_contracts": 1,
        # ATR bracket settings (non-trailing)
        "atr_period": 20,
        "pt_mult": 5.0,
        "sl_mult": 2.0,
        "use_atr_profit": True,
        "use_atr_stop": True,
        "tick_size": 0.1,  # Default GC tick; override if your instrument differs
        # Time-based exit (bars in trade). If None, disabled. In the source this is 180.
        "max_time": 180,
        # Session rules (Topstep/TopstepX style)
        "topstep_flat_time_ct": "15:10",  # Must be flat by 3:10 PM CT
        "topstep_buffer_minutes": 15,  # Safety buffer => flat by 2:55 PM CT
        "evening_reopen_ct": "17:00",  # Reopens at 5:00 PM CT daily
        "use_nyse_overlay_for": {"ES", "NQ"},
        "enforce_weekend_block": True,  # Block entries Fri after buffer until Sun 17:00 CT
        "early_close_ct": None,  # e.g., "12:00" for early close days; None to disable
        "early_close_buffer_minutes": 30,
        # Market bias (one-sided, opposite direction is unimplemented by design)
        "bias": "long_only",  # or "short_only" (no short logic implemented here)
    }

    # --------------- Helper methods for indicators and rounding ---------------
    @staticmethod
    def _wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
        # Wilder's RSI implementation: EMA with alpha=1/period on gains and losses
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _wilder_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
        # True Range then Wilder's smoothing (EMA alpha=1/period)
        prev_close = close.shift(1)
        tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1 / period, adjust=False).mean()
        return atr

    @staticmethod
    def _round_to_tick(price: float, tick: float) -> float:
        if price is None or tick is None or tick <= 0:
            return price
        # Round to nearest tick with appropriate precision to avoid FP noise
        rounded = round(round(price / tick) * tick, 10)
        return rounded

    @staticmethod
    def _format_stamps(dt_utc: datetime) -> str:
        # Produce a log-friendly timestamp string showing UTC, CT, and ET simultaneously
        ct = dt_utc.astimezone(ZoneInfo("US/Central"))
        et = dt_utc.astimezone(ZoneInfo("US/Eastern"))
        return f"UTC {dt_utc.strftime('%Y-%m-%d %H:%M:%S')} | CT {ct.strftime('%Y-%m-%d %H:%M:%S')} | ET {et.strftime('%Y-%m-%d %H:%M:%S')}"

    def _get_ct_et_now(self, dt_utc: datetime):
        ct = dt_utc.astimezone(ZoneInfo("US/Central"))
        et = dt_utc.astimezone(ZoneInfo("US/Eastern"))
        return ct, et

    # -------------------------- Session Gating Logic --------------------------
    def _parse_hhmm(self, s: str) -> time:
        hh, mm = s.split(":")
        return time(int(hh), int(mm))

    def _compute_buffer_cutoff_ct(self, ct_now: datetime) -> datetime:
        # Base flat time (e.g., 15:10 CT), apply buffer minutes (e.g., 15 -> 14:55)
        flat_hhmm = self.parameters.get("topstep_flat_time_ct")
        buf_min = self.parameters.get("topstep_buffer_minutes", 15)
        flat_t = self._parse_hhmm(flat_hhmm)
        flat_dt = ct_now.replace(hour=flat_t.hour, minute=flat_t.minute, second=0, microsecond=0)
        buffer_cutoff = flat_dt - timedelta(minutes=buf_min)
        return buffer_cutoff

    def _compute_evening_reopen_ct(self, ct_now: datetime) -> datetime:
        reopen_hhmm = self.parameters.get("evening_reopen_ct", "17:00")
        r_t = self._parse_hhmm(reopen_hhmm)
        reopen_dt = ct_now.replace(hour=r_t.hour, minute=r_t.minute, second=0, microsecond=0)
        # If already past today's reopen, next reopen is tomorrow at same time
        if ct_now >= reopen_dt:
            reopen_dt = (ct_now + timedelta(days=1)).replace(hour=r_t.hour, minute=r_t.minute, second=0, microsecond=0)
        # Weekend handling: if reopen falls on Saturday, shift to Sunday; if Friday cooldown -> Sunday reopen
        if reopen_dt.weekday() == 5:  # Saturday
            # Move to Sunday at same reopen time
            reopen_dt = reopen_dt + timedelta(days=1)
        return reopen_dt

    def _next_weekend_reopen_ct(self, ct_now: datetime) -> datetime:
        # Sunday 17:00 CT from the reference point
        reopen_hhmm = self.parameters.get("evening_reopen_ct", "17:00")
        r_t = self._parse_hhmm(reopen_hhmm)
        days_ahead = (6 - ct_now.weekday()) % 7  # Days until Sunday
        sunday = (ct_now + timedelta(days=days_ahead)).replace(
            hour=r_t.hour, minute=r_t.minute, second=0, microsecond=0
        )
        if ct_now > sunday:
            sunday = sunday + timedelta(days=7)
        return sunday

    def _entries_allowed_now(self, dt_utc: datetime) -> tuple[bool, str, datetime | None]:
        # Returns (allowed, reason, reopen_utc_if_blocked)
        ct_now, et_now = self._get_ct_et_now(dt_utc)

        # Weekend block
        if self.parameters.get("enforce_weekend_block", True):
            cutoff = self._compute_buffer_cutoff_ct(ct_now)
            # No entries after Friday cutoff until Sunday 17:00 CT
            if (
                (ct_now.weekday() == 4 and ct_now >= cutoff)
                or (ct_now.weekday() in (5,))
                or (ct_now.weekday() == 6 and ct_now < self._next_weekend_reopen_ct(ct_now))
            ):
                reopen_ct = self._next_weekend_reopen_ct(ct_now)
                reopen_utc = reopen_ct.astimezone(ZoneInfo("UTC"))
                return False, "Weekend block (Topstep)", reopen_utc

        # Daily buffer cutoff before required flat time
        cutoff = self._compute_buffer_cutoff_ct(ct_now)
        if ct_now >= cutoff:
            reopen_ct = self._compute_evening_reopen_ct(ct_now)
            reopen_utc = reopen_ct.astimezone(ZoneInfo("UTC"))
            return False, "Daily flat buffer reached (Topstep)", reopen_utc

        # Optional early close hook
        early_close = self.parameters.get("early_close_ct")
        if early_close:
            try:
                ec_t = self._parse_hhmm(early_close)
                ec_dt = ct_now.replace(hour=ec_t.hour, minute=ec_t.minute, second=0, microsecond=0)
                ec_buf = self.parameters.get("early_close_buffer_minutes", 30)
                early_buffer_cutoff = ec_dt - timedelta(minutes=ec_buf)
                if ct_now >= early_buffer_cutoff:
                    reopen_ct = self._compute_evening_reopen_ct(ct_now)
                    reopen_utc = reopen_ct.astimezone(ZoneInfo("UTC"))
                    return False, "Early close buffer reached", reopen_utc
            except Exception:
                pass

        # NYSE overlay window for ES/NQ only
        root = self.parameters.get("symbol_root", "").upper()
        overlay_set = set(self.parameters.get("use_nyse_overlay_for", {"ES", "NQ"}))
        if root in overlay_set:
            nyse_open = et_now.replace(hour=9, minute=30, second=0, microsecond=0)
            nyse_close = et_now.replace(hour=16, minute=0, second=0, microsecond=0)
            if not (nyse_open <= et_now < nyse_close):
                # Outside NYSE session, re-open next ET 09:30 (or today if later)
                if et_now >= nyse_close:
                    next_open_et = (et_now + timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
                else:
                    next_open_et = nyse_open
                reopen_utc = next_open_et.astimezone(ZoneInfo("UTC"))
                return False, "NYSE overlay window closed", reopen_utc

        return True, "Entries allowed", None

    # -------------------------- Strategy lifecycle ---------------------------
    def initialize(self):
        # Market and iteration pacing
        # Futures trade nearly 24/5; this aligns to CME-like hours; Topstep rules enforced separately
        self.set_market("us_futures")
        self.sleeptime = "1M"  # Evaluate once per completed 1-minute bar

        # Strategy state stored in self.vars to persist reliably
        self.vars.base_asset = Asset(self.parameters.get("symbol_root", "GC"), asset_type=Asset.AssetType.CONT_FUTURE)
        self.vars.last_bar_dt = None  # Track last processed bar timestamp to avoid double-processing
        self.vars.entry_dt = None  # Decision bar datetime for the active trade
        self.vars.entry_baseline = None
        self.vars.entry_atr = None
        self.vars.bars_in_trade = 0
        self.vars.cooldown_utc = None  # Used when a forced flatten triggers a cooldown until reopen

        # Cache for historical data to avoid repeated calls
        self.vars.cached_bars = None
        self.vars.cache_last_update = None

        # Performance optimization: cache expensive calculations
        self.vars.rsi_cache = {}  # Cache RSI calculations by timestamp
        self.vars.atr_cache = {}  # Cache ATR calculations by timestamp
        self.vars.sma_cache = {}  # Cache SMA calculations by timestamp
        self.vars.cache_max_size = 1000  # Limit cache size to prevent memory issues

        # Debug counters for cache performance
        self.vars.data_fetch_count = 0
        self.vars.cache_hit_count = 0

        # Progress monitoring for debugging
        self.vars.total_iterations = 0
        self.vars.progress_log_interval = 1000  # Log every 1000 iterations
        self.vars.start_time = None

        # Friendly log of configuration
        self.log_message(
            f"Initialized for {self.vars.base_asset.symbol} on 1-minute bars. Tick={self.parameters.get('tick_size')}, bias={self.parameters.get('bias')}"
        )

    def _manage_cache_size(self, cache_dict):
        """Manage cache size to prevent memory issues"""
        if len(cache_dict) > self.vars.cache_max_size:
            # Remove oldest 20% of entries
            keys_to_remove = list(cache_dict.keys())[: int(self.vars.cache_max_size * 0.2)]
            for key in keys_to_remove:
                del cache_dict[key]

    def _get_cache_key(self, calculation_type: str, timestamp: datetime, **kwargs) -> str:
        """Generate cache key for calculations"""
        params_str = "_".join(f"{k}:{v}" for k, v in sorted(kwargs.items()))
        return f"{calculation_type}_{timestamp.isoformat()}_{params_str}"

    # Utility to determine current position quantity for our base asset
    def _position_qty(self) -> float:
        pos = self.get_position(self.vars.base_asset)
        if pos is None:
            return 0.0
        return float(pos.quantity)

    # Compute trading quantities (fixed contracts only)
    def _compute_quantity(self) -> int:
        q = max(1, int(self.parameters.get("broker_min_contracts", 1)), int(self.parameters.get("fixed_contracts", 1)))
        return q

    def _entry_signal_long(self, df: pd.DataFrame) -> bool:
        # Signal mirrors the source:
        # - Three strictly descending lows over the last 3 completed bars
        # - RSI(14) crosses below 70 (prev >= 70 and curr < 70)
        # - 200-bar simple average rising (current > value 2 bars ago)
        # Source: see EasyLanguage condition and inputs/outputs in EL_test.txt
        # (three descending lows, RSI cross below 70, SMA200 rising) — ported faithfully.
        if len(df) < 205:
            return False

        # Last three lows strictly descending - use only last 3 values
        low_series = df["low"]
        lows_desc = low_series.iloc[-3] > low_series.iloc[-2] > low_series.iloc[-1]

        # RSI(14) Wilder - compute only on last needed data
        close_series = df["close"]
        if len(close_series) < 16:  # Need at least 16 for RSI(14) with 2 previous values
            return False

        # Use cached RSI calculation for better performance
        current_dt = self.get_datetime()
        rsi_recent = self._cached_rsi(close_series.tail(16), 14, current_dt)
        if rsi_recent.isna().iloc[-1] or rsi_recent.isna().iloc[-2]:
            return False
        rsi_prev, rsi_curr = rsi_recent.iloc[-2], rsi_recent.iloc[-1]
        rsi_cross_below_70 = (rsi_prev >= 70) and (rsi_curr < 70)

        # SMA200 rising - compute rolling only on needed data
        if len(close_series) < 203:  # Need 200 + 3 for comparison
            return False

        # Compute SMA200 only on last 203 values
        sma200_recent = close_series.tail(203).rolling(200).mean()
        if sma200_recent.isna().iloc[-1] or sma200_recent.isna().iloc[-3]:
            return False
        sma200_rising = sma200_recent.iloc[-1] > sma200_recent.iloc[-3]

        signal = lows_desc and rsi_cross_below_70 and sma200_rising
        return signal

    def _cached_rsi(self, close_series: pd.Series, period: int, timestamp: datetime) -> pd.Series:
        """Cached RSI calculation to avoid repeated computations"""
        cache_key = self._get_cache_key("rsi", timestamp, period=period, len=len(close_series))

        if cache_key in self.vars.rsi_cache:
            return self.vars.rsi_cache[cache_key]

        # Manage cache size
        self._manage_cache_size(self.vars.rsi_cache)

        # Compute and cache RSI
        rsi_result = self._wilder_rsi(close_series, period)
        self.vars.rsi_cache[cache_key] = rsi_result
        return rsi_result

    def _cached_atr(
        self, high: pd.Series, low: pd.Series, close: pd.Series, period: int, timestamp: datetime
    ) -> pd.Series:
        """Cached ATR calculation to avoid repeated computations"""
        cache_key = self._get_cache_key("atr", timestamp, period=period, len=len(close))

        if cache_key in self.vars.atr_cache:
            return self.vars.atr_cache[cache_key]

        # Manage cache size
        self._manage_cache_size(self.vars.atr_cache)

        # Compute and cache ATR
        atr_result = self._wilder_atr(high, low, close, period)
        self.vars.atr_cache[cache_key] = atr_result
        return atr_result

    def _compute_bracket_prices_long(self, df: pd.DataFrame) -> tuple[float | None, float | None, float | None]:
        # Baseline is decision bar close; ATR is Wilder(20) by default
        atr_period = int(self.parameters.get("atr_period", 20))
        pt_mult = float(self.parameters.get("pt_mult", 0.0))
        sl_mult = float(self.parameters.get("sl_mult", 0.0))
        use_tp = bool(self.parameters.get("use_atr_profit", True))
        use_sl = bool(self.parameters.get("use_atr_stop", True))
        tick = float(self.parameters.get("tick_size", 0.1))

        # Compute ATR only on the needed data (period + 1 for calculation)
        atr_data_length = atr_period + 1
        high_series = df["high"].tail(atr_data_length)
        low_series = df["low"].tail(atr_data_length)
        close_series = df["close"].tail(atr_data_length)

        # Use cached ATR calculation for better performance
        current_dt = self.get_datetime()
        atr = self._cached_atr(high_series, low_series, close_series, atr_period, current_dt)
        if atr.isna().iloc[-1]:
            return None, None, None

        baseline = float(close_series.iloc[-1])
        atr_now = float(atr.iloc[-1])

        tp = baseline + atr_now * pt_mult if use_tp else None
        sl = baseline - atr_now * sl_mult if use_sl else None

        tp = self._round_to_tick(tp, tick) if tp is not None else None
        sl = self._round_to_tick(sl, tick) if sl is not None else None

        return baseline, tp, sl

    def _update_time_exit(self, df: pd.DataFrame) -> bool:
        # Returns True if time-based exit was executed
        max_time = self.parameters.get("max_time", None)
        if max_time is None:
            return False
        qty = self._position_qty()
        if qty <= 0:
            return False
        if self.vars.entry_dt is None:
            return False
        # Count bars since the first completed bar AFTER entry_decision_dt
        bars_since = int((df.index > self.vars.entry_dt).sum())
        self.vars.bars_in_trade = bars_since
        if bars_since >= int(max_time):
            # Exit market immediately, cancel any open child orders first
            self.cancel_open_orders()
            order = self.close_position(self.vars.base_asset)
            dt_utc = self.get_datetime()
            self.log_message(f"TimeX({bars_since}) -> Market exit sent | {self._format_stamps(dt_utc)}", color="yellow")
            # Add a marker to visualize time exit
            last_close = float(df["close"].iloc[-1])
            self.add_marker("Time Exit", last_close, color="yellow", symbol="arrow-down", size=8)
            # Reset entry tracking; cooldown is not set by time exit
            self.vars.entry_dt = None
            self.vars.entry_baseline = None
            self.vars.entry_atr = None
            self.vars.bars_in_trade = 0
            return True
        return False

    def _maybe_force_flat_and_set_cooldown(self, df: pd.DataFrame) -> None:
        # If within buffer cutoff (or early close buffer), close positions immediately and set cooldown until reopen
        dt_utc = self.get_datetime()
        ct_now, _ = self._get_ct_et_now(dt_utc)
        cutoff = self._compute_buffer_cutoff_ct(ct_now)

        should_flatten = False
        reason = None
        if ct_now >= cutoff:
            should_flatten = True
            reason = "Daily flat buffer reached"

        early_close = self.parameters.get("early_close_ct")
        if early_close and not should_flatten:
            try:
                ec_t = self._parse_hhmm(early_close)
                ec_dt = ct_now.replace(hour=ec_t.hour, minute=ec_t.minute, second=0, microsecond=0)
                ec_buf = self.parameters.get("early_close_buffer_minutes", 30)
                early_buffer_cutoff = ec_dt - timedelta(minutes=ec_buf)
                if ct_now >= early_buffer_cutoff:
                    should_flatten = True
                    reason = "Early close buffer reached"
            except Exception:
                pass

        if should_flatten:
            qty = self._position_qty()
            if qty > 0:
                self.cancel_open_orders()
                self.close_position(self.vars.base_asset)
                self.log_message(f"Force flatten ({reason}). {self._format_stamps(dt_utc)}", color="yellow")
                # Track cooldown until evening reopen
                reopen_ct = self._compute_evening_reopen_ct(ct_now)
                reopen_utc = reopen_ct.astimezone(ZoneInfo("UTC"))
                self.vars.cooldown_utc = reopen_utc
                self.log_message(
                    f"Entries blocked until session reopen (UTC {reopen_utc}) or cooldown (UTC {reopen_utc}), whichever is later.",
                    color="yellow",
                )
                # Clear entry tracking
                self.vars.entry_dt = None
                self.vars.entry_baseline = None
                self.vars.entry_atr = None
                self.vars.bars_in_trade = 0

    def on_trading_iteration(self):
        # Always work off the last COMPLETED 1-minute bar
        asset = self.vars.base_asset

        # Check if we need to refresh our cache (fetch new data only when needed)
        current_dt = self.get_datetime()

        # Initialize cache or refresh if it's been more than 5 minutes since last update
        # More aggressive caching to reduce get_historical_prices calls
        if (
            self.vars.cached_bars is None
            or self.vars.cache_last_update is None
            or (current_dt - self.vars.cache_last_update).total_seconds() >= 300  # 5 minutes instead of 1
        ):

            # Log cache miss and fetch data
            self.vars.data_fetch_count += 1
            bars = self.get_historical_prices(asset, length=400, timestep="minute", include_after_hours=True)
            if bars is None or bars.df is None or bars.df.empty:
                self.log_message("No bars available yet; skipping.")
                return

            self.vars.cached_bars = bars
            self.vars.cache_last_update = current_dt
            if self.vars.data_fetch_count % 10 == 0:  # Log every 10th fetch
                self.log_message(f"Data fetch #{self.vars.data_fetch_count} at {current_dt}")
        else:
            self.vars.cache_hit_count += 1
            bars = self.vars.cached_bars

        df = bars.df  # Don't copy - use reference directly
        last_dt = df.index[-1]

        # Only evaluate when a new 1-minute bar has completed
        if self.vars.last_bar_dt is not None and last_dt <= self.vars.last_bar_dt:
            return
        self.vars.last_bar_dt = last_dt

        # Progress monitoring
        self.vars.total_iterations += 1
        if self.vars.start_time is None:
            self.vars.start_time = current_dt

        # Log progress at intervals
        if self.vars.total_iterations % self.vars.progress_log_interval == 0:
            elapsed = (current_dt - self.vars.start_time).total_seconds()
            rate = self.vars.total_iterations / elapsed if elapsed > 0 else 0
            cache_hit_rate = (
                (self.vars.cache_hit_count / (self.vars.data_fetch_count + self.vars.cache_hit_count) * 100)
                if (self.vars.data_fetch_count + self.vars.cache_hit_count) > 0
                else 0
            )
            self.log_message(
                f"📊 Progress: {self.vars.total_iterations:,} iterations | "
                f"Rate: {rate:.1f} iter/sec | "
                f"Cache hit rate: {cache_hit_rate:.1f}% | "
                f"Data fetches: {self.vars.data_fetch_count}"
            )

        # Collect data for CSV export - Phase 1 focus
        if FuturesOneSidedATRStrategy._collected_data is None:
            FuturesOneSidedATRStrategy._collected_data = []
        # Store only the last bar to avoid expensive copying
        if len(df) > 0:
            last_bar = df.iloc[[-1]].copy()  # Copy only the last row
            FuturesOneSidedATRStrategy._collected_data.append(last_bar)

        # REMOVED: Charting calls that were causing performance issues
        # The self.add_line() calls were taking 779s (23% of runtime)
        # Charting should only be done after backtest completes

        # Session controls: exit if needed (buffer cutoff), and compute if entries are allowed
        self._maybe_force_flat_and_set_cooldown(df)

        dt_utc = self.get_datetime()
        allowed, reason, reopen_utc = self._entries_allowed_now(dt_utc)
        if not allowed:
            self.log_message(f"Entries blocked: {reason}. {self._format_stamps(dt_utc)}")

        # If a cooldown is active, enforce it
        if self.vars.cooldown_utc is not None and dt_utc < self.vars.cooldown_utc:
            self.log_message(f"Cooldown active until {self.vars.cooldown_utc} UTC. {self._format_stamps(dt_utc)}")
            # Time exits should still be checked even on cooldown
            self._update_time_exit(df)
            return
        else:
            # Clear cooldown once passed
            if self.vars.cooldown_utc is not None and dt_utc >= self.vars.cooldown_utc:
                self.log_message("Cooldown expired; entries may resume if windows allow.")
                self.vars.cooldown_utc = None

        # First, evaluate time-based exit BEFORE any new entries
        if self._update_time_exit(df):
            # If time exit fired, skip new entries on this bar
            return

        # One-sided bias: only long logic is implemented; short side is intentionally not evaluated
        bias = self.parameters.get("bias", "long_only")
        if bias != "long_only":
            # Opposite side is unimplemented by design per spec
            return

        # Block new entries if windows disallow
        if not allowed:
            # Just return; exits already handled above
            return

        # Do not enter if already in a position
        if self._position_qty() > 0:
            return

        # Compute entry signal (long-only)
        signal_long = self._entry_signal_long(df)
        self.log_message(f"Signal check -> long={signal_long}. {self._format_stamps(dt_utc)}")

        if signal_long:
            baseline, tp, sl = self._compute_bracket_prices_long(df)
            if baseline is None:
                self.log_message("ATR baseline unavailable; skipping entry this bar.", color="red")
                return

            qty = self._compute_quantity()
            # Create a MARKET entry with non-trailing BRACKET children (TP/SL if enabled)
            order = self.create_order(
                asset,
                qty,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                order_class=Order.OrderClass.BRACKET,
                secondary_limit_price=tp,
                secondary_stop_price=sl,
            )
            submitted = self.submit_order(order)

            # Store entry context to manage time-based exit counting
            self.vars.entry_dt = last_dt  # decision bar datetime
            self.vars.entry_baseline = baseline
            self.vars.entry_atr = None  # can be filled if needed
            self.vars.bars_in_trade = 0

            # Log and mark the entry
            self.log_message(
                f"BUY {qty} @ MARKET | TP={tp} SL={sl} baseline={baseline}. {self._format_stamps(dt_utc)}",
                color="green",
            )
            self.add_marker("Buy Signal", baseline, color="green", symbol="arrow-up", size=10)

    # ------------------------------ Order events ------------------------------
    def on_filled_order(self, position, order, price, quantity, multiplier):
        # Helpful log to track fills
        dt_utc = self.get_datetime()
        side = order.side if hasattr(order, "side") else "?"
        self.log_message(
            f"Filled: {side} {quantity} {position.asset.symbol} @ {price}. {self._format_stamps(dt_utc)}",
            color="green",
        )


def plot_raw_prices_and_histogram(df: pd.DataFrame, symbol: str, plot_filename: str = "latestplot.png"):
    """
    Standalone function to create price plot and histogram.
    Creates two plots:
    1. Price plot of raw prices (sampled for large datasets)
    2. Histogram showing the number of minutes of data per day
    """
    try:
        if df is None or df.empty:
            print("No data available for plotting.")
            return False

        print(f"Creating plots with {len(df)} data points...")

        # Sample data for large datasets to prevent hanging
        max_plot_points = 50000  # Limit to prevent matplotlib hangs
        if len(df) > max_plot_points:
            print(f"⚠️  Large dataset detected ({len(df)} points). Sampling to {max_plot_points} points for plotting...")
            # Sample evenly across the dataset
            step = len(df) // max_plot_points
            df_plot = df.iloc[::step]  # Take every Nth point
            print(f"✓ Sampled down to {len(df_plot)} points for plotting")
        else:
            df_plot = df

        # Create figure with two subplots stacked vertically
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle(
            f"Raw Price Data Analysis - {symbol} (Sampled from {len(df)} points)", fontsize=14, fontweight="bold"
        )

        # Plot 1: Sampled prices
        ax1.plot(df_plot.index, df_plot["close"], linewidth=0.8, color="blue", alpha=0.7, label="Close Price (Sampled)")
        ax1.set_xlabel("Time")
        ax1.set_ylabel("Price")
        ax1.set_title(f"Raw Prices (1-minute granularity) - {len(df)} bars")
        ax1.grid(True, alpha=0.3)
        ax1.legend()

        # Format x-axis dates
        fig.autofmt_xdate()

        # Plot 2: Histogram of minutes per day
        # Group by date and count minutes per day
        df_copy = df.copy()
        df_copy["date"] = df_copy.index.date
        daily_counts = df_copy.groupby("date").size()

        if len(daily_counts) == 0:
            print("No daily data found for histogram.")
            plt.close()
            return False

        ax2.bar(range(len(daily_counts)), daily_counts.values, color="steelblue", alpha=0.7, edgecolor="black")
        ax2.set_xlabel("Day Index")
        ax2.set_ylabel("Number of Minutes")
        ax2.set_title(f"Histogram: Minutes of Data per Day ({len(daily_counts)} days)")
        ax2.grid(True, alpha=0.3, axis="y")

        # Add value labels on top of bars (only if not too many bars)
        if len(daily_counts) <= 50:
            for i, v in enumerate(daily_counts.values):
                ax2.text(i, v + max(daily_counts.values) * 0.01, str(int(v)), ha="center", va="bottom", fontsize=8)

        # Set x-axis labels to show dates (every Nth date to avoid crowding)
        n_ticks = min(10, len(daily_counts))
        if len(daily_counts) > 1:
            tick_indices = np.linspace(0, len(daily_counts) - 1, n_ticks, dtype=int)
            ax2.set_xticks(tick_indices)
            ax2.set_xticklabels(
                [daily_counts.index[i].strftime("%Y-%m-%d") for i in tick_indices], rotation=45, ha="right"
            )
        else:
            ax2.set_xticks([0])
            ax2.set_xticklabels([daily_counts.index[0].strftime("%Y-%m-%d")], rotation=45, ha="right")

        plt.tight_layout()

        # Save the plot
        plot_path = os.path.abspath(plot_filename)
        plt.savefig(plot_path, dpi=150, bbox_inches="tight")
        print(f"✓ Saved price plot and histogram to: {plot_path}")
        print(f"  File location: {os.path.dirname(plot_path)}")

        plt.close()
        return True

    except Exception as e:
        print(f"Error creating plots: {str(e)}")
        import traceback

        print(traceback.format_exc())
        try:
            plt.close()
        except:
            pass
        return False


def export_data_to_csv(collected_data, symbol_root: str, csv_filename: str = "latestplot.csv"):
    """
    Export collected data to CSV with daily minute count aggregations.
    This is Step 1 - focus on data generation without plotting.
    """
    print("\n" + "=" * 80)
    print("Exporting backtest data to CSV")
    print("=" * 80)

    try:
        if collected_data is None or len(collected_data) == 0:
            print("ERROR: No data collected during backtest")
            return False, None

        # Combine all collected dataframes
        df = pd.concat(collected_data)
        # Remove duplicates in case there were any
        df = df[~df.index.duplicated(keep="last")]
        # Sort by index to ensure chronological order
        df = df.sort_index()

        print(f"Combined {len(df)} bars from collected data")
        print(f"Date range: {df.index[0]} to {df.index[-1]}")

        # Create daily minute count aggregations for histogram data
        df_copy = df.copy()
        df_copy["date"] = df_copy.index.date
        daily_counts = df_copy.groupby("date").size().reset_index()
        daily_counts.columns = ["date", "minutes_count"]

        # Save main price data to CSV
        df.to_csv(csv_filename, index=True)
        print(f"✓ Saved price data to {os.path.abspath(csv_filename)}")

        # Save daily minute counts to a separate CSV for reference
        daily_csv = csv_filename.replace(".csv", "_daily_counts.csv")
        daily_counts.to_csv(daily_csv, index=False)
        print(f"✓ Saved daily minute counts to {os.path.abspath(daily_csv)}")

        # Display sample data for verification
        print("\nSample price data (first 5 rows):")
        print(df.head())
        print("\nSample daily counts (first 5 rows):")
        print(daily_counts.head())

        print("\n✓ Data export completed successfully!")
        print(f"  Total price bars: {len(df)}")
        print(f"  Total days: {len(daily_counts)}")
        print(f"  Average minutes per day: {daily_counts['minutes_count'].mean():.1f}")

        return True, df

    except Exception as e:
        print(f"ERROR exporting data: {e}")
        import traceback

        print(traceback.format_exc())
        return False, None


if __name__ == "__main__":
    # Assemble parameters (users can edit here or pass via environment)
    params = {
        "symbol_root": "GC",  # Continuous futures root
        "timestep": "minute",
        "fixed_contracts": 1,
        "broker_min_contracts": 1,
        "atr_period": 20,
        "pt_mult": 5.0,
        "sl_mult": 2.0,
        "use_atr_profit": True,
        "use_atr_stop": True,
        "tick_size": 0.1,
        "max_time": 180,
        "topstep_flat_time_ct": "15:10",
        "topstep_buffer_minutes": 15,
        "evening_reopen_ct": "17:00",
        "use_nyse_overlay_for": {"ES", "NQ"},
        "enforce_weekend_block": True,
        "early_close_ct": None,
        "early_close_buffer_minutes": 30,
        "bias": "long_only",
    }

    if IS_BACKTESTING:
        # --------------- Backtesting path (futures require DataBento) ---------------
        trading_fee = TradingFee(flat_fee=0.50)  # Example per-contract fee
        # Benchmark to SPY for convenience; strategy itself trades futures only
        benchmark_asset = Asset("SPY", Asset.AssetType.STOCK)

        # Get backtest date range from environment if available
        backtesting_start_str = os.getenv("BACKTESTING_START")
        backtesting_end_str = os.getenv("BACKTESTING_END")

        # Parse environment variables into datetime objects
        backtesting_start = None
        backtesting_end = None
        if backtesting_start_str:
            backtesting_start = pd.to_datetime(backtesting_start_str)
        if backtesting_end_str:
            backtesting_end = pd.to_datetime(backtesting_end_str)

        results = FuturesOneSidedATRStrategy.backtest(
            datasource_class=DataBentoDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            benchmark_asset=benchmark_asset,
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            parameters=params,
            budget=100000,
            # Add the four required parameters
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
        )

        # Export data to CSV and create plots after backtest completes
        print("\nBacktest completed. Exporting data to CSV and creating plots...")
        success, df = export_data_to_csv(FuturesOneSidedATRStrategy._collected_data, params["symbol_root"])

        if success and df is not None:
            print("✓ CSV export successful! Creating plots...")

            # Create price plot and histogram
            plot_success = plot_raw_prices_and_histogram(df, params["symbol_root"], "latestplot.png")

            if plot_success:
                print("✓ Plot creation successful! Check latestplot.png")
            else:
                print("✗ Plot creation failed - check logs above")
        else:
            print("✗ CSV export failed - check logs above")

    else:
        # -------------------------- Live trading path ---------------------------
        trader = Trader()
        strategy = FuturesOneSidedATRStrategy(
            parameters=params,
        )
        trader.add_strategy(strategy)
        trader.run_all()
