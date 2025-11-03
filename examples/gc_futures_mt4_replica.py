"""
Gold (GC) Futures Trading Strategy - MT4 Replica

This strategy replicates an MT4 Expert Advisor for Gold futures trading.
It implements a 4-condition long-only entry system with multiple exit mechanisms.

Entry Logic (ALL conditions must be met):
1. Price action: Current bar low <= Previous bar close
2. RSI oversold: RSI(14) <= 30
3. SMA trend: SMA(8) declining (current < 2 bars ago)
4. Keltner cross: Close crosses below lower Keltner Channel(3, -1.5)

Exit Logic (ANY can trigger):
1. Standard SL: 3.5 × ATR below entry
2. Standard TP: 3.5 × ATR above entry
3. Time Exit: Close position after 32 bars
4. HH Exit: Profit target at highest high of last 1 bar

Features:
- Exact replication of MT4 strategy parameters
- Continuous futures (no expiration management needed)
- Prefetch optimization for faster backtesting
- LONG only (no short trades)

Requirements:
- DataBento API key (set in .env file as DATABENTO_API_KEY)
- databento Python package: pip install databento
"""

import os
from datetime import datetime

import pandas as pd

from lumibot.backtesting import DataBentoDataBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy

# =============================================================================
# DIAGNOSTICS TOGGLE
# =============================================================================
PLOT_PRICE = True  # Set to True to plot close prices after prefetch for data validation
PLOT_SAVE_PATH = "logs/gc_futures_price_data.png"  # Where to save the plot

# Live Equity Curve GUI
SHOW_LIVE_EQUITY_CURVE = True  # Set to True to show live-updating equity curve during backtest
SHOW_DRAWDOWN = True  # Set to True to show drawdown subplot on live equity curve
EQUITY_CURVE_DARK_MODE = True  # Set to True to use dark mode theme for equity curve

# =============================================================================


class GCFuturesMT4Replica(Strategy):
    """
    MT4 Expert Advisor Replica for Gold (GC) Futures

    This strategy exactly replicates an MT4 EA with a 4-condition long-only entry system.

    Entry System (LONG only, ALL conditions must be met):
    1. Current bar low <= Previous bar close (price action filter)
    2. RSI(14) <= 30 (oversold condition)
    3. SMA(8) declining: SMA[0] < SMA[2]
    4. Keltner cross: Close crosses below lower band (period=3, multiplier=-1.5)

    Exit System (ANY can trigger):
    1. Stop Loss: Entry - 3.5 × ATR(20)
    2. Take Profit: Entry + 3.5 × ATR(20)
    3. Time Exit: 32 bars since entry
    4. HH Exit: Highest high of last 1 bar (profit target)

    Contract Specifications:
    - Symbol: GC (COMEX Gold)
    - Contract Size: 100 troy ounces
    - Tick Size: $0.10/oz = $10/contract
    - Margin: ~$10,000 (simulated in backtesting)
    - Trading Hours: Nearly 24-hour (Sun-Fri)
    """

    def initialize(self):
        """Initialize the strategy"""
        # CRITICAL: Set market calendar for futures trading hours
        # Gold (GC) trades on CME: Sun 6pm - Fri 5pm ET with ~1hr daily maintenance break
        # NOT 24/7 - weekends are closed, and TopStepX may have more restrictive hours
        # TODO: Verify exact CME calendar or create custom calendar for TopStepX
        self.set_market("us_futures")  # CME futures calendar (proper for GC)
        self.log_message("✅ Market calendar set to: us_futures (CME calendar)")

        # Set the sleep time between iterations
        # NOTE: Due to a lumibot bug, integer sleeptime is multiplied by 60 (treated as minutes)
        # Use string format to specify seconds explicitly: "300S" = 300 seconds = 5 minutes
        self.sleeptime = "300S"  # 300 seconds = 5 minutes

        # Define the futures contract we want to trade
        # Using continuous Gold futures (no expiration needed)
        self.asset = Asset(symbol="GC", asset_type="cont_future")  # Continuous futures for seamless backtesting

        # Prefetch optimization for faster backtesting
        # This loads all required data upfront, reducing API calls from 2500+ to ~5
        if hasattr(self._data_source, "initialize_data_for_backtest"):
            self._data_source.initialize_data_for_backtest(
                strategy_assets=[self.asset], timestep="minute"  # MUST use minute-level data for intraday strategy
            )
            self.log_message("✅ Data prefetch complete - optimization enabled")

            # Diagnostic: Plot price data if enabled
            if PLOT_PRICE:
                self._plot_prefetched_data()

        # =================================================================
        # MT4 STRATEGY PARAMETERS - Exact replication
        # =================================================================

        # Entry indicator parameters
        self.RSI_PERIOD = 14
        self.RSI_THRESHOLD = 30  # Oversold threshold
        self.SMA_PERIOD = 8
        self.KELTNER_PERIOD = 3
        self.KELTNER_MULTIPLIER = -1.5  # Lower band (negative for lower)

        # Exit parameters
        self.ATR_PERIOD = 20
        self.SL_MULTIPLIER = 3.5  # Stop loss distance in ATR
        self.TP_MULTIPLIER = 3.5  # Take profit distance in ATR
        self.MAX_TIME_BARS = 32  # Max bars to stay in position
        self.HH_LOOKBACK = 1  # Highest high lookback period (bars)

        # Position sizing
        self.position_size = 1  # Fixed 1 contract

        # =================================================================
        # STATE TRACKING
        # =================================================================

        # Track position entry details
        self.entry_bar_datetime = None
        self.entry_price = None
        self.bars_since_entry = 0

        # Track exit levels
        self.stop_loss_price = None
        self.take_profit_price = None
        self.hh_limit_price = None

        # Previous bar values for crossover detection
        self.prev_close = None
        self.prev_keltner_lower = None

        self.log_message("=" * 60)
        self.log_message("🎯 MT4 Gold (GC) Futures Strategy - REPLICA")
        self.log_message("=" * 60)
        self.log_message(f"Trading asset: {self.asset.symbol} continuous futures")
        self.log_message("Entry System: 4-condition LONG only")
        self.log_message("  - Price action: Low <= Previous Close")
        self.log_message(f"  - RSI({self.RSI_PERIOD}) <= {self.RSI_THRESHOLD}")
        self.log_message(f"  - SMA({self.SMA_PERIOD}) declining")
        self.log_message(f"  - Keltner({self.KELTNER_PERIOD}, {self.KELTNER_MULTIPLIER}) cross")
        self.log_message("Exit System:")
        self.log_message(f"  - SL: {self.SL_MULTIPLIER} × ATR({self.ATR_PERIOD})")
        self.log_message(f"  - TP: {self.TP_MULTIPLIER} × ATR({self.ATR_PERIOD})")
        self.log_message(f"  - Time: {self.MAX_TIME_BARS} bars")
        self.log_message(f"  - HH: {self.HH_LOOKBACK} bar lookback")
        self.log_message("=" * 60)

        # Track statistics for diagnostics
        self.iteration_count = 0
        self.insufficient_data_count = 0
        self.signal_count = 0

        # SIGNAL ANALYSIS: Track condition states for every iteration
        self.signal_analysis_data = []

        # Diagnostic: Track iteration timestamps for debugging
        self.iteration_timestamps = []
        self.iteration_log_file = "logs/gc_iteration_log.txt"

        # Clear previous log and write header
        os.makedirs("logs", exist_ok=True)
        with open(self.iteration_log_file, "w") as f:
            f.write("=== GC Futures Iteration Log ===\n")
            f.write(f"Strategy start: {self.get_datetime()}\n")
            f.write(f"Market setting: {self.broker.market if hasattr(self, 'broker') else 'N/A'}\n")
            f.write(f"Data source: {self._data_source.__class__.__name__}\n")
            f.write(f"Sleeptime: {self.sleeptime} seconds\n\n")
            f.write("Iteration Log:\n")

        # For deferred plotting: collect 1-minute bars during backtest
        self._bars_dataframes = []  # List of DataFrames with 1-minute bars

    def _plot_prefetched_data(self):
        """Plot the prefetched price data for diagnostic purposes"""
        try:
            import matplotlib

            matplotlib.use("Agg")  # Non-interactive backend
            import matplotlib.pyplot as plt
        except ImportError:
            self.log_message("⚠️ matplotlib not available - cannot plot price data")
            return

        try:
            # Try multiple ways to access the prefetched data
            quote_asset = Asset("USD", "forex")
            search_key = (self.asset, quote_asset)

            df = None
            data_source = self._data_source

            # Method 1: Try pandas_data
            if hasattr(data_source, "pandas_data"):
                if search_key in data_source.pandas_data:
                    data_obj = data_source.pandas_data[search_key]
                    if hasattr(data_obj, "df"):
                        df = data_obj.df
                        self.log_message(
                            f"✅ Found data in pandas_data: {len(df) if df is not None and not df.empty else 0} bars"
                        )

            # Method 2: Try _data_store
            if (df is None or df.empty) and hasattr(data_source, "_data_store"):
                if search_key in data_source._data_store:
                    data_obj = data_source._data_store[search_key]
                    if hasattr(data_obj, "df"):
                        df = data_obj.df
                        self.log_message(
                            f"✅ Found data in _data_store: {len(df) if df is not None and not df.empty else 0} bars"
                        )

            # Method 3: Try to get data by requesting a large historical window
            if df is None or df.empty:
                self.log_message("⚠️ Data not found in prefetched storage, attempting to fetch directly...")
                try:
                    # Try to get all data for the backtest period
                    bars = self.get_historical_prices(
                        asset=self.asset,
                        length=1000000,  # Very large number to get all available data
                        timestep="minute",
                    )
                    if bars is not None and hasattr(bars, "df") and not bars.df.empty:
                        df = bars.df
                        self.log_message(f"✅ Retrieved data directly: {len(df)} bars")
                except Exception as e:
                    self.log_message(f"⚠️ Could not fetch data directly: {e}")

            # If we still don't have data, log what's available
            if df is None or df.empty:
                self.log_message("⚠️ WARNING: Could not find or access prefetched data for plotting!")
                if hasattr(data_source, "pandas_data"):
                    keys = list(data_source.pandas_data.keys()) if data_source.pandas_data else []
                    self.log_message(f"   Available keys in pandas_data: {len(keys)} keys")
                    for key in keys[:5]:  # Show first 5 keys
                        self.log_message(f"     - {key}")
                if hasattr(data_source, "_data_store"):
                    keys = list(data_source._data_store.keys()) if data_source._data_store else []
                    self.log_message(f"   Available keys in _data_store: {len(keys)} keys")
                return

            # Create the plot
            if df is not None and not df.empty:
                plt.figure(figsize=(16, 8))
                plt.plot(df.index, df["close"], linewidth=2.0, color="black")
                plt.title(
                    f"GC Futures Close Prices (1-minute bars)\n"
                    f"Date Range: {df.index[0]} to {df.index[-1]}\n"
                    f"Total Bars: {len(df)}",
                    fontsize=14,
                )
                plt.xlabel("Time", fontsize=12)
                plt.ylabel("Price ($)", fontsize=12)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()

                # Save the plot
                os.makedirs(os.path.dirname(PLOT_SAVE_PATH), exist_ok=True)
                plt.savefig(PLOT_SAVE_PATH, dpi=150, bbox_inches="tight")
                plt.close()

                self.log_message(f"📊 Price plot saved to: {PLOT_SAVE_PATH}")
                self.log_message(f"📈 Data summary: {len(df)} bars from {df.index[0]} to {df.index[-1]}")
                self.log_message(f"📈 Price range: ${df['close'].min():.2f} - ${df['close'].max():.2f}")

                # Additional diagnostic: check for gaps in data
                time_diff = df.index.to_series().diff()
                expected_diff = pd.Timedelta(minutes=1)
                gaps = time_diff[time_diff > expected_diff * 2]  # Gaps > 2 minutes
                if len(gaps) > 0:
                    self.log_message(f"⚠️ Found {len(gaps)} data gaps (intervals > 2 minutes)")
                    self.log_message(f"   Largest gap: {gaps.max()} at {gaps.idxmax()}")

        except Exception as e:
            self.log_message(f"⚠️ Error plotting price data: {e}")
            import traceback

            self.log_message(f"   Traceback: {traceback.format_exc()}")

    # =========================================================================
    # INDICATOR CALCULATION METHODS
    # =========================================================================

    def _calculate_rsi(self, prices, period=14):
        """
        Calculate RSI (Relative Strength Index)

        Args:
            prices: pandas Series of close prices
            period: RSI period (default 14)

        Returns:
            pandas Series of RSI values
        """
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _calculate_sma(self, prices, period):
        """Calculate Simple Moving Average"""
        return prices.rolling(window=period).mean()

    def _calculate_atr(self, df, period=14):
        """
        Calculate ATR (Average True Range)

        Args:
            df: pandas DataFrame with 'high', 'low', 'close' columns
            period: ATR period

        Returns:
            pandas Series of ATR values
        """
        high = df["high"]
        low = df["low"]
        close = df["close"]

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        return atr

    def _calculate_keltner_channel(self, df, period=20, multiplier=2.0):
        """
        Calculate Keltner Channel

        Args:
            df: pandas DataFrame with OHLC data
            period: EMA period for middle band
            multiplier: ATR multiplier for bands

        Returns:
            tuple: (middle_band, upper_band, lower_band)
        """
        # Middle band: EMA of close
        middle_band = df["close"].ewm(span=period, adjust=False).mean()

        # Calculate ATR for band width
        atr = self._calculate_atr(df, period)

        # Upper and lower bands
        # For MT4 replica: we only use lower band with negative multiplier
        # multiplier is -1.5, so lower_band = middle_band + (-1.5 * atr)
        upper_band = middle_band + (abs(multiplier) * atr)  # Positive for upper
        lower_band = middle_band + (multiplier * atr)  # Negative for lower

        return middle_band, upper_band, lower_band

    def _get_highest_high(self, df, lookback):
        """
        Get highest high over lookback period

        Args:
            df: pandas DataFrame with 'high' column
            lookback: number of bars to look back

        Returns:
            float: highest high value
        """
        if len(df) < lookback:
            return None
        return df["high"].iloc[-lookback:].max()

    # =========================================================================
    # MAIN TRADING LOGIC
    # =========================================================================

    def on_trading_iteration(self):
        """
        Main trading logic - MT4 Replica

        Implements:
        - 5-condition entry logic (LONG only)
        - 4 exit mechanisms (SL, TP, Time, HH)
        - Bar-by-bar state tracking
        """

        current_time = self.get_datetime()
        self.iteration_count += 1
        self.iteration_timestamps.append(current_time)

        # Log every iteration to file for detailed analysis
        with open(self.iteration_log_file, "a") as f:
            f.write(f"{self.iteration_count}: {current_time}\n")

        # Log summary every 100 iterations to console
        if self.iteration_count % 100 == 0:
            self.log_message(f"📊 Iteration {self.iteration_count} at {current_time}")

        # Get historical price data (need enough for all indicators)
        # Max period needed: max(RSI_PERIOD, ATR_PERIOD, SMA_PERIOD) + buffer
        required_bars = max(self.RSI_PERIOD, self.ATR_PERIOD, self.SMA_PERIOD) + 10
        bars = self.get_historical_prices(
            asset=self.asset,
            length=required_bars,
            timestep="minute",
        )

        if bars is None or len(bars.df) < required_bars:
            self.insufficient_data_count += 1
            data_length = len(bars.df) if bars is not None else 0
            if self.insufficient_data_count % 100 == 1 or self.insufficient_data_count <= 5:
                self.log_message(
                    f"⚠️ Insufficient data at {current_time}: " f"got {data_length} bars, need {required_bars} bars"
                )
                with open(self.iteration_log_file, "a") as f:
                    f.write(f"  └─ INSUFFICIENT DATA: got {data_length} bars, need {required_bars}\n")
            return

        df = bars.df

        # Collect data for plotting
        if PLOT_PRICE:
            if not hasattr(self, "_bars_dataframes"):
                self._bars_dataframes = []
            if bars is not None and hasattr(bars, "df") and not bars.df.empty:
                self._bars_dataframes.append(bars.df.copy())

        # =================================================================
        # CALCULATE ALL INDICATORS
        # =================================================================

        # RSI
        rsi = self._calculate_rsi(df["close"], self.RSI_PERIOD)

        # SMA
        sma = self._calculate_sma(df["close"], self.SMA_PERIOD)

        # Keltner Channel
        _, _, keltner_lower = self._calculate_keltner_channel(df, self.KELTNER_PERIOD, self.KELTNER_MULTIPLIER)

        # ATR (for exit calculations)
        atr = self._calculate_atr(df, self.ATR_PERIOD)

        # Get current bar values
        current_price = df["close"].iloc[-1]
        current_low = df["low"].iloc[-1]
        prev_close = df["close"].iloc[-2]

        current_rsi = rsi.iloc[-1]
        current_sma = sma.iloc[-1]
        sma_2bars_ago = sma.iloc[-3] if len(sma) >= 3 else None
        current_keltner_lower = keltner_lower.iloc[-1]
        prev_keltner_lower = keltner_lower.iloc[-2] if len(keltner_lower) >= 2 else None
        current_atr = atr.iloc[-1]

        # =================================================================
        # UPDATE STATE - Track bars since entry
        # =================================================================

        current_position = self.get_position(self.asset)

        if current_position and current_position.quantity > 0:
            # We have a long position - update bars since entry
            if self.entry_bar_datetime:
                time_diff = current_time - self.entry_bar_datetime
                # Each iteration is 5 minutes, so bars = minutes / 5
                self.bars_since_entry = int(time_diff.total_seconds() / 300)
        else:
            # No position - reset state
            self.bars_since_entry = 0
            self.entry_bar_datetime = None
            self.entry_price = None
            self.stop_loss_price = None
            self.take_profit_price = None
            self.hh_limit_price = None

        # =================================================================
        # EXIT LOGIC - Check all exit conditions FIRST
        # =================================================================

        if current_position and current_position.quantity > 0:
            exit_triggered = False
            exit_reason = ""

            # Exit 1: Stop Loss
            if self.stop_loss_price and current_price <= self.stop_loss_price:
                exit_triggered = True
                exit_reason = f"Stop Loss @ {self.stop_loss_price:.2f}"

            # Exit 2: Take Profit
            elif self.take_profit_price and current_price >= self.take_profit_price:
                exit_triggered = True
                exit_reason = f"Take Profit @ {self.take_profit_price:.2f}"

            # Exit 3: Time Exit (32 bars)
            elif self.bars_since_entry >= self.MAX_TIME_BARS:
                exit_triggered = True
                exit_reason = f"Time Exit ({self.bars_since_entry} bars)"

            # Exit 4: HH Exit (highest high of last N bars)
            elif self.HH_LOOKBACK > 0:
                hh = self._get_highest_high(df, self.HH_LOOKBACK)
                if hh and current_price >= hh:
                    exit_triggered = True
                    exit_reason = f"HH Exit @ {hh:.2f} (lookback={self.HH_LOOKBACK})"

            # Execute exit
            if exit_triggered:
                self.sell_all(self.asset)
                self.signal_count += 1
                self.log_message(f"🔴 EXIT: {exit_reason}")
                self.log_message(f"   Entry: {self.entry_price:.2f}, Exit: {current_price:.2f}")
                pnl = (current_price - self.entry_price) * 100  # $100 per point
                self.log_message(f"   P&L: ${pnl:.2f}")

                # Reset state
                self.entry_bar_datetime = None
                self.entry_price = None
                self.bars_since_entry = 0
                self.stop_loss_price = None
                self.take_profit_price = None
                self.hh_limit_price = None

                return  # Exit early, no new entry on same bar

        # =================================================================
        # ENTRY LOGIC - Check all 5 conditions (LONG only)
        # =================================================================

        # Only check entry if we don't have a position
        if not current_position or current_position.quantity == 0:

            entry_valid = True
            entry_reasons = []

            # Track each condition state for analysis
            cond_price_action = current_low <= prev_close
            cond_rsi = pd.notna(current_rsi) and current_rsi <= self.RSI_THRESHOLD
            cond_sma = pd.notna(current_sma) and pd.notna(sma_2bars_ago) and current_sma < sma_2bars_ago
            cond_keltner = (
                pd.notna(current_keltner_lower)
                and pd.notna(prev_keltner_lower)
                and current_price < current_keltner_lower
                and prev_close >= prev_keltner_lower
            )

            # Record analysis data every 10 iterations to avoid memory bloat
            if self.iteration_count % 10 == 0:
                self.signal_analysis_data.append(
                    {
                        "timestamp": current_time,
                        "price": current_price,
                        "rsi": current_rsi if pd.notna(current_rsi) else None,
                        "sma_current": current_sma if pd.notna(current_sma) else None,
                        "sma_2bars_ago": sma_2bars_ago if pd.notna(sma_2bars_ago) else None,
                        "keltner_lower": current_keltner_lower if pd.notna(current_keltner_lower) else None,
                        "prev_close": prev_close,
                        "current_low": current_low,
                        "cond_price_action": cond_price_action,
                        "cond_rsi": cond_rsi,
                        "cond_sma": cond_sma,
                        "cond_keltner": cond_keltner,
                        "all_conditions_met": cond_price_action and cond_rsi and cond_sma and cond_keltner,
                    }
                )

            # Condition 1: Price action - current low <= previous close
            if cond_price_action:
                entry_reasons.append(f"✓ Low ({current_low:.2f}) <= Prev Close ({prev_close:.2f})")
            else:
                entry_valid = False

            # Condition 2: RSI <= 30
            if entry_valid:
                if cond_rsi:
                    entry_reasons.append(f"✓ RSI ({current_rsi:.2f}) <= {self.RSI_THRESHOLD}")
                else:
                    entry_valid = False

            # Condition 3: SMA declining (current < 2 bars ago)
            if entry_valid:
                if cond_sma:
                    entry_reasons.append(f"✓ SMA declining ({current_sma:.2f} < {sma_2bars_ago:.2f})")
                else:
                    entry_valid = False

            # Condition 4: Keltner cross - close crosses below lower band
            if entry_valid:
                if cond_keltner:
                    entry_reasons.append(
                        f"✓ Keltner cross (Close: {current_price:.2f} < Lower: {current_keltner_lower:.2f})"
                    )
                else:
                    entry_valid = False

            # Execute entry if all conditions met
            if entry_valid:
                # Calculate exit levels
                self.entry_price = current_price
                self.entry_bar_datetime = current_time
                self.stop_loss_price = current_price - (self.SL_MULTIPLIER * current_atr)
                self.take_profit_price = current_price + (self.TP_MULTIPLIER * current_atr)
                self.bars_since_entry = 0

                # Submit buy order
                order = self.create_order(asset=self.asset, quantity=self.position_size, side="buy")
                self.submit_order(order)

                self.signal_count += 1
                self.log_message("=" * 60)
                self.log_message(f"🟢 LONG ENTRY @ {current_price:.2f}")
                for reason in entry_reasons:
                    self.log_message(f"   {reason}")
                self.log_message(f"   SL: {self.stop_loss_price:.2f} (-{self.SL_MULTIPLIER} × ATR)")
                self.log_message(f"   TP: {self.take_profit_price:.2f} (+{self.TP_MULTIPLIER} × ATR)")
                self.log_message(f"   ATR: {current_atr:.2f}")
                self.log_message("=" * 60)

        # =================================================================
        # POSITION MONITORING
        # =================================================================

        if current_position and current_position.quantity > 0:
            unrealized_pnl = (current_price - self.entry_price) * 100
            if self.iteration_count % 50 == 0:  # Log every 50 iterations
                self.log_message(
                    f"Position: {self.bars_since_entry} bars, "
                    f"Entry: {self.entry_price:.2f}, "
                    f"Current: {current_price:.2f}, "
                    f"P&L: ${unrealized_pnl:.2f}"
                )

    def on_strategy_end(self):
        """Called when backtesting ends - log diagnostic summary"""
        self.log_message("=" * 80)
        self.log_message("📊 BACKTEST DIAGNOSTICS SUMMARY")
        self.log_message("=" * 80)
        self.log_message(f"Total iterations: {self.iteration_count}")

        # Calculate iteration frequency
        if len(self.iteration_timestamps) > 1:
            time_diffs = [
                (self.iteration_timestamps[i] - self.iteration_timestamps[i - 1]).total_seconds() / 60.0
                for i in range(1, min(len(self.iteration_timestamps), 100))
            ]  # Sample first 100
            avg_minutes_between = sum(time_diffs) / len(time_diffs) if time_diffs else 0

            # Calculate expected iterations
            total_days = 304  # Jan 1 - Oct 31 = ~304 days
            expected_24_7 = (total_days * 24 * 60) / 5  # 5-minute intervals, 24/7
            expected_nyse = total_days * 78  # 6.5 hours * 12 intervals per hour
            iterations_per_day = self.iteration_count / total_days

            self.log_message(f"Average time between iterations: {avg_minutes_between:.1f} minutes")
            self.log_message(f"Iterations per day: {iterations_per_day:.1f}")
            self.log_message(
                f"Expected if 24/7 market (5min intervals): {expected_24_7:.0f} iterations "
                f"({expected_24_7/total_days:.1f}/day)"
            )
            self.log_message(
                f"Expected if NYSE hours (5min intervals): {expected_nyse:.0f} iterations "
                f"({expected_nyse/total_days:.1f}/day)"
            )

            if iterations_per_day < 20:
                self.log_message("⚠️ WARNING: Very few iterations per day - likely using stock market hours!")
            elif iterations_per_day > 250:
                self.log_message("✅ High iteration count - likely using 24/7 or futures market hours")

        if self.iteration_count > 0:
            insufficient_pct = 100 * self.insufficient_data_count / self.iteration_count
            self.log_message(
                f"Iterations with insufficient data: {self.insufficient_data_count} " f"({insufficient_pct:.1f}%)"
            )
        else:
            self.log_message(f"Iterations with insufficient data: {self.insufficient_data_count}")

        self.log_message(f"Total entry signals generated: {self.signal_count}")

        # =================================================================
        # SIGNAL ANALYSIS - Create DataFrame and Analyze Conditions
        # =================================================================

        if len(self.signal_analysis_data) > 0:
            self.log_message("")
            self.log_message("=" * 80)
            self.log_message("📊 SIGNAL CONDITION ANALYSIS")
            self.log_message("=" * 80)

            # Create DataFrame
            df_signals = pd.DataFrame(self.signal_analysis_data)

            # Save to CSV for detailed inspection
            csv_path = "logs/signal_analysis.csv"
            df_signals.to_csv(csv_path, index=False)
            self.log_message(f"📁 Signal analysis saved to: {csv_path}")
            self.log_message("")

            # Calculate success rates for each condition
            total_samples = len(df_signals)
            self.log_message(f"Total samples analyzed: {total_samples}")
            self.log_message("")
            self.log_message("Condition Pass Rates:")
            self.log_message("-" * 60)

            cond_cols = ["cond_price_action", "cond_rsi", "cond_sma", "cond_keltner"]
            cond_names = ["Price Action", "RSI <= 30", "SMA Declining", "Keltner Cross"]

            for col, name in zip(cond_cols, cond_names):
                pass_count = df_signals[col].sum()
                pass_rate = 100 * pass_count / total_samples
                self.log_message(f"  {name:20s}: {pass_count:5d} / {total_samples:5d} ({pass_rate:5.1f}%)")

            self.log_message("")
            all_met = df_signals["all_conditions_met"].sum()
            all_met_rate = 100 * all_met / total_samples
            self.log_message(f"  {'ALL CONDITIONS MET':20s}: {all_met:5d} / {total_samples:5d} ({all_met_rate:5.1f}%)")

            # Show sample data where conditions were closest to being met
            self.log_message("")
            self.log_message("=" * 80)
            self.log_message("🔍 CLOSEST TO ENTRY (samples with most conditions met)")
            self.log_message("=" * 80)

            # Count how many conditions were met for each row
            df_signals["num_conditions_met"] = (
                df_signals["cond_price_action"].astype(int)
                + df_signals["cond_rsi"].astype(int)
                + df_signals["cond_sma"].astype(int)
                + df_signals["cond_keltner"].astype(int)
            )

            # Get top 10 closest to entry
            top_closest = df_signals.nlargest(10, "num_conditions_met")

            for _, row in top_closest.iterrows():
                self.log_message(f"\n{row['timestamp']} - {row['num_conditions_met']}/4 conditions met:")
                self.log_message(f"  Price: {row['price']:.2f}, RSI: {row['rsi']:.1f}")
                check = "✓" if row["cond_price_action"] else "✗"
                self.log_message(
                    f"  {check} Price Action " f"(Low: {row['current_low']:.2f} <= Prev Close: {row['prev_close']:.2f})"
                )
                self.log_message(f"  {'✓' if row['cond_rsi'] else '✗'} RSI ({row['rsi']:.1f} <= 30)")
                check = "✓" if row["cond_sma"] else "✗"
                self.log_message(
                    f"  {check} SMA Declining "
                    f"(Current: {row['sma_current']:.2f} < 2-bars-ago: {row['sma_2bars_ago']:.2f})"
                )
                check = "✓" if row["cond_keltner"] else "✗"
                self.log_message(
                    f"  {check} Keltner Cross " f"(Price: {row['price']:.2f} < Lower: {row['keltner_lower']:.2f})"
                )

            self.log_message("")
            self.log_message("=" * 80)
        else:
            self.log_message("")
            self.log_message("⚠️ No signal analysis data collected")

        # Write summary to iteration log
        with open(self.iteration_log_file, "a") as f:
            f.write("\n=== SUMMARY ===\n")
            f.write(f"Total iterations: {self.iteration_count}\n")
            f.write(f"Insufficient data: {self.insufficient_data_count}\n")
            f.write(f"Signals generated: {self.signal_count}\n")
            if len(self.iteration_timestamps) > 1:
                f.write(f"Avg minutes between iterations: {avg_minutes_between:.1f}\n")
                f.write(f"Iterations per day: {iterations_per_day:.1f}\n")

        self.log_message(f"Iteration log saved to: {self.iteration_log_file}")

        # Try to plot from collected bars if prefetch plot didn't work
        if PLOT_PRICE and len(self._bars_dataframes) > 0:
            self._plot_from_samples()

        if self.iteration_count > 0 and self.insufficient_data_count > self.iteration_count * 0.5:
            self.log_message("⚠️ WARNING: More than 50% of iterations had insufficient data!")
            self.log_message("   This suggests potential data quality or availability issues.")
            self.log_message("   Check the price plot if PLOT_PRICE=True to visualize data gaps.")

        if self.signal_count < 10:
            self.log_message("⚠️ WARNING: Very few signals generated for the backtest period!")
            self.log_message("   This could indicate:")
            self.log_message("   1. Data gaps or missing bars")
            self.log_message("   2. Moving averages not crossing frequently enough")
            self.log_message("   3. Insufficient data causing most iterations to be skipped")

        self.log_message("=" * 80)

    def _plot_from_samples(self):
        """Plot price data from collected 1-minute bars during backtest"""
        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            if len(self._bars_dataframes) == 0:
                return

            # Combine all collected dataframes and remove duplicates (keep latest)
            # This ensures we get all 1-minute bars at full resolution
            df_combined = pd.concat(self._bars_dataframes, ignore_index=False)
            # Remove duplicates by index (datetime), keeping the last occurrence
            df_combined = df_combined[~df_combined.index.duplicated(keep="last")]
            df_combined = df_combined.sort_index()

            if len(df_combined) < 2:
                self.log_message("⚠️ Not enough data collected for plotting")
                return

            # Create the plot with 1-minute granularity
            plt.figure(figsize=(16, 8))
            plt.plot(df_combined.index, df_combined["close"], linewidth=2.0, color="black")
            plt.title(
                f"GC Futures Close Prices (1-minute bars from backtest)\n"
                f"Date Range: {df_combined.index[0]} to {df_combined.index[-1]}\n"
                f"Total Bars: {len(df_combined)}",
                fontsize=14,
            )
            plt.xlabel("Time", fontsize=12)
            plt.ylabel("Price ($)", fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            # Save with different filename
            sample_plot_path = PLOT_SAVE_PATH.replace(".png", "_samples.png")
            os.makedirs(os.path.dirname(sample_plot_path), exist_ok=True)
            plt.savefig(sample_plot_path, dpi=150, bbox_inches="tight")
            plt.close()

            self.log_message(f"📊 Price plot from collected bars saved to: {sample_plot_path}")
            self.log_message(f"📈 Total 1-minute bars plotted: {len(df_combined)}")
            self.log_message(f"📈 Price range: ${df_combined['close'].min():.2f} - ${df_combined['close'].max():.2f}")

        except Exception as e:
            self.log_message(f"⚠️ Error plotting from collected bars: {e}")
            import traceback

            self.log_message(f"   Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    """
    Backtest the Gold futures strategy using DataBento data

    Before running this, make sure to:
    1. Install databento: pip install databento
    2. Set your DataBento API key in .env file:
       DATABENTO_API_KEY=your_api_key_here
    3. Run from project root: python examples/gc_futures_optimized.py

    Expected Performance (with prefetch optimization):
    - Backtest time: ~8 minutes (vs 45 min without optimization)
    - API calls: ~5 (vs 2,500+ without optimization)
    - Log lines: ~50 (vs 15,000+ without optimization)
    """

    # Define backtest parameters - January 2025 only for testing
    backtest_start = datetime(2025, 1, 1)
    backtest_end = datetime(2025, 1, 31)

    # Full backtest period (uncomment when ready):
    # backtest_start = datetime(2024, 1, 1)
    # backtest_end = datetime(2025, 10, 31)

    # Load DataBento API key from environment (.env file)
    api_key = os.getenv("DATABENTO_API_KEY")

    if not api_key:
        print("❌ ERROR: DATABENTO_API_KEY not found in environment")
        print("Please set it in your .env file or export it:")
        print("  export DATABENTO_API_KEY='your_api_key_here'")
        exit(1)

    print("=" * 80)
    print("🥇 Gold (GC) Futures - MT4 Strategy Replica")
    print("=" * 80)
    print(f"Period: {backtest_start.date()} to {backtest_end.date()}")
    print("Contract: GC continuous futures (COMEX Gold)")
    print("Strategy: MT4 EA Replica - 4-Condition LONG Only")
    print("")
    print("Entry Conditions (ALL must be met):")
    print("  1. Current low <= Previous close")
    print("  2. RSI(14) <= 30")
    print("  3. SMA(8) declining")
    print("  4. Keltner(3, -1.5) cross below")
    print("")
    print("Exit Conditions (ANY can trigger):")
    print("  1. Stop Loss: Entry - 3.5 × ATR(20)")
    print("  2. Take Profit: Entry + 3.5 × ATR(20)")
    print("  3. Time Exit: 32 bars")
    print("  4. HH Exit: Highest high of last 1 bar")
    print("")
    print("Data Source: DataBento (optimized with prefetch)")
    print(f"API Key: {'✅ Loaded from .env' if api_key else '❌ Not found'}")
    print("=" * 80)
    print()

    # Set up backtesting with DataBento data source
    print("📊 Starting backtest...")
    print("⏱️  With prefetch optimization, this should take ~8 minutes")
    print()

    # Launch live equity curve GUI if enabled
    gui_process = None
    if SHOW_LIVE_EQUITY_CURVE:
        import subprocess
        import sys
        from pathlib import Path

        equity_file = Path("logs/backtest_live_equity.jsonl")

        # Clear existing equity file
        if equity_file.exists():
            equity_file.unlink()
            print("✅ Cleared existing equity curve data")

        # Launch GUI in separate process
        try:
            # Build command with optional flags
            gui_cmd = [sys.executable, "show_live_backtest_progress.py"]
            if SHOW_DRAWDOWN:
                gui_cmd.append("--show-drawdown")
            if EQUITY_CURVE_DARK_MODE:
                gui_cmd.append("--dark-mode")

            gui_process = subprocess.Popen(
                gui_cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            print("📈 Launched live equity curve GUI")
            if SHOW_DRAWDOWN:
                print("   Drawdown display: ENABLED")
            if EQUITY_CURVE_DARK_MODE:
                print("   Dark mode: ENABLED")
            print(f"   GUI PID: {gui_process.pid}")
            print()
        except Exception as e:
            print(f"⚠️  Failed to launch GUI: {e}")
            print()

    # Run backtest using run_backtest method (matches lumibot pattern)
    results, strategy = GCFuturesMT4Replica.run_backtest(
        datasource_class=DataBentoDataBacktesting,
        backtesting_start=backtest_start,
        backtesting_end=backtest_end,
        api_key=api_key,
        write_live_equity_file=SHOW_LIVE_EQUITY_CURVE,  # Enable equity file writing
        show_plot=True,
        show_tearsheet=True,
        save_tearsheet=True,
        show_progress_bar=True,
    )

    print()
    print("=" * 80)
    print("✅ Backtest Complete!")
    print("=" * 80)
    print("Check the output for:")
    print("  - Performance tearsheet (PDF)")
    print("  - Equity curve plot (PNG)")
    print("  - Trade statistics")
    if SHOW_LIVE_EQUITY_CURVE:
        print()
        print("📈 Live Equity Curve GUI:")
        print("  - GUI is still running in separate window")
        print("  - Close the GUI window when you're done reviewing")
        print("  - Or re-launch anytime: python3 show_live_backtest_progress.py")
    print("=" * 80)
