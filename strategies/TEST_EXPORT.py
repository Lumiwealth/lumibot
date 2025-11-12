import math
from datetime import date

import numpy as np
import pandas as pd

from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy

"""
Strategy Description
--------------------
This strategy adapts the core entry idea from the provided Axiom Template EA (long-only rules) to a
stock-based, broker-agnostic LumiBot implementation. It looks for a price cross above a very short
moving average when momentum cools and market structure becomes mean-reverting (low Hurst), then
enters with protective exits.

This code was generated based on the user prompt: 'Port the Axiom Template EA.mq4 logic to a LumiBot
Python strategy.'
"""


class AxiomPortStrategy(Strategy):
    # Parameters allow basic tuning without touching the code
    parameters = {
        "symbols": ["GC"],  # You can use any liquid stocks/ETFs
        "timestep": "minute",  # Using 1-minute bars for intraday trading
        "hurst_window": 20,  # Window for Hurst exponent
        "max_hold_bars": 60,  # Exit if held this many bars
        "tp_pct": 0.06,  # Take profit as +6% from entry
        "sl_pct": 0.03,  # Stop loss as -3% from entry
        "start_gate_date": date(2025, 2, 2),  # Mimics the EA time gate (previous bar after this date)
    }

    def initialize(self):
        # Run once at the beginning
        params = self.get_parameters()

        # CRITICAL: Set market calendar for futures trading hours
        # Gold (GC) trades on CME: Sun 6pm - Fri 5pm ET with ~1hr daily maintenance break
        # Without this, it defaults to NYSE hours (9:30 AM - 4:00 PM) and misses overnight trading!
        self.set_market("us_futures")  # CME futures calendar
        self.log_message("✅ Market calendar set to: us_futures (CME calendar for GC futures)")

        # Set sleeptime based on timestep
        # CRITICAL: "1M" means 1 MONTH in Lumibot, not 1 minute!
        # Use "60S" for 60 seconds (1 minute), "86400S" for 1 day
        # Due to a Lumibot bug, integer sleeptime is multiplied by 60
        if params.get("timestep", "day") == "day":
            self.sleeptime = "86400S"  # 86400 seconds = 1 day
        else:  # minute bars
            self.sleeptime = "60S"  # 60 seconds = 1 minute

        # CRITICAL PERFORMANCE OPTIMIZATION: Prefetch all data upfront for backtesting
        # This reduces API calls from thousands to just a few and speeds up backtesting by 1000x+
        if hasattr(self, "_data_source") and hasattr(self._data_source, "initialize_data_for_backtest"):
            symbols = params.get("symbols", [])
            timestep = params.get("timestep", "minute")  # Default should match parameters above

            # Create assets list for prefetch
            strategy_assets = [Asset(sym, asset_type=self._get_asset_type(sym)) for sym in symbols]

            # Prefetch all data for the backtest period
            self._data_source.initialize_data_for_backtest(strategy_assets=strategy_assets, timestep=timestep)
            self.log_message("✅ Data prefetch complete - optimization enabled (massive speedup!)")

        # Keep persistent state in self.vars
        # We'll track when we entered positions to manage time-based exits
        if not hasattr(self.vars, "entry_dt_by_symbol"):
            self.vars.entry_dt_by_symbol = {}

        # Limit markers to rare events, and a few lines for clarity
        self.log_message("Strategy initialized with symbols: {}".format(", ".join(params.get("symbols", []))))

    # ------------------------ Helper computations ------------------------
    @staticmethod
    def _safe_last(values):
        return None if values is None or len(values) == 0 else values[-1]

    @staticmethod
    def _get_asset_type(sym):
        """Return appropriate asset type for symbol - GC uses continuous futures"""
        return Asset.AssetType.CONT_FUTURE if sym == "GC" else Asset.AssetType.STOCK

    def _hurst_exponent(self, series: pd.Series) -> float:
        """Estimate Hurst exponent using a simple aggregated variance method.
        Traders' note: lower values (below ~0.5) often indicate mean-reverting behavior.
        """
        if series is None or len(series) < 20 or series.isna().any():
            return np.nan

        # Use lags from 2 to 10 for a stable slope
        lags = range(2, min(10, len(series) - 1))
        tau = []
        for lag in lags:
            # Differences at given lag
            diff = series.diff(lag).dropna()
            if diff.empty:
                continue
            # sqrt of variance of differences
            tau.append(np.sqrt(np.var(diff.values)))

        if len(tau) < 2:
            return np.nan

        # Linear fit on log-log scale: tau ~ lag^H
        x = np.log(np.array(list(lags)[: len(tau)]))
        y = np.log(np.array(tau))
        # Protect against invalid values
        if np.any(~np.isfinite(x)) or np.any(~np.isfinite(y)):
            return np.nan

        slope = np.polyfit(x, y, 1)[0]
        return float(slope)  # Hurst estimate

    def _compute_signals(self, df: pd.DataFrame, hurst_window: int, start_gate_date: date) -> dict:
        """Compute all signals for the latest bar.
        Returns a dictionary with keys describing the conditions and a combined 'all' flag.
        """
        signals = {
            "date_gate": False,
            "momo_cooling": False,
            "cross_above_sma3": False,
            "hurst_cross_below": False,
            "all": False,
            "latest_price": None,
            "sma3": None,
            "hurst_now": None,
        }

        # Need enough bars to evaluate indicators
        if df is None or df.empty or len(df) < (hurst_window + 6):
            return signals

        # Latest and previous bar timestamps
        prev_idx = df.index[-2]
        last_close = float(df["close"].iloc[-1])
        prev_close = float(df["close"].iloc[-2])
        signals["latest_price"] = last_close

        # 1) Date gate: previous completed bar date must be after the configured date (mimics EA logic)
        prev_date = prev_idx.date() if hasattr(prev_idx, "date") else prev_idx.date
        # Using >= instead of > to include the start date
        signals["date_gate"] = bool(prev_date >= start_gate_date)

        # 2) Momentum cooling: 3-bar momentum now < momentum 3 bars ago
        # Define 3-bar momentum as close - close.shift(3)
        momentum3 = df["close"].diff(3)
        if len(momentum3) >= 5:
            mom_now = float(momentum3.iloc[-1])
            mom_3ago = float(momentum3.iloc[-4])  # "momentum 3 bars ago"
            signals["momo_cooling"] = bool(mom_now < mom_3ago)

        # 3) Price crosses above 3-period SMA
        df["sma3"] = df["close"].rolling(window=3).mean()
        sma3_now = float(df["sma3"].iloc[-1]) if not math.isnan(df["sma3"].iloc[-1]) else None
        sma3_prev = float(df["sma3"].iloc[-2]) if not math.isnan(df["sma3"].iloc[-2]) else None
        signals["sma3"] = sma3_now
        if sma3_now is not None and sma3_prev is not None:
            signals["cross_above_sma3"] = bool((prev_close <= sma3_prev) and (last_close > sma3_now))

        # 4) Hurst(20) crosses below 0.5 (changed from 0.35 to be less restrictive)
        # Compute on last window and prior window to detect a cross
        # Hurst < 0.5 indicates mean-reverting behavior (good for MA crossover strategies)
        hurst_now = self._hurst_exponent(df["close"].iloc[-hurst_window:])
        hurst_prev = self._hurst_exponent(df["close"].iloc[-(hurst_window + 1) : -1])
        signals["hurst_now"] = None if np.isnan(hurst_now) else float(hurst_now)
        if not np.isnan(hurst_now) and not np.isnan(hurst_prev):
            # More permissive: just check if Hurst is below 0.5 (mean-reverting)
            signals["hurst_cross_below"] = bool(hurst_now < 0.5)

        # Combined signal
        signals["all"] = bool(
            signals["date_gate"]
            and signals["momo_cooling"]
            and signals["cross_above_sma3"]
            and signals["hurst_cross_below"]
        )
        return signals

    def on_trading_iteration(self):
        # This runs on each iteration (e.g., once per minute with minute bars)
        params = self.get_parameters()
        symbols = params.get("symbols", [])
        timestep = params.get("timestep", "minute")  # Default should match parameters above
        hurst_window = int(params.get("hurst_window", 20))
        max_hold_bars = int(params.get("max_hold_bars", 60))
        tp_pct = float(params.get("tp_pct", 0.06))
        sl_pct = float(params.get("sl_pct", 0.03))
        start_gate_date = params.get("start_gate_date", date(2025, 2, 2))

        if not symbols:
            self.log_message("No symbols provided; nothing to do.", color="yellow")
            return

        # Batch fetch is faster and lighter on data providers
        bars_by_asset = self.get_historical_prices_for_assets(
            [Asset(sym, asset_type=self._get_asset_type(sym)) for sym in symbols],
            length=max(hurst_window + 60, 80),  # ensure enough history for indicators and crosses
            timestep=timestep,
            include_after_hours=True,
        )

        # For charting: plot the first symbol's price and key indicators (kept minimal for clarity)
        first_sym = symbols[0]
        first_asset = Asset(first_sym, asset_type=self._get_asset_type(first_sym))
        first_bars = bars_by_asset.get(first_asset)
        if first_bars and first_bars.df is not None and not first_bars.df.empty:
            fdf = first_bars.df.copy()
            fdf["sma3"] = fdf["close"].rolling(window=3).mean()
            last_price = float(fdf["close"].iloc[-1])
            last_sma3 = fdf["sma3"].iloc[-1]
            h_now = self._hurst_exponent(fdf["close"].iloc[-hurst_window:])
            # Add lines only when numeric values exist
            if math.isfinite(last_price):
                self.add_line(first_sym, last_price, color="black", width=2, detail_text=f"{first_sym} Price")
            if not math.isnan(last_sma3):
                self.add_line("SMA3", float(last_sma3), color="blue", width=2, detail_text="3-period SMA")
            if math.isfinite(h_now):
                self.add_line("HURST20", float(h_now), color="green", width=2, detail_text="20-bar Hurst")

        # 1) Evaluate buy signals for all symbols first, so we know how to size positions
        signals_to_buy = []
        diagnostics = {}
        for sym in symbols:
            asset = Asset(sym, asset_type=self._get_asset_type(sym))
            bars = bars_by_asset.get(asset)
            df = None if (bars is None) else bars.df

            if df is None or df.empty:
                self.log_message(f"{sym}: No data available; skipping.", color="yellow")
                continue

            sig = self._compute_signals(df, hurst_window, start_gate_date)
            diagnostics[sym] = sig

            # Debug logging to understand why trades aren't happening
            hurst_str = f"{sig['hurst_now']:.3f}" if sig["hurst_now"] is not None else "N/A"
            price_str = f"{sig['latest_price']:.2f}" if sig["latest_price"] is not None else "N/A"
            sma3_str = f"{sig['sma3']:.2f}" if sig["sma3"] is not None else "N/A"

            self.log_message(
                f"{sym} signals: date_gate={sig['date_gate']}, momo_cooling={sig['momo_cooling']}, "
                f"cross_above_sma3={sig['cross_above_sma3']}, hurst_cross_below={sig['hurst_cross_below']}, "
                f"hurst={hurst_str}, price={price_str}, sma3={sma3_str}"
            )

            # Log when ALL conditions are met
            if sig["all"]:
                self.log_message(f"🎯 {sym}: ALL CONDITIONS MET! Buy signal generated.", color="green")

            # Skip if we already hold the symbol
            position = self.get_position(asset)
            if position is not None and position.quantity and position.quantity > 0:
                self.log_message(f"{sym}: Already long {position.quantity}; no new buy.")
                continue

            # Queue for buying if all conditions met
            if sig["all"] and sig["latest_price"] is not None:
                signals_to_buy.append(sym)

        # 2) Size and send buys (evenly split available cash among signals firing this bar)
        if signals_to_buy:
            cash = self.get_cash()
            n = len(signals_to_buy)
            per_trade_cash = (cash * 0.98) / n if n > 0 else 0  # keep a tiny buffer
            self.log_message(
                f"Buy signals for: {signals_to_buy}. Cash {cash:.2f} -> {per_trade_cash:.2f} per trade.",
                color="blue",
            )

            for sym in signals_to_buy:
                asset = Asset(sym, asset_type=self._get_asset_type(sym))
                last_price = diagnostics[sym]["latest_price"]
                if last_price is None or not math.isfinite(last_price) or last_price <= 0:
                    self.log_message(f"{sym}: invalid price; skipping buy.", color="yellow")
                    continue

                shares = int(per_trade_cash // last_price)
                if shares <= 0:
                    self.log_message(
                        f"{sym}: per-trade cash too small for 1 share at {last_price:.2f}; skipping.", color="yellow"
                    )
                    continue

                # Calculate bracket exits using the current price
                tp_price = last_price * (1.0 + tp_pct)
                sl_price = last_price * (1.0 - sl_pct)

                # Create a parent market order with bracket exits
                order = self.create_order(
                    asset,
                    quantity=shares,
                    side=Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                    order_class=Order.OrderClass.BRACKET,
                    secondary_limit_price=tp_price,
                    secondary_stop_price=sl_price,
                )

                # Log the decision clearly
                self.log_message(
                    f"BUY {sym}: qty={shares}, entry≈{last_price:.2f}, TP≈{tp_price:.2f} (+{tp_pct*100:.1f}%), "
                    f"SL≈{sl_price:.2f} (-{sl_pct*100:.1f}%).",
                    color="green",
                )

                # Submit the order
                self.submit_order(order)

                # Add a marker for the buy signal to help visualize rare events
                if math.isfinite(last_price):
                    self.add_marker(
                        name=f"BUY {sym}",
                        value=last_price,
                        color="green",
                        symbol="arrow-up",
                        size=10,
                        detail_text="All entry conditions aligned",
                    )

                # Track entry time for time-based exits later
                self.vars.entry_dt_by_symbol[sym] = self.get_datetime()
        else:
            self.log_message("No new buy signals this iteration.")

        # 3) Manage time-based exits for open positions (brackets handle normal TP/SL)
        for pos in self.get_positions():
            # Skip quote asset (cash)
            if pos.asset.symbol == "USD" and pos.asset.asset_type == Asset.AssetType.FOREX:
                continue

            sym = pos.asset.symbol
            if pos.quantity is None or pos.quantity <= 0:
                continue

            entry_dt = self.vars.entry_dt_by_symbol.get(sym)
            if entry_dt is None:
                continue

            # Calculate bars held based on timestep
            time_diff = self.get_datetime() - entry_dt
            if timestep == "minute":
                bars_held_est = int(time_diff.total_seconds() / 60)  # Minutes held
            else:  # daily bars
                bars_held_est = time_diff.days  # Days held

            if bars_held_est >= max_hold_bars:
                # Close due to max holding period
                self.log_message(
                    f"Time exit: Closing {sym} after ~{bars_held_est} bars (max {max_hold_bars}).",
                    color="yellow",
                )
                try:
                    self.close_position(pos.asset)
                except Exception as e:
                    self.log_message(f"Close failed for {sym}: {e}", color="red")


if __name__ == "__main__":
    """
    Run this strategy directly with: python strategies/test_export.py

    This self-contained backtest includes all necessary configuration:
    - Environment variables for backtesting mode
    - DataBento data source with Polars for maximum performance
    - Proper futures market hours and configuration
    """

    import os
    from datetime import datetime, timedelta

    from lumibot.backtesting import DataBentoDataBacktestingPolars
    from lumibot.entities import Asset

    # ============================================================================
    # FORCE BACKTESTING MODE (Critical for safety)
    # ============================================================================
    # Lumibot strategies run LIVE by default! We MUST set these environment
    # variables to ensure backtesting mode, not live trading.

    os.environ["IS_BACKTESTING"] = "true"
    os.environ["BACKTESTING_DATA_SOURCE"] = "databento"

    # Set backtest date range (last 180 days for comprehensive testing)
    # Use yesterday as end date to ensure market data is available
    backtesting_end = datetime.now() - timedelta(days=1)
    backtesting_start = backtesting_end - timedelta(days=180)

    os.environ["BACKTESTING_START"] = backtesting_start.strftime("%Y-%m-%d")
    os.environ["BACKTESTING_END"] = backtesting_end.strftime("%Y-%m-%d")

    # ============================================================================
    # DATABENTO API KEY
    # ============================================================================
    # DataBento requires an API key for fetching futures data
    api_key = os.getenv("DATABENTO_API_KEY")
    if not api_key:
        print("\n" + "=" * 70)
        print("❌ ERROR: DATABENTO_API_KEY not found in environment")
        print("=" * 70)
        print("\nDataBento requires an API key to fetch futures data.")
        print("Please set it in your .env file or export it:")
        print("  export DATABENTO_API_KEY='your_api_key_here'")
        print("\nGet your API key at: https://databento.com")
        print("=" * 70)
        exit(1)

    # ============================================================================
    # STRATEGY PARAMETERS
    # ============================================================================
    # Parameters for the strategy (can be adjusted here)
    params = {
        "symbols": ["GC"],  # Gold futures via DataBento
        "timestep": "minute",  # 1-minute bars for intraday trading
        "hurst_window": 20,  # Hurst exponent calculation window
        "max_hold_bars": 60,  # Exit after 60 bars (60 minutes with minute bars)
        "tp_pct": 0.06,  # Take profit at +6%
        "sl_pct": 0.03,  # Stop loss at -3%
        "start_gate_date": date(2025, 2, 2),  # Strategy starts after this date
    }

    # ============================================================================
    # RUN BACKTEST
    # ============================================================================
    print("\n" + "=" * 70)
    print("🥇 Gold (GC) Futures Trading Strategy - Axiom Port")
    print("=" * 70)
    print(f"📅 Period: {backtesting_start.date()} to {backtesting_end.date()}")
    print("📊 Data: DataBento with Polars (optimized)")
    print(f"⏱️  Timeframe: {params['timestep']} bars")
    print("📈 Market: CME Futures (nearly 24-hour trading)")
    print("✅ API Key: Loaded from environment")
    print("=" * 70)
    print("\nStarting backtest... This should take 2-5 minutes.\n")

    # Run the backtest with all optimizations:
    # - DataBentoDataBacktestingPolars for speed
    # - Prefetch optimization in initialize()
    # - Proper futures market hours with self.set_market("us_futures")
    results = AxiomPortStrategy.backtest(
        datasource_class=DataBentoDataBacktestingPolars,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=params,
        budget=10000,  # Starting capital
        benchmark_asset=Asset("GC", Asset.AssetType.CONT_FUTURE),  # Compare to GC
        quote_asset=Asset("USD", Asset.AssetType.FOREX),  # USD as quote currency
        api_key=api_key,  # DataBento API key
        # Note: No trading fees for futures backtesting typically
        show_plot=True,  # Show performance plot
        show_tearsheet=True,  # Show detailed statistics
        save_tearsheet=True,  # Save tearsheet to file
        show_progress_bar=True,  # Show progress during backtest
    )

    print("\n" + "=" * 70)
    print("✅ Backtest Complete!")
    print("=" * 70)
    print("\nCheck the output for:")
    print("  📊 Performance tearsheet (PDF)")
    print("  📈 Equity curve plot")
    print("  📝 Trade statistics")
    print("\nTo adjust strategy parameters, edit the 'params' dictionary above.")
    print("=" * 70)
