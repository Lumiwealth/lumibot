import math
import os
from datetime import date

import numpy as np
import pandas as pd

from lumibot.backtesting import DataBentoDataBacktestingPolars
from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

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
        "timestep": "minute",  # Use daily bars (simple, robust)
        "hurst_window": 20,  # Window for Hurst exponent
        "max_hold_bars": 60,  # Exit if held this many bars
        "tp_pct": 0.06,  # Take profit as +6% from entry
        "sl_pct": 0.03,  # Stop loss as -3% from entry
        "start_gate_date": date(2025, 2, 2),  # Mimics the EA time gate (previous bar after this date)
    }

    def initialize(self):
        # Run once at the beginning
        params = self.get_parameters()

        # Set sleeptime based on timestep: "1D" for daily bars, "1M" for minute bars (matches example)
        self.sleeptime = "1D" if params.get("timestep", "day") == "day" else "1M"

        # CRITICAL PERFORMANCE OPTIMIZATION: Prefetch all data upfront for backtesting
        # This reduces API calls from thousands to just a few and speeds up backtesting by 1000x+
        if hasattr(self, "_data_source") and hasattr(self._data_source, "initialize_data_for_backtest"):
            symbols = params.get("symbols", [])
            timestep = params.get("timestep", "day")

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
        prev_date = prev_idx.date()
        signals["date_gate"] = bool(prev_date > start_gate_date)

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

        # 4) Hurst(20) crosses below 0.35
        # Compute on last window and prior window to detect a cross
        hurst_now = self._hurst_exponent(df["close"].iloc[-hurst_window:])
        hurst_prev = self._hurst_exponent(df["close"].iloc[-(hurst_window + 1) : -1])
        signals["hurst_now"] = None if np.isnan(hurst_now) else float(hurst_now)
        if not np.isnan(hurst_now) and not np.isnan(hurst_prev):
            signals["hurst_cross_below"] = bool((hurst_prev >= 0.35) and (hurst_now < 0.35))

        # Combined signal
        signals["all"] = bool(
            signals["date_gate"]
            and signals["momo_cooling"]
            and signals["cross_above_sma3"]
            and signals["hurst_cross_below"]
        )
        return signals

    def on_trading_iteration(self):
        # This runs on each iteration (e.g., once per day with daily bars)
        params = self.get_parameters()
        symbols = params.get("symbols", [])
        timestep = params.get("timestep", "day")
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

            self.log_message(
                f"{sym} checks -> date_gate={sig['date_gate']}, momo_cooling={sig['momo_cooling']}, "
                f"cross_above_sma3={sig['cross_above_sma3']}, hurst_cross_below={sig['hurst_cross_below']}"
            )

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

            bars_held_est = (self.get_datetime().date() - entry_dt.date()).days
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
    # Configure fees (example: 0.10% each side)
    trading_fee = TradingFee(percent_fee=0.001)

    # Default parameters for both backtesting and live; adjust via environment or config as needed
    params = {
        "symbols": ["GC"],  # GC futures via DataBento
        "timestep": "minute",  # Minute bars like the example
        "hurst_window": 20,
        "max_hold_bars": 60,
        "tp_pct": 0.06,
        "sl_pct": 0.03,
        "start_gate_date": date(2025, 2, 2),
    }

    if IS_BACKTESTING:
        # Check if BACKTESTING_DATA_SOURCE environment variable might override datasource
        env_data_source = os.getenv("BACKTESTING_DATA_SOURCE", "").lower()
        if env_data_source and env_data_source not in ("", "none", "databento"):
            print(f"⚠️  WARNING: BACKTESTING_DATA_SOURCE is set to '{env_data_source}', which may override DataBento.")
            print("   To use DataBento, either unset BACKTESTING_DATA_SOURCE or set it to 'databento'")
            print("   Example: export BACKTESTING_DATA_SOURCE=databento")
            print()

        # Load DataBento API key from environment (same as example)
        api_key = os.getenv("DATABENTO_API_KEY")
        if not api_key:
            raise ValueError(
                "DATABENTO_API_KEY not found in environment. "
                "Please set it in your .env file or export it: "
                "export DATABENTO_API_KEY='your_api_key_here'"
            )

        # Backtesting: uses DataBentoDataBacktestingPolars for GC futures data (Polars = faster!)
        # Must pass api_key as keyword argument for DataBento to work
        results = AxiomPortStrategy.backtest(
            datasource_class=DataBentoDataBacktestingPolars,
            benchmark_asset=Asset("GC", Asset.AssetType.CONT_FUTURE),  # Compare to GC
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            parameters=params,
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            api_key=api_key,  # Required for DataBentoDataBacktesting
        )
    else:
        # Live trading: broker is selected via environment; no broker-specific code here
        trader = Trader()
        strategy = AxiomPortStrategy(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            parameters=params,
        )
        trader.add_strategy(strategy)
        trader.run_all()
