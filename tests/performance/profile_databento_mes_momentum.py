"""Profile DataBento with realistic MES Momentum SMA-9 strategy over 2-3 days.

This uses a real trading strategy (not a toy test) to find actual bottlenecks.
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import yappi
import pytz
import pandas as pd
import math

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class MESMomentumSMA9(Strategy):
    """Real MES trading strategy with SMA-9 gate and ATR-based risk management."""

    parameters = {
        # Technicals
        "sma_period": 9,                 # 9-minute SMA gate
        "atr_period": 14,               # ATR lookback for volatility
        "atr_stop_mult": 1.0,           # Stop distance = ATR * this multiplier
        "rr_ratio": 3.0,                # Reward:Risk ratio (3:1)

        # Risk and sizing
        "risk_per_trade_pct": 0.01,     # Risk 1% of portfolio per trade
        "mes_point_value": 5.0,         # MES dollar value per 1.0 index point
        "max_contracts": 5,             # Cap position size

        # Data & cadence
        "bars_lookback": 200,           # Pull enough bars for indicators
        "timestep": "minute",          # 1-minute bars
    }

    def initialize(self):
        self.set_market("us_futures")
        self.sleeptime = "1M"  # Run every 1 minute
        self.vars.mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.vars.last_signal = "cash"
        self.vars.last_sma = None
        self.vars.last_price = None

    def _compute_indicators(self, df: pd.DataFrame, last_price: float, sma_period: int, atr_period: int) -> tuple:
        """Calculate SMA and ATR indicators."""
        df = df.copy()
        df["sma"] = df["close"].rolling(window=sma_period).mean()

        # True Range for ATR
        df["prev_close"] = df["close"].shift(1)
        tr1 = df["high"] - df["low"]
        tr2 = (df["high"] - df["prev_close"]).abs()
        tr3 = (df["low"] - df["prev_close"]).abs()
        df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df["atr"] = df["tr"].rolling(window=atr_period).mean()

        sma_latest = df["sma"].iloc[-1] if not pd.isna(df["sma"].iloc[-1]) else None
        atr_latest = df["atr"].iloc[-1] if not pd.isna(df["atr"].iloc[-1]) else None

        # Blend SMA with latest price
        if last_price is not None and len(df) >= sma_period:
            closes = df["close"].iloc[-(sma_period-1):].tolist()
            closes.append(last_price)
            sma_live = sum(closes) / sma_period
        else:
            sma_live = sma_latest

        return sma_live, atr_latest

    def _has_open_orders(self) -> bool:
        """Check if there are any pending orders."""
        for o in self.get_orders():
            if o.status in [Order.OrderStatus.SUBMITTED, Order.OrderStatus.OPEN, Order.OrderStatus.NEW]:
                return True
        return False

    def on_trading_iteration(self):
        params = self.get_parameters()
        asset = self.vars.mes_asset

        # Get historical minute bars (this is a key profiling point)
        bars = self.get_historical_prices(asset, params["bars_lookback"], params["timestep"])
        if bars is None or bars.df is None:
            return

        # Convert to pandas for strategy logic (strategy uses pandas operations)
        # Note: Polars optimization happens internally - we only convert at the boundary
        df = bars.pandas_df
        if df is None or df.empty:
            return

        last_price = self.get_last_price(asset)  # Another key profiling point
        if last_price is None:
            last_price = df["close"].iloc[-1]
        self.vars.last_price = float(last_price)

        # Compute indicators (DataFrame operations to profile)
        sma_live, atr_val = self._compute_indicators(df, last_price, params["sma_period"], params["atr_period"])
        self.vars.last_sma = float(sma_live) if sma_live is not None else None

        if sma_live is None or atr_val is None or atr_val <= 0:
            return

        price_above_sma = last_price > sma_live
        pos = self.get_position(asset)
        has_pos = pos is not None and abs(pos.quantity) > 0

        # Exit rule: price drops below SMA
        if has_pos and not price_above_sma:
            self.cancel_open_orders()
            close_order = self.close_position(asset)
            if close_order is not None:
                self.submit_order(close_order)
            self.vars.last_signal = "cash"
            return

        # Entry rule: flat, gate passes, no open orders
        if not has_pos and price_above_sma and not self._has_open_orders():
            portfolio_value = self.get_portfolio_value()
            risk_cash = portfolio_value * float(params["risk_per_trade_pct"])

            stop_points = float(params["atr_stop_mult"]) * float(atr_val)
            if stop_points <= 0:
                return

            contracts = math.floor(risk_cash / (stop_points * float(params["mes_point_value"])))
            contracts = int(max(1, min(contracts, int(params["max_contracts"]))))

            if contracts <= 0:
                return

            entry_ref = float(last_price)
            stop_price = entry_ref - stop_points
            take_profit_price = entry_ref + float(params["rr_ratio"]) * stop_points

            order = self.create_order(
                asset=asset,
                quantity=contracts,
                side=Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                order_class=Order.OrderClass.BRACKET,
                secondary_limit_price=take_profit_price,
                secondary_stop_price=stop_price,
            )

            self.submit_order(order)
            self.vars.last_signal = "long"
            return


def run_mes_momentum_profile(mode: str) -> float:
    """Run MES Momentum strategy backtest over 2-3 days and profile it."""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas
    label = f"mes_momentum_{mode}"
    profile_path = OUTPUT_DIR / f"{label}.prof"

    # 3 trading days: Jan 3-5, 2024 (market open hours)
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime(2024, 1, 5, 16, 0))

    print(f"\n{'='*60}")
    print(f"Starting {mode.upper()} backtest...")
    print(f"Period: {start} to {end}")
    print(f"{'='*60}")

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    wall_start = time.time()

    # Run backtest
    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = MESMomentumSMA9(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start
    yappi.stop()

    # Save profile
    yappi.get_func_stats().save(str(profile_path), type="pstat")

    # Print results
    print(f"\n{'='*60}")
    print(f"MODE: {mode.upper()}")
    print(f"{'='*60}")
    print(f"Elapsed time: {elapsed:.2f}s")
    print(f"Profile saved: {profile_path}")

    # Show iteration count if available
    if hasattr(strat.broker, 'iteration_count'):
        print(f"Iterations: {strat.broker.iteration_count}")

    print(f"{'='*60}\n")

    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile MES Momentum strategy on DataBento")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        return

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = {}

    for m in modes:
        elapsed = run_mes_momentum_profile(m)
        results[m] = elapsed

    # Print comparison
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("COMPARISON")
        print(f"{'='*60}")
        pandas_time = results.get("pandas", 0)
        polars_time = results.get("polars", 0)
        if pandas_time > 0 and polars_time > 0:
            speedup = pandas_time / polars_time
            print(f"Pandas: {pandas_time:.2f}s")
            print(f"Polars: {polars_time:.2f}s")
            print(f"Speedup: {speedup:.2f}x")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
