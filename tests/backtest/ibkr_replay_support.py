"""Frozen-input IBKR engine replay, shared by portable and private-data checks.

Only the historical-data boundary is replaced. The real IBKR backtesting data
source, Strategy executor, broker fills and accounting run unchanged. Provider
decoding, cache completeness and roll stitching require their separate checks.
No network credentials, proprietary fixtures or machine paths belong here.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.strategies import Strategy


def frame_records(frame):
    """Preserve every price and timestamp; never round a financial comparison."""
    columns = [c for c in ("open", "high", "low", "close", "volume", "dividend", "stock_splits") if c in frame]
    return [
        {"time": pd.Timestamp(stamp).isoformat(), **{
            name: None if pd.isna(row[name]) else float(row[name]) for name in columns
        }}
        for stamp, row in frame[columns].iterrows()
    ]


class ReplayMomentum(Strategy):
    """Small deterministic signal used to exercise real fills and accounting."""

    def initialize(self, parameters=None):
        p = self.parameters
        self.asset = p["asset"]
        self.sleeptime = p["sleeptime"]
        self.lookback = p["lookback"]
        self.cadence = p["cadence"]
        self.trace = []
        self.replay_errors = []
        if p.get("market"):
            self.set_market(p["market"])

    def on_trading_iteration(self):
        bars = self.get_historical_prices(self.asset, self.lookback, timestep=self.cadence)
        frame = bars.df if bars is not None else pd.DataFrame()
        closes = frame["close"].dropna() if "close" in frame else pd.Series(dtype=float)
        if len(closes) < self.lookback:
            message = f"Replay requires {self.lookback} completed closes, received {len(closes)}"
            self.replay_errors.append(message)
            raise AssertionError(message)
        # The signal is fixed across package versions. It never sees future bars.
        desired = 1 if float(closes.iloc[-1]) > float(closes.iloc[:-1].mean()) else 0
        position = self.get_position(self.asset)
        held = float(position.quantity) if position is not None else 0.0
        pending = [order for order in self.get_orders() if order.is_active()]
        if not pending and desired != held:
            self.submit_order(self.create_order(self.asset, abs(desired - held), "buy" if desired > held else "sell"))
        self.trace.append({
            "time": self.get_datetime().isoformat(),
            "last_input_time": pd.Timestamp(closes.index[-1]).isoformat(),
            "last_close": float(closes.iloc[-1]),
            "signal": desired, "held_before": held, "cash_before": float(self.cash),
            "equity_before": float(self.portfolio_value),
            "input_bars": frame_records(frame),
        })


def run_engine_replay(frame: pd.DataFrame, *, symbol: str, start: datetime, end: datetime,
                      timestep: str = "day", lookback: int = 3, asset_type: str = "stock",
                      expiration=None, multiplier: int = 1, market=None, auxiliary_frames=None, contract_frames=None, trading_fees=None,
                      routed: bool = False):
    """Run fixed data through the actual engine and return comparison artifacts."""
    if frame.index.has_duplicates or not frame.index.is_monotonic_increasing:
        raise ValueError("Replay prices must have unique ordered timestamps")
    if not {"open", "high", "low", "close", "volume"}.issubset(frame.columns):
        raise ValueError("Replay requires complete OHLCV")
    if frame[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError("Replay cannot silently substitute missing prices")
    asset = Asset(symbol, asset_type=asset_type, expiration=expiration, multiplier=multiplier)
    original_records = frame_records(frame)
    frames = {**(auxiliary_frames or {}), timestep: frame}
    requests = []
    fixture_errors = []

    def prices(**kwargs):
        requested = kwargs["asset"]
        requested_symbol = getattr(requested, "symbol", None)
        if requested_symbol != symbol:
            fixture_errors.append(f"Unexpected replay instrument {requested_symbol}")
            raise AssertionError(f"Unexpected replay instrument {requested_symbol}")
        requested_step = str(kwargs["timestep"])
        selected_frames = frames
        if getattr(requested, "expiration", None) is not None and contract_frames is not None:
            selected_frames = contract_frames.get(requested.expiration, {})
        if requested_step not in selected_frames:
            fixture_errors.append(f"Missing frozen {requested_step} fixture for {symbol}")
            raise AssertionError(f"Missing frozen {requested_step} fixture for {symbol}")
        requests.append({"symbol": symbol, "timestep": requested_step,
                         "start": str(kwargs["start_dt"]), "end": str(kwargs["end_dt"]),
                         "expiration": str(getattr(requested, "expiration", None))})
        # A real provider only returns the requested window. Returning the whole
        # corpus hides adapter underfetch at weekends and longer later lookbacks.
        selected = selected_frames[requested_step]
        return selected.loc[(selected.index >= kwargs["start_dt"]) &
                            (selected.index <= kwargs["end_dt"])].copy(deep=True)

    def no_network(*args, **kwargs):
        raise AssertionError("Frozen replay attempted a network request")

    from lumibot.tools import ibkr_helper

    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    source = RoutedBacktestingPandas if routed else InteractiveBrokersRESTBacktesting
    source_kwargs = ({"config": {"backtesting_data_routing": {"default": "ibkr"}}}
                     if routed else {"history_source": "Trades"})
    started = time.perf_counter()
    with patch.object(ibkr_helper, "get_price_data", prices), patch("socket.socket.connect", no_network):
        _, strategy = ReplayMomentum.run_backtest(
            source, start, end,
            budget=100_000, benchmark_asset=None, risk_free_rate=0,
            buy_trading_fees=trading_fees or [], sell_trading_fees=trading_fees or [],
            parameters={"asset": asset, "lookback": lookback, "cadence": timestep,
                        "sleeptime": "1D" if timestep == "day" else "30M", "market": market},
            **source_kwargs, analyze_backtest=False,
            show_plot=False, show_tearsheet=False, save_tearsheet=False,
            show_indicators=False, save_logfile=False, save_stats_file=False,
            show_progress_bar=False, quiet_logs=True,
        )
    assert isinstance(strategy.broker.data_source, source)
    assert not fixture_errors, fixture_errors
    assert not strategy.replay_errors, strategy.replay_errors
    assert frame_records(frame) == original_records, "Replay mutated the immutable input"
    trades = strategy.broker._trade_event_log_df
    fills = trades.loc[trades["status"] == "fill"] if "status" in trades else pd.DataFrame()
    fill_columns = [c for c in ("time", "symbol", "side", "price", "filled_quantity", "multiplier") if c in fills]
    records = fills[fill_columns].copy()
    if "time" in records:
        records["time"] = records["time"].map(lambda x: pd.Timestamp(x).isoformat())
    position = strategy.get_position(asset)
    from lumibot.tools.ibkr_history_health import ibkr_history_health_snapshot

    return {
        "bars": frame_records(frame), "signals": strategy.trace,
        "fills": records.to_dict(orient="records"), "cash": float(strategy.cash),
        "quantity": float(position.quantity) if position is not None else 0.0,
        "equity": float(strategy.portfolio_value), "history_requests": requests,
        "frame_sha256": hashlib.sha256(frame.to_json(date_format="iso").encode()).hexdigest(),
        "engine_seconds": time.perf_counter() - started,
        "data_health": ibkr_history_health_snapshot(),
    }
