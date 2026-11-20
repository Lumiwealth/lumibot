"""Daily split lookups read a per-frame index instead of scanning the frame per price read.

Why: every daily stock backtest reads a price from the same daily frame many times per
bar. Before, each read re-checked the whole ``_split_adjusted`` column and scanned every
index stamp for the current date (profiled on a 10-year daily production run: about
35,000 reads, 12 profiled seconds). Results must be identical with and without the index.
"""

from __future__ import annotations

import hashlib
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.data_sources import data_source as data_source_module
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class _SwingThroughSplits(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.asset = self.parameters["asset"]
        self.rows = []

    def on_trading_iteration(self):
        price = self.get_last_price(self.asset)
        position = self.get_position(self.asset)
        qty = float(position.quantity) if position is not None else 0.0
        day = self.get_datetime().day
        if qty == 0 and day % 5 == 0:
            self.submit_order(self.create_order(self.asset, 10, "buy"))
        elif qty > 0 and day % 7 == 0:
            self.submit_order(self.create_order(self.asset, qty, "sell"))
        self.rows.append(
            (self.get_datetime().isoformat(), round(float(price), 8), round(float(self.portfolio_value), 6), qty)
        )


def _frame():
    index = pd.bdate_range("2023-01-02", "2024-06-28", tz="America/New_York") + pd.Timedelta(hours=16)
    rng = np.random.default_rng(7)
    close = 100.0 * np.cumprod(1.0 + rng.normal(0.0003, 0.01, len(index)))
    splits = np.zeros(len(index))
    # Raw (unadjusted) prices with two splits: a 2-for-1 and a 1-for-4 reverse split.
    first, second = 120, 300
    close[first:] = close[first:] / 2.0
    splits[first] = 2.0
    close[second:] = close[second:] * 4.0
    splits[second] = 0.25
    return pd.DataFrame(
        {
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": 1_000_000,
            "stock_splits": splits,
        },
        index=index,
    )


def _run():
    asset = Asset("SPLT", Asset.AssetType.STOCK)
    frame = _frame()
    data_source = PandasDataBacktesting(
        pandas_data={asset: Data(asset, frame, timestep="day")},
        datetime_start=datetime(2023, 1, 3),
        datetime_end=datetime(2024, 6, 27),
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = _SwingThroughSplits(
        broker=broker,
        data_source=data_source,
        budget=10_000.0,
        risk_free_rate=0,
        benchmark_asset=None,
        analyze_backtest=False,
        parameters={"asset": asset},
    )
    trader = Trader(backtest=True, logfile="")
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

    log = broker._trade_event_log_df
    cols = [c for c in ("event_kind", "status", "side", "filled_quantity", "price") if c in log.columns]
    log_text = log[cols].astype(str).to_csv(index=False)
    rows_text = repr(strategy.rows)
    return (
        hashlib.sha256(log_text.encode()).hexdigest(),
        hashlib.sha256(rows_text.encode()).hexdigest(),
        len(strategy.rows),
        int((log["event_kind"] == "corporate_action").sum()) if "event_kind" in log.columns else 0,
    )


def _count_full_scans(monkeypatch):
    calls = {"n": 0}
    original = data_source_module._frame_prices_are_split_adjusted_uncached

    def counting(frame):
        calls["n"] += 1
        return original(frame)

    monkeypatch.setattr(data_source_module, "_frame_prices_are_split_adjusted_uncached", counting)
    return calls


def test_split_facts_index_gives_identical_results_and_scans_each_frame_once(monkeypatch):
    calls = _count_full_scans(monkeypatch)
    data_source_module._SPLIT_FACTS_CACHE.clear()
    with_index = _run()
    scans_with_index = calls["n"]

    calls["n"] = 0
    monkeypatch.setattr(data_source_module, "_split_frame_facts", lambda frame, with_events=False: None)
    without_index = _run()
    scans_without_index = calls["n"]

    assert with_index == without_index
    assert with_index[2] > 300
    # Old path: a full-frame scan on every price read.
    assert scans_without_index > 300
    # Remaining scans come from callers that hand over a fresh frame object each time
    # (unchanged cost); the per-read price path no longer rescans.
    assert scans_with_index * 4 < scans_without_index, (scans_with_index, scans_without_index)


def test_split_facts_refresh_when_frame_grows():
    index = pd.DatetimeIndex(["2025-01-02 16:00:00-05:00", "2025-01-03 16:00:00-05:00"])
    frame = pd.DataFrame({"close": [1.0, 1.0], "stock_splits": [0.0, 0.0]}, index=index)
    assert data_source_module._split_frame_facts(frame, with_events=True)[1] == {}
    frame.loc[pd.Timestamp("2025-01-06 16:00:00-05:00")] = [1.0, 3.0]
    facts = data_source_module._split_frame_facts(frame, with_events=True)
    assert list(facts[1]) == [pd.Timestamp("2025-01-06 16:00:00-05:00").date()]
    assert facts[1][pd.Timestamp("2025-01-06").date()][0][1] == pytest.approx(3.0)
