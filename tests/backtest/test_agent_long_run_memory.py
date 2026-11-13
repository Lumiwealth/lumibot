"""Long agent backtests must not keep every past agent call in memory.

A backtest that runs two agents on every bar for hours used to keep every
observability row (prompts, tool results, model text) of every past call in
Python lists and rewrote the whole agent detail file after every call. Memory
grew by megabytes per call until the container was killed. These tests use a
synthetic two-agent strategy with a stub runtime, so no model is called.
"""

import gc
import tracemalloc
import json
import resource
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def _event(kind, *, text=None, tool_name=None, payload=None):
    from lumibot.components.agents import AgentTraceEvent

    return AgentTraceEvent(
        kind=kind,
        text=text,
        tool_name=tool_name,
        payload=payload,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def _bars(count=300):
    return [
        {
            "datetime": f"2025-01-01T{(index // 60) % 24:02d}:{index % 60:02d}:00",
            "open": 100.0 + index,
            "high": 101.0 + index,
            "low": 99.0 + index,
            "close": 100.5 + index,
            "volume": 1000 + index,
        }
        for index in range(count)
    ]


class LargeResultRuntime:
    calls = 0

    def run(self, request):
        type(self).calls += 1
        events = [_event("thinking", text="Check the trend and the open position. " * 20)]
        for symbol in ("AAA", "BBB", "CCC"):
            events.append(_event("tool_call", tool_name="market_historical_prices", payload={"symbol": symbol}))
            events.append(
                _event("tool_result", tool_name="market_historical_prices", payload={"symbol": symbol, "bars": _bars()})
            )
        usage = {"prompt_tokens": 9000, "completion_tokens": 400, "total_tokens": 9400}
        events.append(_event("usage", payload=usage))
        events.append(_event("text", text="RESULT: hold. " * 30))
        return AgentRunResult(summary="RESULT: hold.", model=request.model, events=events, usage=usage)


class TwoAgentMemoryStrategy(Strategy):
    samples: list[tuple[int, int]] = []

    def initialize(self):
        self.sleeptime = "1D"
        for name in ("researcher", "trader"):
            self.agents.create(
                name=name,
                system_prompt=f"Synthetic {name} prompt. " * 40,
                default_model="stub-long-run",
                tools=[],
                _runtime=LargeResultRuntime(),
            )

    def on_trading_iteration(self):
        note = self.agents["researcher"].run(context={"symbols": ["AAA", "BBB", "CCC"]})
        self.agents["trader"].run(context={"research": note.summary})
        gc.collect()
        type(self).samples.append((LargeResultRuntime.calls, tracemalloc.get_traced_memory()[0]))


def _daily_data():
    index = pd.date_range("2024-01-02 16:00", "2024-12-31 16:00", freq="B", tz="America/New_York")
    frame = pd.DataFrame({"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}, index=index)
    asset = Asset("AAA", Asset.AssetType.STOCK)
    return {asset: Data(asset, frame, timestep="day")}


@pytest.mark.usefixtures("disable_datasource_override")
def test_long_two_agent_backtest_memory_stays_flat_and_keeps_every_detail_row(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    LargeResultRuntime.calls = 0
    TwoAgentMemoryStrategy.samples = []
    tracemalloc.start()
    try:
        _, strategy = TwoAgentMemoryStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=datetime(2024, 1, 3),
            backtesting_end=datetime(2024, 3, 1),
            pandas_data=_daily_data(),
            benchmark_asset=None,
            analyze_backtest=False,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=True,
        )
    finally:
        tracemalloc.stop()

    samples = TwoAgentMemoryStrategy.samples
    assert len(samples) >= 30
    early_calls, early_bytes = samples[9]
    late_calls, late_bytes = samples[-1]
    growth_per_call = (late_bytes - early_bytes) / (late_calls - early_calls)
    # Before the fix every call kept about 1 MB of rows alive. A bounded run
    # keeps only small bookkeeping per call.
    assert growth_per_call < 100_000, f"retained {growth_per_call:.0f} bytes per agent call"

    detail_parquet = Path(strategy.parameters["agent_researcher_detail_parquet"])
    assert detail_parquet == Path(strategy.parameters["agent_trader_detail_parquet"])
    frame = pd.read_parquet(detail_parquet)
    summaries = frame[frame["event_kind"] == "call_summary"]
    assert len(summaries) == LargeResultRuntime.calls
    assert summaries.groupby("agent_name").size().to_dict() == {
        "researcher": LargeResultRuntime.calls // 2,
        "trader": LargeResultRuntime.calls // 2,
    }
    assert summaries["call_index"].max() == LargeResultRuntime.calls // 2
    assert '"bars"' in frame[frame["event_kind"] == "tool_result"]["event_payload_json"].iloc[-1]
    leftovers = [
        path.name
        for path in detail_parquet.parent.iterdir()
        if path.name.startswith(detail_parquet.stem + ".") and path != detail_parquet
    ]
    assert leftovers == []


def test_detail_spool_combines_calls_whose_column_types_differ(tmp_path):
    """One call can record a latency as a number and a later call as text
    (or nothing at all). The combined file must still be written: a CI example
    backtest failed with "Unable to merge: Field call_first_event_latency_ms
    has incompatible types: string vs int64" (2026-11-11)."""
    import logging

    from lumibot.components.agents.manager import _AgentDetailSpool

    path = tmp_path / "run_agent_detail.parquet"
    spool = _AgentDetailSpool(path)
    logger = logging.getLogger("test")
    spool.append(pd.DataFrame({"call_index": [1], "latency": [120], "note": [None]}), artifact="a", logger=logger)
    spool.append(pd.DataFrame({"call_index": [2], "latency": ["slow"], "note": [None]}), artifact="a", logger=logger)
    spool.append(pd.DataFrame({"call_index": [3], "latency": [None], "note": ["ok"]}), artifact="a", logger=logger)
    spool.finalize()

    frame = pd.read_parquet(path)
    assert frame["call_index"].tolist() == [1, 2, 3]
    assert [None if pd.isna(value) else str(value) for value in frame["latency"]] == ["120", "slow", None]
    assert frame["note"].tolist()[2] == "ok"
    assert not path.with_name(f"{path.stem}.parts").exists()


class HourlyTwoAgentStrategy(TwoAgentMemoryStrategy):
    iterations = 0
    fills = 0

    def initialize(self):
        super().initialize()
        self.sleeptime = "1h"
        self.set_market("24/7")

    def on_trading_iteration(self):
        import psutil

        note = self.agents["researcher"].run(context={"symbols": ["AAA", "BBB", "CCC"]})
        self.agents["trader"].run(context={"research": note.summary})
        type(self).iterations += 1
        if type(self).iterations <= 20:
            self.submit_order(self.create_order("AAA", 1, "buy" if type(self).iterations % 2 else "sell"))
        type(self).samples.append((LargeResultRuntime.calls, psutil.Process().memory_info().rss))

    def on_filled_order(self, position, order, price, quantity, multiplier):
        type(self).fills += 1
        self.agents["trader"].run(context={"filled_order": order.identifier, "price": price, "quantity": quantity})


@pytest.mark.usefixtures("disable_datasource_override")
def test_hourly_two_agent_replay_bounds_process_memory_and_keeps_fill_callbacks(monkeypatch, tmp_path):
    import pyarrow.parquet as pq
    import lumibot

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    LargeResultRuntime.calls = 0
    HourlyTwoAgentStrategy.samples = []
    HourlyTwoAgentStrategy.iterations = HourlyTwoAgentStrategy.fills = 0
    index = pd.date_range("2026-01-02", "2026-01-16", freq="h", tz="America/New_York")
    frame = pd.DataFrame({"open": 100., "high": 101., "low": 99., "close": 100.5, "volume": 1000}, index=index)
    asset = Asset("AAA", Asset.AssetType.STOCK)
    peak_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    _, strategy = HourlyTwoAgentStrategy.run_backtest(
        datasource_class=PandasDataBacktesting, backtesting_start=index[0].to_pydatetime(),
        backtesting_end=index[-1].to_pydatetime(), pandas_data={asset: Data(asset, frame, timestep="hour")},
        benchmark_asset=None, analyze_backtest=False, show_plot=False, save_tearsheet=False,
        show_tearsheet=False, show_indicators=False, save_logfile=False, show_progress_bar=False, quiet_logs=True,
    )
    samples = HourlyTwoAgentStrategy.samples
    assert LargeResultRuntime.calls >= 600
    assert HourlyTwoAgentStrategy.fills == 20
    assert LargeResultRuntime.calls == 2 * HourlyTwoAgentStrategy.iterations + HourlyTwoAgentStrategy.fills
    early_calls, early_rss = samples[20]
    late_calls, late_rss = samples[-1]
    assert (late_rss - early_rss) / (late_calls - early_calls) < 250_000
    peak_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    native_unit = 1 if sys.platform == "darwin" else 1024
    assert (peak_after - peak_before) * native_unit < 512 * 1024 * 1024
    detail = Path(strategy.parameters["agent_researcher_detail_parquet"])
    counts = {"researcher": [], "trader": []}
    tool_rows = 0
    for batch in pq.ParquetFile(detail).iter_batches(batch_size=64, columns=["agent_name", "call_index", "event_kind", "event_payload_json"]):
        for row in batch.to_pylist():
            if row["event_kind"] == "call_summary":
                counts[row["agent_name"]].append(row["call_index"])
            elif row["event_kind"] == "tool_result":
                payload = json.loads(row["event_payload_json"])
                assert payload["symbol"] in {"AAA", "BBB", "CCC"}
                assert payload["bars"] == _bars()
                tool_rows += 1
    expected = {"researcher": HourlyTwoAgentStrategy.iterations, "trader": HourlyTwoAgentStrategy.iterations + 20}
    for name, count in expected.items():
        assert counts[name] == list(range(1, count + 1))
    assert tool_rows == LargeResultRuntime.calls * 3
    assert not detail.with_name(f"{detail.stem}.parts").exists()
    receipt = {"version": lumibot.__version__, "calls": LargeResultRuntime.calls, "iterations": HourlyTwoAgentStrategy.iterations,
               "fills": HourlyTwoAgentStrategy.fills, "toolRows": tool_rows, "earlyRssBytes": early_rss,
               "lateRssBytes": late_rss, "peakGrowthBytes": (peak_after - peak_before) * native_unit}
    (tmp_path / "hourly-recorder-result.json").write_text(json.dumps(receipt))
    print("HOURLY_RECORDER_RESULT", json.dumps(receipt))
