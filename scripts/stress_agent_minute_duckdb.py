#!/usr/bin/env python3

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yappi

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import AgentRunResult, BuiltinTools
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


def _utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _event(kind: str, *, text: str | None = None, tool_name: str | None = None, payload: dict | None = None):
    from lumibot.components.agents import AgentTraceEvent

    return AgentTraceEvent(
        kind=kind,
        text=text,
        tool_name=tool_name,
        payload=payload,
        timestamp=_utc_iso_timestamp(),
    )


def _invoke_tool(request, events, tool_name: str, **kwargs):
    tool_map = {tool.name: tool.function for tool in request.bound_tools}
    events.append(_event("tool_call", tool_name=tool_name, payload=kwargs))
    result = tool_map[tool_name](**kwargs)
    payload = result if isinstance(result, dict) else {"value": result}
    events.append(_event("tool_result", tool_name=tool_name, payload=payload))
    return result


class MinuteDuckDBStressRuntime:
    call_count = 0

    def run(self, request):
        type(self).call_count += 1
        events = [_event("thinking", text="Running minute-level DuckDB stress iteration.")]
        positions = _invoke_tool(request, events, "account_positions")
        table = _invoke_tool(
            request,
            events,
            "market_load_history_table",
            symbol=request.context["symbol"],
            length=request.context["length"],
            timestep=request.context["timestep"],
            asset_type="stock",
            table_name="minute_window",
        )
        _invoke_tool(
            request,
            events,
            "duckdb_query",
            sql=f"SELECT COUNT(*) AS rows_seen, AVG(close) AS avg_close, MAX(close) AS max_close FROM {table['table_name']}",
        )
        has_position = any(
            pos.get("asset", {}).get("symbol") == request.context["symbol"] and float(pos.get("quantity") or 0) > 0
            for pos in positions["positions"]
            if isinstance(pos, dict) and isinstance(pos.get("asset"), dict)
        )
        if not has_position:
            _invoke_tool(
                request,
                events,
                "orders_submit_order",
                symbol=request.context["symbol"],
                quantity=1,
                side="buy",
                asset_type="stock",
                order_type="market",
            )
            summary = "Minute stress run bought once."
        else:
            summary = "Minute stress run held."
        events.append(_event("text", text=summary))
        return AgentRunResult(summary=summary, model=request.model, events=events)


class MinuteDuckDBStressStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"
        self.asset = Asset("AGMS", Asset.AssetType.STOCK)
        self.agents.create(
            name="research",
            system_prompt="Stress test minute DuckDB history refreshes and only buy once.",
            default_model="stub-minute-stress",
            tools=[
                BuiltinTools.account.positions(),
                BuiltinTools.market.load_history_table(),
                BuiltinTools.duckdb.query(),
                BuiltinTools.orders.submit(),
            ],
            _runtime=MinuteDuckDBStressRuntime(),
        )

    def on_trading_iteration(self):
        result = self.agents["research"].run(
            context={
                "symbol": self.asset.symbol,
                "length": 120,
                "timestep": "minute",
            }
        )
        summaries = self.vars.get("summaries", [])
        summaries.append(result.summary or "")
        self.vars.summaries = summaries


def _minute_stress_data():
    day_one = pd.date_range("2025-01-06 09:30", periods=390, freq="min", tz="America/New_York")
    day_two = pd.date_range("2025-01-07 09:30", periods=390, freq="min", tz="America/New_York")
    index = day_one.append(day_two)
    base = [100.0 + i * 0.02 for i in range(len(index))]
    df = pd.DataFrame(
        {
            "open": base,
            "high": [value + 0.15 for value in base],
            "low": [value - 0.15 for value in base],
            "close": [value + 0.05 for value in base],
            "volume": [1000 + (i % 100) for i in range(len(index))],
        },
        index=index,
    )
    asset = Asset("AGMS", Asset.AssetType.STOCK)
    return {asset: Data(asset, df, timestep="minute")}


def _write_yappi_csv(path: Path) -> None:
    stats = yappi.get_func_stats()
    stats.sort("ttot", "desc")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "full_name",
                "module",
                "lineno",
                "name",
                "ncall",
                "nactualcall",
                "ttot_s",
                "tsub_s",
                "tavg_s",
                "ctx_name",
            ]
        )
        for entry in stats:
            writer.writerow(
                [
                    getattr(entry, "full_name", ""),
                    getattr(entry, "module", ""),
                    getattr(entry, "lineno", ""),
                    getattr(entry, "name", ""),
                    getattr(entry, "ncall", ""),
                    getattr(entry, "nactualcall", ""),
                    getattr(entry, "ttot", ""),
                    getattr(entry, "tsub", ""),
                    getattr(entry, "tavg", ""),
                    getattr(entry, "ctx_name", ""),
                ]
            )


def main() -> int:
    output_dir = ROOT / "logs" / "agent_runtime_validation" / f"minute_stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(output_dir / "cache")
    os.environ["BACKTESTING_DATA_SOURCE"] = "none"
    MinuteDuckDBStressRuntime.call_count = 0

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    started = time.perf_counter()
    try:
        _, strategy = MinuteDuckDBStressStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=datetime(2025, 1, 6, 9, 30),
            backtesting_end=datetime(2025, 1, 7, 15, 59),
            pandas_data=_minute_stress_data(),
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
        elapsed_s = time.perf_counter() - started
    finally:
        yappi.stop()
        yappi_csv = output_dir / "minute_duckdb_stress_profile_yappi.csv"
        _write_yappi_csv(yappi_csv)
        yappi.clear_stats()

    metrics = strategy.agents.duckdb.get_metrics()
    summary = {
        "elapsed_s": elapsed_s,
        "runtime_calls": MinuteDuckDBStressRuntime.call_count,
        "duckdb_metrics": metrics,
        "summary_count": len(strategy.vars.get("summaries", [])),
        "yappi_csv": str(yappi_csv),
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output_dir": str(output_dir), "summary_path": str(summary_path), "summary": summary}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
