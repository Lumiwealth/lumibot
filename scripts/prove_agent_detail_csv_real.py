#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

print("PROOF_SCRIPT_START", flush=True)

import pandas as pd

print("IMPORTS_STAGE_1_OK", flush=True)

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.backtesting.pandas_backtesting import PandasDataBacktesting

print("IMPORT_BACKTESTING_OK", flush=True)
from lumibot.components.agents import agent_tool

print("IMPORT_AGENT_TOOL_OK", flush=True)
from lumibot.entities.asset import Asset

print("IMPORT_ASSET_OK", flush=True)
from lumibot.entities.data import Data

print("IMPORT_DATA_OK", flush=True)
from lumibot.strategies.strategy import Strategy

print("IMPORT_STRATEGY_OK", flush=True)


class AgentCsvProofStrategy(Strategy):
    @agent_tool(
        description="Return a deterministic synthetic momentum signal for one symbol. Used only to prove AI observability artifacts.",
    )
    def get_signal(self, symbol: str) -> dict:
        return {
            "symbol": symbol,
            "momentum": "strong",
            "trend": "uptrend",
            "volatility": "cooling",
            "reason": "Synthetic proof signal for observability testing.",
        }

    def initialize(self):
        self.sleeptime = "1D"
        self.did_run = False
        self.agents.create(
            name="trader",
            system_prompt="Use the available tool output, explain your reasoning, and decide what to do. Do not place real orders in this proof run.",
            default_model=os.environ.get("AGENT_MODEL", "gemini-3.1-pro-preview"),
            tools=[self.get_signal],
        )

    def on_trading_iteration(self):
        if self.did_run:
            return
        self.did_run = True
        self.agents["trader"].run(
            task_prompt="Call get_signal for AAPL, reason through the result, and return a concise final decision. Do not submit orders.",
            context={"date": str(self.get_datetime())},
        )


def _make_pandas_data():
    idx = pd.date_range("2026-03-01", periods=3, freq="1D")
    df = pd.DataFrame(
        {
            "open": [100.0, 101.0, 103.0],
            "high": [101.0, 104.0, 105.0],
            "low": [99.0, 100.0, 102.0],
            "close": [100.5, 103.5, 104.5],
            "volume": [1000, 1200, 1100],
        },
        index=idx,
    )
    return [Data(Asset("AAPL"), df, timestep="day")]


def main():
    results = AgentCsvProofStrategy.backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=_make_pandas_data(),
        backtesting_start=datetime(2026, 3, 1),
        backtesting_end=datetime(2026, 3, 4),
        benchmark_asset=None,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
        quiet_logs=True,
    )
    detail_candidates = sorted((REPO_ROOT / "logs").glob("AgentCsvProofStrategy_*_agent_detail.csv"), key=lambda p: p.stat().st_mtime)
    detail_csv = str(detail_candidates[-1]) if detail_candidates else None
    detail_parquet = str(Path(detail_csv).with_suffix(".parquet")) if detail_csv else None
    print(f"DETAIL_CSV={detail_csv}")
    print(f"DETAIL_PARQUET={detail_parquet}")
    print(f"RESULTS={json.dumps(results, default=str)}")
    if detail_csv and os.path.exists(detail_csv):
        df = pd.read_csv(detail_csv)
        parquet_df = pd.read_parquet(detail_parquet) if detail_parquet and os.path.exists(detail_parquet) else None
        print(f"DETAIL_ROWS={len(df)}")
        print(f"PARQUET_ROWS={len(parquet_df) if parquet_df is not None else 'MISSING'}")
        thinking_rows = df[df['event_kind'] == 'thinking']
        print(f"THINKING_ROWS={len(thinking_rows)}")
        if not thinking_rows.empty:
            print("THINKING_SAMPLE_START")
            print(thinking_rows[["event_kind", "event_text"]].head(3).to_csv(index=False))
            print("THINKING_SAMPLE_END")
        print("DETAIL_SAMPLE_START")
        print(
            df[
                [
                    "event_kind",
                    "tool_name",
                    "event_text",
                    "event_input_tokens",
                    "event_output_tokens",
                    "event_thinking_tokens",
                    "call_input_tokens",
                    "call_output_tokens",
                    "call_thinking_tokens",
                ]
            ].head(12).to_csv(index=False)
        )
        print("DETAIL_SAMPLE_END")
    else:
        print("DETAIL_CSV_MISSING")


if __name__ == "__main__":
    main()
