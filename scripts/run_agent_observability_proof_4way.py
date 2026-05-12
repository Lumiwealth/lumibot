#!/usr/bin/env python3
"""Run a tiny real AI-agent backtest across four providers.

This is a cheap proof script for the agent observability artifact. It makes
one real provider call per model, runs through the LumiBot backtesting stack,
and prints the resulting *_agent_detail.parquet path plus token/cache/latency
totals. It intentionally does not estimate pricing.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.backtesting import PandasDataBacktesting
from lumibot.components.agents import BuiltinTools
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy

DEFAULT_MODELS = [
    "gemini-3.1-pro-preview",
    "openai/gpt-5.4",
    "xai/grok-4.20-0309-reasoning",
    "anthropic/claude-sonnet-4-6",
]


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value and key not in os.environ:
            os.environ[key] = value


def _load_known_env_files() -> None:
    for path in (
        Path("/Users/robertgrzesik/Development/botspot_node/.env-local"),
        Path("/Users/robertgrzesik/Development/lumibot_strategy_tester/.env"),
        Path("/Users/robertgrzesik/Development/botspot_dreaming/.env.local"),
    ):
        _load_env_file(path)
    if not os.environ.get("GOOGLE_API_KEY") and os.environ.get("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
    if not os.environ.get("XAI_API_KEY") and os.environ.get("GROK_API_KEY"):
        os.environ["XAI_API_KEY"] = os.environ["GROK_API_KEY"]


def _required_key_for_model(model: str) -> str | tuple[str, ...] | None:
    lower = model.lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        return ("GEMINI_API_KEY", "GOOGLE_API_KEY")
    if lower.startswith("openai/"):
        return "OPENAI_API_KEY"
    if lower.startswith("xai/"):
        return ("XAI_API_KEY", "GROK_API_KEY")
    if lower.startswith("anthropic/"):
        return "ANTHROPIC_API_KEY"
    return None


def _has_required_key(model: str) -> bool:
    required = _required_key_for_model(model)
    if required is None:
        return True
    if isinstance(required, tuple):
        return any(os.environ.get(key) for key in required)
    return bool(os.environ.get(required))


def _build_pandas_data() -> dict[Asset, Data]:
    asset = Asset("AAPL", Asset.AssetType.STOCK)
    index = pd.date_range("2026-04-20 09:30", periods=6, freq="B", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [200.0, 201.0, 202.0, 201.5, 203.0, 204.0],
            "high": [202.0, 203.0, 203.5, 204.0, 205.0, 206.0],
            "low": [199.0, 200.5, 201.0, 200.0, 202.0, 203.5],
            "close": [201.0, 202.0, 201.5, 203.0, 204.0, 205.0],
            "volume": [1000, 1100, 1200, 1300, 1400, 1500],
        },
        index=index,
    )
    return {asset: Data(asset, df, timestep="day")}


class ObservabilityProofStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.asset = Asset("AAPL", Asset.AssetType.STOCK)
        self.vars.did_run_agent = False
        self.agents.create(
            name="trader",
            system_prompt="You are validating LumiBot AI observability. Follow the task exactly and keep the final answer short.",
            default_model=os.environ["AGENT_MODEL"],
            tools=[
                BuiltinTools.account.portfolio(),
                BuiltinTools.account.positions(),
                BuiltinTools.market.last_price(),
                BuiltinTools.market.load_history_table(),
                BuiltinTools.duckdb.query(),
                BuiltinTools.orders.submit(),
            ],
        )

    def on_trading_iteration(self):
        if self.vars.did_run_agent:
            return
        self.vars.did_run_agent = True
        self.agents["trader"].run(
            task_prompt=(
                "This is a one-call observability proof. Call account_portfolio, "
                "account_positions, market_last_price for AAPL, market_load_history_table "
                "for AAPL with length=3 timestep='day' table_name='aapl_history', then "
                "duckdb_query on that table. If the data looks valid, submit exactly one "
                "market buy order for one share of AAPL. Then summarize what you did."
            ),
            context={"symbol": "AAPL"},
        )


def _summarize_artifact(path: Path) -> dict[str, Any]:
    df = pd.read_parquet(path)
    summaries = df[df["event_kind"] == "call_summary"]
    return {
        "agent_detail_parquet": str(path),
        "rows": int(len(df)),
        "event_kinds": sorted(str(item) for item in df["event_kind"].dropna().unique().tolist()),
        "thinking_rows": int((df["event_kind"] == "thinking").sum()),
        "tool_call_rows": int((df["event_kind"] == "tool_call").sum()),
        "input_tokens": int(pd.to_numeric(summaries["call_input_tokens"], errors="coerce").fillna(0).sum()),
        "output_tokens": int(pd.to_numeric(summaries["call_output_tokens"], errors="coerce").fillna(0).sum()),
        "total_tokens": int(pd.to_numeric(summaries["call_total_tokens"], errors="coerce").fillna(0).sum()),
        "thinking_tokens": int(pd.to_numeric(summaries["call_thinking_tokens"], errors="coerce").fillna(0).sum()),
        "cached_input_tokens": int(pd.to_numeric(summaries["call_cached_input_tokens"], errors="coerce").fillna(0).sum()),
        "uncached_input_tokens": int(pd.to_numeric(summaries["call_uncached_input_tokens"], errors="coerce").fillna(0).sum()),
        "latency_ms": int(pd.to_numeric(summaries["call_latency_ms"], errors="coerce").fillna(0).sum()),
        "sample_thinking_text": str(df.loc[df["event_kind"] == "thinking", "event_text"].head(1).iloc[0])
        if (df["event_kind"] == "thinking").any()
        else "",
    }


def run_model(model: str) -> dict[str, Any]:
    if not _has_required_key(model):
        return {"model": model, "status": "skipped", "reason": "missing provider API key"}

    os.environ["AGENT_MODEL"] = model
    cache_dir = Path("/tmp") / f"lumibot_agent_observability_proof_{int(time.time() * 1000)}_{abs(hash(model))}"
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(cache_dir)
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"

    started = time.time()
    try:
        _, strategy = ObservabilityProofStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=datetime(2026, 4, 20),
            backtesting_end=datetime(2026, 4, 27),
            pandas_data=_build_pandas_data(),
            benchmark_asset=None,
            analyze_backtest=False,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=True,
            budget=10_000,
        )
        detail_path = Path(strategy.parameters["agent_trader_detail_parquet"])
        artifact = _summarize_artifact(detail_path)
        return {
            "model": model,
            "status": "passed",
            "wall_seconds": round(time.time() - started, 2),
            **artifact,
        }
    except Exception as exc:
        error_text = str(exc)[:800]
        lower_error = error_text.lower()
        if "available credits" in lower_error or "spending limit" in lower_error or "out of credits" in lower_error:
            return {
                "model": model,
                "status": "blocked",
                "reason": "provider billing or spending limit",
                "wall_seconds": round(time.time() - started, 2),
                "error_type": exc.__class__.__name__,
                "error": error_text,
            }
        return {
            "model": model,
            "status": "failed",
            "wall_seconds": round(time.time() - started, 2),
            "error_type": exc.__class__.__name__,
            "error": error_text,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS)
    args = parser.parse_args()

    _load_known_env_files()
    results = [run_model(model) for model in args.models]
    print(json.dumps({"results": results}, indent=2, sort_keys=True))
    return 0 if all(item["status"] in {"passed", "skipped", "blocked"} for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
