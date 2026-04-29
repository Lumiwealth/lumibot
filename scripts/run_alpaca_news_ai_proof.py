#!/usr/bin/env python3
"""Run a tiny real backtest proving Alpaca news scan + full-content retrieval.

This script makes one real AI-agent call and real Alpaca News API calls. It
verifies the resulting *_agent_detail.parquet contains the actual tool calls,
tool results, thinking rows when the provider exposes them, and per-call token
usage. It does not estimate pricing.
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


DEFAULT_MODEL = "gemini-3.1-pro-preview"


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
        REPO_ROOT / ".env.local",
        Path("/Users/robertgrzesik/Development/botspot_node/.env-local"),
        Path("/Users/robertgrzesik/Development/lumibot_strategy_tester/.env"),
        Path("/Users/robertgrzesik/Development/botspot_dreaming/.env.local"),
    ):
        _load_env_file(path)
    if not os.environ.get("GOOGLE_API_KEY") and os.environ.get("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]
    if not os.environ.get("XAI_API_KEY") and os.environ.get("GROK_API_KEY"):
        os.environ["XAI_API_KEY"] = os.environ["GROK_API_KEY"]


def _require_key_for_model(model: str) -> None:
    lower = model.lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        keys = ("GEMINI_API_KEY", "GOOGLE_API_KEY")
    elif lower.startswith("openai/"):
        keys = ("OPENAI_API_KEY",)
    elif lower.startswith("xai/"):
        keys = ("XAI_API_KEY", "GROK_API_KEY")
    elif lower.startswith("anthropic/"):
        keys = ("ANTHROPIC_API_KEY",)
    else:
        keys = ()
    if keys and not any(os.environ.get(key) for key in keys):
        raise RuntimeError(f"Missing provider API key for model {model}: expected one of {', '.join(keys)}")


def _require_alpaca_news_keys() -> None:
    has_key = bool(os.environ.get("ALPACA_API_KEY") or os.environ.get("APCA_API_KEY_ID"))
    has_secret = bool(os.environ.get("ALPACA_API_SECRET") or os.environ.get("APCA_API_SECRET_KEY"))
    if not (has_key and has_secret):
        raise RuntimeError("Missing Alpaca News API credentials: set ALPACA_API_KEY and ALPACA_API_SECRET.")


def _build_pandas_data() -> dict[Asset, Data]:
    asset = Asset("SPY", Asset.AssetType.STOCK)
    index = pd.date_range("2025-04-22 09:30", periods=3, freq="B", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [515.0, 518.0, 520.0],
            "high": [520.0, 522.0, 524.0],
            "low": [512.0, 516.0, 518.0],
            "close": [519.0, 521.0, 523.0],
            "volume": [1_000_000, 1_100_000, 1_200_000],
        },
        index=index,
    )
    return {asset: Data(asset, df, timestep="day")}


class AlpacaNewsDeepReadProofStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.vars.did_run_agent = False
        self.agents.create(
            name="trader",
            system_prompt=(
                "You are validating LumiBot's Alpaca news tool. Follow the task exactly. "
                "Do not place trades. Keep the final answer short and factual."
            ),
            default_model=os.environ["AGENT_MODEL"],
            tools=[BuiltinTools.news.alpaca_news()],
        )

    def on_trading_iteration(self):
        if self.vars.did_run_agent:
            return
        self.vars.did_run_agent = True
        self.agents["trader"].run(
            task_prompt=(
                "Call alpaca_news exactly twice for historical broad-market news. "
                "First call: symbols='SPY,QQQ,DIA,IWM', start='2025-04-21T00:00:00Z', "
                "end='2025-04-21T23:59:59Z', limit=10, include_content=False, sort='asc'. "
                "Review the timestamps and headlines. Second call: use the same symbols/start/end, "
                "limit=3, include_content=True, exclude_contentless=True, sort='asc' to fetch full article content. "
                "Do not call any other tools. Final answer must include SCAN_COUNT, FULL_ARTICLE_CHARS, "
                "FIRST_HEADLINE, CONTENT_TRUNCATED, and one sentence on whether the articles are market-relevant."
            ),
            context={"proof": "alpaca_news_scan_then_full_content"},
        )


def _json_loads(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _summarize_and_assert(detail_path: Path) -> dict[str, Any]:
    df = pd.read_parquet(detail_path)
    tool_calls = df[(df["event_kind"] == "tool_call") & (df["tool_name"] == "alpaca_news")]
    tool_results = df[(df["event_kind"] == "tool_result") & (df["tool_name"] == "alpaca_news")]
    summaries = df[df["event_kind"] == "call_summary"]

    call_args = [_json_loads(value) for value in tool_calls["event_payload_json"].tolist()]
    result_payloads = [_json_loads(value) for value in tool_results["event_payload_json"].tolist()]

    scan_calls = [args for args in call_args if args.get("include_content") is False]
    full_calls = [args for args in call_args if args.get("include_content") is True]
    full_articles = [
        article
        for payload in result_payloads
        for article in payload.get("articles", [])
        if isinstance(article, dict) and article.get("content")
    ]
    scan_payloads = [payload for payload in result_payloads if payload.get("ok") and payload.get("articles")]
    combined_scan_text = " ".join(
        f"{article.get('headline') or ''} {article.get('summary') or ''}".lower()
        for payload in scan_payloads
        for article in payload.get("articles", [])
        if isinstance(article, dict)
    )

    assert len(tool_calls) >= 2, f"expected at least 2 alpaca_news tool calls, got {len(tool_calls)}"
    assert scan_calls, "expected a scan call with include_content=False"
    assert full_calls, "expected a deep-read call with include_content=True"
    assert full_articles, "expected at least one full-content article"
    assert max(len(str(article.get("content") or "")) for article in full_articles) > 1000
    assert all(article.get("content_truncated") is False for article in full_articles)
    assert any(keyword in combined_scan_text for keyword in ("fed", "dollar", "market", "stocks", "treasury", "tariff", "volatility"))

    return {
        "agent_detail_parquet": str(detail_path),
        "rows": int(len(df)),
        "event_kinds": sorted(str(item) for item in df["event_kind"].dropna().unique().tolist()),
        "alpaca_news_tool_calls": int(len(tool_calls)),
        "scan_calls": len(scan_calls),
        "full_content_calls": len(full_calls),
        "full_content_articles": len(full_articles),
        "max_full_article_chars": max(len(str(article.get("content") or "")) for article in full_articles),
        "thinking_rows": int((df["event_kind"] == "thinking").sum()),
        "input_tokens": int(pd.to_numeric(summaries["call_input_tokens"], errors="coerce").fillna(0).sum()),
        "output_tokens": int(pd.to_numeric(summaries["call_output_tokens"], errors="coerce").fillna(0).sum()),
        "thinking_tokens": int(pd.to_numeric(summaries["call_thinking_tokens"], errors="coerce").fillna(0).sum()),
        "cached_input_tokens": int(pd.to_numeric(summaries["call_cached_input_tokens"], errors="coerce").fillna(0).sum()),
        "latency_ms": int(pd.to_numeric(summaries["call_latency_ms"], errors="coerce").fillna(0).sum()),
        "sample_full_headline": str(full_articles[0].get("headline") or ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default=os.environ.get("AGENT_MODEL", DEFAULT_MODEL))
    args = parser.parse_args()

    _load_known_env_files()
    _require_alpaca_news_keys()
    _require_key_for_model(args.model)

    os.environ["AGENT_MODEL"] = args.model
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(Path("/tmp") / f"lumibot_alpaca_news_ai_proof_{int(time.time() * 1000)}")
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"

    started = time.time()
    try:
        _, strategy = AlpacaNewsDeepReadProofStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=datetime(2025, 4, 22),
            backtesting_end=datetime(2025, 4, 25),
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
    except Exception as exc:
        error_text = str(exc)
        lower_error = error_text.lower()
        if "available credits" in lower_error or "spending limit" in lower_error or "out of credits" in lower_error:
            print(json.dumps({
                "model": args.model,
                "status": "blocked",
                "reason": "provider billing or spending limit",
                "wall_seconds": round(time.time() - started, 2),
                "error_type": exc.__class__.__name__,
                "error": error_text[:800],
            }, indent=2, sort_keys=True))
            return 2
        raise
    detail_path = Path(strategy.parameters["agent_trader_detail_parquet"])
    summary = _summarize_and_assert(detail_path)
    summary["model"] = args.model
    summary["wall_seconds"] = round(time.time() - started, 2)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
