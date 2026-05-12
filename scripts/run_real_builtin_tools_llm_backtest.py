#!/usr/bin/env python3
"""Run a real LLM LumiBot backtest against the built-in agent tool surface.

This is intentionally a paid-provider validation harness. It uses the normal
Google ADK runtime, creates an agent without passing explicit tools, and then
audits the generated agent_detail parquet for actual tool calls, look-ahead
warnings, and provider usage metadata.
"""

from __future__ import annotations

import argparse
import json
import math
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
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


REQUIRED_TOOL_SURFACE = {
    "account_positions",
    "account_portfolio",
    "market_last_price",
    "market_load_history_table",
    "duckdb_query",
    "alpaca_news",
    "list_indicators",
    "get_indicator",
    "get_indicators",
    "get_income_statement",
    "get_balance_sheet",
    "get_cash_flow",
    "get_company_facts",
    "get_filings",
    "search_filing",
    "get_filing_document",
    "list_fred_series",
    "get_fred_series",
    "get_fred_latest",
    "get_fred_snapshot",
}

REQUIRED_CALL_GROUPS = {
    "account": {"account_positions", "account_portfolio"},
    "market": {"market_last_price", "market_load_history_table", "duckdb_query"},
    "indicators": {"list_indicators", "get_indicator", "get_indicators"},
    "news": {"alpaca_news"},
    "fred": {"list_fred_series", "get_fred_series", "get_fred_latest", "get_fred_snapshot"},
    "sec_financials": {"get_income_statement", "get_balance_sheet", "get_cash_flow", "get_company_facts"},
    "sec_filings": {"get_filings", "search_filing", "get_filing_document"},
}


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


def _load_known_env() -> None:
    for path in (
        REPO_ROOT / ".env.local",
        REPO_ROOT / ".env",
        Path("/Users/robertgrzesik/Development/botspot_node/.env-local"),
        Path("/Users/robertgrzesik/Development/botspot_agent/.env"),
    ):
        _load_env_file(path)
    if os.environ.get("GEMINI_API_KEY") and not os.environ.get("GOOGLE_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]


def _require_provider_key(model: str) -> None:
    lower = model.strip().lower()
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
        raise RuntimeError(f"Missing provider key for {model}: expected one of {', '.join(keys)}")


def _require_data_keys() -> None:
    missing = []
    if not os.environ.get("FRED_API_KEY"):
        missing.append("FRED_API_KEY")
    if not os.environ.get("ALPACA_NEWS_API_KEY"):
        missing.append("ALPACA_NEWS_API_KEY")
    if not os.environ.get("ALPACA_NEWS_API_SECRET"):
        missing.append("ALPACA_NEWS_API_SECRET")
    if missing:
        raise RuntimeError(f"Missing required data credentials: {', '.join(missing)}")


def _make_bars(symbol: str, closes: list[float]) -> tuple[Asset, Data]:
    asset = Asset(symbol, Asset.AssetType.STOCK)
    index = pd.date_range("2025-02-03 09:30", periods=len(closes), freq="B", tz="America/New_York")
    df = pd.DataFrame(
        {
            "open": [price * 0.995 for price in closes],
            "high": [price * 1.01 for price in closes],
            "low": [price * 0.99 for price in closes],
            "close": closes,
            "volume": [1_000_000 + idx * 25_000 for idx in range(len(closes))],
        },
        index=index,
    )
    return asset, Data(asset, df, timestep="day")


def _build_pandas_data() -> dict[Asset, Data]:
    def synthetic_closes(base: float, drift: float, amplitude: float) -> list[float]:
        return [
            round(base + drift * idx + amplitude * math.sin(idx / 4.0) + ((idx % 5) - 2) * amplitude * 0.12, 2)
            for idx in range(80)
        ]

    series = {
        "SPY": synthetic_closes(485, 0.72, 5.0),
        "QQQ": synthetic_closes(410, 0.95, 7.0),
        "AAPL": synthetic_closes(155, 0.38, 3.0),
        "NVDA": synthetic_closes(780, 4.60, 28.0),
    }
    return dict(_make_bars(symbol, closes) for symbol, closes in series.items())


class RealBuiltinToolsLLMValidationStrategy(Strategy):
    parameters = {
        "agent_max_model_calls": 1,
    }

    def initialize(self) -> None:
        self.sleeptime = "1D"
        self.vars.did_run = False
        self.agents.create(
            name="validator",
            model=os.environ["VALIDATION_AGENT_MODEL"],
            system_prompt="Make as much money as you possibly can.",
            # Deliberately no explicit tools. This validates built-in auto-inclusion.
        )

    def on_trading_iteration(self) -> None:
        if self.vars.did_run:
            return
        self.vars.did_run = True
        if os.environ.get("VALIDATION_TASK_MODE", "guided") == "autonomous":
            result = self.agents["validator"].run()
        else:
            result = self.agents["validator"].run(
                task_prompt=(
                    "This is a real validation run for the built-in LumiBot tools during a daily backtest. Make one "
                    "realistic trading decision, but first you must call each evidence category below. Use these exact "
                    "tool names so the run can be audited: account_positions, account_portfolio, market_last_price, "
                    "market_load_history_table, duckdb_query, list_indicators, get_indicator or get_indicators, "
                    "alpaca_news, list_fred_series, get_fred_series or get_fred_latest or get_fred_snapshot, "
                    "get_income_statement, get_balance_sheet or get_cash_flow, get_company_facts, get_filings, and "
                    "get_filing_document. For DuckDB, call market_load_history_table with symbol='SPY', length=30, "
                    "timestep='day', table_name='validation_prices', then query that exact validation_prices table. "
                    "For SEC filings, call get_filing_document with symbol='AAPL', accession_number='0000320193-25-000008', "
                    "primary_document='aapl-20241228.htm', max_chars=2000. Use SPY and QQQ for market/indicator/news/macro context; "
                    "use AAPL or NVDA for SEC financial statements and SEC filings because ETFs may not have useful "
                    "company filings. Keep this bounded: use short limits, do not place trades during this validation "
                    "run, and do not call unrelated tools. In backtesting, every tool call with a date or time range "
                    "must end at or before the current simulated datetime. Finish with RESULT: and state which evidence "
                    "categories you used."
                ),
                context={"symbols": ["SPY", "QQQ", "AAPL", "NVDA"], "validation": "builtins_auto_included"},
            )
        self.log_message(f"[validator] {result.summary}", color="yellow")


def _json_loads(value: Any) -> dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_runtime_context(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not text:
        return result
    for part in text.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def _extract_tool_surface(summary_payload: dict[str, Any]) -> set[str]:
    request = summary_payload.get("request")
    if not isinstance(request, dict):
        return set()
    surface = request.get("tool_surface")
    if not isinstance(surface, list):
        return set()
    return {str(tool.get("name")) for tool in surface if isinstance(tool, dict) and tool.get("name")}


def _result_payloads(df: pd.DataFrame, tool_name: str) -> list[dict[str, Any]]:
    rows = df[(df["event_kind"] == "tool_result") & (df["tool_name"] == tool_name)]
    return [_json_loads(value) for value in rows["event_payload_json"].tolist()]


def _timestamp_after(left: Any, right: Any) -> bool:
    if not left or not right:
        return False
    try:
        left_ts = pd.Timestamp(str(left))
        right_ts = pd.Timestamp(str(right))
    except Exception:
        return False
    if left_ts.tzinfo is None and right_ts.tzinfo is not None:
        left_ts = left_ts.tz_localize(right_ts.tzinfo)
    if right_ts.tzinfo is None and left_ts.tzinfo is not None:
        right_ts = right_ts.tz_localize(left_ts.tzinfo)
    return bool(left_ts > right_ts)


def _audit_detail(detail_path: Path, *, strict_calls: bool, expected_tool_surface: set[str] | None = None) -> dict[str, Any]:
    df = pd.read_parquet(detail_path)
    summaries = df[df["event_kind"] == "call_summary"]
    if summaries.empty:
        raise AssertionError("No call_summary row found in agent detail parquet.")
    summary_payload = _json_loads(summaries.iloc[-1]["event_payload_json"])
    tool_surface = set(expected_tool_surface or set()) or _extract_tool_surface(summary_payload)
    missing_surface = sorted(REQUIRED_TOOL_SURFACE - tool_surface)

    calls = df[df["event_kind"] == "tool_call"]
    tool_calls = [str(name) for name in calls["tool_name"].dropna().tolist() if str(name)]
    called = set(tool_calls)
    missing_call_groups = {
        group: sorted(names)
        for group, names in REQUIRED_CALL_GROUPS.items()
        if not called.intersection(names)
    }

    warnings = str(summaries.iloc[-1].get("warning_messages") or "")
    if "future timestamp" in warnings.lower() or "after simulated time" in warnings.lower():
        raise AssertionError(f"Future timestamp warning found: {warnings}")
    if "returned an error" in warnings.lower() or "tool_error" in warnings.lower():
        raise AssertionError(f"Tool error warning found: {warnings}")

    tool_error_rows = []
    for _, row in df[df["event_kind"] == "tool_result"].iterrows():
        payload = _json_loads(row.get("event_payload_json"))
        if payload.get("tool_error") is True or payload.get("ok") is False:
            tool_error_rows.append(
                {
                    "tool_name": str(row.get("tool_name") or ""),
                    "event_detail": str(row.get("event_detail") or ""),
                    "payload": payload,
                }
            )
    if tool_error_rows:
        raise AssertionError(f"Tool result errors found: {json.dumps(tool_error_rows, default=str)[:4000]}")

    runtime_context = _parse_runtime_context(str(summaries.iloc[-1].get("runtime_context_text") or ""))
    simulated_time = runtime_context.get("current_datetime", "")

    news_payloads = _result_payloads(df, "alpaca_news")
    news_checks = []
    for payload in news_payloads:
        if not payload:
            continue
        news_checks.append(
            {
                "ok": payload.get("ok"),
                "count": payload.get("count"),
                "requested_end": payload.get("requested_end"),
                "effective_end": payload.get("effective_end"),
                "lookahead_clamped": payload.get("lookahead_clamped"),
                "credential_source": payload.get("credential_source"),
                "max_created_at": max(
                    [str(article.get("created_at") or "") for article in payload.get("articles", []) if isinstance(article, dict)]
                    or [""]
                ),
            }
        )
        effective_end = news_checks[-1]["effective_end"]
        if news_checks[-1]["max_created_at"] and effective_end and _timestamp_after(news_checks[-1]["max_created_at"], effective_end):
            raise AssertionError(f"Alpaca news returned article after effective_end: {news_checks[-1]}")

    fred_payloads = []
    for name in ("get_fred_series", "get_fred_latest", "get_fred_snapshot"):
        for payload in _result_payloads(df, name):
            if payload:
                fred_payloads.append({"tool": name, "payload": payload})
    fred_checks = []
    for item in fred_payloads:
        payload = item["payload"]
        if item["tool"] == "get_fred_snapshot":
            values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
            fred_checks.append(
                {
                    "tool": item["tool"],
                    "source": payload.get("source"),
                    "as_of": payload.get("as_of"),
                    "series": sorted(values.keys()),
                    "values": values,
                }
            )
            as_of = payload.get("as_of")
            for series_id, value in values.items():
                if isinstance(value, dict):
                    realtime_start = value.get("realtime_start")
                    realtime_end = value.get("realtime_end")
                    if _timestamp_after(realtime_start, as_of) or _timestamp_after(realtime_end, as_of):
                        raise AssertionError(f"FRED value after as_of for {series_id}: {value}")
        else:
            fred_checks.append(
                {
                    "tool": item["tool"],
                    "source": payload.get("source"),
                    "point_in_time_safe": payload.get("point_in_time_safe"),
                    "uses_revised_data": payload.get("uses_revised_data"),
                    "as_of": payload.get("as_of"),
                    "latest": payload.get("latest"),
                    "count": len(payload.get("observations") or []),
                }
            )

    usage_totals = {
        "input_tokens": int(pd.to_numeric(summaries["call_input_tokens"], errors="coerce").fillna(0).sum()),
        "output_tokens": int(pd.to_numeric(summaries["call_output_tokens"], errors="coerce").fillna(0).sum()),
        "total_tokens": int(pd.to_numeric(summaries["call_total_tokens"], errors="coerce").fillna(0).sum()),
        "thinking_tokens": int(pd.to_numeric(summaries["call_thinking_tokens"], errors="coerce").fillna(0).sum()),
        "cached_input_tokens": int(pd.to_numeric(summaries["call_cached_input_tokens"], errors="coerce").fillna(0).sum()),
        "uncached_input_tokens": int(pd.to_numeric(summaries["call_uncached_input_tokens"], errors="coerce").fillna(0).sum()),
        "tool_use_input_tokens": int(pd.to_numeric(summaries["call_tool_use_input_tokens"], errors="coerce").fillna(0).sum()),
    }
    if usage_totals["total_tokens"] <= 0:
        raise AssertionError(f"Provider usage totals are zero: {usage_totals}")
    if missing_surface:
        raise AssertionError(f"Built-in tools missing from auto-included surface: {missing_surface}")
    if strict_calls and missing_call_groups:
        raise AssertionError(f"Required tool call groups were not used: {missing_call_groups}; tool_calls={tool_calls}")

    return {
        "agent_detail_parquet": str(detail_path),
        "rows": int(len(df)),
        "model": str(summaries.iloc[-1]["model"]),
        "mode": str(summaries.iloc[-1]["mode"]),
        "simulated_time": simulated_time,
        "tool_surface_count": len(tool_surface),
        "required_tool_surface_present": True,
        "tool_calls": tool_calls,
        "tool_call_count": len(tool_calls),
        "missing_call_groups": missing_call_groups,
        "warnings": warnings,
        "usage": usage_totals,
        "news_checks": news_checks,
        "fred_checks": fred_checks,
        "trace_paths": sorted(set(str(path) for path in summaries["trace_path"].dropna().tolist() if str(path))),
        "effective_system_prompt_excerpt": str(summaries.iloc[-1].get("effective_system_prompt") or "")[:2400],
        "final_text": str(summaries.iloc[-1].get("final_text") or "")[:2400],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--task-mode", choices=("autonomous", "guided"), default="guided")
    parser.add_argument("--start", default="2025-04-07")
    parser.add_argument("--end", default="2025-05-07")
    parser.add_argument("--strict-calls", action="store_true")
    args = parser.parse_args()

    _load_known_env()
    _require_provider_key(args.model)
    _require_data_keys()

    run_id = f"{int(time.time() * 1000)}_{args.model.replace('/', '_').replace(':', '_')}_{args.task_mode}"
    os.environ["VALIDATION_AGENT_MODEL"] = args.model
    os.environ["VALIDATION_TASK_MODE"] = args.task_mode
    os.environ["LUMIBOT_AGENT_MAX_MODEL_CALLS"] = "1"
    os.environ["LUMIBOT_AGENT_MAX_RUN_ATTEMPTS"] = "1"
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(REPO_ROOT / "artifacts" / "real_builtin_tools_llm" / run_id / "cache")
    os.environ["LUMIBOT_MEMORY_DIR"] = str(REPO_ROOT / "artifacts" / "real_builtin_tools_llm" / run_id / "memory")
    os.environ["LUMIBOT_SEC_CACHE_DIR"] = str(REPO_ROOT / "artifacts" / "real_builtin_tools_llm" / run_id / "sec")
    os.environ["LUMIBOT_FRED_CACHE_DIR"] = str(REPO_ROOT / "artifacts" / "real_builtin_tools_llm" / run_id / "fred")
    output_dir = REPO_ROOT / "artifacts" / "real_builtin_tools_llm" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    _, strategy = RealBuiltinToolsLLMValidationStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime.fromisoformat(args.start),
        backtesting_end=datetime.fromisoformat(args.end),
        pandas_data=_build_pandas_data(),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        show_progress_bar=False,
        quiet_logs=True,
        budget=10_000,
    )
    detail_path = Path(strategy.parameters["agent_validator_detail_parquet"])
    expected_tool_surface = {tool.name for tool in strategy.agents["validator"]._ensure_bound_tools()}
    audit = _audit_detail(detail_path, strict_calls=args.strict_calls, expected_tool_surface=expected_tool_surface)
    audit.update(
        {
            "status": "passed",
            "task_mode": args.task_mode,
            "wall_seconds": round(time.time() - started, 2),
            "output_dir": str(output_dir),
            "agent_model_calls": strategy.parameters.get("agent_model_calls"),
        }
    )
    (output_dir / "audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(audit, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
