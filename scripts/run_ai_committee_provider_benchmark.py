"""Run paid AI Investment Committee benchmarks across provider/model strings.

This runner does not store API keys. Set keys in the environment, then opt into
paid calls explicitly:

    LUMIBOT_ALLOW_PAID_AI_COMMITTEE_BACKTEST=1 \
    CEREBRAS_API_KEY=... \
    TOGETHER_API_KEY=... \
    python scripts/run_ai_committee_provider_benchmark.py --models cerebras/gpt-oss-120b
"""

import argparse
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("BACKTESTING_DATA_SOURCE", "none")
os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")
os.environ.setdefault("LUMIBOT_DISABLE_BACKTEST_PERFORMANCE_TRACKING", "1")

from lumibot.backtesting import YahooDataBacktesting
from lumibot.components.agents.manager import _usage_breakdown
from lumibot.entities import Asset, TradingFee
from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
)


ARTIFACT_ROOT = Path("artifacts") / "ai_committee_provider_benchmarks"
DEFAULT_MODELS = [
    "cerebras/gpt-oss-120b",
    "together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput",
    "openai/gpt-5.4-mini",
    "gemini-3.5-flash",
]
MODEL_PRICE_PER_M_TOKEN: dict[str, dict[str, float]] = {
    "openai/gpt-5.4-mini": {"input": 0.75, "output": 4.50, "cached_input": 0.075},
    "gemini-3.5-flash": {"input": 1.50, "output": 9.00, "cached_input": 0.15},
    "gemini-3-flash-preview": {"input": 0.50, "output": 3.00, "cached_input": 0.05},
    "gemini-3.1-flash-lite": {"input": 0.25, "output": 1.50, "cached_input": 0.025},
    "deepseek/deepseek-v4-flash": {"input": 0.14, "output": 0.28, "cached_input": 0.0028},
    "deepseek/deepseek-v4-pro": {"input": 0.435, "output": 0.87, "cached_input": 0.003625},
    "together_ai/deepseek-ai/DeepSeek-V4-Pro": {"input": 2.10, "output": 4.40, "cached_input": 0.20},
    "together_ai/moonshotai/Kimi-K2.6": {"input": 1.20, "output": 4.50, "cached_input": 0.20},
    "together_ai/moonshotai/Kimi-K2.5": {"input": 0.50, "output": 2.80},
    "together_ai/Qwen/Qwen3.6-Plus": {"input": 0.50, "output": 3.00},
    "together_ai/Qwen/Qwen3-235B-A22B-Instruct-2507-tput": {"input": 0.20, "output": 0.60},
    "together_ai/openai/gpt-oss-120b": {"input": 0.15, "output": 0.60},
    "cerebras/gpt-oss-120b": {"input": 0.25, "output": 0.69},
    "cerebras/zai-glm-4.7": {"input": 2.25, "output": 2.75},
}


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(v) for v in value]
        return repr(value)


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-zA-Z._-]+", "_", value).strip("_")[:120]


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _required_key_options(model: str) -> list[str]:
    lower = model.strip().lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        return ["GOOGLE_API_KEY", "GEMINI_API_KEY"]
    if lower.startswith("openai/"):
        return ["OPENAI_API_KEY"]
    if lower.startswith("anthropic/"):
        return ["ANTHROPIC_API_KEY"]
    if lower.startswith("xai/"):
        return ["XAI_API_KEY", "GROK_API_KEY"]
    if lower.startswith("deepseek/"):
        return ["DEEPSEEK_API_KEY"]
    if lower.startswith("together_ai/"):
        return ["TOGETHERAI_API_KEY", "TOGETHER_API_KEY"]
    if lower.startswith("cerebras/"):
        return ["CEREBRAS_API_KEY"]
    return []


def _missing_key_label(model: str) -> str | None:
    keys = _required_key_options(model)
    if not keys:
        return None
    if any(os.environ.get(key) for key in keys):
        return None
    return " or ".join(keys)


def _estimate_cost(model: str, usage: dict[str, int]) -> dict[str, Any]:
    prices = MODEL_PRICE_PER_M_TOKEN.get(model)
    if not prices:
        return {"estimated_usd": None, "price_source": "unknown"}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    cached_input_tokens = int(usage.get("cached_input_tokens") or 0)
    uncached_input_tokens = int(usage.get("uncached_input_tokens") or 0)
    if input_tokens <= 0 and (cached_input_tokens > 0 or uncached_input_tokens > 0):
        input_tokens = cached_input_tokens + uncached_input_tokens
    if uncached_input_tokens <= 0:
        uncached_input_tokens = max(input_tokens - cached_input_tokens, 0)
    no_cache = (input_tokens / 1_000_000) * prices["input"] + (output_tokens / 1_000_000) * prices["output"]
    cache_hit_rate = cached_input_tokens / input_tokens if input_tokens > 0 else None
    payload = {
        "estimated_usd": round(no_cache, 6),
        "input_per_m": prices["input"],
        "output_per_m": prices["output"],
        "price_source": "static_2026_05_24_provider_docs",
        "cache_hit_rate": round(cache_hit_rate, 6) if cache_hit_rate is not None else None,
        "uncached_input_tokens": uncached_input_tokens,
        "cached_input_tokens": cached_input_tokens,
    }
    if "cached_input" in prices:
        cache_adjusted = (
            (uncached_input_tokens / 1_000_000) * prices["input"]
            + (cached_input_tokens / 1_000_000) * prices["cached_input"]
            + (output_tokens / 1_000_000) * prices["output"]
        )
        payload["cache_adjusted_estimated_usd"] = round(cache_adjusted, 6)
        payload["cached_input_per_m"] = prices["cached_input"]
    return payload


def _usage_from_raw_traces(run_dir: Path) -> dict[str, Any]:
    trace_root = run_dir / "cache" / "agent_runtime" / "traces"
    trace_files = sorted(trace_root.glob("*/*.json")) if trace_root.exists() else []
    usage = {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cached_input_tokens": 0,
        "cache_write_input_tokens": 0,
        "uncached_input_tokens": 0,
        "thinking_tokens": 0,
        "tool_use_input_tokens": 0,
    }
    by_agent: dict[str, Any] = {}
    tool_call_count = 0
    latency_ms = 0
    parsed_trace_count = 0
    for trace_file in trace_files:
        try:
            trace = json.loads(trace_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        parsed_trace_count += 1
        agent_name = str(trace.get("agent") or trace_file.parent.name)
        trace_usage = trace.get("usage") or {}
        trace_timing = trace.get("timing") or {}
        trace_tool_calls = trace.get("tool_calls") or []
        usage_breakdown = _usage_breakdown(trace_usage, cache_hit=False)
        input_tokens = usage_breakdown["input_tokens"]
        output_tokens = usage_breakdown["output_tokens"]
        total_tokens = usage_breakdown["total_tokens"]
        cached_input_tokens = usage_breakdown["cached_input_tokens"]
        cache_write_input_tokens = usage_breakdown["cache_write_input_tokens"]
        uncached_input_tokens = usage_breakdown["uncached_input_tokens"]
        thinking_tokens = usage_breakdown["thinking_tokens"]
        tool_use_input_tokens = usage_breakdown["tool_use_input_tokens"]
        call_latency_ms = int(trace_timing.get("call_latency_ms") or 0)
        call_tool_count = len(trace_tool_calls)
        usage["input_tokens"] += input_tokens
        usage["output_tokens"] += output_tokens
        usage["total_tokens"] += total_tokens
        usage["cached_input_tokens"] += cached_input_tokens
        usage["cache_write_input_tokens"] += cache_write_input_tokens
        usage["uncached_input_tokens"] += uncached_input_tokens
        usage["thinking_tokens"] += thinking_tokens
        usage["tool_use_input_tokens"] += tool_use_input_tokens
        tool_call_count += call_tool_count
        latency_ms += call_latency_ms
        agent_payload = by_agent.setdefault(
            agent_name,
            {
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "cached_input_tokens": 0,
                "cache_write_input_tokens": 0,
                "uncached_input_tokens": 0,
                "thinking_tokens": 0,
                "tool_use_input_tokens": 0,
                "tool_call_count": 0,
                "latency_ms": 0,
            },
        )
        agent_payload["call_count"] += 1
        agent_payload["input_tokens"] += input_tokens
        agent_payload["output_tokens"] += output_tokens
        agent_payload["total_tokens"] += total_tokens
        agent_payload["cached_input_tokens"] += cached_input_tokens
        agent_payload["cache_write_input_tokens"] += cache_write_input_tokens
        agent_payload["uncached_input_tokens"] += uncached_input_tokens
        agent_payload["thinking_tokens"] += thinking_tokens
        agent_payload["tool_use_input_tokens"] += tool_use_input_tokens
        agent_payload["tool_call_count"] += call_tool_count
        agent_payload["latency_ms"] += call_latency_ms
    return {
        "detail_path": str(trace_root.resolve()) if parsed_trace_count else None,
        "usage": usage,
        "tool_call_count": tool_call_count,
        "call_count": parsed_trace_count,
        "latency_ms": latency_ms,
        "by_agent": by_agent,
        "source": "raw_traces",
        "trace_file_count": len(trace_files),
        "parsed_trace_count": parsed_trace_count,
    }


def _read_agent_detail(detail_path: Path) -> dict[str, Any]:
    if not detail_path.exists():
        return {
            "detail_path": None,
            "usage": {},
            "tool_call_count": 0,
            "call_count": 0,
            "latency_ms": 0,
            "by_agent": {},
        }
    try:
        import pandas as pd
    except ImportError:
        return {
            "detail_path": str(detail_path.resolve()),
            "usage": {},
            "tool_call_count": None,
            "call_count": None,
            "latency_ms": None,
            "warning": "pandas unavailable; could not summarize parquet",
        }
    df = pd.read_parquet(detail_path)
    summaries = df[df["is_call_summary"].astype(bool)].copy()
    if summaries.empty:
        summaries = df[df["event_kind"] == "usage"].copy()

    def total(column: str) -> int:
        if column not in summaries:
            return 0
        return int(pd.to_numeric(summaries[column], errors="coerce").fillna(0).sum())

    usage = {
        "input_tokens": total("call_input_tokens"),
        "output_tokens": total("call_output_tokens"),
        "total_tokens": total("call_total_tokens"),
        "cached_input_tokens": total("call_cached_input_tokens"),
        "cache_write_input_tokens": total("call_cache_write_input_tokens"),
        "uncached_input_tokens": total("call_uncached_input_tokens"),
        "thinking_tokens": total("call_thinking_tokens"),
        "tool_use_input_tokens": total("call_tool_use_input_tokens"),
    }
    by_agent: dict[str, Any] = {}
    if "agent_name" in summaries:
        for agent_name, group in summaries.groupby("agent_name"):
            by_agent[str(agent_name)] = {
                "call_count": int(len(group)),
                "input_tokens": int(pd.to_numeric(group.get("call_input_tokens"), errors="coerce").fillna(0).sum()),
                "output_tokens": int(pd.to_numeric(group.get("call_output_tokens"), errors="coerce").fillna(0).sum()),
                "total_tokens": int(pd.to_numeric(group.get("call_total_tokens"), errors="coerce").fillna(0).sum()),
                "cached_input_tokens": int(pd.to_numeric(group.get("call_cached_input_tokens"), errors="coerce").fillna(0).sum()),
                "cache_write_input_tokens": int(pd.to_numeric(group.get("call_cache_write_input_tokens"), errors="coerce").fillna(0).sum()),
                "uncached_input_tokens": int(pd.to_numeric(group.get("call_uncached_input_tokens"), errors="coerce").fillna(0).sum()),
                "thinking_tokens": int(pd.to_numeric(group.get("call_thinking_tokens"), errors="coerce").fillna(0).sum()),
                "tool_use_input_tokens": int(pd.to_numeric(group.get("call_tool_use_input_tokens"), errors="coerce").fillna(0).sum()),
                "tool_call_count": int(pd.to_numeric(group.get("tool_call_count"), errors="coerce").fillna(0).sum()),
                "latency_ms": int(pd.to_numeric(group.get("call_latency_ms"), errors="coerce").fillna(0).sum()),
            }
    return {
        "detail_path": str(detail_path.resolve()),
        "usage": usage,
        "tool_call_count": total("tool_call_count"),
        "call_count": int(len(summaries)),
        "latency_ms": total("call_latency_ms"),
        "by_agent": by_agent,
        "source": "stats_agent_detail",
    }


def _read_agent_usage(run_dir: Path) -> dict[str, Any]:
    trace_detail = _usage_from_raw_traces(run_dir)
    if trace_detail.get("call_count"):
        return trace_detail
    return _read_agent_detail(run_dir / "stats_agent_detail.parquet")


def _run_one_model(model: str, args: argparse.Namespace, root: Path) -> dict[str, Any]:
    run_dir = root / _slug(model)
    run_dir.mkdir(parents=True, exist_ok=True)
    print(
        json.dumps(
            {
                "event": "model_start",
                "model": model,
                "artifact_dir": str(run_dir.resolve()),
                "window": {"start": args.start, "end": args.end},
                "max_model_calls": args.max_model_calls,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    os.environ["LUMIBOT_CACHE_FOLDER"] = str(run_dir / "cache")
    os.environ["LUMIBOT_MEMORY_DIR"] = str(run_dir / "memory")
    os.environ["LUMIBOT_SEC_CACHE_DIR"] = str(run_dir / "sec")
    os.environ["LUMIBOT_FRED_CACHE_DIR"] = str(run_dir / "fred")
    os.environ["COMMITTEE_RESEARCH_MODEL"] = model
    os.environ["COMMITTEE_BULL_MODEL"] = model
    os.environ["COMMITTEE_BEAR_MODEL"] = model
    os.environ["COMMITTEE_TRADER_MODEL"] = model
    os.environ["LUMIBOT_AGENT_MAX_MODEL_CALLS"] = str(args.max_model_calls)
    os.environ["LUMIBOT_AGENT_MAX_RUN_ATTEMPTS"] = str(args.max_run_attempts)
    if args.agent_run_timeout_seconds:
        os.environ["LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS"] = str(args.agent_run_timeout_seconds)

    stats_file = run_dir / "stats.csv"
    trades_file = run_dir / "trades.csv"
    settings_file = run_dir / "settings.json"
    logfile = run_dir / "backtest.log"
    started = time.perf_counter()
    trading_fee = TradingFee(percent_fee=0.001)

    result, strategy = AITradingTeamBullBearLargeCapStocksStrategy.run_backtest(
        datasource_class=YahooDataBacktesting,
        backtesting_start=_parse_date(args.start),
        backtesting_end=_parse_date(args.end),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        budget=args.budget,
        parameters={
            "max_position_pct": args.max_position_pct,
            "max_new_positions_per_run": args.max_new_positions_per_run,
            "enable_notifications": False,
        },
        stats_file=str(stats_file),
        trades_file=str(trades_file),
        settings_file=str(settings_file),
        logfile=str(logfile),
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=True,
        show_progress_bar=False,
        quiet_logs=True,
    )
    wall_ms = int((time.perf_counter() - started) * 1000)
    detail = _read_agent_usage(run_dir)
    positions = [repr(position) for position in strategy.get_positions(include_cash_positions=True)]
    payload = {
        "model": model,
        "status": "passed",
        "artifact_dir": str(run_dir.resolve()),
        "window": {"start": args.start, "end": args.end},
        "wall_ms": wall_ms,
        "backtest_result": _json_safe(result),
        "positions": positions,
        "stats_file": str(stats_file.resolve()) if stats_file.exists() else None,
        "trades_file": str(trades_file.resolve()) if trades_file.exists() else None,
        "settings_file": str(settings_file.resolve()) if settings_file.exists() else None,
        "logfile": str(logfile.resolve()) if logfile.exists() else None,
        "agent_detail": detail,
        "cost": _estimate_cost(model, detail.get("usage") or {}),
    }
    (run_dir / "result.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "event": "model_finished",
                "model": model,
                "status": "passed",
                "wall_ms": wall_ms,
                "call_count": detail.get("call_count"),
                "tool_call_count": detail.get("tool_call_count"),
                "estimated_usd": payload["cost"].get("estimated_usd"),
                "cache_adjusted_estimated_usd": payload["cost"].get("cache_adjusted_estimated_usd"),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return payload


def _failure_payload(model: str, root: Path, args: argparse.Namespace, exc: BaseException) -> dict[str, Any]:
    run_dir = root / _slug(model)
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "model": model,
        "status": "failed",
        "artifact_dir": str(run_dir.resolve()),
        "window": {"start": args.start, "end": args.end},
        "error": {
            "type": exc.__class__.__name__,
            "message": str(exc),
            "traceback": traceback.format_exc() if sys.exc_info()[0] is not None else "",
        },
    }
    (run_dir / "result.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "event": "model_finished",
                "model": model,
                "status": "failed",
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI committee provider benchmarks with paid model calls.")
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS, help="Provider-prefixed model strings to test.")
    parser.add_argument("--start", default="2026-03-30", help="Backtest start date, YYYY-MM-DD.")
    parser.add_argument("--end", default="2026-03-31", help="Backtest end date, YYYY-MM-DD.")
    parser.add_argument("--budget", type=float, default=10000.0)
    parser.add_argument("--max-position-pct", type=float, default=0.20)
    parser.add_argument("--max-new-positions-per-run", type=int, default=2)
    parser.add_argument("--max-model-calls", type=int, default=80)
    parser.add_argument("--max-run-attempts", type=int, default=2)
    parser.add_argument(
        "--agent-run-timeout-seconds",
        type=int,
        default=None,
        help="Optional per-agent runtime timeout override for slow provider qualifiers.",
    )
    parser.add_argument("--env-file", default=".env.local", help="Local ignored env file to load before key checks.")
    parser.add_argument("--allow-missing-keys", action="store_true", help="Record failures instead of stopping on missing keys.")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue to the next model if one run fails.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file:
        env_path = Path(args.env_file)
        if not env_path.is_absolute():
            env_path = REPO_ROOT / env_path
        _load_env_file(env_path)
    if os.environ.get("LUMIBOT_ALLOW_PAID_AI_COMMITTEE_BACKTEST") != "1":
        raise RuntimeError(
            "This script makes real paid model calls. Set "
            "LUMIBOT_ALLOW_PAID_AI_COMMITTEE_BACKTEST=1 before running."
        )

    missing = {model: _missing_key_label(model) for model in args.models}
    missing = {model: label for model, label in missing.items() if label}
    if missing and not args.allow_missing_keys:
        details = "; ".join(f"{model}: {label}" for model, label in missing.items())
        raise RuntimeError(f"Missing required provider API key(s): {details}")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = ARTIFACT_ROOT / run_id
    root.mkdir(parents=True, exist_ok=True)
    results = []
    for model in args.models:
        if missing.get(model):
            exc = RuntimeError(f"Missing required provider API key(s): {missing[model]}")
            results.append(_failure_payload(model, root, args, exc))
            if not args.continue_on_error:
                break
            continue
        try:
            results.append(_run_one_model(model, args, root))
        except BaseException as exc:  # noqa: BLE001 - benchmark artifact should capture provider failures
            results.append(_failure_payload(model, root, args, exc))
            if not args.continue_on_error:
                break

    summary = {
        "run_id": run_id,
        "artifact_dir": str(root.resolve()),
        "models": args.models,
        "window": {"start": args.start, "end": args.end},
        "max_model_calls": args.max_model_calls,
        "max_run_attempts": args.max_run_attempts,
        "results": results,
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"artifact_dir": str(root.resolve()), "summary": str((root / "summary.json").resolve())}, indent=2))


if __name__ == "__main__":
    main()
