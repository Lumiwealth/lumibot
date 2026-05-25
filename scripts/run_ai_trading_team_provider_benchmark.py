"""Run paid AI Trading Team benchmarks across provider/model strings.

Set provider keys in an ignored env file or the shell, then opt into paid calls:

    LUMIBOT_ALLOW_PAID_AI_TRADING_TEAM_BACKTEST=1 \
    python scripts/run_ai_trading_team_provider_benchmark.py --models gemini-3.1-flash-lite
"""

import argparse
import json
import os
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
from lumibot.entities import Asset
from lumibot.example_strategies.ai_trading_team import AITradingTeamStrategy
from scripts.run_ai_committee_provider_benchmark import (
    _estimate_cost,
    _json_safe,
    _load_env_file,
    _missing_key_label,
    _parse_date,
    _read_agent_usage,
    _slug,
)


ARTIFACT_ROOT = Path("artifacts") / "ai_trading_team_provider_benchmarks"
DEFAULT_MODELS = [
    "deepseek/deepseek-v4-flash",
    "deepseek/deepseek-v4-pro",
    "gemini-3.1-flash-lite",
]


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
            },
            sort_keys=True,
        ),
        flush=True,
    )

    previous_model = os.environ.get("AI_TRADING_TEAM_MODEL")
    os.environ["AI_TRADING_TEAM_MODEL"] = model
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(run_dir / "cache")
    os.environ["LUMIBOT_MEMORY_DIR"] = str(run_dir / "memory")
    if args.max_run_attempts is not None:
        os.environ["LUMIBOT_AGENT_MAX_RUN_ATTEMPTS"] = str(args.max_run_attempts)
    if args.agent_run_timeout_seconds:
        os.environ["LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS"] = str(args.agent_run_timeout_seconds)

    stats_file = run_dir / "stats.csv"
    trades_file = run_dir / "trades.csv"
    settings_file = run_dir / "settings.json"
    logfile = run_dir / "backtest.log"
    started = time.perf_counter()
    try:
        result, strategy = AITradingTeamStrategy.run_backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=_parse_date(args.start),
            backtesting_end=_parse_date(args.end),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=args.budget,
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
    finally:
        if previous_model is None:
            os.environ.pop("AI_TRADING_TEAM_MODEL", None)
        else:
            os.environ["AI_TRADING_TEAM_MODEL"] = previous_model

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
    parser = argparse.ArgumentParser(description="Run AI trading team provider benchmarks with paid model calls.")
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS, help="Provider-prefixed model strings to test.")
    parser.add_argument("--start", default="2026-05-21", help="Backtest start date, YYYY-MM-DD.")
    parser.add_argument("--end", default="2026-05-22", help="Backtest end date, YYYY-MM-DD.")
    parser.add_argument("--budget", type=float, default=100000.0)
    parser.add_argument("--max-run-attempts", type=int, default=2)
    parser.add_argument(
        "--agent-run-timeout-seconds",
        type=int,
        default=1800,
        help="Per-agent runtime timeout for slow providers.",
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
    if os.environ.get("LUMIBOT_ALLOW_PAID_AI_TRADING_TEAM_BACKTEST") != "1":
        raise RuntimeError(
            "This script makes real paid model calls. Set "
            "LUMIBOT_ALLOW_PAID_AI_TRADING_TEAM_BACKTEST=1 before running."
        )

    missing = {model: _missing_key_label(model) for model in args.models}
    missing = {model: label for model, label in missing.items() if label}
    if missing and not args.allow_missing_keys:
        details = "; ".join(f"{model}: {label}" for model, label in missing.items())
        raise RuntimeError(f"Missing required provider API key(s): {details}")

    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{os.getpid()}"
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
        "max_run_attempts": args.max_run_attempts,
        "results": results,
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"artifact_dir": str(root.resolve()), "summary": str((root / "summary.json").resolve())}, indent=2))


if __name__ == "__main__":
    main()
