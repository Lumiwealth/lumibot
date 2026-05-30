"""Run paid README-window backtests for the AI trading team examples."""

import argparse
import json
import os
import re
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
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
from lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated import (
    AITradingTeamBillAckmanConcentratedStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
    AITradingTeamBullBearLargeCapStocksStrategy,
)
from lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf import (
    AITradingTeamBullBearLeveragedETFStrategy,
)
from lumibot.example_strategies.ai_trading_team_citadel_sector_pods import (
    AITradingTeamCitadelSectorPodsStrategy,
)
from lumibot.example_strategies.ai_trading_team_ray_dalio_idea_meritocracy import (
    AITradingTeamRayDalioIdeaMeritocracyStrategy,
)
from lumibot.example_strategies.ai_trading_team_warren_buffett_value import (
    AITradingTeamWarrenBuffettValueStrategy,
)


ARTIFACT_ROOT = Path("artifacts") / "ai_trading_team_example_benchmarks"
STRATEGIES = {
    "bull-bear-leveraged-etf": AITradingTeamBullBearLeveragedETFStrategy,
    "bull-bear-large-cap-stocks": AITradingTeamBullBearLargeCapStocksStrategy,
    "ray-dalio-idea-meritocracy": AITradingTeamRayDalioIdeaMeritocracyStrategy,
    "warren-buffett-value": AITradingTeamWarrenBuffettValueStrategy,
    "bill-ackman-concentrated": AITradingTeamBillAckmanConcentratedStrategy,
    "citadel-sector-pods": AITradingTeamCitadelSectorPodsStrategy,
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


def _slug(value: str) -> str:
    return re.sub(r"[^0-9a-zA-Z._-]+", "_", value).strip("_")[:120]


def _run_one_strategy(name: str, args: argparse.Namespace, root: str) -> dict[str, Any]:
    strategy_class = STRATEGIES[name]
    run_dir = Path(root) / _slug(name)
    run_dir.mkdir(parents=True, exist_ok=True)

    os.environ["BACKTESTING_DATA_SOURCE"] = "none"
    os.environ["LUMIBOT_DISABLE_DOTENV"] = "1"
    os.environ["LUMIBOT_DISABLE_BACKTEST_PERFORMANCE_TRACKING"] = "1"
    os.environ["LUMIBOT_CACHE_FOLDER"] = str(run_dir / "cache")
    os.environ["LUMIBOT_MEMORY_DIR"] = str(run_dir / "memory")
    os.environ["LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS"] = str(args.agent_run_timeout_seconds)
    os.environ["LUMIBOT_AGENT_MAX_RUN_ATTEMPTS"] = str(args.max_run_attempts)

    stats_file = run_dir / "stats.csv"
    trades_file = run_dir / "trades.csv"
    settings_file = run_dir / "settings.json"
    logfile = run_dir / "backtest.log"
    started = time.perf_counter()
    try:
        result, strategy = strategy_class.run_backtest(
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
        payload = {
            "strategy": name,
            "status": "passed",
            "artifact_dir": str(run_dir.resolve()),
            "window": {"start": args.start, "end": args.end},
            "wall_ms": int((time.perf_counter() - started) * 1000),
            "backtest_result": _json_safe(result),
            "positions": [repr(position) for position in strategy.get_positions(include_cash_positions=True)],
            "stats_file": str(stats_file.resolve()) if stats_file.exists() else None,
            "trades_file": str(trades_file.resolve()) if trades_file.exists() else None,
            "settings_file": str(settings_file.resolve()) if settings_file.exists() else None,
            "logfile": str(logfile.resolve()) if logfile.exists() else None,
        }
    except Exception as exc:
        payload = {
            "strategy": name,
            "status": "failed",
            "artifact_dir": str(run_dir.resolve()),
            "window": {"start": args.start, "end": args.end},
            "wall_ms": int((time.perf_counter() - started) * 1000),
            "error": repr(exc),
            "traceback": traceback.format_exc(),
            "logfile": str(logfile.resolve()) if logfile.exists() else None,
        }
    (run_dir / "result.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-04-07")
    parser.add_argument("--end", default="2026-05-22")
    parser.add_argument("--budget", type=float, default=100000)
    parser.add_argument("--env-file", default=".env.local")
    parser.add_argument("--max-workers", type=int, default=2)
    parser.add_argument("--max-run-attempts", type=int, default=3)
    parser.add_argument("--agent-run-timeout-seconds", type=int, default=1800)
    parser.add_argument("--strategy", action="append", choices=sorted(STRATEGIES))
    args = parser.parse_args()

    _load_env_file(Path(args.env_file))
    if not os.environ.get("GOOGLE_API_KEY") and not os.environ.get("GEMINI_API_KEY"):
        raise RuntimeError("GOOGLE_API_KEY or GEMINI_API_KEY is required for these examples.")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    root = ARTIFACT_ROOT / run_id
    root.mkdir(parents=True, exist_ok=True)
    strategy_names = args.strategy or list(STRATEGIES)
    results = []
    with ProcessPoolExecutor(max_workers=min(args.max_workers, len(strategy_names))) as executor:
        futures = {executor.submit(_run_one_strategy, name, args, str(root)): name for name in strategy_names}
        for future in as_completed(futures):
            payload = future.result()
            results.append(payload)
            print(json.dumps({k: payload.get(k) for k in ("strategy", "status", "wall_ms", "artifact_dir")}, sort_keys=True), flush=True)

    summary = {
        "artifact_dir": str(root.resolve()),
        "window": {"start": args.start, "end": args.end},
        "results": sorted(results, key=lambda item: item["strategy"]),
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"artifact_dir": str(root.resolve()), "summary": str((root / "summary.json").resolve())}, indent=2), flush=True)


if __name__ == "__main__":
    main()
