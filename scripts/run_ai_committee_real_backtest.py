"""Run the AI Investment Committee example with real model calls.

This contributor smoke runner writes a stable artifact folder containing the
committee text, agent traces, memory files, and backtest outputs. It expects the
relevant model provider key, usually ``OPENAI_API_KEY``, to already be present in
the environment.

Usage:
    OPENAI_API_KEY=... python scripts/run_ai_committee_real_backtest.py
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("BACKTESTING_DATA_SOURCE", "none")
os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")
os.environ.setdefault("LUMIBOT_DISABLE_BACKTEST_PERFORMANCE_TRACKING", "1")

from lumibot.backtesting import YahooDataBacktesting
from lumibot.entities import Asset, TradingFee
from lumibot.example_strategies.ai_investment_committee import AIInvestmentCommitteeStrategy


ARTIFACT_ROOT = Path("artifacts") / "ai_committee_real_backtests"


def _json_safe(value):
    try:
        json.dumps(value)
        return value
    except TypeError:
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(v) for v in value]
        return repr(value)


def main() -> None:
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is required for the default OpenAI committee models.")

    os.environ["BACKTESTING_DATA_SOURCE"] = "none"
    os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")
    os.environ.setdefault("LUMIBOT_DISABLE_BACKTEST_PERFORMANCE_TRACKING", "1")
    os.environ.setdefault("COMMITTEE_RESEARCH_MODEL", "openai/gpt-5.4-mini")
    os.environ.setdefault("COMMITTEE_BULL_MODEL", "openai/gpt-5.5")
    os.environ.setdefault("COMMITTEE_BEAR_MODEL", "openai/gpt-5.5")
    os.environ.setdefault("COMMITTEE_TRADER_MODEL", "openai/gpt-5.5")

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = ARTIFACT_ROOT / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    os.environ["LUMIBOT_CACHE_FOLDER"] = str(artifact_dir / "cache")
    os.environ["LUMIBOT_MEMORY_DIR"] = str(artifact_dir / "memory")
    os.environ["LUMIBOT_SEC_CACHE_DIR"] = str(artifact_dir / "sec")

    stats_file = artifact_dir / "stats.csv"
    trades_file = artifact_dir / "trades.csv"
    settings_file = artifact_dir / "settings.json"
    logfile = artifact_dir / "backtest.log"

    trading_fee = TradingFee(percent_fee=0.001)
    result, strategy = AIInvestmentCommitteeStrategy.run_backtest(
        datasource_class=YahooDataBacktesting,
        backtesting_start=datetime(2023, 1, 3),
        backtesting_end=datetime(2024, 1, 10),
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        budget=10000,
        parameters={
            "universe": ["AAPL"],
            "max_position_pct": 0.20,
            "max_new_positions_per_run": 1,
            "committee_start_on_or_after": "2024-01-08",
            "run_once": True,
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

    positions = [repr(position) for position in strategy.get_positions(include_cash_positions=True)]
    summary = {
        "run_id": run_id,
        "artifact_dir": str(artifact_dir.resolve()),
        "models": {
            "research": os.environ["COMMITTEE_RESEARCH_MODEL"],
            "bull": os.environ["COMMITTEE_BULL_MODEL"],
            "bear": os.environ["COMMITTEE_BEAR_MODEL"],
            "portfolio_manager": os.environ["COMMITTEE_TRADER_MODEL"],
        },
        "backtest_result": _json_safe(result),
        "last_evidence_pack": strategy.vars.get("last_evidence_pack"),
        "last_bull_case": strategy.vars.get("last_bull_case"),
        "last_bear_case": strategy.vars.get("last_bear_case"),
        "positions": positions,
        "stats_file": str(stats_file.resolve()) if stats_file.exists() else None,
        "trades_file": str(trades_file.resolve()) if trades_file.exists() else None,
        "settings_file": str(settings_file.resolve()) if settings_file.exists() else None,
        "logfile": str(logfile.resolve()) if logfile.exists() else None,
    }

    result_path = artifact_dir / "result.json"
    result_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    readme = [
        "# AI Investment Committee Real-Model Backtest",
        "",
        f"- Run ID: `{run_id}`",
        f"- Artifact folder: `{artifact_dir.resolve()}`",
        f"- Result JSON: `{result_path.resolve()}`",
        f"- Stats CSV: `{summary['stats_file']}`",
        f"- Trades CSV: `{summary['trades_file']}`",
        f"- Log file: `{summary['logfile']}`",
        "",
        "## Models",
        "",
        *[f"- {name}: `{model}`" for name, model in summary["models"].items()],
        "",
        "## Final Positions",
        "",
        *[f"- `{position}`" for position in positions],
    ]
    (artifact_dir / "README.md").write_text("\n".join(readme), encoding="utf-8")

    print(json.dumps({"artifact_dir": str(artifact_dir.resolve()), "result": str(result_path.resolve())}, indent=2))


if __name__ == "__main__":
    main()
