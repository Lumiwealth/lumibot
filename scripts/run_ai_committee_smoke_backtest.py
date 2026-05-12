"""Run an AI committee tool-smoke backtest and write inspectable artifacts.

This is a contributor smoke runner, not a unit test. It executes the same
stubbed committee runtime used by the integration test so every built-in tool is
exercised deterministically without spending LLM tokens.

Usage:
    python scripts/run_ai_committee_smoke_backtest.py
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tests.backtest.test_ai_committee_builtin_tools_backtest import (
    CommitteeToolBacktestStrategy,
    CommitteeToolRuntime,
    _build_committee_pandas_data,
    _fake_requests_get,
    _fake_telegram_post,
)


ARTIFACT_ROOT = Path("artifacts") / "ai_committee_backtests"


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
    os.environ.setdefault("LUMIBOT_DISABLE_BACKTEST_PERFORMANCE_TRACKING", "1")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = ARTIFACT_ROOT / run_id
    cache_dir = artifact_dir / "cache"
    memory_dir = artifact_dir / "memory"
    sec_dir = artifact_dir / "sec"
    fred_dir = artifact_dir / "fred"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    os.environ["LUMIBOT_CACHE_FOLDER"] = str(cache_dir)
    os.environ["LUMIBOT_MEMORY_DIR"] = str(memory_dir)
    os.environ["LUMIBOT_SEC_CACHE_DIR"] = str(sec_dir)
    os.environ["LUMIBOT_FRED_CACHE_DIR"] = str(fred_dir)
    os.environ["ALPACA_NEWS_API_KEY"] = "fixture-key"
    os.environ["ALPACA_NEWS_API_SECRET"] = "fixture-secret"
    os.environ["FRED_API_KEY"] = "fixture-fred-key"

    CommitteeToolRuntime.call_count = 0
    CommitteeToolRuntime.last_results = None

    with (
        patch("lumibot.fundamentals.sec.requests.get", _fake_requests_get),
        patch("lumibot.components.agents.builtins.requests.get", _fake_requests_get),
        patch("lumibot.macro.fred.requests.get", _fake_requests_get),
        patch("lumibot.components.notifications.telegram.requests.post", _fake_telegram_post),
    ):
        result, strategy = CommitteeToolBacktestStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=datetime(2025, 1, 6, 9, 30),
            backtesting_end=datetime(2025, 1, 6, 9, 37),
            pandas_data=_build_committee_pandas_data(),
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

    position = strategy.get_position(Asset("AAPL", Asset.AssetType.STOCK))
    memory_files = sorted(str(path) for path in memory_dir.rglob("*.jsonl"))
    summary_file = cache_dir / "agent_runtime" / "agent_run_summaries.jsonl"

    payload = {
        "run_id": run_id,
        "artifact_dir": str(artifact_dir.resolve()),
        "backtest_result": _json_safe(result),
        "committee_call_count": CommitteeToolRuntime.call_count,
        "committee_summary": strategy.vars.committee_summary,
        "position": repr(position),
        "results": _json_safe(CommitteeToolRuntime.last_results),
        "memory_files": memory_files,
        "summary_file": str(summary_file.resolve()) if summary_file.exists() else None,
    }

    json_path = artifact_dir / "result.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    markdown = [
        "# AI Committee Tool-Smoke Backtest",
        "",
        f"- Run ID: `{run_id}`",
        f"- Committee calls: `{CommitteeToolRuntime.call_count}`",
        f"- Summary: {strategy.vars.committee_summary}",
        f"- Final AAPL position: `{repr(position)}`",
        f"- JSON: `{json_path.resolve()}`",
        f"- Agent summary file: `{payload['summary_file']}`",
        "",
        "## Verified Tool Outputs",
        "",
    ]
    for key in sorted((CommitteeToolRuntime.last_results or {}).keys()):
        markdown.append(f"- `{key}`")
    markdown.append("")
    markdown.append("## Memory Files")
    markdown.extend(f"- `{Path(path).resolve()}`" for path in memory_files)
    markdown_path = artifact_dir / "README.md"
    markdown_path.write_text("\n".join(markdown), encoding="utf-8")

    print(json.dumps({"artifact_dir": str(artifact_dir.resolve()), "json": str(json_path.resolve())}, indent=2))


if __name__ == "__main__":
    main()
