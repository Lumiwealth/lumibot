#!/usr/bin/env python3
"""
Generate machine-readable baselines for LumiBot acceptance backtests.

Source of truth:
- Strategy Library artifacts under `Strategy Library/logs/` (settings + tearsheet CSV).

Output:
- `tests/backtest/acceptance_backtests_baselines.json`

This file is used by `tests/backtest/test_acceptance_backtests_ci.py` to make CI assertions
strict and stable without hand-copying numbers into test code.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class Case:
    slug: str
    strategy_name: str
    script_filename: str
    start_date: str  # BACKTESTING_START (YYYY-MM-DD)
    end_date: str  # BACKTESTING_END (YYYY-MM-DD, exclusive)
    data_source: str
    baseline_run_id: str
    # Explicit CI runtime cap (seconds). This is intentionally *not* expressed as a multiplier:
    # we want a concrete number that can be tightened once CI timing stabilizes.
    max_backtest_time_seconds: int


CASES: list[Case] = [
    Case(
        slug="aapl_deep_dip_calls",
        strategy_name="AAPLDeepDipCalls",
        script_filename="AAPL Deep Dip Calls (Copy 4).py",
        start_date="2020-01-01",
        end_date="2025-12-01",
        data_source="thetadata",
        baseline_run_id="AAPLDeepDipCalls_2026-01-05_09-33_aLJW35",
        max_backtest_time_seconds=300,
    ),
    Case(
        slug="leaps_alpha_picks_short",
        strategy_name="LeapsCallDebitSpread",
        script_filename="Leaps Buy Hold (Alpha Picks).py",
        start_date="2025-10-01",
        end_date="2025-10-15",
        data_source="thetadata",
        baseline_run_id="LeapsCallDebitSpread_2026-01-05_09-34_qqykop",
        max_backtest_time_seconds=120,
    ),
    Case(
        slug="tqqq_sma200_thetadata",
        strategy_name="TqqqSma200Strategy",
        script_filename="TQQQ 200-Day MA.py",
        start_date="2013-01-01",
        end_date="2025-12-01",
        data_source="thetadata",
        baseline_run_id="TqqqSma200Strategy_2026-01-05_09-35_IibziX",
        max_backtest_time_seconds=180,
    ),
    # NOTE: Yahoo is recorded once in docs/ACCEPTANCE_BACKTESTS.md (manual parity note),
    # but is intentionally not part of the CI acceptance suite.
    Case(
        slug="backdoor_butterfly_full_year",
        strategy_name="BackdoorButterfly0DTE",
        script_filename="Backdoor Butterfly 0 DTE (Copy).py",
        start_date="2025-01-01",
        end_date="2025-12-01",
        data_source="thetadata",
        baseline_run_id="BackdoorButterfly0DTE_2026-01-05_09-36_7AP0H8",
        max_backtest_time_seconds=600,
    ),
    Case(
        slug="meli_deep_drawdown",
        strategy_name="MeliDeepDrawdownCalls",
        script_filename="Meli Deep Drawdown Calls.py",
        start_date="2013-01-01",
        end_date="2025-12-18",
        data_source="thetadata",
        baseline_run_id="MeliDeepDrawdownCalls_2026-01-05_09-42_bMjRNX",
        max_backtest_time_seconds=300,
    ),
    Case(
        slug="backdoor_smartlimit",
        strategy_name="BackdoorButterfly0DTESmartLimit",
        script_filename="Backdoor Butterfly 0 DTE (Copy) - with SMART LIMITS.py",
        start_date="2025-01-01",
        end_date="2025-12-01",
        data_source="thetadata",
        baseline_run_id="BackdoorButterfly0DTESmartLimit_2026-01-05_09-44_qLKdxw",
        max_backtest_time_seconds=600,
    ),
    # SPX Short Straddle: CI uses the stall repro / prod parity window (docs include the speed baseline historically).
    Case(
        slug="spx_short_straddle_repro",
        strategy_name="SPXShortStraddle",
        script_filename="SPX Short Straddle Intraday (Copy).py",
        start_date="2025-01-06",
        end_date="2025-12-26",
        data_source="thetadata",
        baseline_run_id="SPXShortStraddle_2026-01-05_09-49_35TJJl",
        max_backtest_time_seconds=600,
    ),
    Case(
        slug="ibkr_crypto_acceptance_btc_usd",
        strategy_name="IbkrCryptoAcceptance",
        script_filename="IBKR Crypto Acceptance.py",
        start_date="2025-12-15",
        end_date="2025-12-18",
        data_source="ibkr",
        baseline_run_id="IbkrCryptoAcceptance_2026-01-14_17-39_k9nEa1",
        max_backtest_time_seconds=180,
    ),
    Case(
        slug="ibkr_mes_futures_acceptance",
        strategy_name="IbkrMesFuturesAcceptance",
        script_filename="IBKR MES Futures Acceptance.py",
        start_date="2025-12-08",
        end_date="2025-12-11",
        data_source="ibkr",
        baseline_run_id="IbkrMesFuturesAcceptance_2026-01-14_17-39_N8iDK2",
        max_backtest_time_seconds=300,
    ),
]


def _centipercent(value: str) -> int:
    """
    Convert percent strings into *centipercent* integers (0.01% units).

    Examples:
    - "48.86%" -> 4886
    - "-17%" -> -1700
    - "8,585%" -> 858500
    """
    s = str(value).strip()
    if not s.endswith("%"):
        raise ValueError(f"Expected percent string ending with '%', got {value!r}")
    s = s[:-1].replace(",", "").strip()
    d = Decimal(s)
    scaled = d * Decimal("100")
    if scaled != scaled.to_integral_value():
        raise ValueError(f"Percent value {value!r} is not representable at 0.01% resolution.")
    return int(scaled)


def _read_tearsheet(tearsheet_csv: Path) -> dict[str, dict[str, Any]]:
    df = pd.read_csv(tearsheet_csv)
    if "Metric" not in df.columns or "Strategy" not in df.columns:
        raise RuntimeError(f"Unexpected tearsheet format: {tearsheet_csv} columns={list(df.columns)}")

    def _get(metric_name: str) -> str:
        row = df.loc[df["Metric"] == metric_name, "Strategy"]
        if row.empty:
            raise RuntimeError(f"Missing metric {metric_name!r} in {tearsheet_csv}")
        return str(row.iloc[0]).strip()

    raw_tearsheet = {
        "total_return": _get("Total Return"),
        "cagr": _get("CAGR% (Annual Return)"),
        "max_drawdown": _get("Max Drawdown"),
    }
    centi = {k: _centipercent(v) for k, v in raw_tearsheet.items()}

    def _format_centipercent(value: int) -> str:
        """Render a centipercent integer as a canonical percent string with 2 decimals."""
        sign = "-" if value < 0 else ""
        value = abs(int(value))
        whole = value // 100
        frac = value % 100
        return f"{sign}{whole:,}.{frac:02d}%"

    raw = {k: _format_centipercent(v) for k, v in centi.items()}
    return {"raw": raw, "centipercent": centi}


def _expected_settings_end_date(end_date_exclusive: str) -> str:
    end = date.fromisoformat(end_date_exclusive)
    return (end - timedelta(days=1)).isoformat()


def _assert_settings_match_window(case: Case, payload: dict[str, Any]) -> None:
    start = str(payload.get("backtesting_start") or "")
    end = str(payload.get("backtesting_end") or "")

    if not start.startswith(case.start_date):
        raise RuntimeError(f"{case.slug}: settings backtesting_start={start!r} does not start with {case.start_date!r}")
    if "00:00:00" not in start:
        raise RuntimeError(f"{case.slug}: settings backtesting_start={start!r} does not contain 00:00:00")

    expected_end_date = _expected_settings_end_date(case.end_date)
    # backtesting_end should be previous day @ 23:59:00 with some timezone offset.
    if not re.match(rf"^{re.escape(expected_end_date)}\s+23:59:00", end):
        raise RuntimeError(
            f"{case.slug}: settings backtesting_end={end!r} does not match expected date {expected_end_date!r} @ 23:59:00"
        )


def _load_case(logs_dir: Path, case: Case) -> dict[str, Any]:
    tearsheet = logs_dir / f"{case.baseline_run_id}_tearsheet.csv"
    settings = logs_dir / f"{case.baseline_run_id}_settings.json"
    if not tearsheet.exists():
        raise FileNotFoundError(tearsheet)
    if not settings.exists():
        raise FileNotFoundError(settings)

    payload = json.loads(settings.read_text(encoding="utf-8"))
    _assert_settings_match_window(case, payload)

    metrics = _read_tearsheet(tearsheet)
    return {
        "slug": case.slug,
        "strategy_name": case.strategy_name,
        "script_filename": case.script_filename,
        "data_source": case.data_source,
        "start_date": case.start_date,
        "end_date": case.end_date,
        "baseline_run_id": case.baseline_run_id,
        "max_backtest_time_seconds": case.max_backtest_time_seconds,
        "lumibot_version": payload.get("lumibot_version"),
        "backtest_time_seconds": payload.get("backtest_time_seconds"),
        "settings_backtesting_start": payload.get("backtesting_start"),
        "settings_backtesting_end": payload.get("backtesting_end"),
        "metrics_raw": metrics["raw"],
        "metrics_centipercent": metrics["centipercent"],
    }


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    default_logs = Path("/Users/robertgrzesik/Documents/Development/Strategy Library/logs")
    default_output = repo_root / "tests" / "backtest" / "acceptance_backtests_baselines.json"

    parser = argparse.ArgumentParser()
    parser.add_argument("--logs-dir", type=Path, default=default_logs)
    parser.add_argument("--output", type=Path, default=default_output)
    args = parser.parse_args()

    logs_dir: Path = args.logs_dir
    output: Path = args.output

    cases_out = [_load_case(logs_dir, c) for c in CASES]
    payload = {
        "schema_version": 1,
        "notes": "Percent metrics are stored as centipercent integers (0.01% units) for strict comparisons.",
        "cases": cases_out,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
