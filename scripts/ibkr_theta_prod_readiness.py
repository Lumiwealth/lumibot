#!/usr/bin/env python3
"""Run production-readiness IBKR-vs-Theta parity matrix and emit gate results.

Matrix:
- Stock canonical: strategy_slowibkr.py (2013-01-01 -> 2026-01-31)
- Options smoke: ZeroDTELastFridayStraddle.py
- Options stress: SPX Short Straddle Intraday (Copy 4).py (3-month minimum by default)

Sources:
- thetadata baseline
- mixed routed: {default: thetadata, stock/index/future/crypto/cont_future: ibkr, option: thetadata}
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "run_backtest_prodlike.py"

DEFAULT_DOTENV = "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/.env"
DEFAULT_STOCK_STRATEGY = "/Users/robertgrzesik/Downloads/strategy_slowibkr.py"
DEFAULT_SMOKE_STRATEGY = "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/ZeroDTELastFridayStraddle.py"
DEFAULT_STRESS_STRATEGY = "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/SPX Short Straddle Intraday (Copy 4).py"
DEFAULT_LUMIBOT_ROOT = "/Users/robertgrzesik/Documents/Development/lumibot"
DEFAULT_OUT_ROOT = "/Users/robertgrzesik/Documents/Development/backtest_runs"
DEFAULT_STRESS_START = "2025-01-01"
DEFAULT_STRESS_END = "2025-03-31"

MIXED_SOURCE = json.dumps(
    {
        "default": "thetadata",
        "stock": "ibkr",
        "index": "ibkr",
        "option": "thetadata",
        "future": "ibkr",
        "crypto": "ibkr",
        "cont_future": "ibkr",
    },
    separators=(",", ":"),
)


@dataclass
class RunResult:
    scenario: str
    source_name: str
    source_value: str
    start: str
    end: str
    workdir: str
    exit_code: int | None
    elapsed_s: float | None
    tearsheet_html: str | None
    tearsheet_csv: str | None
    trades_csv: str | None
    trade_events_csv: str | None
    stats_parquet: str | None
    logs_csv: str | None
    subprocess_log: str | None
    queue_submits: int | None


def _extract_value(lines: list[str], prefix: str) -> str | None:
    for line in lines:
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip()
    return None


def _extract_exit_elapsed(lines: list[str]) -> tuple[int | None, float | None]:
    for line in lines:
        if line.startswith("[run] exit_code="):
            m = re.search(r"exit_code=([-0-9]+)\s+elapsed_s=([0-9.]+)", line)
            if m:
                return int(m.group(1)), float(m.group(2))
    return None, None


def _derive_stats_parquet(tearsheet_html: str | None) -> str | None:
    if not tearsheet_html:
        return None
    p = Path(tearsheet_html)
    stem = p.name
    if not stem.endswith("_tearsheet.html"):
        return None
    stats = p.with_name(stem.replace("_tearsheet.html", "_stats.parquet"))
    return str(stats) if stats.exists() else None


def run_backtest(
    *,
    scenario: str,
    source_name: str,
    source_value: str,
    main_py: str,
    start: str,
    end: str,
    dotenv: str,
    lumibot_root: str,
    out_root: Path,
    timeout_s: int,
) -> RunResult:
    workdir = out_root / f"{scenario}_{source_name}_{start}_{end}".replace(":", "-")
    workdir.mkdir(parents=True, exist_ok=True)
    label = f"{scenario}_{source_name}".replace(" ", "_")

    cmd = [
        sys.executable,
        str(RUNNER),
        "--main",
        str(main_py),
        "--start",
        start,
        "--end",
        end,
        "--data-source",
        source_value,
        "--dotenv",
        dotenv,
        "--lumibot-root",
        lumibot_root,
        "--workdir",
        str(workdir),
        "--label",
        label,
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    lines = (proc.stdout or "").splitlines() + (proc.stderr or "").splitlines()

    exit_code, elapsed_s = _extract_exit_elapsed(lines)
    tearsheet_html = _extract_value(lines, "[artifacts] tearsheet_html=")
    tearsheet_csv = _extract_value(lines, "[artifacts] tearsheet_csv=")
    trades_csv = _extract_value(lines, "[artifacts] trades=")
    trade_events_csv = _extract_value(lines, "[artifacts] trade_events=")
    logs_csv = _extract_value(lines, "[artifacts] logs=")
    subprocess_log = _extract_value(lines, "[run] subprocess_log=")
    queue_submits_raw = _extract_value(lines, "[metrics] queue_submits=")
    try:
        queue_submits = int(queue_submits_raw) if queue_submits_raw is not None else None
    except Exception:
        queue_submits = None

    return RunResult(
        scenario=scenario,
        source_name=source_name,
        source_value=source_value,
        start=start,
        end=end,
        workdir=str(workdir),
        exit_code=exit_code if exit_code is not None else proc.returncode,
        elapsed_s=elapsed_s,
        tearsheet_html=tearsheet_html,
        tearsheet_csv=tearsheet_csv,
        trades_csv=trades_csv,
        trade_events_csv=trade_events_csv,
        stats_parquet=_derive_stats_parquet(tearsheet_html),
        logs_csv=logs_csv,
        subprocess_log=subprocess_log,
        queue_submits=queue_submits,
    )


def _read_tearsheet_csv(path: str | None) -> dict[str, tuple[str, str]]:
    if not path or not Path(path).exists():
        return {}
    out: dict[str, tuple[str, str]] = {}
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            metric = (row.get("Metric") or "").strip()
            if not metric:
                continue
            out[metric] = ((row.get("SPY") or "").strip(), (row.get("Strategy") or "").strip())
    return out


def _to_float_percent(text: str) -> float | None:
    s = (text or "").strip().replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_float(text: Any) -> float | None:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_timestamp(text: str | None) -> datetime | None:
    if not text:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _count_fills(path: str | None) -> tuple[int | None, str | None, str | None]:
    if not path or not Path(path).exists():
        return None, None, None
    fills = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("status") or "").strip().lower() == "fill":
                fills.append(row)
    if not fills:
        return 0, None, None
    return len(fills), fills[0].get("time"), fills[-1].get("time")


def _read_fill_rows(path: str | None) -> list[dict[str, str]]:
    if not path or not Path(path).exists():
        return []
    fills: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("status") or "").strip().lower() == "fill":
                fills.append(row)
    return fills


def _entry_exit_counts(path: str | None) -> tuple[int, int]:
    entries = 0
    exits = 0
    for row in _read_fill_rows(path):
        side = (row.get("side") or "").strip().lower()
        if "open" in side:
            entries += 1
        elif "close" in side:
            exits += 1
    return entries, exits


def _final_portfolio_value(path: str | None) -> float | None:
    if not path or not Path(path).exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df is None or df.empty or "portfolio_value" not in df.columns:
            return None
        series = pd.to_numeric(df["portfolio_value"], errors="coerce").dropna()
        if series.empty:
            return None
        return float(series.iloc[-1])
    except Exception:
        return None


def build_stock_parity_gate(theta: RunResult, mixed: RunResult) -> dict[str, Any]:
    theta_ts = _read_tearsheet_csv(theta.tearsheet_csv)
    mixed_ts = _read_tearsheet_csv(mixed.tearsheet_csv)

    def metric_delta(metric: str) -> float | None:
        t = _to_float_percent(theta_ts.get(metric, ("", ""))[1])
        m = _to_float_percent(mixed_ts.get(metric, ("", ""))[1])
        if t is None or m is None:
            return None
        return abs(m - t)

    cagr_delta = metric_delta("CAGR% (Annual Return)")
    total_return_delta = metric_delta("Total Return")
    max_dd_delta = metric_delta("Max Drawdown")
    total_return_relative_delta_pct = None
    theta_total_return = _to_float_percent(theta_ts.get("Total Return", ("", ""))[1])
    if total_return_delta is not None and theta_total_return is not None and abs(theta_total_return) > 1e-9:
        total_return_relative_delta_pct = abs(total_return_delta) / abs(theta_total_return) * 100.0

    theta_fills, theta_first, theta_last = _count_fills(theta.trade_events_csv)
    mixed_fills, mixed_first, mixed_last = _count_fills(mixed.trade_events_csv)
    first_fill_day_delta = None
    theta_first_dt = _parse_timestamp(theta_first)
    mixed_first_dt = _parse_timestamp(mixed_first)
    if theta_first_dt is not None and mixed_first_dt is not None:
        first_fill_day_delta = abs((mixed_first_dt.date() - theta_first_dt.date()).days)

    trade_count_delta = None
    if theta_fills is not None and mixed_fills is not None:
        trade_count_delta = abs(mixed_fills - theta_fills)

    benchmark_total_delta = None
    benchmark_cagr_delta = None
    t_bm_total = _to_float_percent(theta_ts.get("Total Return", ("", ""))[0])
    m_bm_total = _to_float_percent(mixed_ts.get("Total Return", ("", ""))[0])
    if t_bm_total is not None and m_bm_total is not None:
        benchmark_total_delta = abs(m_bm_total - t_bm_total)

    t_bm_cagr = _to_float_percent(theta_ts.get("CAGR% (Annual Return)", ("", ""))[0])
    m_bm_cagr = _to_float_percent(mixed_ts.get("CAGR% (Annual Return)", ("", ""))[0])
    if t_bm_cagr is not None and m_bm_cagr is not None:
        benchmark_cagr_delta = abs(m_bm_cagr - t_bm_cagr)

    checks = {
        "cagr_delta_le_0_5": cagr_delta is not None and cagr_delta <= 0.5,
        "total_return_relative_delta_pct_le_1": (
            total_return_relative_delta_pct is not None and total_return_relative_delta_pct <= 1.0
        ),
        "max_dd_delta_le_1": max_dd_delta is not None and max_dd_delta <= 1.0,
        "trade_count_delta_le_4": trade_count_delta is not None and trade_count_delta <= 4,
        "first_fill_day_delta_le_14": first_fill_day_delta is not None and first_fill_day_delta <= 14,
        "last_fill_match": theta_last is not None and theta_last == mixed_last,
        "benchmark_total_delta_le_5": benchmark_total_delta is not None and benchmark_total_delta <= 5.0,
        "benchmark_cagr_delta_le_1": benchmark_cagr_delta is not None and benchmark_cagr_delta <= 1.0,
    }

    return {
        "thresholds": {
            "strategy_cagr_delta_max": 0.5,
            "strategy_total_return_relative_delta_pct_max": 1.0,
            "strategy_max_dd_delta_max": 1.0,
            "trade_count_delta_max": 4,
            "first_fill_day_delta_max": 14,
            "benchmark_total_return_delta_max": 5.0,
            "benchmark_cagr_delta_max": 1.0,
        },
        "deltas": {
            "cagr_delta": cagr_delta,
            "total_return_delta": total_return_delta,
            "total_return_relative_delta_pct": total_return_relative_delta_pct,
            "max_dd_delta": max_dd_delta,
            "trade_count_delta": trade_count_delta,
            "first_fill_day_delta": first_fill_day_delta,
            "benchmark_total_return_delta": benchmark_total_delta,
            "benchmark_cagr_delta": benchmark_cagr_delta,
        },
        "fills": {
            "theta_first": theta_first,
            "theta_last": theta_last,
            "mixed_first": mixed_first,
            "mixed_last": mixed_last,
            "theta_count": theta_fills,
            "mixed_count": mixed_fills,
        },
        "checks": checks,
        "pass": all(checks.values()),
    }


def build_options_parity_gate(theta: RunResult, mixed: RunResult) -> dict[str, Any]:
    theta_entries, theta_exits = _entry_exit_counts(theta.trade_events_csv)
    mixed_entries, mixed_exits = _entry_exit_counts(mixed.trade_events_csv)
    theta_fills = _read_fill_rows(theta.trade_events_csv)
    mixed_fills = _read_fill_rows(mixed.trade_events_csv)

    fill_count_delta = abs(len(mixed_fills) - len(theta_fills))
    entry_count_delta = abs(mixed_entries - theta_entries)
    exit_count_delta = abs(mixed_exits - theta_exits)

    pair_count = min(len(theta_fills), len(mixed_fills))
    time_deltas_s: list[float] = []
    price_abs_deltas: list[float] = []
    price_rel_deltas_pct: list[float] = []
    for idx in range(pair_count):
        t_row = theta_fills[idx]
        m_row = mixed_fills[idx]

        t_time = _parse_timestamp(t_row.get("time"))
        m_time = _parse_timestamp(m_row.get("time"))
        if t_time is not None and m_time is not None:
            time_deltas_s.append(abs((m_time - t_time).total_seconds()))

        t_price = _to_float(t_row.get("price"))
        m_price = _to_float(m_row.get("price"))
        if t_price is not None and m_price is not None:
            price_abs_deltas.append(abs(m_price - t_price))
            if abs(t_price) > 1e-9:
                price_rel_deltas_pct.append(abs(m_price - t_price) / abs(t_price) * 100.0)

    max_fill_time_delta_s = max(time_deltas_s) if time_deltas_s else None
    avg_fill_time_delta_s = (sum(time_deltas_s) / len(time_deltas_s)) if time_deltas_s else None
    avg_fill_price_abs_delta = (sum(price_abs_deltas) / len(price_abs_deltas)) if price_abs_deltas else None
    avg_fill_price_rel_delta_pct = (
        (sum(price_rel_deltas_pct) / len(price_rel_deltas_pct)) if price_rel_deltas_pct else None
    )

    theta_final_equity = _final_portfolio_value(theta.stats_parquet)
    mixed_final_equity = _final_portfolio_value(mixed.stats_parquet)
    final_equity_relative_delta_pct = None
    if (
        theta_final_equity is not None
        and mixed_final_equity is not None
        and abs(theta_final_equity) > 1e-9
    ):
        final_equity_relative_delta_pct = abs(mixed_final_equity - theta_final_equity) / abs(theta_final_equity) * 100.0

    checks = {
        "fill_count_delta_le_4": fill_count_delta <= 4,
        "entry_count_delta_le_2": entry_count_delta <= 2,
        "exit_count_delta_le_2": exit_count_delta <= 2,
        "max_fill_time_delta_s_le_300": (
            (max_fill_time_delta_s is not None and max_fill_time_delta_s <= 300.0)
            if pair_count > 0
            else fill_count_delta == 0
        ),
        "avg_fill_price_rel_delta_pct_le_5": (
            (avg_fill_price_rel_delta_pct is not None and avg_fill_price_rel_delta_pct <= 5.0)
            if pair_count > 0
            else fill_count_delta == 0
        ),
        "final_equity_relative_delta_pct_le_3": (
            (final_equity_relative_delta_pct is not None and final_equity_relative_delta_pct <= 3.0)
            if final_equity_relative_delta_pct is not None
            else fill_count_delta == 0
        ),
    }

    return {
        "thresholds": {
            "fill_count_delta_max": 4,
            "entry_count_delta_max": 2,
            "exit_count_delta_max": 2,
            "max_fill_time_delta_s_max": 300.0,
            "avg_fill_price_rel_delta_pct_max": 5.0,
            "final_equity_relative_delta_pct_max": 3.0,
        },
        "counts": {
            "theta_fills": len(theta_fills),
            "mixed_fills": len(mixed_fills),
            "theta_entries": theta_entries,
            "mixed_entries": mixed_entries,
            "theta_exits": theta_exits,
            "mixed_exits": mixed_exits,
            "paired_fills": pair_count,
        },
        "deltas": {
            "fill_count_delta": fill_count_delta,
            "entry_count_delta": entry_count_delta,
            "exit_count_delta": exit_count_delta,
            "max_fill_time_delta_s": max_fill_time_delta_s,
            "avg_fill_time_delta_s": avg_fill_time_delta_s,
            "avg_fill_price_abs_delta": avg_fill_price_abs_delta,
            "avg_fill_price_rel_delta_pct": avg_fill_price_rel_delta_pct,
            "theta_final_equity": theta_final_equity,
            "mixed_final_equity": mixed_final_equity,
            "final_equity_relative_delta_pct": final_equity_relative_delta_pct,
        },
        "checks": checks,
        "pass": all(checks.values()),
    }


def _write_markdown(
    path: Path,
    runs: list[RunResult],
    stock_gate: dict[str, Any],
    options_parity: dict[str, dict[str, Any]],
) -> None:
    lines: list[str] = []
    lines.append("# IBKR Production Readiness Summary")
    lines.append("")
    lines.append("## Runs")
    lines.append("")
    lines.append("| scenario | source | start | end | exit | elapsed_s | queue_submits | tearsheet_html |")
    lines.append("|---|---|---|---|---:|---:|---:|---|")
    for r in runs:
        lines.append(
            f"| {r.scenario} | {r.source_name} | {r.start} | {r.end} | {r.exit_code} | "
            f"{'' if r.elapsed_s is None else f'{r.elapsed_s:.1f}'} | {r.queue_submits if r.queue_submits is not None else ''} | "
            f"`{r.tearsheet_html or ''}` |"
        )
    lines.append("")
    lines.append("## Stock Parity Gate")
    lines.append("")
    lines.append(f"- pass: **{stock_gate.get('pass')}**")
    lines.append(f"- deltas: `{json.dumps(stock_gate.get('deltas', {}), sort_keys=True)}`")
    lines.append(f"- checks: `{json.dumps(stock_gate.get('checks', {}), sort_keys=True)}`")
    lines.append("")
    lines.append("## Options Parity")
    lines.append("")
    if not options_parity:
        lines.append("- No options scenarios were executed.")
    else:
        for scenario, gate in sorted(options_parity.items()):
            lines.append(f"### {scenario}")
            lines.append(f"- pass: **{gate.get('pass')}**")
            lines.append(f"- counts: `{json.dumps(gate.get('counts', {}), sort_keys=True)}`")
            lines.append(f"- deltas: `{json.dumps(gate.get('deltas', {}), sort_keys=True)}`")
            lines.append(f"- checks: `{json.dumps(gate.get('checks', {}), sort_keys=True)}`")
            lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run IBKR-vs-Theta production readiness matrix and gates")
    parser.add_argument("--dotenv", default=DEFAULT_DOTENV)
    parser.add_argument("--lumibot-root", default=DEFAULT_LUMIBOT_ROOT)
    parser.add_argument("--stock-strategy", default=DEFAULT_STOCK_STRATEGY)
    parser.add_argument("--smoke-strategy", default=DEFAULT_SMOKE_STRATEGY)
    parser.add_argument("--stress-strategy", default=DEFAULT_STRESS_STRATEGY)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    parser.add_argument("--run-stress", action="store_true", help="Include SPX stress strategy matrix")
    parser.add_argument("--stress-start", default=DEFAULT_STRESS_START, help="Stress scenario start date (YYYY-MM-DD)")
    parser.add_argument("--stress-end", default=DEFAULT_STRESS_END, help="Stress scenario end date (YYYY-MM-DD)")
    parser.add_argument("--timeout-stock", type=int, default=7200)
    parser.add_argument("--timeout-smoke", type=int, default=3600)
    parser.add_argument("--timeout-stress", type=int, default=14400)
    args = parser.parse_args()

    run_root = Path(args.out_root).resolve() / f"ibkr_prod_readiness_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_root.mkdir(parents=True, exist_ok=True)

    scenarios = [
        {
            "name": "stock_tqqq_full",
            "main": args.stock_strategy,
            "start": "2013-01-01",
            "end": "2026-01-31",
            "timeout": args.timeout_stock,
        },
        {
            "name": "options_xsp_smoke",
            "main": args.smoke_strategy,
            "start": "2025-01-01",
            "end": "2025-01-31",
            "timeout": args.timeout_smoke,
        },
    ]
    if args.run_stress:
        scenarios.append(
            {
                "name": "options_spx_stress",
                "main": args.stress_strategy,
                "start": args.stress_start,
                "end": args.stress_end,
                "timeout": args.timeout_stress,
            }
        )

    sources = {
        "theta": "thetadata",
        "mixed": MIXED_SOURCE,
    }

    runs: list[RunResult] = []
    for sc in scenarios:
        for source_name, source_value in sources.items():
            print(f"[harness] running scenario={sc['name']} source={source_name}")
            result = run_backtest(
                scenario=sc["name"],
                source_name=source_name,
                source_value=source_value,
                main_py=sc["main"],
                start=sc["start"],
                end=sc["end"],
                dotenv=args.dotenv,
                lumibot_root=args.lumibot_root,
                out_root=run_root,
                timeout_s=int(sc["timeout"]),
            )
            runs.append(result)

    by_key = {(r.scenario, r.source_name): r for r in runs}
    theta_stock = by_key.get(("stock_tqqq_full", "theta"))
    mixed_stock = by_key.get(("stock_tqqq_full", "mixed"))
    stock_gate = {
        "pass": False,
        "error": "Missing stock_tqqq_full theta/mixed runs",
    }
    if theta_stock and mixed_stock:
        stock_gate = build_stock_parity_gate(theta_stock, mixed_stock)

    options_parity: dict[str, dict[str, Any]] = {}
    for scenario_name in ("options_xsp_smoke", "options_spx_stress"):
        theta_run = by_key.get((scenario_name, "theta"))
        mixed_run = by_key.get((scenario_name, "mixed"))
        if theta_run and mixed_run:
            options_parity[scenario_name] = build_options_parity_gate(theta_run, mixed_run)

    options_pass = all(g.get("pass") for g in options_parity.values()) if options_parity else True
    overall_pass = bool(stock_gate.get("pass")) and bool(options_pass)

    summary = {
        "run_root": str(run_root),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "runs": [asdict(r) for r in runs],
        "stock_parity_gate": stock_gate,
        "options_parity_gates": options_parity,
        "overall_pass": overall_pass,
    }

    summary_json = run_root / "summary.json"
    summary_md = run_root / "summary.md"
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_markdown(summary_md, runs, stock_gate, options_parity)

    print(f"[harness] summary_json={summary_json}")
    print(f"[harness] summary_md={summary_md}")
    print(f"[harness] stock_parity_pass={stock_gate.get('pass')}")
    print(f"[harness] options_parity_pass={options_pass}")
    print(f"[harness] overall_pass={overall_pass}")

    return 0 if overall_pass else 2


if __name__ == "__main__":
    raise SystemExit(main())
