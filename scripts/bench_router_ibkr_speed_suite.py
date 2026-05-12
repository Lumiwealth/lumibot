#!/usr/bin/env python3
from __future__ import annotations

"""
bench_router_ibkr_speed_suite.py

Run a repeatable, production-like router speed suite for:
- NQ client strategy
- GC client strategy
- (optional) IBKR warm-cache speed burner

Key properties:
- Uses `scripts/run_backtest_prodlike.py` so runs are "prod-like" (downloader + S3 cache).
- Writes an append-only speed ledger (md + csv) so we never "forget to log" runs.
- Designed to be safe locally (does not print secrets; writes artifacts under ~/Documents/Development/).
"""

import argparse
import csv
import datetime as dt
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_LEDGER_MD = REPO_ROOT / "docs/investigations/2026-01-27_ROUTER_IBKR_SPEED.md"
DEFAULT_LEDGER_CSV = REPO_ROOT / "docs/investigations/2026-01-27_ROUTER_IBKR_SPEED.csv"

DEFAULT_NQ_MAIN = Path.home() / "Documents/Development/backtest_strategies/nq_double_ema_test/main.py"
DEFAULT_GC_MAIN = Path.home() / "Documents/Development/backtest_strategies/goldfutures_ema_crossover/main.py"

# Canonical production routing JSON.
DEFAULT_ROUTING_JSON = json.dumps(
    {"default": "thetadata", "crypto": "ibkr", "future": "ibkr", "cont_future": "ibkr"},
    separators=(",", ":"),
)


@dataclass(frozen=True)
class BenchSpec:
    name: str
    kind: str  # "strategy" or "script"
    target: Path


def _iso_local_now() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def _git_sha_short() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
        )
        return out.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _ensure_ledger_csv(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ts",
                "git",
                "bench",
                "window_start",
                "window_end",
                "elapsed_s",
                "exit_code",
                "queue_submits",
                "top_paths_json",
                "workdir",
                "metrics_json",
                "yappi_csv",
                "data_source",
                "cache_folder",
                "cache_s3_bucket",
                "cache_s3_prefix",
                "cache_s3_version",
                "note",
            ]
        )


def _append_ledger_csv(path: Path, row: list[Any]) -> None:
    _ensure_ledger_csv(path)
    with path.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def _ensure_ledger_md_has_section(md_path: Path) -> None:
    md_path.parent.mkdir(parents=True, exist_ok=True)
    if not md_path.exists():
        md_path.write_text(
            "# Router IBKR Speed Investigation (Futures + Crypto)\n\n"
            "## Automated Suite Runs (append-only)\n"
            "| ts | git | bench | window | elapsed_s | queue_submits | top_paths | workdir | yappi_csv | note |\n"
            "|---|---|---|---|---:|---:|---|---|---|---|\n",
            encoding="utf-8",
        )
        return

    text = md_path.read_text(encoding="utf-8", errors="replace")
    if "## Automated Suite Runs (append-only)" in text:
        return

    with md_path.open("a", encoding="utf-8") as f:
        f.write("\n\n## Automated Suite Runs (append-only)\n")
        f.write("| ts | git | bench | window | elapsed_s | queue_submits | top_paths | workdir | yappi_csv | note |\n")
        f.write("|---|---|---|---|---:|---:|---|---|---|---|\n")


def _append_ledger_md_row(
    md_path: Path,
    *,
    ts: str,
    git: str,
    bench: str,
    window: str,
    elapsed_s: float | None,
    queue_submits: int | None,
    top_paths: list[tuple[str, int]],
    workdir: str,
    yappi_csv: str,
    note: str,
) -> None:
    _ensure_ledger_md_has_section(md_path)
    top_paths_str = ", ".join([f"`{p}`×{n}" for p, n in top_paths[:5]]) if top_paths else ""
    elapsed_str = f"{elapsed_s:.1f}" if isinstance(elapsed_s, (int, float)) else ""
    qs_str = str(queue_submits) if queue_submits is not None else ""
    with md_path.open("a", encoding="utf-8") as f:
        f.write(
            f"| {ts} | {git} | {bench} | {window} | {elapsed_str} | {qs_str} | {top_paths_str} | `{workdir}` | `{yappi_csv}` | {note} |\n"
        )


def _find_yappi_csv(workdir: Path) -> str:
    log_dir = workdir / "logs"
    if not log_dir.exists():
        return ""
    candidates = sorted(log_dir.glob("*_profile_yappi.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(candidates[0]) if candidates else ""


def _run_prodlike_strategy(
    *,
    strategy_main: Path,
    start: str,
    end: str,
    data_source: str,
    dotenv: Path,
    cache_folder: Path | None,
    cache_version: str | None,
    cache_prefix: str | None,
    use_dotenv_s3_keys: bool,
    profile: str | None,
    label: str,
    suite_dir: Path,
    timeout_s: int,
) -> dict[str, Any]:
    workdir = suite_dir / label
    workdir.mkdir(parents=True, exist_ok=True)
    subprocess_log = workdir / "subprocess.log"

    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts/run_backtest_prodlike.py"),
        "--main",
        str(strategy_main),
        "--start",
        start,
        "--end",
        end,
        "--data-source",
        data_source,
        "--dotenv",
        str(dotenv),
        "--workdir",
        str(workdir),
        "--lumibot-root",
        str(REPO_ROOT),
        "--subprocess-log",
        str(subprocess_log),
    ]
    if cache_folder is not None:
        cmd.extend(["--cache-folder", str(cache_folder)])
    if cache_version:
        cmd.extend(["--cache-version", cache_version])
    if cache_prefix:
        cmd.extend(["--cache-prefix", cache_prefix])
    if use_dotenv_s3_keys:
        cmd.append("--use-dotenv-s3-keys")
    if profile:
        cmd.extend(["--profile", profile])
    started = time.time()
    exit_code: int
    try:
        proc = subprocess.run(cmd, cwd=str(REPO_ROOT), timeout=timeout_s)
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        exit_code = 124
    elapsed = time.time() - started

    metrics_path = workdir / "metrics.json"
    metrics: dict[str, Any] = {}
    if metrics_path.exists():
        try:
            metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            metrics = {}

    return {
        "exit_code": exit_code,
        "elapsed_s": elapsed,
        "workdir": str(workdir),
        "metrics_json": str(metrics_path) if metrics_path.exists() else "",
        "metrics": metrics,
        "yappi_csv": _find_yappi_csv(workdir) if profile else "",
    }


def _run_script(
    *,
    script: Path,
    args: list[str],
    suite_dir: Path,
    label: str,
    timeout_s: int,
) -> dict[str, Any]:
    workdir = suite_dir / label
    workdir.mkdir(parents=True, exist_ok=True)
    log_path = workdir / "stdout.log"
    started = time.time()
    exit_code: int
    try:
        with log_path.open("w", encoding="utf-8") as f:
            proc = subprocess.run(
                [sys.executable, str(script), *args],
                cwd=str(REPO_ROOT),
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=timeout_s,
            )
        exit_code = proc.returncode
    except subprocess.TimeoutExpired:
        exit_code = 124
    elapsed = time.time() - started
    return {
        "exit_code": exit_code,
        "elapsed_s": elapsed,
        "workdir": str(workdir),
        "metrics_json": "",
        "metrics": {},
        "yappi_csv": "",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the router IBKR speed benchmark suite and append results to the speed ledger.")
    parser.add_argument("--start", required=True, help="BACKTESTING_START (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="BACKTESTING_END (YYYY-MM-DD)")
    parser.add_argument("--repeats", type=int, default=3, help="Repeat each benchmark N times and log each run + median.")
    parser.add_argument("--note", default="", help="Short note describing what changed (logged into the ledger).")
    parser.add_argument("--profile", choices=["", "yappi"], default="", help="Optionally profile ONE run per benchmark.")
    parser.add_argument("--include-burner", action="store_true", help="Also run the warm-cache burner benchmark script.")
    parser.add_argument("--dotenv", default=str(Path.home() / "Documents/Development/botspot_node/.env-local"))
    parser.add_argument("--cache-folder", default=str(Path.home() / "Documents/Development/backtest_cache/router_speed"))
    parser.add_argument("--cache-version", default="", help="Override LUMIBOT_CACHE_S3_VERSION (fresh namespace).")
    parser.add_argument("--cache-prefix", default="", help="Override LUMIBOT_CACHE_S3_PREFIX.")
    parser.add_argument("--use-dotenv-s3-keys", action="store_true", help="Use S3 keys from dotenv file.")
    parser.add_argument("--data-source", default=DEFAULT_ROUTING_JSON, help="BACKTESTING_DATA_SOURCE override (router JSON recommended).")
    parser.add_argument("--ledger-md", default=str(DEFAULT_LEDGER_MD))
    parser.add_argument("--ledger-csv", default=str(DEFAULT_LEDGER_CSV))
    parser.add_argument("--out-dir", default=str(Path.home() / "Documents/Development/backtest_suites"))
    parser.add_argument("--timeout-s", type=int, default=60 * 60, help="Per-run timeout in seconds.")
    args = parser.parse_args()

    start = args.start.strip()
    end = args.end.strip()
    repeats = max(int(args.repeats), 1)
    note = str(args.note or "").strip()

    suite_root = Path(args.out_dir).expanduser().resolve()
    suite_root.mkdir(parents=True, exist_ok=True)
    suite_dir = suite_root / f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_router_ibkr_suite"
    suite_dir.mkdir(parents=True, exist_ok=True)

    ledger_md = Path(args.ledger_md).resolve()
    ledger_csv = Path(args.ledger_csv).resolve()

    dotenv = Path(args.dotenv).expanduser().resolve()
    cache_folder = Path(args.cache_folder).expanduser().resolve() if args.cache_folder else None
    cache_version = args.cache_version.strip() or None
    cache_prefix = args.cache_prefix.strip() or None

    benches: list[BenchSpec] = [
        BenchSpec("nq", "strategy", DEFAULT_NQ_MAIN),
        BenchSpec("gc", "strategy", DEFAULT_GC_MAIN),
    ]
    if args.include_burner:
        benches.append(
            BenchSpec(
                "burner_ibkr_warm_cache",
                "script",
                REPO_ROOT / "scripts/bench_ibkr_speed_burner_warm_cache.py",
            )
        )

    sha = _git_sha_short()
    ts = _iso_local_now()

    failures = 0
    for bench in benches:
        run_rows: list[dict[str, Any]] = []

        profile = args.profile.strip() or None
        for i in range(repeats):
            label = f"{bench.name}_run{i+1}"
            if bench.kind == "strategy":
                result = _run_prodlike_strategy(
                    strategy_main=bench.target,
                    start=start,
                    end=end,
                    data_source=args.data_source,
                    dotenv=dotenv,
                    cache_folder=cache_folder,
                    cache_version=cache_version,
                    cache_prefix=cache_prefix,
                    use_dotenv_s3_keys=bool(args.use_dotenv_s3_keys),
                    profile=(profile if i == 0 else None),
                    label=label,
                    suite_dir=suite_dir,
                    timeout_s=int(args.timeout_s),
                )
            else:
                result = _run_script(
                    script=bench.target,
                    args=["--iterations", "2000"],
                    suite_dir=suite_dir,
                    label=label,
                    timeout_s=int(args.timeout_s),
                )

            metrics = result.get("metrics") or {}
            subprocess_metrics = (metrics.get("metrics") or {}) if isinstance(metrics, dict) else {}
            top_paths = subprocess_metrics.get("top_paths") or []
            queue_submits = None
            try:
                queue_submits = int(subprocess_metrics.get("queue_submits", 0))
            except Exception:
                queue_submits = None

            row = {
                "ts": ts,
                "git": sha,
                "bench": bench.name,
                "window_start": start,
                "window_end": end,
                "elapsed_s": result.get("elapsed_s"),
                "exit_code": result.get("exit_code"),
                "queue_submits": queue_submits,
                "top_paths": top_paths,
                "workdir": result.get("workdir"),
                "metrics_json": result.get("metrics_json"),
                "yappi_csv": result.get("yappi_csv") or "",
                "data_source": args.data_source,
                "cache_folder": str(cache_folder) if cache_folder else "",
                "cache_s3_bucket": ((metrics.get("cache") or {}) if isinstance(metrics, dict) else {}).get("s3_bucket", ""),
                "cache_s3_prefix": ((metrics.get("cache") or {}) if isinstance(metrics, dict) else {}).get("s3_prefix", ""),
                "cache_s3_version": ((metrics.get("cache") or {}) if isinstance(metrics, dict) else {}).get("s3_version", ""),
                "note": note,
            }
            run_rows.append(row)

            _append_ledger_csv(
                ledger_csv,
                [
                    row["ts"],
                    row["git"],
                    row["bench"],
                    row["window_start"],
                    row["window_end"],
                    row["elapsed_s"],
                    row["exit_code"],
                    row["queue_submits"],
                    json.dumps(row["top_paths"]),
                    row["workdir"],
                    row["metrics_json"],
                    row["yappi_csv"],
                    row["data_source"],
                    row["cache_folder"],
                    row["cache_s3_bucket"],
                    row["cache_s3_prefix"],
                    row["cache_s3_version"],
                    row["note"],
                ],
            )

            _append_ledger_md_row(
                ledger_md,
                ts=row["ts"],
                git=row["git"],
                bench=row["bench"],
                window=f"{start}→{end}",
                elapsed_s=float(row["elapsed_s"]) if row["elapsed_s"] is not None else None,
                queue_submits=row["queue_submits"],
                top_paths=top_paths,
                workdir=str(row["workdir"]),
                yappi_csv=str(row["yappi_csv"]),
                note=note,
            )

            if int(row["exit_code"] or 0) != 0:
                failures += 1

        elapsed_list = [r["elapsed_s"] for r in run_rows if isinstance(r.get("elapsed_s"), (int, float))]
        if elapsed_list:
            med = float(median(elapsed_list))
            _append_ledger_md_row(
                ledger_md,
                ts=ts,
                git=sha,
                bench=f"{bench.name}_median_{repeats}",
                window=f"{start}→{end}",
                elapsed_s=med,
                queue_submits=None,
                top_paths=[],
                workdir=str(suite_dir),
                yappi_csv="",
                note=note,
            )

    print(f"[suite] out_dir={suite_dir}")
    if failures:
        print(f"[suite] failures={failures}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

