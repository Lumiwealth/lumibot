#!/usr/bin/env python3
"""
Run the DiscretionaryTraderStrategy in parallel across three providers.

Launches three subprocesses (Gemini 3.1 Pro, GPT-5.4, Grok 4.2 reasoning),
streams per-provider logs to /tmp, monitors aggregate + per-process memory,
kills all processes if thresholds are exceeded, and reports a summary at
the end with tearsheet paths and scalar metrics pulled from each tearsheet's
_metrics.json artifact.

Usage:
    export GOOGLE_API_KEY=... OPENAI_API_KEY=... XAI_API_KEY=...
    export BACKTESTING_START=2026-03-01 BACKTESTING_END=2026-03-31
    python scripts/run_discretionary_3way.py

Safety:
    - Memory watchdog polls every 5s.
    - Aborts all three if combined RSS > 10 GB OR any single process > 5 GB.
    - Handles provider errors via the runtime-level retry wrapper added in
      4.5.4 (lumibot.components.agents.runtime.GoogleADKRuntime.run).
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = Path("/tmp")
LUMIBOT_LOGS = REPO_ROOT / "logs"

# Pro-tier models for a fair apples-to-apples comparison.
PROVIDERS = [
    ("gemini_pro", "gemini-3.1-pro-preview"),
    ("openai_54", "openai/gpt-5.4"),
    ("grok_42", "xai/grok-4.20-0309-reasoning"),
]

# Memory watchdog thresholds (bytes). M2 MBP should have lots of headroom;
# these are safety rails, not typical use.
TOTAL_RSS_LIMIT_BYTES = 10 * 1024 * 1024 * 1024  # 10 GB combined
PER_PROC_RSS_LIMIT_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB single process
POLL_INTERVAL_SECONDS = 5.0


def _proc_rss(pid: int) -> int | None:
    """Return RSS in bytes for a pid using macOS `ps`, or None if gone."""
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return int(out) * 1024 if out else None
    except Exception:
        return None


def _free_memory_snapshot() -> str:
    try:
        vm = subprocess.check_output(["vm_stat"], text=True)
        return vm.splitlines()[0] + " ... " + (vm.splitlines()[1] if len(vm.splitlines()) > 1 else "")
    except Exception:
        return "vm_stat unavailable"


def _kill_tree(pid: int) -> None:
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass


def _launch(label: str, model_id: str) -> tuple[subprocess.Popen, Path, float]:
    log_path = LOG_DIR / f"discretionary_{label}.log"
    env = os.environ.copy()
    env["AGENT_MODEL"] = model_id
    env.setdefault("LUMIBOT_DISABLE_DOTENV", "true")
    env.setdefault("BACKTESTING_DATA_SOURCE", "yahoo")
    env.setdefault("LUMIBOT_TELEMETRY", "false")
    env.setdefault("SHOW_PLOT", "false")
    env.setdefault("SHOW_INDICATORS", "false")
    env.setdefault("BACKTESTING_QUIET_LOGS", "true")
    env.setdefault("BACKTESTING_END", (datetime.now() - timedelta(days=1)).date().isoformat())
    env.setdefault(
        "BACKTESTING_START",
        (datetime.fromisoformat(env["BACKTESTING_END"]) - timedelta(days=30)).date().isoformat(),
    )
    # SHOW_TEARSHEET stays default (True) so tearsheets are generated.
    fh = open(log_path, "wb")
    proc = subprocess.Popen(
        [sys.executable, "-m", "lumibot.example_strategies.agent_discretionary"],
        cwd=REPO_ROOT,
        env=env,
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,  # own process group so we can kill children
    )
    return proc, log_path, time.time()


def _watchdog(procs: list[tuple[str, subprocess.Popen]], abort_flag: threading.Event) -> None:
    while not abort_flag.is_set():
        still_running = [(lbl, p) for lbl, p in procs if p.poll() is None]
        if not still_running:
            return
        rss_by_label: dict[str, int] = {}
        total = 0
        for lbl, p in still_running:
            r = _proc_rss(p.pid)
            if r is not None:
                rss_by_label[lbl] = r
                total += r
        killed_reason: str | None = None
        for lbl, r in rss_by_label.items():
            if r > PER_PROC_RSS_LIMIT_BYTES:
                killed_reason = f"{lbl} RSS {r / 1e9:.2f} GB exceeded per-process limit {PER_PROC_RSS_LIMIT_BYTES / 1e9:.1f} GB"
                break
        if not killed_reason and total > TOTAL_RSS_LIMIT_BYTES:
            killed_reason = f"Combined RSS {total / 1e9:.2f} GB exceeded total limit {TOTAL_RSS_LIMIT_BYTES / 1e9:.1f} GB"
        if killed_reason:
            sys.stderr.write(f"\n[watchdog] ABORTING: {killed_reason}\n")
            sys.stderr.flush()
            for _lbl, p in still_running:
                _kill_tree(p.pid)
            abort_flag.set()
            return
        time.sleep(POLL_INTERVAL_SECONDS)


def _new_run_artifacts(run_start_ts: float) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for settings_path in LUMIBOT_LOGS.glob("DiscretionaryTraderStrategy_*_settings.json"):
        if settings_path.stat().st_mtime < run_start_ts - 1:
            continue
        try:
            settings = json.loads(settings_path.read_text())
        except Exception:
            continue
        params = settings.get("parameters") or {}
        model = params.get("agent_trader_model")
        if not model:
            continue
        base = str(settings_path).removesuffix("_settings.json")
        rows[model] = {
            "settings": str(settings_path),
            "tearsheet": f"{base}_tearsheet.html",
            "metrics": f"{base}_tearsheet_metrics.json",
            "trades": f"{base}_trades.csv",
        }
    return rows


def main() -> int:
    requested_start = os.environ.get("BACKTESTING_START") or ""
    requested_end = os.environ.get("BACKTESTING_END") or ""
    print(f"[start] {_free_memory_snapshot()}")
    print(f"[start] Launching {len(PROVIDERS)} providers in parallel. Window: {requested_start or 'default'} -> {requested_end or 'default'}")
    print()

    # Record wall-clock start so we can identify this run's artifacts later.
    run_start_ts = time.time()
    procs: list[tuple[str, subprocess.Popen, Path, float]] = []
    for label, model_id in PROVIDERS:
        p, log, t0 = _launch(label, model_id)
        procs.append((label, p, log, t0))
        print(f"[launch] {label:10s}  model={model_id}  pid={p.pid}  log={log}")

    abort_flag = threading.Event()
    watchdog = threading.Thread(
        target=_watchdog,
        args=([(lbl, p) for lbl, p, _, _ in procs], abort_flag),
        daemon=True,
    )
    watchdog.start()

    # Wait loop with light-touch progress every 30s.
    last_status_ts = time.time()
    while any(p.poll() is None for _, p, _, _ in procs):
        time.sleep(3)
        now = time.time()
        if now - last_status_ts > 30:
            last_status_ts = now
            parts = []
            for label, p, _log, t0 in procs:
                if p.poll() is None:
                    rss = _proc_rss(p.pid) or 0
                    parts.append(f"{label}: running {int(now - t0)}s rss={rss / 1e9:.2f}GB")
                else:
                    parts.append(f"{label}: exit={p.returncode} after {int(now - t0)}s")
            print(f"[progress] {' | '.join(parts)}")

    abort_flag.set()
    watchdog.join(timeout=2)

    print()
    print("=" * 72)
    print("FINAL STATUS")
    print("=" * 72)
    artifact_map = _new_run_artifacts(run_start_ts)
    rows: list[dict] = []
    for label, p, log_path, t0 in procs:
        elapsed = time.time() - t0
        rc = p.returncode
        metrics = {}
        model_id = dict(PROVIDERS)[label]
        artifact_entry = artifact_map.get(model_id) or {}
        ts_for_this_run = Path(artifact_entry["tearsheet"]) if artifact_entry.get("tearsheet") else None
        m_for_this_run = Path(artifact_entry["metrics"]) if artifact_entry.get("metrics") else None
        if m_for_this_run and m_for_this_run.exists():
            try:
                metrics = json.loads(m_for_this_run.read_text())
            except Exception:
                metrics = {}
        rows.append({
            "label": label,
            "model": model_id,
            "exit_code": rc,
            "wall_time_seconds": round(elapsed, 1),
            "log": str(log_path),
            "tearsheet": str(ts_for_this_run) if ts_for_this_run else None,
            "settings": artifact_entry.get("settings"),
            "trades": artifact_entry.get("trades"),
            "scalar_metrics": metrics.get("scalar_metrics"),
            "benchmark_metrics": metrics.get("benchmark_scalar_metrics"),
        })
        print(f"\n{label:10s}  model={model_id}  exit={rc}  {elapsed:.1f}s")
        print(f"  log:       {log_path}")
        if ts_for_this_run:
            print(f"  tearsheet: {ts_for_this_run}")
        if artifact_entry.get("trades"):
            print(f"  trades:    {artifact_entry['trades']}")
        sm = metrics.get("scalar_metrics") or {}
        if sm:
            print(f"  total_return={sm.get('Total Return')}  CAGR={sm.get('CAGR% (Annual Return)')}  "
                  f"Sharpe={sm.get('Sharpe')}  Sortino={sm.get('Sortino')}  MaxDD={sm.get('Max Drawdown')}  "
                  f"Calmar={sm.get('RoMaD')}")

    summary_path = REPO_ROOT / "logs" / "discretionary_3way_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps({"run_start": run_start_ts, "rows": rows}, indent=2))
    print(f"\n[summary written] {summary_path}")
    return 0 if all(r["exit_code"] == 0 for r in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
