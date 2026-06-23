#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime


def load_dotenv(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
            value = value[1:-1]
        env[key] = value
    return env


def _bool_str(v: bool) -> str:
    return "true" if v else "false"


def sha256_file(path: Path) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except FileNotFoundError:
        return None


def manifest_file(path: Path) -> dict[str, object]:
    exists = path.exists()
    payload: dict[str, object] = {
        "path": str(path),
        "exists": exists,
    }
    if exists:
        try:
            payload["size_bytes"] = path.stat().st_size
        except OSError:
            pass
        payload["sha256"] = sha256_file(path)
    return payload


def git_value(args: list[str], cwd: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def collect_git_provenance(repo_root: Path) -> dict[str, object]:
    status = git_value(["status", "--porcelain=v1"], repo_root)
    status_lines = [line for line in (status or "").splitlines() if line]
    return {
        "repo_root": str(repo_root),
        "branch": git_value(["rev-parse", "--abbrev-ref", "HEAD"], repo_root),
        "sha": git_value(["rev-parse", "HEAD"], repo_root),
        "dirty": bool(status_lines),
        "status_short": status_lines,
    }


def collect_child_import_provenance(env: dict[str, str], cwd: Path, output_path: Path) -> dict[str, object]:
    probe = r'''
import json
import os
import sys

payload = {
    "python": sys.executable,
    "sys_path_head": sys.path[:5],
    "cwd": os.getcwd(),
}

try:
    import lumibot
    from lumibot.backtesting import backtesting_broker, routed_backtesting

    payload.update(
        {
            "ok": True,
            "lumibot_file": getattr(lumibot, "__file__", None),
            "lumibot_version": getattr(lumibot, "__version__", None),
            "backtesting_broker_file": getattr(backtesting_broker, "__file__", None),
            "routed_backtesting_file": getattr(routed_backtesting, "__file__", None),
        }
    )
except Exception as exc:
    payload.update({"ok": False, "error": repr(exc)})

output_path = os.environ.get("LUMIBOT_PROVENANCE_OUTPUT")
if output_path:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
else:
    print(json.dumps(payload, sort_keys=True))
'''
    output_path.parent.mkdir(parents=True, exist_ok=True)
    child_env = dict(env)
    child_env["LUMIBOT_PROVENANCE_OUTPUT"] = str(output_path)
    try:
        proc = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=str(cwd),
            env=child_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
        if output_path.exists():
            payload = json.loads(output_path.read_text(errors="replace"))
        else:
            raw = (proc.stdout or "").strip()
            payload = json.loads(raw) if raw else {}
        payload["exit_code"] = proc.returncode
        if proc.stdout:
            payload["stdout_tail"] = proc.stdout[-2000:]
        if proc.stderr:
            payload["stderr"] = proc.stderr[-2000:]
    except Exception as exc:
        payload = {"ok": False, "error": repr(exc), "exit_code": None}

    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return payload


def find_latest_prefix(log_dir: Path, started_at: float) -> str | None:
    """Best-effort resolve the run prefix from the newest artifact in `logs/`.

    Why: when backtests are stopped early (timeout) or crash mid-run, they often still emit
    `*_logs.csv` but may not reach tearsheet generation. We still want the runner to surface and
    copy whatever artifacts exist, and to loudly warn when the tearsheet is missing.
    """

    suffixes = (
        "_logs.csv",
        "_settings.json",
        "_trades.csv",
        "_trade_events.csv",
        "_tearsheet.html",
        "_tearsheet.csv",
    )

    candidates: list[Path] = []
    for suffix in suffixes:
        for p in log_dir.glob(f"*{suffix}"):
            try:
                if p.stat().st_mtime >= started_at - 1.0:
                    candidates.append(p)
            except FileNotFoundError:
                continue

    if not candidates:
        return None

    latest = max(candidates, key=lambda p: p.stat().st_mtime)
    for suffix in suffixes:
        if latest.name.endswith(suffix):
            return latest.name.removesuffix(suffix)
    return None


def count_queue_submits(log_csv: Path) -> int | None:
    try:
        with log_csv.open("r", errors="replace") as f:
            return sum(1 for line in f if "Submitted to queue" in line)
    except FileNotFoundError:
        return None


def parse_subprocess_metrics(log_path: Path) -> dict[str, object]:
    metrics: dict[str, object] = {
        "queue_submits": 0,
        "thetadata_cache_stale": 0,
        "paths": {},
        "top_paths": [],
    }

    try:
        raw = log_path.read_text(errors="replace")
    except FileNotFoundError:
        return metrics

    submits = 0
    stales = 0
    paths: dict[str, int] = {}
    path_re = re.compile(r"\bpath=([^ ]+)")

    for line in raw.splitlines():
        if "Submitted to queue" in line:
            submits += 1
            m = path_re.search(line)
            if m:
                p = m.group(1)
                paths[p] = paths.get(p, 0) + 1
        if "[THETA][CACHE][STALE]" in line:
            stales += 1

    metrics["queue_submits"] = submits
    metrics["thetadata_cache_stale"] = stales
    metrics["paths"] = paths
    metrics["top_paths"] = sorted(paths.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return metrics


def _default_workdir(label: str) -> Path:
    # IMPORTANT: Use a clean directory outside Strategy Library to avoid LumiBot's recursive `.env`
    # discovery accidentally loading unrelated env files (and to reduce startup overhead from
    # scanning large folder trees).
    runid = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (label or "backtest"))
    return Path(f"/Users/robertgrzesik/Documents/Development/backtest_runs/{runid}_{safe_label}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a LumiBot backtest with prod-like flags using env from botspot_node/.env-local without printing secrets")
    parser.add_argument("--main", required=True, help="Path to extracted strategy main.py")
    parser.add_argument("--start", required=True, help="BACKTESTING_START (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="BACKTESTING_END (YYYY-MM-DD)")
    parser.add_argument(
        "--data-source",
        default="thetadata",
        help="Set BACKTESTING_DATA_SOURCE (e.g., thetadata, ibkr, databento, router).",
    )
    parser.add_argument(
        "--ibkr-futures-exchange",
        default=None,
        help="Optional override for IBKR_FUTURES_EXCHANGE (defaults to CME when data-source=ibkr).",
    )
    parser.add_argument(
        "--ibkr-history-source",
        default=None,
        help="Optional override for IBKR_HISTORY_SOURCE (Trades, Midpoint, Bid_Ask). For futures parity use Trades.",
    )
    parser.add_argument(
        "--dotenv",
        default="/Users/robertgrzesik/Documents/Development/botspot_node/.env-local",
        help="dotenv file containing DATADOWNLOADER + S3 cache creds",
    )
    parser.add_argument(
        "--workdir",
        default=None,
        help="Clean working directory where `logs/` artifacts are written (defaults to a new folder under ~/Documents/Development/backtest_runs/)",
    )
    parser.add_argument(
        "--copy-artifacts-to",
        default=None,
        help="Optional directory to copy the final `logs/*` artifacts into (e.g., Strategy Library/logs). No files are deleted.",
    )
    parser.add_argument(
        "--lumibot-root",
        default="/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot",
        help="Local lumibot repo root to add to PYTHONPATH",
    )
    parser.add_argument(
        "--cache-folder",
        default=None,
        help="Override LUMIBOT_CACHE_FOLDER (simulate fresh ECS task cache)",
    )
    parser.add_argument(
        "--cache-version",
        default=None,
        help="Override LUMIBOT_CACHE_S3_VERSION (use fresh namespace without deleting existing S3)",
    )
    parser.add_argument(
        "--cache-prefix",
        default=None,
        help="Override LUMIBOT_CACHE_S3_PREFIX (alternative to cache-version)",
    )
    parser.add_argument(
        "--cache-bucket",
        default=None,
        help="Override LUMIBOT_CACHE_S3_BUCKET without editing the dotenv file.",
    )
    parser.add_argument(
        "--cache-mode",
        default=None,
        choices=["disabled", "readonly", "readwrite"],
        help="Override LUMIBOT_CACHE_MODE without editing the dotenv file.",
    )
    parser.add_argument(
        "--use-dotenv-s3-keys",
        action="store_true",
        help="Use LUMIBOT_CACHE_S3_ACCESS_KEY_ID/SECRET from the dotenv file instead of the host AWS credential chain.",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Label printed in output (no functional impact)",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="Enable backtest trade audit telemetry (sets LUMIBOT_BACKTEST_AUDIT=1; adds audit.* columns to trade logs)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Enable backtest profiling (sets BACKTESTING_PROFILE, e.g. 'yappi')",
    )
    parser.add_argument(
        "--perf-mode",
        action="store_true",
        help="Disable plotting/tearsheet/progress for cleaner runtime benchmarking.",
    )
    parser.add_argument(
        "--subprocess-log",
        default=None,
        help="Write the child backtest process stdout/stderr to this file (keeps runner output small)",
    )

    args = parser.parse_args()

    main_py = Path(args.main).resolve()
    if not main_py.exists():
        raise SystemExit(f"main.py not found: {main_py}")

    dotenv_path = Path(args.dotenv)
    dotenv = load_dotenv(dotenv_path)

    label = args.label or main_py.parent.name

    workdir = Path(args.workdir).resolve() if args.workdir else _default_workdir(label)
    workdir.mkdir(parents=True, exist_ok=True)
    log_dir = workdir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    # Prevent recursive `.env` discovery from loading unrelated files near strategy paths
    # (for example in Downloads). We inject required vars explicitly below.
    env["LUMIBOT_DISABLE_DOTENV"] = "1"

    # Ensure we use local lumibot source
    lumibot_root = str(Path(args.lumibot_root).resolve())
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = lumibot_root if not existing_pythonpath else f"{lumibot_root}:{existing_pythonpath}"

    # Backtest wiring / prod-like flags
    env["IS_BACKTESTING"] = "True"
    env["BACKTESTING_DATA_SOURCE"] = (args.data_source or "thetadata").strip()
    env["BACKTESTING_START"] = args.start
    env["BACKTESTING_END"] = args.end

    if args.perf_mode:
        env["SHOW_PLOT"] = "False"
        env["SHOW_INDICATORS"] = "False"
        env["SHOW_TEARSHEET"] = "False"
        env["SAVE_LOGFILE"] = "false"
        env["BACKTESTING_QUIET_LOGS"] = "true"
        env["BACKTESTING_SHOW_PROGRESS_BAR"] = "false"
    else:
        env["SHOW_PLOT"] = "True"
        env["SHOW_INDICATORS"] = "True"
        env["SHOW_TEARSHEET"] = "True"
        env["SAVE_LOGFILE"] = "true"
        env["BACKTESTING_QUIET_LOGS"] = "false"
        env["BACKTESTING_SHOW_PROGRESS_BAR"] = "true"
    # Prevent LumiBot from auto-opening HTML artifacts (tearsheet/trades) in the user's browser.
    # Artifacts are still generated in `logs/`; we just avoid UI spam during repeated perf runs.
    env.setdefault("LUMIBOT_DISABLE_UI", "1")

    if args.audit:
        # WHY: Investigations (NVDA/SPX) require a bulletproof per-fill record of quotes/inputs.
        # Keep this behind a flag because it increases CSV width and can add overhead.
        env["LUMIBOT_BACKTEST_AUDIT"] = "1"

    if args.profile:
        env["BACKTESTING_PROFILE"] = args.profile

    if env["BACKTESTING_DATA_SOURCE"].strip().lower() in {
        "ibkr",
        "interactivebrokersrest",
        "interactive_brokers_rest",
        "interactivebrokers_rest",
    }:
        env.setdefault("IBKR_FUTURES_EXCHANGE", (args.ibkr_futures_exchange or "CME").strip().upper())
        if args.ibkr_history_source:
            env["IBKR_HISTORY_SOURCE"] = args.ibkr_history_source.strip()

    # Data downloader config
    # WHY: In practice, `.env-local` can get stale for the downloader URL (host migrations,
    # local vs remote testing). Allow an explicit process env override for the base URL,
    # while still sourcing secrets (API key) from the dotenv file by default.
    if "DATADOWNLOADER_BASE_URL" in dotenv:
        env.setdefault("DATADOWNLOADER_BASE_URL", dotenv["DATADOWNLOADER_BASE_URL"])
    for k in [
        "DATADOWNLOADER_API_KEY",
        "DATADOWNLOADER_API_KEY_HEADER",
        "DATADOWNLOADER_SKIP_LOCAL_START",
        "THETADATA_USERNAME",
        "THETADATA_PASSWORD",
        "THETADATA_API_KEY",
    ]:
        if k in dotenv:
            env[k] = dotenv[k]

    # S3 cache backend config
    for k in [
        "LUMIBOT_CACHE_BACKEND",
        "LUMIBOT_CACHE_MODE",
        "LUMIBOT_CACHE_S3_BUCKET",
        "LUMIBOT_CACHE_S3_PREFIX",
        "LUMIBOT_CACHE_S3_REGION",
        "LUMIBOT_CACHE_S3_VERSION",
    ]:
        if k in dotenv:
            env[k] = dotenv[k]
    if args.use_dotenv_s3_keys:
        for k in [
            "LUMIBOT_CACHE_S3_ACCESS_KEY_ID",
            "LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY",
            "LUMIBOT_CACHE_S3_SESSION_TOKEN",
        ]:
            if k in dotenv:
                env[k] = dotenv[k]

    if args.cache_folder:
        env["LUMIBOT_CACHE_FOLDER"] = args.cache_folder
        Path(args.cache_folder).mkdir(parents=True, exist_ok=True)

    if args.cache_mode:
        env["LUMIBOT_CACHE_MODE"] = args.cache_mode

    if args.cache_version:
        env["LUMIBOT_CACHE_S3_VERSION"] = args.cache_version

    if args.cache_prefix:
        env["LUMIBOT_CACHE_S3_PREFIX"] = args.cache_prefix

    if args.cache_bucket:
        env["LUMIBOT_CACHE_S3_BUCKET"] = args.cache_bucket

    strategy_hash = sha256_file(main_py)
    git_provenance = collect_git_provenance(Path(lumibot_root))
    import_provenance_path = workdir / "child_import_provenance.json"
    import_provenance = collect_child_import_provenance(env, workdir, import_provenance_path)

    started_at = time.time()
    print(f"[run] label={label}")
    print(f"[run] main={main_py}")
    print(f"[run] strategy_sha256={strategy_hash}")
    print(f"[run] lumibot_root={lumibot_root}")
    print(f"[run] git_branch={git_provenance.get('branch')}")
    print(f"[run] git_sha={git_provenance.get('sha')}")
    print(f"[run] git_dirty={_bool_str(bool(git_provenance.get('dirty')))}")
    print(f"[run] child_lumibot_file={import_provenance.get('lumibot_file')}")
    print(f"[run] child_lumibot_version={import_provenance.get('lumibot_version')}")
    print(f"[run] child_backtesting_broker_file={import_provenance.get('backtesting_broker_file')}")
    print(f"[run] child_routed_backtesting_file={import_provenance.get('routed_backtesting_file')}")
    print(f"[run] child_import_provenance={import_provenance_path}")
    print(f"[run] data_source={env.get('BACKTESTING_DATA_SOURCE')}")
    print(f"[run] window={args.start} -> {args.end}")
    print(f"[run] workdir={workdir}")
    print(f"[run] cache_folder={env.get('LUMIBOT_CACHE_FOLDER')}")
    print(f"[run] cache_mode={env.get('LUMIBOT_CACHE_MODE')}")
    print(f"[run] cache_s3_bucket={env.get('LUMIBOT_CACHE_S3_BUCKET')}")
    print(f"[run] cache_s3_prefix={env.get('LUMIBOT_CACHE_S3_PREFIX')}")
    print(f"[run] cache_s3_version={env.get('LUMIBOT_CACHE_S3_VERSION')}")
    if env.get("BACKTESTING_DATA_SOURCE", "").strip().lower() in {"ibkr", "interactivebrokersrest", "interactive_brokers_rest", "interactivebrokers_rest"}:
        print(f"[run] ibkr_futures_exchange={env.get('IBKR_FUTURES_EXCHANGE')}")
        print(f"[run] ibkr_history_source={env.get('IBKR_HISTORY_SOURCE')}")
    print(f"[run] audit={_bool_str(args.audit)}")
    print(f"[run] profile={env.get('BACKTESTING_PROFILE')}")
    print(f"[run] perf_mode={_bool_str(args.perf_mode)}")

    subprocess_log = Path(args.subprocess_log) if args.subprocess_log else (workdir / f"subprocess_{label}.log")
    subprocess_log.parent.mkdir(parents=True, exist_ok=True)
    print(f"[run] subprocess_log={subprocess_log}")

    with subprocess_log.open("w") as log_f:
        proc = subprocess.run(
            [sys.executable, str(main_py)],
            cwd=str(workdir),
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )

    elapsed_s = time.time() - started_at
    print(f"[run] exit_code={proc.returncode} elapsed_s={elapsed_s:.1f}")

    # Scoreboard metrics: prefer the LumiBot `*_logs.csv` if present, but fall back to parsing the
    # subprocess stdout/stderr (useful for strategies that don't emit `*_logs.csv`).
    subprocess_metrics = parse_subprocess_metrics(subprocess_log)

    prefix = find_latest_prefix(log_dir, started_at)
    artifact_manifest: dict[str, object] = {
        "prefix": prefix,
        "log_dir": str(log_dir),
        "files": {},
        "copied_files": {},
    }
    if prefix:
        trades = log_dir / f"{prefix}_trades.csv"
        trade_events = log_dir / f"{prefix}_trade_events.csv"
        logs = log_dir / f"{prefix}_logs.csv"
        settings = log_dir / f"{prefix}_settings.json"
        tearsheet_html = log_dir / f"{prefix}_tearsheet.html"
        tearsheet_csv = log_dir / f"{prefix}_tearsheet.csv"
        source_artifacts = {
            "tearsheet_html": tearsheet_html,
            "tearsheet_csv": tearsheet_csv,
            "trades": trades,
            "trade_events": trade_events,
            "logs": logs,
            "settings": settings,
            "subprocess_log": subprocess_log,
            "child_import_provenance": import_provenance_path,
        }
        artifact_manifest["files"] = {name: manifest_file(path) for name, path in source_artifacts.items()}

        if tearsheet_html.exists():
            print(f"[artifacts] tearsheet_html={tearsheet_html}")
        else:
            print(f"[warn] tearsheet_html missing (prefix={prefix}); backtest may have crashed or been stopped early")

        if tearsheet_csv.exists():
            print(f"[artifacts] tearsheet_csv={tearsheet_csv}")

        print(f"[artifacts] trades={trades if trades.exists() else '(missing)'}")
        print(f"[artifacts] trade_events={trade_events if trade_events.exists() else '(missing)'}")
        print(f"[artifacts] logs={logs if logs.exists() else '(missing)'}")
        print(f"[artifacts] settings={settings if settings.exists() else '(missing)'}")

        submits = count_queue_submits(logs)
        if submits is not None:
            print(f"[metrics] queue_submits={submits}")
        else:
            print(f"[metrics] queue_submits={subprocess_metrics.get('queue_submits', 0)}")

        print(f"[metrics] thetadata_cache_stale={subprocess_metrics.get('thetadata_cache_stale', 0)}")
        top_paths = subprocess_metrics.get("top_paths") or []
        if top_paths:
            print(f"[metrics] top_paths={top_paths}")

        if args.copy_artifacts_to:
            dest_root = Path(args.copy_artifacts_to).resolve()
            dest_root.mkdir(parents=True, exist_ok=True)
            copied: dict[str, dict[str, object]] = {}
            for name, src in source_artifacts.items():
                if not src.exists():
                    continue
                dst = dest_root / src.name
                try:
                    shutil.copy2(src, dst)
                    copied[name] = manifest_file(dst)
                except Exception as exc:
                    print(f"[warn] failed to copy {src} -> {dst}: {exc}")
            artifact_manifest["copied_files"] = copied
            print(f"[artifacts] copied_to={dest_root}")
    else:
        print(f"[warn] no artifacts found in {log_dir}; see subprocess_log={subprocess_log}")

    try:
        artifact_manifest_path = workdir / "artifact_manifest.json"
        artifact_manifest_path.write_text(json.dumps(artifact_manifest, indent=2, sort_keys=True) + "\n")
        print(f"[artifacts] manifest={artifact_manifest_path}")

        metrics_path = workdir / "metrics.json"
        payload = {
            "label": label,
            "window": {"start": args.start, "end": args.end},
            "elapsed_s": elapsed_s,
            "exit_code": proc.returncode,
            "subprocess_log": str(subprocess_log),
            "main": str(main_py),
            "strategy_sha256": strategy_hash,
            "data_source": env.get("BACKTESTING_DATA_SOURCE"),
            "git": git_provenance,
            "child_import_provenance": import_provenance,
            "child_import_provenance_path": str(import_provenance_path),
            "artifact_manifest_path": str(artifact_manifest_path),
            "artifacts": artifact_manifest,
            "metrics": subprocess_metrics,
            "cache": {
                "folder": env.get("LUMIBOT_CACHE_FOLDER"),
                "mode": env.get("LUMIBOT_CACHE_MODE"),
                "s3_bucket": env.get("LUMIBOT_CACHE_S3_BUCKET"),
                "s3_prefix": env.get("LUMIBOT_CACHE_S3_PREFIX"),
                "s3_version": env.get("LUMIBOT_CACHE_S3_VERSION"),
            },
        }
        metrics_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(f"[metrics] json={metrics_path}")
    except Exception as exc:
        print(f"[warn] failed to write metrics.json: {exc}")

    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
