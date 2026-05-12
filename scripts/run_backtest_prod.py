#!/usr/bin/env python3
"""
Run a *single* production backtest via the BotManager API and download artifacts.

Why this exists
---------------
- We frequently need to repro "prod-only" behavior (startup latency, ECS exits/OOMs, S3 hydration).
- The BotSpot UI is great for humans, but for engineering we need a repeatable, scriptable harness.

Safety properties
-----------------
- Reads secrets from `botspot_node/.env-local` (or an explicit --dotenv path).
- Never prints secrets.
- Writes artifacts into a deterministic folder under `~/Documents/Development/Strategy Library/logs/`.

Notes
-----
- Production runs in ECS tasks; for cold-cache simulations, override `--cache-version` to a fresh
  namespace (no deletes needed).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import re
import time
from pathlib import Path
from typing import Any

import requests

DEFAULT_DOTENV_PATHS = (
    Path.home() / "Documents/Development/botspot_node/.env-local",
    Path.home() / "Documents/Development/botspot_node/.env",
)


def _parse_dotenv(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([A-Za-z0-9_]+)=(.*)$", line)
        if not m:
            continue
        key, value = m.group(1), m.group(2)
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        values[key] = value
    return values


def _load_config(dotenv_path: Path | None) -> dict[str, str]:
    if dotenv_path is not None:
        return _parse_dotenv(dotenv_path)

    for candidate in DEFAULT_DOTENV_PATHS:
        if candidate.exists():
            return _parse_dotenv(candidate)

    raise FileNotFoundError(
        "Could not find botspot_node .env-local. Provide --dotenv PATH or create one at "
        f"{DEFAULT_DOTENV_PATHS[0]}"
    )


def _random_suffix(length: int = 8) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(alphabet) for _ in range(length))


def _iso_utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).isoformat().replace("+00:00", "Z")


def _http_headers(api_key: str) -> dict[str, str]:
    return {"x-api-key": api_key}


def _request_json(session: requests.Session, method: str, url: str, *, headers: dict[str, str], json_body: Any = None, timeout_s: int = 45) -> Any:
    resp = session.request(method, url, headers=headers, json=json_body, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()


def _download_text(session: requests.Session, url: str, *, headers: dict[str, str], timeout_s: int = 60) -> str:
    resp = session.get(url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return resp.text


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _build_backtest_env(dotenv: dict[str, str], *, profile: str | None, audit: bool, cache_version: str | None) -> dict[str, str]:
    required_keys = (
        "DATADOWNLOADER_BASE_URL",
        "DATADOWNLOADER_API_KEY",
        "DATADOWNLOADER_API_KEY_HEADER",
        "LUMIBOT_CACHE_BACKEND",
        "LUMIBOT_CACHE_MODE",
        "LUMIBOT_CACHE_S3_BUCKET",
        "LUMIBOT_CACHE_S3_PREFIX",
        "LUMIBOT_CACHE_S3_REGION",
        "LUMIBOT_CACHE_S3_VERSION",
        "LUMIBOT_CACHE_S3_ACCESS_KEY_ID",
        "LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY",
    )
    missing = [k for k in required_keys if not dotenv.get(k)]
    if missing:
        raise RuntimeError(f"Missing required env vars in dotenv: {', '.join(missing)}")

    env: dict[str, str] = {
        "IS_BACKTESTING": "True",
        "BACKTESTING_DATA_SOURCE": "thetadata",
        # Production backtests rely on injected env vars; recursive .env scanning is both slow and risky.
        "LUMIBOT_DISABLE_DOTENV": "1",
        "DATADOWNLOADER_BASE_URL": dotenv["DATADOWNLOADER_BASE_URL"],
        "DATADOWNLOADER_API_KEY": dotenv["DATADOWNLOADER_API_KEY"],
        "DATADOWNLOADER_API_KEY_HEADER": dotenv["DATADOWNLOADER_API_KEY_HEADER"],
        "LUMIBOT_CACHE_BACKEND": dotenv["LUMIBOT_CACHE_BACKEND"],
        "LUMIBOT_CACHE_MODE": dotenv["LUMIBOT_CACHE_MODE"],
        "LUMIBOT_CACHE_S3_BUCKET": dotenv["LUMIBOT_CACHE_S3_BUCKET"],
        "LUMIBOT_CACHE_S3_PREFIX": dotenv["LUMIBOT_CACHE_S3_PREFIX"],
        "LUMIBOT_CACHE_S3_REGION": dotenv["LUMIBOT_CACHE_S3_REGION"],
        "LUMIBOT_CACHE_S3_VERSION": cache_version or dotenv["LUMIBOT_CACHE_S3_VERSION"],
        "LUMIBOT_CACHE_S3_ACCESS_KEY_ID": dotenv["LUMIBOT_CACHE_S3_ACCESS_KEY_ID"],
        "LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY": dotenv["LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY"],
        # Prod-like artifacts/logging flags
        "SHOW_PLOT": "True",
        "SHOW_INDICATORS": "True",
        "SHOW_TEARSHEET": "True",
        "BACKTESTING_QUIET_LOGS": "false",
        "BACKTESTING_SHOW_PROGRESS_BAR": "true",
    }

    if audit:
        env["LUMIBOT_BACKTEST_AUDIT"] = "1"

    if profile:
        env["BACKTESTING_PROFILE"] = profile

    return env


def _wait_for_terminal_status(
    session: requests.Session,
    base_url: str,
    api_key: str,
    bot_id: str,
    *,
    poll_s: float = 3.0,
    timeout_s: int = 60 * 60,
) -> dict[str, Any]:
    headers = _http_headers(api_key)
    url = f"{base_url}/status?bot_id={bot_id}"
    started = time.time()

    terminal = {"completed", "failed", "completed_with_errors", "force_stopped", "stopped", "error", "not_found"}
    while True:
        status_data = _request_json(session, "GET", url, headers=headers, timeout_s=30)
        status = str(status_data.get("status") or "").lower()
        if status in terminal:
            return status_data

        if time.time() - started > timeout_s:
            return {"status": "timeout", "bot_id": bot_id, "last_status": status_data}

        time.sleep(poll_s)


def _download_artifacts(
    session: requests.Session,
    base_url: str,
    api_key: str,
    bot_id: str,
    out_dir: Path,
) -> dict[str, str]:
    headers = _http_headers(api_key)
    _safe_mkdir(out_dir)

    artifacts: dict[str, str] = {}

    def _save(file_type: str, suffix: str) -> None:
        url = f"{base_url}/backtest_results?bot_id={bot_id}&file={file_type}"
        content = _download_text(session, url, headers=headers, timeout_s=120)
        path = out_dir / f"{bot_id}_{suffix}"
        path.write_text(content, encoding="utf-8")
        artifacts[file_type] = str(path)

    # Best-effort; not all runs emit all files.
    #
    # NOTE: BotManager `/backtest_results` only supports a fixed set of `file` values.
    # Keep this list in sync with BotManager's source of truth:
    # - bot_manager/BotManager.py: `BotManager.BACKTEST_RESULTS_ARTIFACT_FILENAMES`
    # - bot_manager/flask_app.py: `/api/backtest_results` validation
    for file_type, suffix in (
        ("logs_csv", "logs.csv"),
        ("settings_json", "settings.json"),
        ("trades_csv", "trades.csv"),
        ("stats_csv", "stats.csv"),
        ("tearsheet_html", "tearsheet.html"),
        ("tearsheet_csv", "tearsheet.csv"),
        ("trades_html", "trades.html"),
        ("indicators_html", "indicators.html"),
        ("indicators_csv", "indicators.csv"),
        ("completion_json", "completion.json"),
        ("profile_yappi_csv", "profile_yappi.csv"),
    ):
        try:
            _save(file_type, suffix)
        except Exception:
            continue

    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a single production backtest via BotManager API.")
    parser.add_argument("--dotenv", type=str, default=None, help="Path to botspot_node .env-local (default: auto-detect)")
    parser.add_argument("--main", type=str, required=True, help="Path to strategy main.py to run in production")
    parser.add_argument("--start", type=str, required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--label", type=str, default="backtest", help="Label used in bot_id and output folder")
    parser.add_argument("--cache-version", type=str, default=None, help="Override LUMIBOT_CACHE_S3_VERSION (cold namespace without deletes)")
    parser.add_argument("--profile", type=str, default=None, help="Set BACKTESTING_PROFILE (e.g., 'yappi')")
    parser.add_argument("--audit", action="store_true", help="Enable LUMIBOT_BACKTEST_AUDIT=1")
    parser.add_argument(
        "--out",
        type=str,
        default=str(Path.home() / "Documents/Development/Strategy Library/logs/prod_runs"),
        help="Output directory for downloaded artifacts",
    )
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv).expanduser() if args.dotenv else None
    dotenv = _load_config(dotenv_path)

    base_url = dotenv.get("BACKTEST_SERVICE_URL", "").rstrip("/")
    api_key = dotenv.get("BACKTEST_API_KEY", "")
    if not base_url or not api_key:
        raise RuntimeError("BACKTEST_SERVICE_URL or BACKTEST_API_KEY missing from dotenv.")

    strategy_file = Path(args.main).expanduser()
    strategy_code = strategy_file.read_text(encoding="utf-8")

    env_vars = _build_backtest_env(
        dotenv,
        profile=args.profile,
        audit=bool(args.audit),
        cache_version=args.cache_version,
    )

    session = requests.Session()
    headers = _http_headers(api_key)

    bot_id = f"{args.label}-{args.start.replace('-', '')}-{args.end.replace('-', '')}-{_random_suffix(8)}"
    payload = {
        "bot_id": bot_id,
        "main": strategy_code,
        "start_date": args.start,
        "end_date": args.end,
        "bot_config": env_vars,
    }

    submitted_at = time.time()
    submitted_at_iso = _iso_utc(submitted_at)

    session.post(f"{base_url}/backtest", json=payload, headers=headers, timeout=60).raise_for_status()

    # Print a safe "submitted" marker immediately so engineers can stop the script early (timeout)
    # and still have the bot_id to inspect in BotSpot/CloudWatch without leaking secrets.
    print(
        json.dumps(
            {
                "status": "submitted",
                "bot_id": bot_id,
                "submitted_at_utc": submitted_at_iso,
                "cache_version": env_vars.get("LUMIBOT_CACHE_S3_VERSION"),
                "start_date": args.start,
                "end_date": args.end,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    status_data = _wait_for_terminal_status(session, base_url, api_key, bot_id, timeout_s=60 * 60)

    finished_at = time.time()
    finished_at_iso = _iso_utc(finished_at)

    status = str(status_data.get("status") or "unknown").lower()
    wall_s = None if status == "timeout" else finished_at - submitted_at

    out_root = Path(args.out).expanduser()
    out_dir = out_root / args.label / bot_id
    artifacts = _download_artifacts(session, base_url, api_key, bot_id, out_dir)

    # Print only non-sensitive summary (safe to paste into investigation docs).
    print(
        json.dumps(
            {
                "bot_id": bot_id,
                "status": status,
                "submitted_at_utc": submitted_at_iso,
                "finished_at_utc": finished_at_iso,
                "wall_s": wall_s,
                "cache_version": env_vars.get("LUMIBOT_CACHE_S3_VERSION"),
                "artifacts_dir": str(out_dir),
                "artifacts": artifacts,
                "status_payload_keys": sorted(status_data.keys()),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
