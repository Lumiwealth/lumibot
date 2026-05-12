#!/usr/bin/env python3
"""
Production speed parity benchmark runner (Bot Manager API).

Purpose
-------
Run small, repeatable production backtests with BACKTESTING_PROFILE=yappi enabled, and
download artifacts locally so we can attribute runtime to:
  - downloader/queue (should be ~0 on warm cache)
  - S3 remote-cache hydration
  - compute
  - artifact generation
  - progress/log overhead

This script is designed to be safe to run locally:
  - It reads secrets from botspot_node/.env-local (never prints secrets).
  - It writes downloaded artifacts into Strategy Library/logs/prod_profiles_bench/.

Notes
-----
- Backtests run as separate ECS tasks, so "warm cache" here means:
    1) Run #1 may populate S3 cache objects
    2) Run #2 should avoid *ThetaData downloader* calls (queue submits -> 0)
  Even on a warm run, production may still be slower than local because each task still
  needs to hydrate cached data from S3 into its local filesystem.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import random
import re
import time
from collections.abc import Iterable
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


def _iso_utc(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.UTC).isoformat().replace("+00:00", "Z")


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_strategy(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _random_suffix(length: int = 6) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(alphabet) for _ in range(length))


@dataclasses.dataclass(frozen=True)
class RunSpec:
    name: str
    strategy_file: Path
    start_date: str
    end_date: str
    profile: bool = True
    repeats: int = 2


@dataclasses.dataclass
class RunResult:
    spec_name: str
    bot_id: str
    status: str
    submitted_at_utc: str
    finished_at_utc: str | None
    wall_s: float | None
    queue_submits: int | None
    artifacts_dir: str
    notes: dict[str, Any]


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


def _count_log_events(
    session: requests.Session,
    base_url: str,
    api_key: str,
    bot_id: str,
    *,
    start_time_iso: str,
    end_time_iso: str,
    query: str,
) -> int:
    headers = _http_headers(api_key)
    url = f"{base_url}/logs"
    next_token: str | None = None
    count = 0

    # CloudWatch filter_log_events caps at 10k events per call; BotManager paginates via next_token.
    while True:
        params = {
            "bot_id": bot_id,
            "limit": 10000,
            "sort": "asc",
            "start_time_iso": start_time_iso,
            "end_time_iso": end_time_iso,
            "query": query,
        }
        if next_token:
            params["next_token"] = next_token

        resp = session.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        events = payload.get("events") or []
        count += len(events)
        next_token = payload.get("next_token")
        if not next_token:
            break
    return count


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

    # Always try to download these; missing files are treated as non-fatal.
    for file_type, suffix in (
        ("profile_yappi_csv", "profile_yappi.csv"),
        ("completion_json", "completion.json"),
        ("stats_csv", "stats_csv"),
        ("trades_csv", "trades_csv"),
    ):
        try:
            _save(file_type, suffix)
        except Exception:
            continue

    return artifacts


def _build_backtest_env(dotenv: dict[str, str]) -> dict[str, str]:
    """
    Environment variables passed into the backtest container via bot_config.

    Important: this function must NOT print secrets.
    """
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

    return {
        "IS_BACKTESTING": "True",
        "BACKTESTING_DATA_SOURCE": "thetadata",
        "DATADOWNLOADER_BASE_URL": dotenv["DATADOWNLOADER_BASE_URL"],
        "DATADOWNLOADER_API_KEY": dotenv["DATADOWNLOADER_API_KEY"],
        "DATADOWNLOADER_API_KEY_HEADER": dotenv["DATADOWNLOADER_API_KEY_HEADER"],
        "LUMIBOT_CACHE_BACKEND": dotenv["LUMIBOT_CACHE_BACKEND"],
        "LUMIBOT_CACHE_MODE": dotenv["LUMIBOT_CACHE_MODE"],
        "LUMIBOT_CACHE_S3_BUCKET": dotenv["LUMIBOT_CACHE_S3_BUCKET"],
        "LUMIBOT_CACHE_S3_PREFIX": dotenv["LUMIBOT_CACHE_S3_PREFIX"],
        "LUMIBOT_CACHE_S3_REGION": dotenv["LUMIBOT_CACHE_S3_REGION"],
        "LUMIBOT_CACHE_S3_VERSION": dotenv["LUMIBOT_CACHE_S3_VERSION"],
        "LUMIBOT_CACHE_S3_ACCESS_KEY_ID": dotenv["LUMIBOT_CACHE_S3_ACCESS_KEY_ID"],
        "LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY": dotenv["LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY"],
        # Prod-like artifacts/logging flags
        "SHOW_PLOT": "True",
        "SHOW_INDICATORS": "True",
        "SHOW_TEARSHEET": "True",
        "BACKTESTING_QUIET_LOGS": "false",
        "BACKTESTING_SHOW_PROGRESS_BAR": "true",
        # Opt-in profiling artifact
        "BACKTESTING_PROFILE": "yappi",
    }


def run_prod_benchmark(spec: RunSpec, *, base_url: str, api_key: str, dotenv: dict[str, str], out_root: Path) -> Iterable[RunResult]:
    strategy_code = _read_strategy(spec.strategy_file)
    env_vars = _build_backtest_env(dotenv)

    session = requests.Session()
    headers = _http_headers(api_key)

    for i in range(spec.repeats):
        bot_id = f"bench-{spec.name}-{spec.start_date.replace('-', '')}-{spec.end_date.replace('-', '')}-{_random_suffix(8)}"
        payload = {
            "bot_id": bot_id,
            "main": strategy_code,
            "start_date": spec.start_date,
            "end_date": spec.end_date,
            "bot_config": env_vars,
        }

        submitted_at = time.time()
        submitted_at_iso = _iso_utc(submitted_at)

        # Submit backtest
        session.post(f"{base_url}/backtest", json=payload, headers=headers, timeout=60).raise_for_status()

        # Wait for completion
        status_data = _wait_for_terminal_status(session, base_url, api_key, bot_id, timeout_s=60 * 60)
        status = str(status_data.get("status") or "unknown").lower()

        finished_at = time.time()
        finished_at_iso = _iso_utc(finished_at)

        wall_s = None
        if status != "timeout":
            wall_s = finished_at - submitted_at

        # Count queue submits in logs (proxy for downloader usage)
        queue_submits = None
        try:
            queue_submits = _count_log_events(
                session,
                base_url,
                api_key,
                bot_id,
                start_time_iso=submitted_at_iso,
                end_time_iso=finished_at_iso,
                query="Submitted to queue:",
            )
        except Exception:
            queue_submits = None

        # Download artifacts
        artifacts_dir = out_root / spec.name
        artifacts = _download_artifacts(session, base_url, api_key, bot_id, artifacts_dir)

        yield RunResult(
            spec_name=spec.name,
            bot_id=bot_id,
            status=status,
            submitted_at_utc=submitted_at_iso,
            finished_at_utc=finished_at_iso if status != "timeout" else None,
            wall_s=wall_s,
            queue_submits=queue_submits,
            artifacts_dir=str(artifacts_dir),
            notes={
                "start_date": spec.start_date,
                "end_date": spec.end_date,
                "repeat_index": i + 1,
                "artifacts": artifacts,
            },
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run production speed parity benchmarks via Bot Manager API.")
    parser.add_argument("--dotenv", type=str, default=None, help="Path to botspot_node .env-local (default: auto-detect)")
    parser.add_argument("--out", type=str, default=str(Path.home() / "Documents/Development/Strategy Library/logs/prod_profiles_bench"), help="Output directory for downloaded artifacts")
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv).expanduser() if args.dotenv else None
    dotenv = _load_config(dotenv_path)

    base_url = dotenv.get("BACKTEST_SERVICE_URL", "").rstrip("/")
    api_key = dotenv.get("BACKTEST_API_KEY", "")
    if not base_url or not api_key:
        raise RuntimeError("BACKTEST_SERVICE_URL or BACKTEST_API_KEY missing from dotenv.")

    out_root = Path(args.out).expanduser()
    _safe_mkdir(out_root)

    demo_root = Path.home() / "Documents/Development/Strategy Library/Demos"
    specs = [
        RunSpec(
            name="spx_short_straddle_short",
            strategy_file=demo_root / "SPX Short Straddle Intraday (Copy).py",
            start_date="2024-01-22",
            end_date="2024-01-26",
            repeats=2,
        ),
        RunSpec(
            name="spx_short_straddle_month",
            strategy_file=demo_root / "SPX Short Straddle Intraday (Copy).py",
            start_date="2024-01-22",
            end_date="2024-02-22",
            repeats=2,
        ),
        RunSpec(
            name="backdoor_butterfly_short",
            strategy_file=demo_root / "Backdoor Butterfly 0 DTE (Copy).py",
            start_date="2024-01-22",
            end_date="2024-01-26",
            repeats=2,
        ),
        RunSpec(
            name="backdoor_butterfly_month",
            strategy_file=demo_root / "Backdoor Butterfly 0 DTE (Copy).py",
            start_date="2024-01-22",
            end_date="2024-02-22",
            repeats=2,
        ),
    ]

    results: list[RunResult] = []
    for spec in specs:
        for run in run_prod_benchmark(spec, base_url=base_url, api_key=api_key, dotenv=dotenv, out_root=out_root):
            results.append(run)
            # Print only non-sensitive summary
            print(
                json.dumps(
                    {
                        "spec": run.spec_name,
                        "bot_id": run.bot_id,
                        "status": run.status,
                        "wall_s": run.wall_s,
                        "queue_submits": run.queue_submits,
                        "artifacts_dir": run.artifacts_dir,
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )

    summary_path = out_root / f"bench_summary_{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    summary_path.write_text(
        json.dumps([dataclasses.asdict(r) for r in results], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"Wrote benchmark summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

