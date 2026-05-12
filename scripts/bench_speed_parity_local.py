#!/usr/bin/env python3
"""
Local speed parity benchmark runner (Strategy Library demos).

This runs the same benchmark windows as bench_speed_parity_prod.py, but locally, using:
  - prod-like flags (tearsheet/indicators/progress/logs enabled)
  - BACKTESTING_PROFILE=yappi to generate *_profile_yappi.csv artifacts

Secrets
-------
This script reads secrets from botspot_node/.env-local and injects them into the subprocess
environment WITHOUT printing them. Do not add prints of env values.

Why this exists
---------------
To compare:
  - local (warm disk cache)
  - local (S3 remote cache enabled)
against production runs, using identical strategy code + windows.

Note: local runs will often look *much* faster because the local cache persists between runs,
whereas production runs execute in fresh ECS tasks with empty local filesystem each time.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import re
import subprocess
import time
from collections.abc import Iterable
from pathlib import Path

DEFAULT_DOTENV_PATH = Path.home() / "Documents/Development/botspot_node/.env-local"
SAFE_TIMEOUT = Path.home() / "bin/safe-timeout"


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


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _glob_set(pattern: str) -> set[Path]:
    return set(Path().glob(pattern))


def _newest(paths: Iterable[Path]) -> Path | None:
    paths = list(paths)
    if not paths:
        return None
    return max(paths, key=lambda p: p.stat().st_mtime)


@dataclasses.dataclass(frozen=True)
class LocalRunSpec:
    name: str
    demo_file: str
    start_date: str
    end_date: str
    repeats: int = 2


@dataclasses.dataclass
class LocalRunResult:
    spec_name: str
    run_index: int
    yappi_csv: str | None
    settings_json: str | None
    logs_csv: str | None
    wall_s: float


def _build_env(dotenv: dict[str, str], *, lumibot_path: Path) -> dict[str, str]:
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

    env = dict(os.environ)
    env.update(
        {
            # Ensure local uses source LumiBot
            "PYTHONPATH": str(lumibot_path),
            # Backtesting mode
            "IS_BACKTESTING": "True",
            "BACKTESTING_DATA_SOURCE": "thetadata",
            "BACKTESTING_START": "${BACKTESTING_START}",  # placeholder, overwritten per run
            "BACKTESTING_END": "${BACKTESTING_END}",  # placeholder, overwritten per run
            # Downloader
            "DATADOWNLOADER_BASE_URL": dotenv["DATADOWNLOADER_BASE_URL"],
            "DATADOWNLOADER_API_KEY": dotenv["DATADOWNLOADER_API_KEY"],
            "DATADOWNLOADER_API_KEY_HEADER": dotenv["DATADOWNLOADER_API_KEY_HEADER"],
            # Remote cache
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
            # Profiling
            "BACKTESTING_PROFILE": "yappi",
        }
    )
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local speed parity benchmarks.")
    parser.add_argument("--dotenv", type=str, default=str(DEFAULT_DOTENV_PATH), help="Path to botspot_node .env-local")
    parser.add_argument("--out", type=str, default=str(Path.home() / "Documents/Development/Strategy Library/logs/local_profiles_bench"), help="Output directory for summaries")
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv).expanduser()
    dotenv = _parse_dotenv(dotenv_path)

    strategy_library = Path.home() / "Documents/Development/Strategy Library"
    demos_dir = strategy_library / "Demos"
    logs_dir = strategy_library / "logs"
    out_dir = Path(args.out).expanduser()
    _safe_mkdir(out_dir)

    lumibot_path = Path.home() / "Documents/Development/lumivest_bot_server/strategies/lumibot"
    base_env = _build_env(dotenv, lumibot_path=lumibot_path)

    specs = [
        LocalRunSpec(
            name="spx_short_straddle_short",
            demo_file="SPX Short Straddle Intraday (Copy).py",
            start_date="2024-01-22",
            end_date="2024-01-26",
            repeats=2,
        ),
        LocalRunSpec(
            name="spx_short_straddle_month",
            demo_file="SPX Short Straddle Intraday (Copy).py",
            start_date="2024-01-22",
            end_date="2024-02-22",
            repeats=2,
        ),
        LocalRunSpec(
            name="backdoor_butterfly_short",
            demo_file="Backdoor Butterfly 0 DTE (Copy).py",
            start_date="2024-01-22",
            end_date="2024-01-26",
            repeats=2,
        ),
        LocalRunSpec(
            name="backdoor_butterfly_month",
            demo_file="Backdoor Butterfly 0 DTE (Copy).py",
            start_date="2024-01-22",
            end_date="2024-02-22",
            repeats=2,
        ),
    ]

    all_results: list[LocalRunResult] = []

    os.chdir(strategy_library)
    for spec in specs:
        for run_idx in range(spec.repeats):
            # Snapshot files before run
            before_yappi = set(logs_dir.glob("*_profile_yappi.csv"))
            before_settings = set(logs_dir.glob("*_settings.json"))
            before_logs = set(logs_dir.glob("*_logs.csv"))

            env = dict(base_env)
            env["BACKTESTING_START"] = spec.start_date
            env["BACKTESTING_END"] = spec.end_date

            cmd = [
                str(SAFE_TIMEOUT),
                "2400s",
                "python3",
                str(demos_dir / spec.demo_file),
            ]

            started = time.time()
            subprocess.run(cmd, env=env, check=True)
            finished = time.time()

            after_yappi = set(logs_dir.glob("*_profile_yappi.csv"))
            after_settings = set(logs_dir.glob("*_settings.json"))
            after_logs = set(logs_dir.glob("*_logs.csv"))

            new_yappi = _newest(after_yappi - before_yappi)
            new_settings = _newest(after_settings - before_settings)
            new_logs = _newest(after_logs - before_logs)

            result = LocalRunResult(
                spec_name=spec.name,
                run_index=run_idx + 1,
                yappi_csv=str(new_yappi) if new_yappi else None,
                settings_json=str(new_settings) if new_settings else None,
                logs_csv=str(new_logs) if new_logs else None,
                wall_s=finished - started,
            )
            all_results.append(result)
            print(
                json.dumps(
                    {
                        "spec": result.spec_name,
                        "run": result.run_index,
                        "wall_s": result.wall_s,
                        "yappi_csv": result.yappi_csv,
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )

    summary_path = out_dir / f"bench_summary_{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    summary_path.write_text(json.dumps([dataclasses.asdict(r) for r in all_results], indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote local benchmark summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

