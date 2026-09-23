"""Short real-token backtests for the example AI strategies.

One path only: Strategy.backtest on the class as written. No proof mode.
Stock jobs use Yahoo. The parent stops the batch at $25.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "research" / "2026-09-23-ai-strategy-backtests"
CAP_USD = 25.0
MODEL = "openai/gpt-6-luna"
INPUT_USD_PER_MILLION = 0.10
CACHED_INPUT_USD_PER_MILLION = 0.01
OUTPUT_USD_PER_MILLION = 0.50
TOKEN_LINE = re.compile(
    r"tokens_out=(\d+) tokens_cached_in=(\d+) tokens_uncached_in=(\d+) tokens_thinking=(\d+)"
)

WAVE2 = (
    {
        "name": "congress-pelosi-v2",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 30},
    },
    {
        "name": "buffett",
        "class_path": "lumibot.example_strategies.ai_trading_team_warren_buffett_value:AITradingTeamWarrenBuffettValueStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "source": "yahoo",
        "parameters": {
            "universe": ["AAPL", "KO", "AXP", "JPM"],
            "agent_max_model_calls": 30,
        },
    },
    {
        "name": "ackman",
        "class_path": "lumibot.example_strategies.ai_trading_team_bill_ackman_concentrated:AITradingTeamBillAckmanConcentratedStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "source": "yahoo",
        "parameters": {
            "universe": ["GOOGL", "CMG", "UBER", "HLT"],
            "agent_max_model_calls": 30,
        },
    },
    {
        "name": "congress-pelosi-v3",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 32},
    },
    {
        "name": "iron-condor",
        "class_path": "lumibot.example_strategies.ai_iron_condor:AIIronCondorStrategy",
        "start": "2026-01-05",
        "end": "2026-01-09",
        "source": "alpaca",
        "parameters": {"agent_max_model_calls": 32},
    },
)


WAVE3 = (
    {
        "name": "congress-pelosi-v3",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 32},
    },
    {
        "name": "iron-condor",
        "class_path": "lumibot.example_strategies.ai_iron_condor:AIIronCondorStrategy",
        "start": "2026-01-05",
        "end": "2026-01-09",
        "source": "alpaca",
        "parameters": {"agent_max_model_calls": 32},
    },
)


WAVE1 = (
    {
        "name": "large-cap",
        "class_path": "lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks:AITradingTeamBullBearLargeCapStocksStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "parameters": {
            "universe": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "agent_max_model_calls": 40,
        },
    },
    {
        "name": "leveraged-etf",
        "class_path": "lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf:AITradingTeamBullBearLeveragedETFStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "parameters": {
            "universe": ["TQQQ", "SQQQ", "UPRO", "SPXU"],
            "agent_max_model_calls": 40,
        },
    },
    {
        "name": "congress-pelosi",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "parameters": {"agent_max_model_calls": 40},
    },
    {
        "name": "vwap",
        "class_path": "lumibot.example_strategies.ai_vwap:AIVWAPStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "parameters": {"underlying": "SPY", "agent_max_model_calls": 40},
    },
)


def _load_openai_key() -> str:
    if os.environ.get("OPENAI_API_KEY"):
        return os.environ["OPENAI_API_KEY"]
    for path in (ROOT.parent / "botspot_agent" / ".env", ROOT / ".env.local", ROOT / ".env"):
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if line.startswith("OPENAI_API_KEY="):
                value = line.split("=", 1)[1].strip().strip('"').strip("'")
                if value:
                    return value
    raise SystemExit("OPENAI_API_KEY is missing")


def _env_file_value(filename: str, key: str) -> str:
    path = ROOT / filename
    if not path.exists():
        return ""
    prefix = key + "="
    for line in path.read_text().splitlines():
        if line.startswith(prefix):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _child_env(source: str) -> dict[str, str]:
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("ALPACA_"):
            env.pop(key, None)
    env["LUMIBOT_DISABLE_DOTENV"] = "1"
    env["LUMIBOT_DISABLE_DOTENV_LOCAL"] = "1"
    env["IS_BACKTESTING"] = "true"
    env["PYTHONUNBUFFERED"] = "1"
    env["AI_EXAMPLE_MODEL"] = MODEL
    env["OPENAI_API_KEY"] = _load_openai_key()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    if source == "alpaca":
        paper = _env_file_value(".env", "ALPACA_IS_PAPER").lower()
        if paper not in {"1", "true", "yes", "on"}:
            raise SystemExit("Alpaca backtest refused: .env is not marked paper")
        env["BACKTESTING_DATA_SOURCE"] = "alpaca"
        env["ALPACA_API_KEY"] = _env_file_value(".env", "ALPACA_API_KEY")
        env["ALPACA_API_SECRET"] = _env_file_value(".env", "ALPACA_API_SECRET")
        env["ALPACA_IS_PAPER"] = "True"
        if not env["ALPACA_API_KEY"] or not env["ALPACA_API_SECRET"]:
            raise SystemExit("Alpaca paper keys are missing from .env")
    elif source == "polygon":
        env["BACKTESTING_DATA_SOURCE"] = "polygon"
        env["POLYGON_API_KEY"] = _env_file_value(".env", "POLYGON_API_KEY")
        env["LUMIBOT_OPTION_CHAIN_MAX_DAYS"] = "60"
        env["POLYGON_MAX_RETRY_ATTEMPTS"] = "2"
        env["POLYGON_MAX_RETRY_SLEEP_SECONDS"] = "70"
        if not env["POLYGON_API_KEY"]:
            raise SystemExit("POLYGON_API_KEY is missing from .env")
    else:
        env["BACKTESTING_DATA_SOURCE"] = "yahoo"
    return env


def estimated_usd(text: str) -> float:
    total = 0.0
    for match in TOKEN_LINE.finditer(text):
        out = int(match.group(1))
        cached = int(match.group(2))
        uncached = int(match.group(3))
        thinking = int(match.group(4))
        total += (
            uncached * INPUT_USD_PER_MILLION
            + cached * CACHED_INPUT_USD_PER_MILLION
            + (out + thinking) * OUTPUT_USD_PER_MILLION
        ) / 1_000_000
    return total


def _run_one(job: dict) -> int:
    sys.path.insert(0, str(ROOT))
    source = job.get("source", "yahoo")
    if source == "alpaca":
        from lumibot.backtesting import AlpacaBacktesting

        datasource = AlpacaBacktesting
    elif source == "polygon":
        from lumibot.backtesting import PolygonDataBacktesting

        datasource = PolygonDataBacktesting
    else:
        from lumibot.backtesting import YahooDataBacktesting

        datasource = YahooDataBacktesting

    module_name, class_name = job["class_path"].split(":")
    module = __import__(module_name, fromlist=[class_name])
    strategy_cls = getattr(module, class_name)
    OUT.mkdir(parents=True, exist_ok=True)
    stem = OUT / job["name"]
    start = datetime.fromisoformat(job["start"])
    end = datetime.fromisoformat(job["end"])
    backtest_kwargs = {}
    if source == "alpaca":
        backtest_kwargs["config"] = {
            "API_KEY": os.environ["ALPACA_API_KEY"],
            "API_SECRET": os.environ["ALPACA_API_SECRET"],
            "PAPER": True,
        }
    strategy_cls.backtest(
        datasource,
        backtesting_start=start,
        backtesting_end=end,
        budget=100_000,
        parameters=job["parameters"],
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=True,
        tearsheet_file=str(stem.with_suffix(".html")),
        logfile=str(stem.with_suffix(".log")),
        save_logfile=True,
        quiet_logs=False,
        show_progress_bar=False,
        name=job["name"],
        **backtest_kwargs,
    )
    return 0


def _kill(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    os.killpg(proc.pid, signal.SIGTERM)


WAVE4 = (
    {
        "name": "congress-pelosi-v4",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 32},
    },
    {
        "name": "iron-condor-polygon",
        "class_path": "lumibot.example_strategies.ai_iron_condor:AIIronCondorStrategy",
        "start": "2026-01-05",
        "end": "2026-01-09",
        "source": "polygon",
        "parameters": {"agent_max_model_calls": 32},
    },
)


WAVE5 = (
    {
        "name": "congress-pelosi-v5",
        "class_path": "lumibot.example_strategies.ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 32},
    },
    {
        "name": "iron-condor-v5",
        "class_path": "lumibot.example_strategies.ai_iron_condor:AIIronCondorStrategy",
        "start": "2026-01-05",
        "end": "2026-01-23",
        "source": "polygon",
        "parameters": {"agent_max_model_calls": 48},
    },
)


WAVE6 = (
    {
        "name": "large-cap-long",
        "class_path": "lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks:AITradingTeamBullBearLargeCapStocksStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {
            "universe": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "agent_max_model_calls": 80,
        },
    },
    {
        "name": "leveraged-etf-long",
        "class_path": "lumibot.example_strategies.ai_trading_team_bull_bear_leveraged_etf:AITradingTeamBullBearLeveragedETFStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {
            "universe": ["TQQQ", "SQQQ", "UPRO", "SPXU"],
            "agent_max_model_calls": 80,
        },
    },
    {
        "name": "sec-insider",
        "class_path": "lumibot.example_strategies.ai_sec_insider_filings:AISECInsiderFilingsStrategy",
        "start": "2026-01-05",
        "end": "2026-01-08",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 24},
    },
    {
        "name": "public-fetch",
        "class_path": "lumibot.example_strategies.ai_public_web_fetch:AIPublicWebFetchStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 24},
    },
)


def _jobs(wave: str) -> tuple[dict, ...]:
    if wave == "6":
        return WAVE6
    if wave == "5":
        return WAVE5
    if wave == "4":
        return WAVE4
    if wave == "3":
        return WAVE3
    if wave == "2":
        return WAVE2
    return WAVE1


def _find_job(name: str) -> dict:
    for job in (*WAVE1, *WAVE2, *WAVE3, *WAVE4, *WAVE5, *WAVE6):
        if job["name"] == name:
            return job
    raise SystemExit(f"unknown job {name}")


def _parent(wave: str, already: float) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    procs: list[tuple[dict, subprocess.Popen, Path]] = []
    for job in _jobs(wave):
        log_path = OUT / f"{job['name']}.stdout"
        handle = open(log_path, "w")
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__)), "--job", job["name"]],
            cwd=str(ROOT),
            env=_child_env(job.get("source", "yahoo")),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        handle.close()
        procs.append((job, proc, log_path))
        print(f"started {job['name']} pid={proc.pid}", flush=True)

    stopped_for_cap = False
    while True:
        blobs = []
        for _job, _proc, log_path in procs:
            if log_path.exists():
                blobs.append(log_path.read_text(errors="replace"))
        spend = already + estimated_usd("\n".join(blobs))
        (OUT / "spend.txt").write_text(f"{spend:.4f}\n")
        alive = [proc for _job, proc, _path in procs if proc.poll() is None]
        print(f"spend=${spend:.4f} alive={len(alive)}", flush=True)
        if spend >= CAP_USD and alive:
            stopped_for_cap = True
            print(f"cap ${CAP_USD:.0f} hit, stopping batch", flush=True)
            for proc in alive:
                _kill(proc)
            break
        if not alive:
            break
        time.sleep(20)

    codes = []
    for job, proc, _path in procs:
        try:
            code = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            _kill(proc)
            code = proc.wait(timeout=10)
        codes.append((job["name"], code))
        print(f"finished {job['name']} code={code}", flush=True)
    if stopped_for_cap:
        return 2
    return 0 if all(code == 0 for _name, code in codes) else 1


def _flag(name: str, default: str) -> str:
    if name not in sys.argv:
        return default
    return sys.argv[sys.argv.index(name) + 1]


def main() -> int:
    if "--job" in sys.argv:
        return _run_one(_find_job(_flag("--job", "")))
    return _parent(_flag("--wave", "1"), float(_flag("--already", "0")))


if __name__ == "__main__":
    raise SystemExit(main())
