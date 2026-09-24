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
CAP_USD = 35.0
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
    # Alpaca runs in environment mode (no config), the way BotSpot runs it. An explicit
    # config selects the legacy defaults: history includes the still-forming bar (a
    # one-bar lookahead), no history before the start, and an early stop three trading
    # days before backtesting_end.
    backtest_kwargs = {}
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


_EX = "lumibot.example_strategies."

WAVE7 = (
    {
        "name": "iron-condor-luna",
        "class_path": _EX + "ai_iron_condor:AIIronCondorStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "alpaca",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        "name": "credit-spread-luna",
        "class_path": _EX + "ai_credit_spread:AICreditSpreadStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "alpaca",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        # Alpaca lists no SPX/SPXW contracts, so this run uses SPY 0DTE ($1 strikes).
        "name": "spy-0dte-luna",
        "class_path": _EX + "ai_spx_zero_dte_bear_call_team:AISpxZeroDteBearCallTeamStrategy",
        "start": "2026-01-06",
        "end": "2026-01-08",
        "source": "alpaca",
        "parameters": {
            "underlying": "SPY",
            "wing_width": 1,
            "agent_max_model_calls": 32,
            "sleeptime": "2H",
        },
    },
)


WAVE8 = (
    {
        "name": "large-cap-luna",
        "class_path": _EX + "ai_trading_team_bull_bear_large_cap_stocks:AITradingTeamBullBearLargeCapStocksStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"universe": ["AAPL", "MSFT", "NVDA", "AMZN"], "agent_max_model_calls": 80},
    },
    {
        "name": "leveraged-etf-luna",
        "class_path": _EX + "ai_trading_team_bull_bear_leveraged_etf:AITradingTeamBullBearLeveragedETFStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"universe": ["TQQQ", "SQQQ", "UPRO", "SPXU"], "agent_max_model_calls": 80},
    },
    {
        "name": "congress-pelosi-luna",
        "class_path": _EX + "ai_congress_disclosures:AICongressDisclosuresStrategy",
        "start": "2026-01-22",
        "end": "2026-01-30",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        "name": "buffett-luna",
        "class_path": _EX + "ai_trading_team_warren_buffett_value:AITradingTeamWarrenBuffettValueStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        "name": "ackman-luna",
        "class_path": _EX + "ai_trading_team_bill_ackman_concentrated:AITradingTeamBillAckmanConcentratedStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        "name": "vwap-luna",
        "class_path": _EX + "ai_vwap:AIVWAPStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "alpaca",
        "parameters": {"underlying": "SPY", "agent_max_model_calls": 48},
    },
    {
        "name": "orb-luna",
        "class_path": _EX + "ai_opening_range_breakout:AIOpeningRangeBreakoutStrategy",
        "start": "2026-01-05",
        "end": "2026-01-07",
        "source": "alpaca",
        "parameters": {"agent_max_model_calls": 48},
    },
    {
        "name": "sec-insider-luna",
        "class_path": _EX + "ai_sec_insider_filings:AISECInsiderFilingsStrategy",
        "start": "2026-01-05",
        "end": "2026-01-09",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 32},
    },
    {
        "name": "public-fetch-luna",
        "class_path": _EX + "ai_public_web_fetch:AIPublicWebFetchStrategy",
        "start": "2026-01-22",
        "end": "2026-01-27",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 24},
    },
    {
        "name": "researcher-trader-luna",
        "class_path": _EX + "ai_researcher_trader:ResearcherTraderStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"symbol": "SPY", "max_position_pct": 10},
    },
)


# Reruns of the Alpaca options jobs after trade-only backtests gained last-trade
# pricing. The 0DTE team needs about 5 model calls per iteration.
WAVE9 = (
    {**WAVE7[0], "name": "iron-condor-luna-v2"},
    {**WAVE7[1], "name": "credit-spread-luna-v2"},
    {
        **WAVE7[2],
        "name": "spy-0dte-luna-v2",
        "parameters": {**WAVE7[2]["parameters"], "agent_max_model_calls": 80},
    },
)


# Reruns after the interpreters started judging against each strategy's policy.
WAVE10 = tuple({**job, "name": job["name"].replace("-v2", "-v3")} for job in WAVE9)


def _wave8(name: str) -> dict:
    return next(job for job in WAVE8 if job["name"] == name)


# Reruns after the no-cash-parking trader rule, the intraday session guards,
# and the contract-cap sizing rule. Intraday runs use 2H so each is ~9 cycles.
WAVE11 = (
    {**_wave8("leveraged-etf-luna"), "name": "leveraged-etf-luna-v2"},
    {
        **_wave8("buffett-luna"),
        "name": "buffett-luna-v2",
        "parameters": {"agent_max_model_calls": 64},
    },
    {
        **_wave8("vwap-luna"),
        "name": "vwap-luna-v2",
        "end": "2026-01-07",
        "parameters": {"underlying": "SPY", "sleeptime": "2H", "agent_max_model_calls": 64},
    },
    {
        **_wave8("orb-luna"),
        "name": "orb-luna-v2",
        "parameters": {"sleeptime": "2H", "agent_max_model_calls": 64},
    },
    {**WAVE9[2], "name": "spy-0dte-luna-v4"},
)


# Rerun after the insider team switched to point-in-time Form 4 filings for a
# fixed watchlist. Each cycle opens several filings, so it needs more calls.
WAVE12 = (
    {
        **_wave8("sec-insider-luna"),
        "name": "sec-insider-luna-v2",
        "parameters": {"lookback_days": 14, "agent_max_model_calls": 96},
    },
)


# Reruns after the limit-order guidance fix and the DuckDB table mode for wide
# history scans. The 14-day insider lookback spanned the holidays and found no
# open-market Form 4 trades, so the insider rerun looks back 45 days.
WAVE13 = (
    {**WAVE11[0], "name": "leveraged-etf-luna-v3"},
    {**WAVE11[3], "name": "orb-luna-v3"},
    {
        **WAVE12[0],
        "name": "sec-insider-luna-v3",
        "parameters": {"lookback_days": 45, "agent_max_model_calls": 96},
    },
)


# The 0 DTE team now holds a package and exits on later cycles, so it needs an
# hourly cadence. VWAP signals went stale between 2H cycles.
WAVE14 = (
    {
        **WAVE11[4],
        "name": "spy-0dte-luna-v5",
        "parameters": {**WAVE11[4]["parameters"], "sleeptime": "1H"},
    },
    {
        **WAVE11[2],
        "name": "vwap-luna-v3",
        "parameters": {**WAVE11[2]["parameters"], "sleeptime": "30M"},
    },
)


# At 30M the VWAP team averages about six model calls per cycle, so three
# sessions need roughly 230 calls. The 64 cap stopped v3 after the first day.
WAVE15 = (
    {
        **WAVE14[1],
        "name": "vwap-luna-v4",
        "parameters": {**WAVE14[1]["parameters"], "agent_max_model_calls": 260},
    },
)


# v3 stopped after two of ten buys because account cash still read $100,000
# after fills. Rerun after backtest fills started draining mid-iteration.
WAVE16 = ({**WAVE13[2], "name": "sec-insider-luna-v4"},)

# v1 refused the House PDF as lookahead because http_request stamped it with
# the fetch date. Rerun after published_at began honoring Last-Modified.
WAVE17 = ({**_wave8("public-fetch-luna"), "name": "public-fetch-luna-v2"},)


# v4 rejected the Jan 6 reclaim as stale between 30M evaluations, and public
# fetch v2 refused a published disclosure because it named no size for us.
# Rerun both after those prompt fixes; public fetch runs past Jan 26 so the
# disclosure is published inside the window. Pelosi reruns after the fill fix.
WAVE18 = (
    {**WAVE15[0], "name": "vwap-luna-v5"},
    {
        **_wave8("public-fetch-luna"),
        "name": "public-fetch-luna-v3",
        "end": "2026-01-30",
        "parameters": {"agent_max_model_calls": 48},
    },
    {**_wave8("congress-pelosi-luna"), "name": "congress-pelosi-luna-v2"},
)

# Both bull-bear books sold every position each morning and bought it back,
# because the exit rule forced it. Rerun after the rebalance exit rule.
WAVE19 = (
    {**_wave8("large-cap-luna"), "name": "large-cap-luna-v2"},
    {**_wave8("leveraged-etf-luna"), "name": "leveraged-etf-luna-v4"},
)

# Rerun after the cash rule and the one-direction-per-index rule.
WAVE20 = (
    {**_wave8("leveraged-etf-luna"), "name": "leveraged-etf-luna-v5"},
    {**_wave8("congress-pelosi-luna"), "name": "congress-pelosi-luna-v3"},
)

# Large-cap v2 ended $342 below zero cash; rerun under the cash rule.
WAVE21 = ({**_wave8("large-cap-luna"), "name": "large-cap-luna-v3"},)

# Pelosi v3 refused every ticker because the research line had no asset code.
WAVE22 = ({**_wave8("congress-pelosi-luna"), "name": "congress-pelosi-luna-v4"},)

# Leveraged v5 bought TQQQ on Jan 9 while still holding part of SQQQ.
WAVE23 = ({**_wave8("leveraged-etf-luna"), "name": "leveraged-etf-luna-v6"},)

# Leveraged v6 skipped every rebalance after netting; rerun after the rescale rule.
WAVE24 = ({**_wave8("leveraged-etf-luna"), "name": "leveraged-etf-luna-v7"},)

# ORB v3 and insider v4 ran before the churn, cash, and rescale rules.
WAVE25 = (
    {**WAVE11[3], "name": "orb-luna-v4"},
    {**WAVE13[2], "name": "sec-insider-luna-v5"},
)

# Every run that started before the 15:56 fill-drain fix, rerun on current code.
WAVE26 = (
    {**_wave8("ackman-luna"), "name": "ackman-luna-v2"},
    {**WAVE11[1], "name": "buffett-luna-v3"},
    {**_wave8("researcher-trader-luna"), "name": "researcher-trader-luna-v2"},
    {**WAVE10[0], "name": "iron-condor-luna-v4"},
    {**WAVE10[1], "name": "credit-spread-luna-v4"},
    {**WAVE14[0], "name": "spy-0dte-luna-v6"},
)

# researcher-trader v2 refused to trade because daily bar dates were NaT.
WAVE27 = ({**_wave8("researcher-trader-luna"), "name": "researcher-trader-luna-v3"},)

# Citadel and Ray Dalio have no Luna backtest. Insider and VWAP get a longer
# window so a real filing or VWAP signal can show up. ORB v5 submits the
# take-profit and the stop in the same session as the entry.
WAVE28 = (
    {
        "name": "citadel-luna",
        "class_path": _EX + "ai_trading_team_citadel_sector_pods:AITradingTeamCitadelSectorPodsStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 120},
    },
    {
        "name": "ray-dalio-luna",
        "class_path": _EX + "ai_trading_team_ray_dalio_idea_meritocracy:AITradingTeamRayDalioIdeaMeritocracyStrategy",
        "start": "2026-01-05",
        "end": "2026-01-16",
        "source": "yahoo",
        "parameters": {"agent_max_model_calls": 96},
    },
    {
        **WAVE13[2],
        "name": "sec-insider-luna-v6",
        "end": "2026-02-13",
        "parameters": {"lookback_days": 45, "agent_max_model_calls": 160},
    },
    {
        **WAVE15[0],
        "name": "vwap-luna-v6",
        "end": "2026-01-16",
        "parameters": {**WAVE15[0]["parameters"], "sleeptime": "1H", "agent_max_model_calls": 400},
    },
    {**WAVE11[3], "name": "orb-luna-v5"},
)

# Credit spread v4 never reached an exit. This window runs through early February
# so the 21-day, 50 percent, and loss exits have time to fire.
WAVE29 = (
    {
        **WAVE10[1],
        "name": "credit-spread-luna-v5",
        "end": "2026-02-06",
        "parameters": {"agent_max_model_calls": 140},
    },
)


def _jobs(wave: str) -> tuple[dict, ...]:
    # "7,8" runs several waves under one parent so the spend cap is shared.
    if "," in wave:
        return tuple(job for part in wave.split(",") for job in _jobs(part.strip()))
    waves = _waves()
    if wave not in waves:
        raise SystemExit(f"unknown wave {wave}; known: {', '.join(sorted(waves, key=int))}")
    return waves[wave]


def _waves() -> dict[str, tuple[dict, ...]]:
    # The parent picks a wave and each child looks its job up by name, so both
    # must read the same table or a new wave launches children that cannot start.
    return {
        name[len("WAVE"):]: jobs
        for name, jobs in globals().items()
        if name.startswith("WAVE") and name[len("WAVE"):].isdigit()
    }


def _find_job(name: str) -> dict:
    for jobs in _waves().values():
        for job in jobs:
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
