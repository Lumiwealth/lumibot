"""AI-only multi-ticker opening-range breakout strategy.

Python only creates and runs a LumiBot agent. All entry, exit, sizing, and
ticker selection live in the system prompt. Prefer minute bars when available.

Local backtest:
    GEMINI_API_KEY=... BACKTESTING_DATA_SOURCE=ThetaData \
        python -m lumibot.example_strategies.ai_opening_range_breakout

Optional env overrides (AI_ORB_*):
    AI_ORB_UNIVERSE=SPY,QQQ,AAPL,...
    AI_ORB_OPENING_RANGE_MINUTES=15
    AI_ORB_RISK_FRACTION=0.01
    AI_ORB_MAX_SHARES=200
    AI_ORB_MAX_POSITIONS=1
    AI_ORB_PROFIT_R_MULTIPLE=1.5
    AI_ORB_SLEEPTIME=1H
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.strategies.strategy import Strategy

# Default liquid US mega/large-cap + major ETFs (~100 names) for ORB scanning.
_DEFAULT_ORB_UNIVERSE = (
    "SPY,QQQ,IWM,DIA,XLK,XLF,XLE,XLI,XLV,XLY,XLP,XLU,XLB,XLRE,XLC,"
    "AAPL,MSFT,NVDA,AMZN,GOOGL,GOOG,META,TSLA,BRK.B,JPM,V,UNH,XOM,JNJ,WMT,"
    "MA,PG,HD,CVX,MRK,ABBV,PEP,KO,COST,AVGO,LLY,BAC,TMO,CRM,MCD,CSCO,ACN,"
    "AMD,ADBE,NFLX,TXN,INTC,QCOM,INTU,AMAT,NOW,ORCL,IBM,UBER,ABT,DHR,PFE,"
    "PM,WFC,MS,GS,BLK,SCHW,AXP,C,BA,CAT,GE,HON,UPS,RTX,DE,LMT,UNP,LOW,"
    "NKE,SBUX,TGT,MDT,ISRG,SYK,GILD,AMGN,VRTX,BKNG,TJX,CMCSA,DIS,"
    "T,VZ,NEE,SO,DUK,LIN,COP,SLB,PLD,AMT,EQIX,SPGI,CME,ICE,PYPL,SHOP"
)


def _parse_universe(raw: str | None) -> list[str]:
    text = str(raw or _DEFAULT_ORB_UNIVERSE)
    symbols: list[str] = []
    seen: set[str] = set()
    for part in text.replace("\n", ",").split(","):
        symbol = part.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols or ["SPY"]


def build_orb_system_prompt(params: dict) -> str:
    universe = params.get("universe") or _parse_universe(None)
    if isinstance(universe, str):
        universe = _parse_universe(universe)
    universe = [str(symbol).strip().upper() for symbol in universe if str(symbol).strip()]
    if not universe:
        universe = ["SPY"]
    universe_csv = ",".join(universe)
    universe_count = len(universe)
    opening_range_minutes = int(params.get("opening_range_minutes", 15))
    risk_fraction = float(params.get("risk_fraction", 0.01))
    max_shares = int(params.get("max_shares", 200))
    max_positions = int(params.get("max_positions", 1))
    profit_r_multiple = float(params.get("profit_r_multiple", 1.5))
    return f"""
You are the complete decision-maker for an AI-only multi-ticker opening-range
breakout strategy inside LumiBot. There is no Python trading logic outside you.

STRATEGY PARAMETERS:
- universe ({universe_count} symbols): {universe_csv}
- opening_range_minutes: {opening_range_minutes}
- risk_fraction: {risk_fraction}
- max_shares: {max_shares}
- max_positions: {max_positions}
- profit_r_multiple: {profit_r_multiple}

Rules:
1. Scan the full provided universe and build each symbol's opening range from the
   first {opening_range_minutes} completed minutes of the regular US cash session,
   beginning at 09:30 ET. Skip symbols whose true opening window is unavailable.
2. A valid long breakout requires the latest completed bar's close to be strictly
   greater than that symbol's opening-range high (close > OR high), with confirming
   volume when available. A close equal to or below the OR high is not a breakout.
   Prefer the strongest valid breakout by percent extension above the range high
   and liquidity. Short only when shorting is allowed and evidence is equally clear.
3. Hold at most {max_positions} positions. If already at max_positions, manage exits
   only; do not open another name.
4. Size so approximate stop risk is at most {risk_fraction:.2%} of portfolio value,
   capped at {max_shares} shares. Stop is the opposite side of that symbol's range.
5. Take profit near {profit_r_multiple}R or exit on a close back inside the range.
6. Open at most one new position per symbol per trading day.

Use only evidence available at the current runtime datetime. A no-trade decision
is valid when no universe member has a complete opening range and valid breakout.
""".strip()


class AIOpeningRangeBreakoutStrategy(Strategy):
    parameters = {
        "universe": _parse_universe(_DEFAULT_ORB_UNIVERSE),
        "opening_range_minutes": 15,
        "risk_fraction": 0.01,
        "max_shares": 200,
        "max_positions": 1,
        "profit_r_multiple": 1.5,
        # The agent still analyzes minute bars, but hourly decisions avoid needless calls.
        "sleeptime": "1H",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "1H"))
        self.agents.create(
            name="orb",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_orb_system_prompt(self.parameters),
            rules_path=Path(__file__).with_name("agent_rules") / "ai_opening_range_breakout.rules.json",
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        universe = params.get("universe") or []
        if isinstance(universe, str):
            universe = _parse_universe(universe)
        universe_count = len(universe) if isinstance(universe, list) else 0
        self.agents["orb"].run(
            task_prompt=f"Run the opening-range breakout workflow across the {universe_count}-symbol universe.",
            context={
                "current_datetime": self.get_datetime().isoformat(),
                "strategy_parameters": params,
            },
        )


def _parameters_from_env(defaults: dict) -> dict:
    """Override strategy parameters from AI_ORB_* environment variables when set."""
    params = dict(defaults)
    if os.environ.get("AI_ORB_UNIVERSE"):
        params["universe"] = _parse_universe(os.environ["AI_ORB_UNIVERSE"])
    if os.environ.get("AI_ORB_OPENING_RANGE_MINUTES"):
        params["opening_range_minutes"] = int(os.environ["AI_ORB_OPENING_RANGE_MINUTES"])
    if os.environ.get("AI_ORB_RISK_FRACTION"):
        params["risk_fraction"] = float(os.environ["AI_ORB_RISK_FRACTION"])
    if os.environ.get("AI_ORB_MAX_SHARES"):
        params["max_shares"] = int(os.environ["AI_ORB_MAX_SHARES"])
    if os.environ.get("AI_ORB_MAX_POSITIONS"):
        params["max_positions"] = int(os.environ["AI_ORB_MAX_POSITIONS"])
    if os.environ.get("AI_ORB_PROFIT_R_MULTIPLE"):
        params["profit_r_multiple"] = float(os.environ["AI_ORB_PROFIT_R_MULTIPLE"])
    if os.environ.get("AI_ORB_SLEEPTIME"):
        params["sleeptime"] = os.environ["AI_ORB_SLEEPTIME"].strip()
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(
        os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=5)).date().isoformat())
    )
    AIOpeningRangeBreakoutStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
        parameters=_parameters_from_env(AIOpeningRangeBreakoutStrategy.parameters),
    )
