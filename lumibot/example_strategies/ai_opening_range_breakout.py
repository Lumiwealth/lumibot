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
    AI_ORB_SLEEPTIME=15M
"""

import os
from datetime import datetime, timedelta

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
1. Each iteration call account_portfolio, account_positions, and orders_open_orders.
2. Scan the full provided universe with market_last_prices (symbols_json of the
   universe, or batched subsets of at most 150 symbols). Do not invent prices for
   symbols_missing. You may call market_last_price for a finalist before ordering.
3. Prefer minute history via market_load_history_table with timestep='minute' plus
   duckdb_query. Build each symbol's opening range as the high/low of the first
   {opening_range_minutes} minutes of the regular US cash session (09:30 ET start,
   not 09:00). If minute data for the true opening window is missing, skip that
   symbol. Never invent intraday levels.
4. A valid long breakout requires the latest completed bar's close to be strictly
   greater than that symbol's opening-range high (close > OR high), with confirming
   volume when available. A close equal to or below the OR high is not a breakout.
   Prefer the strongest valid breakout by percent extension above the range high
   and liquidity. Short only when shorting is allowed and evidence is equally clear.
5. Hold at most {max_positions} positions. If already at max_positions, manage exits
   only; do not open another name.
6. Size so approximate stop risk is at most {risk_fraction:.2%} of portfolio value,
   capped at {max_shares} shares. Stop is the opposite side of that symbol's opening
   range. Prefer order_type='market' for entries and exits so backtests/live can
   fill; do not attach a limit_price on market orders.
7. Take profit near {profit_r_multiple}R or exit on a close back inside the range.
8. After any orders_submit_order call, capture identifiers, call orders_get_status
   (and orders_wait_for_terminal with a short timeout if still open), re-read
   account_positions, and never claim a fill unless is_filled is true.

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
        # Prefer minute bars; use a coarser sleeptime in short backtests to bound agent calls.
        "sleeptime": "15M",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "15M"))
        self.agents.create(
            name="orb",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_orb_system_prompt(self.parameters),
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        universe = params.get("universe") or []
        if isinstance(universe, str):
            universe = _parse_universe(universe)
        universe_count = len(universe) if isinstance(universe, list) else 0
        self.agents["orb"].run(
            task_prompt=(
                f"Run the multi-ticker opening-range breakout workflow for this bar across the "
                f"{universe_count}-symbol universe. Use market_last_prices to scan, then minute "
                "history/DuckDB for opening ranges (09:30 ET start). Prefer market equity orders. "
                "After any submission, verify status with orders_get_status."
            ),
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
