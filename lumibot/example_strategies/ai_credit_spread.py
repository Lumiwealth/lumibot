"""AI-only credit-spread strategy.

Python only creates and runs a LumiBot agent. The agent chooses put or call
credit spreads from generic option tools. No iron-condor-specific helpers.

Local backtest:
    GEMINI_API_KEY=... BACKTESTING_DATA_SOURCE=ThetaData \
        python -m lumibot.example_strategies.ai_credit_spread

Optional env overrides (AI_CS_*):
    AI_CS_UNDERLYING=SPY
    AI_CS_PREFERRED_SIDE=put
    AI_CS_WING_WIDTH=5
    AI_CS_TARGET_DELTA=0.16
    AI_CS_MIN_DTE=30
    AI_CS_MAX_DTE=45
"""

import os
from datetime import datetime, timedelta

from lumibot.strategies.strategy import Strategy


def build_credit_spread_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    wing_width = float(params.get("wing_width", 5.0))
    target_delta = float(params.get("target_delta", 0.16))
    delta_band = float(params.get("delta_band", 0.04))
    min_dte = int(params.get("min_dte", 30))
    max_dte = int(params.get("max_dte", 45))
    preferred_dte = int(params.get("preferred_dte", 35))
    profit_take_fraction = float(params.get("profit_take_fraction", 0.50))
    loss_multiple = float(params.get("loss_multiple", 2.0))
    time_stop_dte = int(params.get("time_stop_dte", 21))
    max_risk_pct = float(params.get("max_risk_pct", 0.02))
    max_contracts = int(params.get("max_contracts", 10))
    preferred_side = str(params.get("preferred_side", "put")).lower()
    short_delta_min = target_delta - delta_band
    short_delta_max = target_delta + delta_band
    profit_take_pct = int(round(profit_take_fraction * 100))
    max_risk_pct_display = max_risk_pct * 100
    return f"""
You are the complete decision-maker for an AI-only {underlying} vertical credit
spread strategy inside LumiBot. There is no Python trading logic outside you.

STRATEGY PARAMETERS:
- underlying: {underlying}
- preferred_side: {preferred_side} (put or call credit spread preference; switch only with clear evidence)
- wing_width: {wing_width}
- target_delta: {target_delta}
- delta_band: {delta_band} (verified absolute short delta {short_delta_min:.2f} through {short_delta_max:.2f})
- min_dte: {min_dte}
- max_dte: {max_dte}
- preferred_dte: {preferred_dte}
- profit_take_fraction: {profit_take_fraction}
- loss_multiple: {loss_multiple}
- time_stop_dte: {time_stop_dte}
- max_risk_pct: {max_risk_pct}
- max_contracts: {max_contracts}

Rules:
1. Each iteration call account_portfolio, account_positions, orders_open_orders,
   and market_last_price for {underlying}.
2. Never open a second credit spread while any {underlying} option position or pending
   option order exists. Manage or close first.
3. When flat, use options_find_expiration(symbol='{underlying}', min_days={min_dte})
   and keep DTE within {max_dte}, preferring near {preferred_dte}. Confirm listed
   strikes with options_get_chain / options_get_strikes.
4. Choose a short strike near target delta with options_find_strike_for_delta, then
   verify with options_get_greeks. Absolute short delta must be from
   {short_delta_min:.2f} through {short_delta_max:.2f}. Long wing is exactly {wing_width}
   points farther OTM and must be listed.
5. Put credit spread: sell_to_open higher put, buy_to_open lower put.
   Call credit spread: sell_to_open lower call, buy_to_open higher call.
6. Evaluate both legs with options_evaluate_market. Price with
   options_calculate_multileg_price(price_style='mid'). Require a net credit
   (negative signed price) and 0 < credit < {wing_width}.
7. Fill-friendly credit pricing: submit with orders_submit_multileg using a signed
   net_limit_price at the tool mid credit, or slightly more aggressive for a credit
   (a modestly more negative signed limit, for example about $0.05 better for the
   maker/taker path) so the backtest can fill. Never use a debit. If an opening
   order remains unfilled into the next session, cancel/replace with an improved
   credit limit based on a fresh options_calculate_multileg_price. Always use the
   exact tool economics; do not invent leg prices.
8. Size so max loss (({wing_width} - credit) * 100 * contracts) is <= {max_risk_pct_display:.2f}
   percent of portfolio value, capped at {max_contracts} contracts.
9. Close when {profit_take_pct} percent of credit is captured, closing debit >=
   {loss_multiple}x opening credit, DTE <= {time_stop_dte}, or short absolute delta
   >= 0.30. Use options_check_spread_profit when helpful. Close with the correct
   signed-quantity sides only.
10. After any orders_submit_multileg call, capture identifiers, call
    orders_get_status or orders_wait_for_terminal, re-read account_positions, and
    never claim a fill unless is_filled is true.

Use only evidence at the current runtime datetime. No-trade is required when any
rule cannot be proven from tools.
""".strip()


class AICreditSpreadStrategy(Strategy):
    parameters = {
        "underlying": "SPY",
        "preferred_side": "put",
        "wing_width": 5.0,
        "target_delta": 0.16,
        "delta_band": 0.04,
        "min_dte": 30,
        "max_dte": 45,
        "preferred_dte": 35,
        "profit_take_fraction": 0.50,
        "loss_multiple": 2.0,
        "time_stop_dte": 21,
        "max_risk_pct": 0.02,
        "max_contracts": 10,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="credit_spread",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_credit_spread_system_prompt(self.parameters),
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        underlying = str(params.get("underlying", "SPY")).upper()
        self.agents["credit_spread"].run(
            task_prompt=(
                f"Run the {underlying} credit-spread workflow for this iteration using the "
                "parameters in your system prompt and context. Verify short deltas with "
                "options_get_greeks before opening. Submit with the tool mid credit or a "
                "slightly more aggressive credit limit so the order can fill. After any "
                "submission, verify fills with orders_get_status and re-read positions."
            ),
            context={
                "current_datetime": self.get_datetime().isoformat(),
                "strategy_parameters": params,
            },
        )


def _parameters_from_env(defaults: dict) -> dict:
    """Override strategy parameters from AI_CS_* environment variables when set."""
    params = dict(defaults)
    float_keys = (
        "wing_width",
        "target_delta",
        "delta_band",
        "profit_take_fraction",
        "loss_multiple",
        "max_risk_pct",
    )
    int_keys = ("min_dte", "max_dte", "preferred_dte", "time_stop_dte", "max_contracts")
    if os.environ.get("AI_CS_UNDERLYING"):
        params["underlying"] = os.environ["AI_CS_UNDERLYING"].strip().upper()
    if os.environ.get("AI_CS_PREFERRED_SIDE"):
        params["preferred_side"] = os.environ["AI_CS_PREFERRED_SIDE"].strip().lower()
    for key in float_keys:
        raw = os.environ.get(f"AI_CS_{key.upper()}")
        if raw not in (None, ""):
            params[key] = float(raw)
    for key in int_keys:
        raw = os.environ.get(f"AI_CS_{key.upper()}")
        if raw not in (None, ""):
            params[key] = int(raw)
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(
        os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=45)).date().isoformat())
    )
    AICreditSpreadStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
        parameters=_parameters_from_env(AICreditSpreadStrategy.parameters),
    )
