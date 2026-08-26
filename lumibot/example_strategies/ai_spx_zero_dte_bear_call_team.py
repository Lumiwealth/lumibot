"""Two-agent SPX 0 DTE bear-call-spread experiment.

The researcher is read only. The trader independently validates the evidence,
places any order through LumiBot tools, and verifies the resulting broker state.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.strategies.strategy import Strategy


def build_research_prompt(params: dict) -> str:
    return f"""
Research the current SPX 0 DTE bear call spread opportunity without trading.
Load the options-trading skill and obey the active Rules file. Inspect account
state, positions, open orders, the current SPX market, today's listed option
expiration, exact contract Greeks, and executable bid/ask quality.

Evaluate a short call near +{params['target_delta']:.2f} delta with a long call
exactly {params['wing_width']:.0f} points higher. Report exact contract
identities, timestamps, deltas, quotes, signed package pricing, maximum loss,
and reasons to trade or not trade. Do not claim that an order was submitted.
""".strip()


def build_trader_prompt(params: dict) -> str:
    return f"""
You are the final validation and trading agent for an SPX 0 DTE bear call
spread. Load the options-trading skill and obey every active Rule.

Review the research, then independently refresh account state, positions, open
orders, exact contracts, Greeks, and quotes. Trade only a short call near
+{params['target_delta']:.2f} delta with a listed long call exactly
{params['wing_width']:.0f} points higher. Require a positive net credit below
the {params['wing_width']:.0f}-point width. Risk no more than
{params['max_risk_pct']:.2%} of portfolio value and no more than
{params['max_contracts']} package per trading day.

If all conditions pass, call orders_submit_multileg once for one atomic
multi-leg package. Never submit independent legs. After submission, verify the
submitted order with orders_get_status or orders_wait_for_terminal, then inspect
positions and open orders. If any condition cannot be proven, make a no-trade
decision and state the missing evidence. Manage an existing package before
considering a new entry, and close its legs as one atomic package.
""".strip()


class AISpxZeroDteBearCallTeamStrategy(Strategy):
    parameters = {
        "underlying": "SPX",
        "target_delta": 0.20,
        "wing_width": 5.0,
        "max_risk_pct": 0.01,
        "max_contracts": 1,
        "model": "gemini-3.5-flash-lite",
    }

    def initialize(self):
        self.sleeptime = "5M"
        rules_path = Path(__file__).with_name("agent_rules") / "ai_spx_zero_dte_bear_call_team.rules.json"
        model = os.environ.get("AI_SPX_TEAM_MODEL", self.parameters["model"])
        self.agents.create(
            name="researcher",
            model=model,
            allow_trading=False,
            system_prompt=build_research_prompt(self.parameters),
            rules_path=rules_path,
        )
        self.agents.create(
            name="trader",
            model=model,
            allow_trading=True,
            system_prompt=build_trader_prompt(self.parameters),
            rules_path=rules_path,
        )

    def on_trading_iteration(self):
        context = {
            "current_datetime": self.get_datetime().isoformat(),
            "strategy_parameters": dict(self.parameters),
        }
        research = self.agents["researcher"].run(
            task_prompt="Research today's exact SPX bear call spread opportunity.",
            context=context,
        )
        self.agents["trader"].run(
            task_prompt="Validate the research, make the final decision, and verify any trade.",
            context={**context, "research": research.summary},
        )


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(
        os.environ.get("BACKTESTING_END", datetime.now().date().isoformat())
    )
    backtesting_start = datetime.fromisoformat(
        os.environ.get(
            "BACKTESTING_START",
            (backtesting_end - timedelta(days=7)).date().isoformat(),
        )
    )
    AISpxZeroDteBearCallTeamStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPX",
        budget=100_000,
    )
