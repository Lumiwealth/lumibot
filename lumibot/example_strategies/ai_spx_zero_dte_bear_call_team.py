"""Two-agent SPX 0 DTE bear-call-spread experiment.

The researcher is read only. The trader independently validates the evidence,
places any order through LumiBot tools, and verifies the resulting broker state.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.example_strategies.agent_cycle import add_agent, interpreter_prompt, option_sizing_rule, run_cycle
from lumibot.strategies.strategy import Strategy


_INDEX_UNDERLYINGS = {"SPX", "XSP", "NDX", "RUT", "VIX"}


def underlying_label(params: dict) -> str:
    symbol = str(params.get("underlying", "SPX")).upper()
    asset_type = "index" if symbol in _INDEX_UNDERLYINGS else "stock"
    return f"{symbol} (asset type {asset_type})"


def build_research_prompt(params: dict) -> str:
    underlying = underlying_label(params)
    return f"""
Research the current {underlying} 0 DTE bear call spread opportunity without
trading. Load the options-trading skill and obey the active Rules file. Inspect
account state, positions, open orders, the current {underlying} market, today's
listed option expiration, exact contract Greeks, and executable bid/ask quality.

Evaluate the listed short call whose delta is closest to +{params['target_delta']:.2f} with a long call
exactly {params['wing_width']:.0f} points higher. Report exact contract
identities, timestamps, deltas, quotes, signed package pricing, maximum loss,
and reasons to trade or not trade. Do not claim that an order was submitted.
""".strip()


def build_bear_call_policy(params: dict) -> str:
    underlying = underlying_label(params)
    return f"""
Strategy policy: open a {underlying} bear call spread on today's listed
expiration. Sell the listed call whose delta is closest to +{params['target_delta']:.2f}
and buy a listed call exactly {params['wing_width']:.0f} points higher. Require a
positive net credit below the {params['wing_width']:.0f}-point width.
{option_sizing_rule(params['max_risk_pct'], params['max_contracts'])}
Hold a package opened in this cycle. Exits are checked on later cycles: close
the package when {params['profit_take_fraction']:.0%} of opening credit is captured, the closing
debit reaches {params['loss_multiple']:.1f} times opening credit, the underlying breaches the
short strike, or less than one {params['sleeptime']} sleeptime interval remains before
today's market close. Do not open a new package once the time stop applies.
""".strip()


def build_trader_prompt(params: dict) -> str:
    underlying = underlying_label(params)
    return f"""
You are the final validation and trading agent for a {underlying} 0 DTE bear
call spread. Load the options-trading skill and obey every active Rule.

Review the research, then independently refresh account state, positions, open
orders, exact contracts, Greeks, and quotes. Trade only the package the policy
below allows.

{build_bear_call_policy(params)}

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
        "max_contracts": 2,
        "profit_take_fraction": 0.50,
        "loss_multiple": 2.0,
        "model": "openai/gpt-6-luna",
        "sleeptime": "5M",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "5M"))
        underlying = underlying_label(self.parameters)
        rules_path = Path(__file__).with_name("agent_rules") / "ai_spx_zero_dte_bear_call_team.rules.json"
        add_agent(
            self,
            "researcher",
            build_research_prompt(self.parameters),
            allow_trading=False,
            rules_path=rules_path,
        )
        add_agent(
            self,
            "bull",
            f"Argue for today's {underlying} bear call from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risk case: a squeeze through the short strike or a credit that is too small. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            interpreter_prompt("0 DTE bear call spread", build_bear_call_policy(self.parameters)),
            allow_trading=False,
        )
        add_agent(
            self,
            "trader",
            build_trader_prompt(self.parameters),
            allow_trading=True,
            rules_path=rules_path,
        )

    def on_trading_iteration(self):
        context = {
            "current_datetime": self.get_datetime().isoformat(),
            "strategy_parameters": dict(self.parameters),
        }
        run_cycle(
            self,
            context,
            researcher="researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trader",
            research_task=f"Research today's exact {underlying_label(self.parameters)} bear call spread opportunity.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Decide whether to open one atomic package and how much risk to use.",
            trade_task="Apply the interpreter. Size from the account. Close at the profit, loss, breach, or time stop.",
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
