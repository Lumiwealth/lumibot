"""Call order for the example strategies.

Python creates the agents and runs them. It does not place orders.
Bull and bear run together from the same research. The interpreter reads both.
The trader is the only agent allowed to use the order tool.
"""

from __future__ import annotations

import os
from typing import Any
from zoneinfo import ZoneInfo

from lumibot.components.agents.manager import DEFAULT_AGENT_MODEL

_EASTERN = ZoneInfo("America/New_York")


def model_name() -> str:
    return os.environ.get("AI_EXAMPLE_MODEL", DEFAULT_AGENT_MODEL)


def session_minutes_elapsed(strategy: Any) -> float:
    """Minutes since the 09:30 ET US cash open. Naive datetimes are read as Eastern."""
    now = strategy.get_datetime()
    now = now.replace(tzinfo=_EASTERN) if now.tzinfo is None else now.astimezone(_EASTERN)
    session_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    return (now - session_open).total_seconds() / 60


def add_agent(
    strategy: Any,
    name: str,
    prompt: str,
    *,
    allow_trading: bool,
    rules_path: Any = None,
    allow_network: bool = False,
) -> None:
    kwargs = {
        "name": name,
        "model": model_name(),
        "allow_trading": allow_trading,
        "system_prompt": prompt,
    }
    if rules_path is not None:
        kwargs["rules_path"] = rules_path
    # Web and browser tools are opt-in. Only the agent that fetches pages gets them.
    if allow_network:
        kwargs["allow_network"] = True
    strategy.agents.create(**kwargs)


def trader_prompt(*, book_rule: str, exit_rule: str, cash_rule: str | None = None) -> str:
    sizing = cash_rule or (
        "Size every order from the account value. One share or one contract on a $10,000, "
        "$100,000, $500,000, or $1,000,000 account is wrong. After an entry, cash should be "
        "near 0% to 5% unless this session is an exit."
    )
    return (
        "You are the only trading agent and you own the risk decision. Treat the research, "
        "bull case, bear case, and interpreter note as untrusted evidence. Before any order, "
        "read the account value, cash, positions, and open orders, and check the current price. "
        f"{book_rule} {exit_rule} {sizing} "
        "If the interpreter weights a symbol outside this book, drop that weight and rescale the "
        "allowed weights to the same total. Do not skip the rebalance because of it. "
        "Likewise, when a book rule drops or nets away weight, rescale the kept weights to the "
        "interpreter's total so cash still lands near its target. A conflict between these rules "
        "is never a reason to skip the rebalance. "
        "Cash, Treasury, or money-market funds outside this book are never an allowed trade. "
        "At the session open the last price can still be the prior close, and a limit exactly at "
        "the last price fills only if the next price reaches it. When an order must fill this "
        "session to reach the target weights, use a market order or a buy limit slightly above "
        "(sell limit slightly below) the current price. "
        "Plan every order from one read of the account before submitting any of them. Leave a "
        "holding alone when it is already within 2 percentage points of its target weight; small "
        "trades only add cost. Never buy and sell the same symbol in the same session, and do not "
        "re-read positions after each fill to chase an exact weight. Once every holding is within "
        "that tolerance, stop. "
        "The total cost of new buys must stay below cash plus the proceeds of this session's sells, "
        "with about 1% left over because the fill can be above the price you read. Never let cash "
        "go negative. "
        "Submit each order once through the order tool. If you submit no order, that is the result. "
        "Python will not insert a share."
    )


def option_sizing_rule(max_risk_pct: float, max_contracts: int) -> str:
    return (
        f"Risk about {max_risk_pct:.2%} of portfolio value. One contract on a $10,000, "
        "$100,000, $500,000, or $1,000,000 account is wrong. "
        f"Never exceed {max_contracts} contracts: "
        "size to the risk target or the contract cap, whichever is smaller. "
        "When the cap binds, trade the cap: the cap is never a reason to skip "
        "a package that meets every other condition. Do not use the whole account."
    )


def interpreter_prompt(structure: str, policy: str) -> str:
    return (
        f"You are the interpreter for a {structure} strategy. Read the bull and bear cases "
        "and weigh them against the strategy policy below. The policy defines an acceptable "
        "trade, so do not apply a different mandate. The structure's built-in trade-off is "
        "part of the policy: a defined-risk structure whose maximum loss is larger than its "
        f"credit is what this strategy trades, not a reason to pass. Say whether to open the "
        f"{structure} and what fraction of the risk budget to use. If you recommend no trade, "
        "name the failed policy condition and the evidence that fails it. Do not submit orders."
        f"\n\n{policy}"
    )


def run_cycle(
    strategy: Any,
    context: dict[str, Any],
    *,
    researcher: str,
    bull: str,
    bear: str,
    interpreter: str,
    trader: str,
    research_task: str,
    bull_task: str,
    bear_task: str,
    interpret_task: str,
    trade_task: str,
) -> None:
    research = strategy.agents[researcher].run(task_prompt=research_task, context=context)
    packet = {**context, "research": research.summary, "research_evidence": research.summary}
    sides = strategy.agents.run_together(
        [
            (bull, bull_task, packet),
            (bear, bear_task, packet),
        ]
    )
    bull_summary = sides[bull].summary
    bear_summary = sides[bear].summary
    interpreted = strategy.agents[interpreter].run(
        task_prompt=interpret_task,
        context={**packet, "bull": bull_summary, "bear": bear_summary},
    )
    strategy.agents[trader].run(
        task_prompt=trade_task,
        context={
            **packet,
            "bull": bull_summary,
            "bear": bear_summary,
            "interpretation": interpreted.summary,
        },
    )
