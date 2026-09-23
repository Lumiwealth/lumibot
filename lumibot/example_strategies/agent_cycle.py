"""Call order for the example strategies.

Python creates the agents and runs them. It does not place orders.
Bull and bear run together from the same research. The interpreter reads both.
The trader is the only agent allowed to use the order tool.
"""

from __future__ import annotations

import os
from typing import Any

from lumibot.components.agents.manager import DEFAULT_AGENT_MODEL


def model_name() -> str:
    return os.environ.get("AI_EXAMPLE_MODEL", DEFAULT_AGENT_MODEL)


def add_agent(strategy: Any, name: str, prompt: str, *, allow_trading: bool, rules_path: Any = None) -> None:
    kwargs = {
        "name": name,
        "model": model_name(),
        "allow_trading": allow_trading,
        "system_prompt": prompt,
    }
    if rules_path is not None:
        kwargs["rules_path"] = rules_path
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
        "Submit each order once through the order tool. If you submit no order, that is the result. "
        "Python will not insert a share."
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
