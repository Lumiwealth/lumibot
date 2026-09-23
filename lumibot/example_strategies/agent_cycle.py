"""Call order for the example strategies.

Python creates the agents and runs them. It does not place orders.
Bull and bear run together from the same research. The interpreter reads both.
The trader is the only agent allowed to use the order tool.
"""

from __future__ import annotations

import os
from typing import Any

CHEAP_MODEL = "gemini-3.5-flash-lite"

# Gemini API paid tier for gemini-3.5-flash-lite, checked 2026-09-22:
# https://ai.google.dev/gemini-api/docs/pricing
INPUT_USD_PER_MILLION = 0.30
CACHED_INPUT_USD_PER_MILLION = 0.03
OUTPUT_USD_PER_MILLION = 2.50


def model_name() -> str:
    return os.environ.get("AI_EXAMPLE_MODEL", CHEAP_MODEL)


def add_agent(strategy: Any, name: str, prompt: str, *, allow_trading: bool, rules_path: Any = None) -> None:
    kwargs = {
        "name": name,
        "model": model_name(),
        "allow_trading": allow_trading,
        "system_prompt": prompt,
        "reasoning_effort": "low",
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
        "Submit each order once through the order tool. If you submit no order, that is the result. "
        "Python will not insert a share."
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
