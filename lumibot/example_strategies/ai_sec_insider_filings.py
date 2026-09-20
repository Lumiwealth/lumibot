"""SEC Form 4 public-insider-filings AI strategy example.

This strategy analyzes public filings. It is not based on material non-public
information and should not be described as illegal insider trading.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from lumibot.components.disclosure_signals import visible_insider_transactions
from lumibot.strategies import Strategy

_FIXTURE = Path(__file__).with_name("fixtures") / "sec_form4_transactions.json"


def _records(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    supplied = parameters.get("transactions")
    if supplied is not None:
        return list(supplied)
    path = Path(parameters.get("transactions_path") or _FIXTURE)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("SEC Form 4 fixture must contain a JSON list.")
    return payload


class AISECInsiderFilingsStrategy(Strategy):
    parameters = {
        "transactions": None,
        "transactions_path": str(_FIXTURE),
        "open_market_only": True,
        "include_amendments": False,
        "max_position_pct": 5,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self._processed_transaction_ids = set()
        self.agents.create(
            name="form4_researcher",
            default_model="gemini-3.5-flash-lite",
            allow_trading=False,
            system_prompt=(
                "Analyze supplied public SEC Form 4 rows. Distinguish open-market codes from grants, gifts, option "
                "exercises, automatic plans, derivatives, and amendments. Treat published_at as the first available "
                "moment. Identify issuer, owner, direct/indirect ownership, value, contradictions, and missing "
                "facts. Never use transaction_date as the availability boundary and do not submit orders."
            ),
        )
        self.agents.create(
            name="trading_risk_manager",
            default_model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=(
                "You are the only trading agent and own risk management. Verify the account, positions, open orders, "
                "and market price. Trade only exact tickers supported by supplied open-market Form 4 evidence. "
                "Never short, never trade grants/gifts/options as if they were open-market purchases, and cap each new "
                "position at max_position_pct of portfolio value and available cash. Submit each intent once, inspect "
                "status, and reread account state. Hold when evidence is amended, conflicting, stale, or incomplete."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime()
        visible = visible_insider_transactions(_records(self.parameters), as_of=as_of)
        if self.parameters.get("open_market_only", True):
            visible = [record for record in visible if record.get("open_market") is True]
        if not self.parameters.get("include_amendments", False):
            visible = [record for record in visible if record.get("amendment") is not True]
        current = [record for record in visible if record["id"] not in self._processed_transaction_ids]
        if not current:
            return
        context = {
            "as_of": as_of.isoformat(),
            "transactions": current,
            "max_position_pct": self.parameters["max_position_pct"],
            "availability_rule": "Records become visible at SEC acceptance/published_at.",
        }
        research = self.agents["form4_researcher"].run(
            task_prompt="Evaluate the newly public Form 4 rows and produce a sourced evidence packet.",
            context=context,
        )
        decision = self.agents["trading_risk_manager"].run(
            task_prompt="Review the evidence, enforce risk, and take at most one justified trading action.",
            context={**context, "research_evidence": research.summary},
        )
        self.log_message(f"Form 4 research: {research.summary}")
        self.log_message(f"Form 4 trader: {decision.summary}")
        self._processed_transaction_ids.update(record["id"] for record in current)


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AISECInsiderFilingsStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 1, 1),
        datetime(2026, 4, 1),
        budget=100_000,
        benchmark_asset="SPY",
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )
