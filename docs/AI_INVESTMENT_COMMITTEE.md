# AI Investment Committee

The AI investment committee example is a plain Lumibot multi-agent workflow.
It does not use LangGraph or an external debate service.

Flow:

1. `evidence_researcher`: read-only, gathers market data, indicators, news, SEC fundamentals, and SEC filings.
2. `bull_researcher`: read-only, builds the strongest long-only case.
3. `bear_researcher`: read-only, attacks the trade and identifies risks.
4. `portfolio_manager`: trading-enabled, checks positions/cash/open orders/risk limits and places Lumibot orders.

Reference implementation:

`lumibot/example_strategies/ai_investment_committee.py`

Each agent can use a different model:

```python
self.agents.create(
    name="evidence_researcher",
    model="openai/gpt-5.5-mini",
    allow_trading=False,
)

self.agents.create(
    name="portfolio_manager",
    model="openai/gpt-5.5",
    allow_trading=True,
)
```

The important safety rule is `allow_trading=False` for every research role.
Those agents can inspect read-only state but cannot submit, cancel, or modify orders.
