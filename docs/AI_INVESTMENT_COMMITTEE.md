# AI Trading Team Large-Cap Stocks

This page is kept for compatibility with the older "AI Investment Committee" name. The current example name is **AI Trading Team Bull/Bear Large-Cap Stocks**.

Lumibot agent flows are plain Python. A strategy can use one agent, two agents, a large specialist committee, a multi-model debate, or a hybrid where agents research and deterministic Python places the final order.

Reference implementation:

`lumibot/example_strategies/ai_trading_team_bull_bear_large_cap_stocks.py`

The legacy import path still works:

`lumibot/example_strategies/ai_investment_committee.py`

This specific example uses four roles:

- `evidence_researcher`: read-only, gathers market data, indicators, news, SEC fundamentals, SEC filings, and FRED macro data.
- `bull_researcher`: read-only, builds the strongest long-only case.
- `bear_researcher`: read-only, attacks the trade and identifies risks.
- `portfolio_manager`: trading-enabled, checks positions, cash, open orders, and risk limits before placing Lumibot orders.

Each role can use a different model:

```python
self.agents.create(name="evidence_researcher", model="openai/gpt-5.4-mini", allow_trading=False)
self.agents.create(name="bull_researcher", model="openai/gpt-5.5", allow_trading=False)
self.agents.create(name="bear_researcher", model="openai/gpt-5.5", allow_trading=False)
self.agents.create(name="portfolio_manager", model="openai/gpt-5.5", allow_trading=True)
```

The important safety rule is `allow_trading=False` for every research role. Those agents can inspect read-only state but cannot submit, cancel, or modify orders.

Possible variations:

- Add separate news, filing, macro, technical, valuation, or sector agents.
- Add a neutral agent.
- Run multiple model providers and compare their bull and bear cases.
- Iterate bull and bear rebuttals before the final decision.

For this investment committee example, the default architecture is agent-owned
execution: the trading-enabled `portfolio_manager` places orders through LumiBot
agent tools when justified. Do not also parse the portfolio manager's text in
Python and submit a second deterministic order. A hybrid where agents only
produce research and Python places the final order is a different strategy shape,
and should be used only when explicitly requested.
