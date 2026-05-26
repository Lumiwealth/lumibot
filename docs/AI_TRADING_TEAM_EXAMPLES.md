# AI Trading Team Examples

Lumibot includes several copy-paste AI trading team examples. Each one uses the same simple pattern:

- read-only agents research or debate
- one final trading-enabled agent can submit orders
- normal Lumibot backtests, memory, traces, and order artifacts still apply

These examples are inspired by public investing styles and firms. They are not affiliated with or endorsed by the investors, firms, or companies named.

## Examples

- `lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py`
  Aggressive bull/bear team for leveraged ETF rotation.

- `lumibot/example_strategies/ai_trading_team_bull_bear_large_cap_stocks.py`
  Large-cap stock team with evidence, bull, bear, and portfolio-manager roles.

- `lumibot/example_strategies/ai_trading_team_ray_dalio_all_weather.py`
  Macro-regime and all-weather-style ETF allocation team.

- `lumibot/example_strategies/ai_trading_team_warren_buffett_value.py`
  Annual-report, moat, valuation, and long-term business-quality team.

- `lumibot/example_strategies/ai_trading_team_bill_ackman_concentrated.py`
  Concentrated high-conviction large-cap team with an activist bull case and short-seller bear case.

- `lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py`
  Sector-pod team that compares sector ETFs through cyclical, defensive, and risk lenses.

Legacy imports still work:

- `lumibot/example_strategies/ai_trading_team.py`
- `lumibot/example_strategies/ai_investment_committee.py`

New code should prefer the descriptive filenames above.
