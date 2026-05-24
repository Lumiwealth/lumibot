# AI Trading Team README Handoff - 2026-05-24

## Source Of Truth

Use the Lumibot example strategy directly:

`/Users/robertgrzesik/Development/lumibot/lumibot/example_strategies/ai_trading_team.py`

Do not copy the strategy into MarketingManager. This is Lumibot example code and should live in Lumibot.

## Verified Tear Sheet

Use the fixed tear sheet HTML:

`/Users/robertgrzesik/Development/lumibot/docs/assets/ai-trading-team-example/AITradingTeamStrategy_2026-05-24_16-07_R6B0ul_tearsheet.html`

Original generated copy:

`/Users/robertgrzesik/Development/lumibot/logs/AITradingTeamStrategy_2026-05-24_16-07_R6B0ul_tearsheet.html`

## Screenshot

Use Rob's cropped screenshot from the Codex thread, not regenerated screenshots or alternate crops.

Expected screenshot content:

- Title: `AITradingTeamStrategy Compared to SPY`
- Period: `6 Apr, 2026 - 21 May, 2026`
- Annual Return: `1,135,333.24%`
- Total Return: `216%`
- Max Drawdown: `-20.31%`
- RoMaD: `55,910.7`
- Sharpe: `7.63`
- Sortino: `15.92`
- Header shows `LumiBot 4.5.34` and `QuantStats (Lumiwealth Version) (v.1.1.4)`

## Backtest Metrics

- Benchmark: SPY
- Annual Return: `1,135,333.24%`
- Total Return: `216%`
- Max Drawdown: `-20.31%`
- RoMaD: `55,910.7`
- Sharpe: `7.63`
- Sortino: `15.92`

## README Notes

- Keep the README example short and copy-pasteable.
- Show the backtest code first.
- If live trading is mentioned, add a separate short snippet below the backtest example.
- Do not add benchmark, fees, budget, quiet logs, custom tools, model variables, or extra strategy knobs back into the main README example.
- The example intentionally has one parameter: `universe`.
