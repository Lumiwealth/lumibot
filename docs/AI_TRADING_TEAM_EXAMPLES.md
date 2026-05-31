# AI Trading Team Examples

Lumibot includes several copy-paste AI trading team examples. Each one keeps the
strategy code intentionally simple:

- create agents in `initialize()`
- pass context through those agents in `on_trading_iteration()`
- allow only the final trader or portfolio-manager agent to submit orders
- run the same file with a broker by default, or set `IS_BACKTESTING = True`
  in the runner for the historical demo window

These examples are inspired by public investing styles and firms. They are not
affiliated with or endorsed by the investors, firms, or companies named.

## Examples

- `lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py`
  Inspired by the pod-style structure associated with Ken Griffin's Citadel:
  sector specialists pitch their best ETF ideas, a risk manager challenges the
  setup, and a portfolio manager rotates into the strongest sector.

- `lumibot/example_strategies/ai_trading_team_warren_buffett_value.py`
  A value-investing team where one agent reads for business quality and annual
  report evidence, one agent demands valuation discipline, and the portfolio
  manager buys only the best long-term compounder.

- `lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy.py`
  A Bridgewater-style idea-meritocracy workflow where growth, inflation, and
  liquidity agents argue, a disagreement agent stress-tests the assumptions,
  and the trader chooses one macro ETF.

- `lumibot/example_strategies/ai_trading_team_bill_ackman_concentrated.py`
  A concentrated investing workflow where a quality researcher, activist bull,
  and short-seller bear debate whether one high-conviction large-cap position
  deserves capital.

- `lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py`
  An aggressive bull/bear demo where agents debate leveraged long and inverse
  ETFs before rotating into one high-conviction ETF.

- `lumibot/example_strategies/ai_trading_team_bull_bear_large_cap_stocks.py`
  The same bull/bear debate structure applied to familiar large-cap stocks, so
  the reasoning is easier to inspect before using more volatile instruments.

## Run the examples

Each example defaults to broker-connected execution. With Alpaca, it runs in
paper mode unless `ALPACA_IS_PAPER=false`.

```bash
export GEMINI_API_KEY='your-key-here'
export ALPACA_API_KEY='your-alpaca-key'
export ALPACA_API_SECRET='your-alpaca-secret'
export ALPACA_IS_PAPER=true
python lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py
```

Backtest the same file by changing `IS_BACKTESTING = False` to `IS_BACKTESTING = True` in the runner:

```bash
export GEMINI_API_KEY='your-key-here'
python lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py
```

## Workflow diagrams

- `docs/assets/ai-trading-team-workflows/bull-bear-leveraged-etf.png`
- `docs/assets/ai-trading-team-workflows/bull-bear-large-cap-stocks.png`
- `docs/assets/ai-trading-team-workflows/ray-dalio-idea-meritocracy.png`
- `docs/assets/ai-trading-team-workflows/warren-buffett-value.png`
- `docs/assets/ai-trading-team-workflows/bill-ackman-concentrated.png`
- `docs/assets/ai-trading-team-workflows/citadel-sector-pods.png`

`lumibot/example_strategies/ai_trading_team.py` remains the shortest alias for
the leveraged ETF example. New code should prefer the descriptive filenames
above.
