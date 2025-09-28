# Lumibot Example Strategies

The `lumibot/example_strategies` package is the recommended home for sample
strategies that demonstrate different patterns. Keeping them together makes it
straightforward to discover canonical implementations (for example,
`fundamental_value.FundamentalValueStrategy`).

## Fundamental Value Strategy

`FundamentalValueStrategy` showcases how to combine external fundamental data
with Lumibot. Tickers are maintained in
`config/fundamental_value.yaml`, allowing you to update the watch list without
editing code.

### Requirements

- Install Lumibot in editable mode (ensures dependencies like `yfinance` and
  `PyYAML` are available):

  ```bash
  pip install -e .
  ```

- Ensure outbound network access at runtime so the strategy can query Yahoo
  Finance for fundamentals.

### Configure Tickers

Edit `lumibot/example_strategies/config/fundamental_value.yaml` and list the
symbols you want to analyze:

```yaml
tickers:
  - AAPL
  - NVDA
  - MSFT
```

To use a custom YAML file instead, pass its path through the strategy
parameters (see below).

### Run a Backtest

```python
from datetime import datetime

from lumibot.backtesting import YahooDataBacktesting
from lumibot.example_strategies.fundamental_value import FundamentalValueStrategy

start = datetime(2023, 1, 1)
end = datetime(2023, 12, 31)

results = FundamentalValueStrategy.backtest(
    YahooDataBacktesting,
    start,
    end,
    benchmark_asset="SPY",
)

print(results)
```

Options you can override with the `parameters` argument:

- `config_path`: Point to an alternate YAML file.
- `target_allocation`: Fraction of the portfolio to deploy per buy signal
  (defaults to equal weighting).
- `margin_of_safety`: Discount applied to intrinsic value (e.g. `0.2` for 20%).
- `fundamental_refresh_hours`: Minimum hours before fundamentals are reloaded
  for a symbol.

### Live or Paper Trading

Provide a broker instance and call `run_live()`:

```python
from lumibot.brokers import Alpaca
from lumibot.credentials import ALPACA_CONFIG
from lumibot.example_strategies.fundamental_value import FundamentalValueStrategy

strategy = FundamentalValueStrategy(
    broker=Alpaca(ALPACA_CONFIG),
    parameters={
        "config_path": "~/my_watchlist.yaml",
        "margin_of_safety": 0.15,
    },
)

strategy.run_live()
```

Make sure your broker credentials are configured and that the chosen broker
supports all instruments in your YAML file.
