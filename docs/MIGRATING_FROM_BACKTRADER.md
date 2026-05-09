# Migrating from Backtrader to Lumibot

Backtrader is no longer actively maintained (last commit: 2023). Lumibot is a modern, actively maintained alternative that supports stocks, options, crypto, futures, and forex with the same code for backtesting and live trading.

## Why Switch?

| Feature | Backtrader | Lumibot |
|---------|------------|---------|
| Last update | 2023 (abandoned) | 2026 (active) |
| Python 3.12+ | Compatibility issues | Full support |
| Options trading | No | Yes |
| Crypto broker support | Limited | Yes, through documented CCXT integrations plus Bitunix |
| Futures | Limited | Yes |
| Same code: backtest to live | Partial | Yes |
| AI agent integration | No | Built-in |
| Multiple broker support | Limited | Alpaca, IBKR, Tradier, Schwab, Tradovate, TopstepX (via ProjectX), Bitunix, and selected CCXT paths for Coinbase, Kraken, KuCoin, Binance, BitMEX, WEEX, Bybit, and OKX |

## Concept Mapping

| Backtrader | Lumibot | Notes |
|------------|---------|-------|
| `cerebro = bt.Cerebro()` | `Trader()` | The orchestrator that runs your strategy |
| `class MyStrategy(bt.Strategy)` | `class MyStrategy(Strategy)` | Your strategy class |
| `next()` | `on_trading_iteration()` | Called each bar/iteration |
| `__init__()` | `initialize()` | Setup method |
| `self.buy()` / `self.sell()` | `self.submit_order(order)` | Order submission |
| `self.data.close[0]` | `self.get_last_price(asset)` | Get current price |
| `self.broker.getvalue()` | `self.portfolio_value` | Portfolio value |
| `self.broker.getcash()` | `self.cash` | Available cash |
| `cerebro.adddata(data)` | `YahooDataBacktesting` (or other source) | Data is configured at backtest level |
| `cerebro.run()` | `MyStrategy.backtest(...)` | Run the backtest |

## Side-by-Side Example

### Backtrader (old)

```python
import backtrader as bt

class SmaCross(bt.Strategy):
    params = (('fast', 10), ('slow', 30),)

    def __init__(self):
        sma_fast = bt.ind.SMA(period=self.p.fast)
        sma_slow = bt.ind.SMA(period=self.p.slow)
        self.crossover = bt.ind.CrossOver(sma_fast, sma_slow)

    def next(self):
        if self.crossover > 0:
            self.buy()
        elif self.crossover < 0:
            self.sell()

cerebro = bt.Cerebro()
cerebro.addstrategy(SmaCross)
data = bt.feeds.YahooFinanceData(dataname='AAPL',
                                  fromdate=datetime(2023, 1, 1),
                                  todate=datetime(2024, 1, 1))
cerebro.adddata(data)
cerebro.run()
```

### Lumibot (new)

```python
from datetime import datetime
from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting

class SmaCross(Strategy):
    parameters = {"fast": 10, "slow": 30}

    def on_trading_iteration(self):
        bars = self.get_historical_prices("AAPL", self.parameters["slow"] + 1)
        df = bars.df

        sma_fast = df["close"].rolling(self.parameters["fast"]).mean().iloc[-1]
        sma_slow = df["close"].rolling(self.parameters["slow"]).mean().iloc[-1]

        if sma_fast > sma_slow and not self.get_position("AAPL"):
            order = self.create_order("AAPL", 10, "buy")
            self.submit_order(order)
        elif sma_fast < sma_slow and self.get_position("AAPL"):
            self.sell_all()

SmaCross.backtest(
    YahooDataBacktesting,
    datetime(2023, 1, 1),
    datetime(2024, 1, 1),
)
```

The key difference: this exact Lumibot code works with live brokers. Just swap `YahooDataBacktesting` for `Alpaca`, `InteractiveBrokers`, `Tradier`, or `Schwab`.

## Key Differences

### Data handling
Backtrader requires you to add data feeds to Cerebro. Lumibot handles data through the strategy itself using `self.get_historical_prices()` and `self.get_last_price()`. Data sources are configured at the backtest/broker level.

### Order management
Backtrader uses `self.buy()` and `self.sell()` directly. Lumibot uses a two-step process: create the order, then submit it. This gives you more control over order types (market, limit, stop, bracket, OCO).

### Live trading
In Backtrader, transitioning to live trading requires significant code changes and a compatible broker integration. In Lumibot, your strategy code stays identical. You only change the broker class when initializing.

### Options, futures, forex
Backtrader has limited support for non-equity assets. Lumibot natively supports options chains, futures contracts, and forex pairs through the `Asset` class with configurable asset types.

## Getting Started

```bash
pip install lumibot
```

See the [full documentation](https://lumibot.lumiwealth.com/) for detailed guides on each broker and data source.
